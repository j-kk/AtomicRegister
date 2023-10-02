extern crate serde;
#[macro_use]
extern crate serde_big_array;

use std::sync::Arc;
use std::time::Duration;

pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
pub use transfer_public::*;

pub use crate::domain::*;
use crate::executors::System;
use crate::job_handler::jobs::JobHandler;
use crate::network::network::{ListeningSocket, SenderWrapper};
use crate::stable_storage::build_stable_storage;

mod domain;
mod stable_storage;
mod sectors_manager;
mod atomic_register;
mod network;
mod executors;
mod job_handler;

const REGISTERS_COUNT: usize = 20;

pub async fn run_register_process(config: Configuration) {
    let mut registers = vec![];
    let process_dir = config.public.storage_dir;
    // Initialize sectors manager
    let mut sectors_manager_dir = process_dir.clone();
    sectors_manager_dir.push("data");
    tokio::fs::create_dir(&sectors_manager_dir).await.unwrap();

    // Initialize system
    let mut system = System::new().await;
    // Bind to socket
    let listening_socket = ListeningSocket::init(config.hmac_system_key,
                                                 config.hmac_client_key,
                                                 config.public.tcp_locations[(config.public.self_rank as usize) - 1].clone()).await;
    let listening_module = system.register_module(listening_socket).await;

    // Connect to other processes
    let sender = Arc::new(SenderWrapper::new(config.hmac_system_key,
                                             &config.public.tcp_locations,
                                             &mut system).await);

    let sectors_manager = build_sectors_manager(sectors_manager_dir);

    for register_id in 0..REGISTERS_COUNT {
        let mut register_dir = process_dir.clone();
        register_dir.push(format!("register_{}", register_id));
        tokio::fs::create_dir(&register_dir).await.unwrap();

        let stable_storage = build_stable_storage(register_dir).await;
        let register = build_atomic_register(config.public.self_rank,
                                             stable_storage,
                                             sender.clone(),
                                             sectors_manager.clone(),
                                             config.public.tcp_locations.len(),
        ).await;
        registers.push(register);
    }

    let _job_handler_module = JobHandler::init(config.public.self_rank,
                                               system,
                                               registers,
                                               config.public.max_sector,
                                               listening_module,
                                               config.public.tcp_locations.len() as u8).await;
    loop {
        tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
    }
}

pub mod atomic_register_public {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use crate::{
        ClientRegisterCommand, OperationComplete, RegisterClient, SectorsManager, StableStorage,
        SystemRegisterCommand,
    };
    use crate::atomic_register::atomic_register_impl::AtomicReg;

    #[async_trait::async_trait]
    pub trait AtomicRegister: Send + Sync {
        /// Send client command to the register. After it is completed, we expect
        /// callback to be called. Note that completion of client command happens after
        /// delivery of multiple system commands to the register, as the algorithm specifies.
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            operation_complete: Box<
                dyn FnOnce(OperationComplete) -> Pin<Box<dyn Future<Output=()> + Send>>
                + Send
                + Sync,
            >,
        );

        /// Send system command to the register.
        async fn system_command(&mut self, cmd: SystemRegisterCommand);
    }

    /// Idents are numbered starting at 1 (up to the number of processes in the system).
    /// Storage for atomic register algorithm data is separated into StableStorage.
    /// Communication with other processes of the system is to be done by register_client.
    /// And sectors must be stored in the sectors_manager instance.
    pub async fn build_atomic_register(
        self_ident: u8,
        metadata: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: usize,
    ) -> Box<dyn AtomicRegister> {
        if processes_count > 255 {
            log::error!("Process count exceeds u8 range: {}", processes_count);
            panic!("Process count exceeds u8 range: {}", processes_count);
        }
        let reg = AtomicReg::new(
            self_ident,
            metadata,
            register_client,
            sectors_manager,
            processes_count).await;
        return Box::new(reg);
    }
}

pub mod sectors_manager_public {
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::{SectorIdx, SectorVec};
    use crate::sectors_manager::SManagerWrapper;

    #[async_trait::async_trait]
    pub trait SectorsManager: Send + Sync {
        /// Returns 4096 bytes of sector data by index.
        async fn read_data(&self, idx: SectorIdx) -> SectorVec;

        /// Returns timestamp and write rank of the process which has saved this data.
        /// Timestamps and ranks are relevant for atomic register algorithm, and are described
        /// there.
        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

        /// Writes a new data, along with timestamp and write rank to some sector.
        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
    }

    /// Path parameter points to a directory to which this method has exclusive access.
    pub fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        return Arc::new(SManagerWrapper::new(path));
    }
}

pub mod transfer_public {
    use std::io::Error;

    use tokio::io::{AsyncRead, AsyncWrite};

    use crate::{MAGIC_NUMBER, RegisterCommand};
    use crate::network::netdata::{MsgHeader, NetBuffer, ReadError, serialize_msg};

    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), Error> {
        let mut buffer = NetBuffer::new();
        loop {
            buffer.read_until_magic(data).await?;
            let header: MsgHeader = buffer.read_header(data).await?;
            match buffer.read_msg(&header, hmac_client_key, hmac_system_key, data).await {
                Ok(result) => { return Ok(result); }
                Err(ReadError::IOError(error)) => { return Err(error); }
                Err(ReadError::UnknownMessageID) => {
                    buffer.consume_exact(MAGIC_NUMBER.len());
                }
                Err(ReadError::BincodeError(error)) => {
                    log::warn!("Encountered bincode error when deserializing message: {}", error.to_string());
                    buffer.consume_exact(MAGIC_NUMBER.len());
                }
            }
        }
    }

    pub async fn serialize_register_command(
        cmd: &RegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), Error> {
        serialize_msg(&cmd, writer, hmac_key).await
    }
}

pub mod register_client_public {
    use std::sync::Arc;

    use crate::SystemRegisterCommand;

    #[async_trait::async_trait]
    /// We do not need any public implementation of this trait. It is there for use
    /// in AtomicRegister. In our opinion it is a safe bet to say some structure of
    /// this kind must appear in your solution.
    pub trait RegisterClient: core::marker::Send + core::marker::Sync {
        /// Sends a system message to a single process.
        async fn send(&self, msg: Send);

        /// Broadcasts a system message to all processes in the system, including self.
        async fn broadcast(&self, msg: Broadcast);
    }

    pub struct Broadcast {
        pub cmd: Arc<SystemRegisterCommand>,
    }

    pub struct Send {
        pub cmd: Arc<SystemRegisterCommand>,
        /// Identifier of the target process. Those start at 1.
        pub target: usize,
    }
}

pub mod stable_storage_public {
    #[async_trait::async_trait]
    /// A helper trait for small amount of durable metadata needed by the register algorithm
    /// itself. Again, it is only for AtomicRegister definition. StableStorage in unit tests
    /// is durable, as one could expect.
    pub trait StableStorage: Send + Sync {
        async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

        async fn get(&self, key: &str) -> Option<Vec<u8>>;
    }
}
