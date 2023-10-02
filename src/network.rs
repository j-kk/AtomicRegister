pub mod network {
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::io::AsyncWriteExt;
    use uuid::Uuid;

    use crate::{Broadcast, deserialize_register_command, RegisterClient, RegisterCommand, serialize_register_command, SystemRegisterCommandContent};
    use crate::executors::{Handler, ModuleRef, System, Tick};
    use crate::job_handler::jobs::JobHandler;
    use crate::network::netdata::{ClientReply, serialize_client_reply};

    const TIMEOUT_SEC: u64 = 1;
    const TIMEOUT_NANOS: u32 = 0;

    struct Sender {
        tcp_locations: Vec<(String, u16)>,
        sockets: Vec<tokio::sync::Mutex<Option<tokio::net::TcpStream>>>,
        send_store: Arc<SendStore>,
    }

    struct SendStore {
        to_send: tokio::sync::Mutex<HashMap<(Uuid, usize), Arc<Vec<u8>>>>
    }

    impl SendStore {
        async fn new() -> SendStore {
            SendStore {
                to_send: tokio::sync::Mutex::new(HashMap::new()),
            }
        }
        async fn put(&self,
                     uuid: Uuid,
                     target: usize,
                     data: Arc<Vec<u8>>) {
            {
                let mut to_send = self.to_send.lock().await;
                to_send.insert((uuid, target), data);
            }
        }
        async fn put_multiple(&self,
                              uuid: Uuid,
                              targets: &[usize],
                              data: Arc<Vec<u8>>) {
            {
                let mut to_send = self.to_send.lock().await;
                for target in targets.iter() {
                    to_send.insert((uuid, *target), data.clone());
                }
            }
        }
        async fn remove(&self,
                        uuid: Uuid,
                        target: usize) {
            let mut to_send = self.to_send.lock().await;
            to_send.remove(&(uuid, target));
        }
    }

    #[derive(Debug, Clone)]
    struct SenderCmd {
        target: usize,
        data: Arc<Vec<u8>>,
    }

    #[async_trait::async_trait]
    impl Handler<SenderCmd> for Sender {
        async fn handle(&mut self, msg: SenderCmd) {
            self.try_send(msg.target, &*msg.data).await;
        }
    }

    impl Sender {
        async fn try_send(&self,
                          target: usize,
                          data: &Vec<u8>) -> bool {
            let mut sock_guard = self.sockets[target - 1].lock().await;
            if sock_guard.is_none() {
                match tokio::net::TcpStream::connect(&self.tcp_locations[target - 1]).await {
                    Ok(s) => {
                        *sock_guard = Some(s)
                    }
                    Err(error) => {
                        log::warn!("Connection error encountered when connecting to {:?}. Error details: {}", self.tcp_locations[target - 1], error.to_string());
                    }
                };
            }
            if let Some(sock) = &mut *sock_guard {
                match sock.write_all(data).await {
                    Ok(_) => {
                        log::debug!("Successfully sent command to {}", target);
                        return true;
                    }
                    Err(error) => {
                        log::warn!("An error occurred when sending data to {:?}. Error details: {}", self.tcp_locations[target - 1], error.to_string());
                    }
                }
            }
            *sock_guard = None;
            false
        }
    }

    #[async_trait::async_trait]
    impl Handler<Tick> for Sender {
        async fn handle(&mut self, _: Tick) {
            {
                let mut sent_guard = self.send_store.to_send.lock().await;
                let mut to_remove: Vec<(Uuid, usize)> = vec![];
                for ((id, target_id), data) in &*sent_guard {
                    if self.try_send(*target_id, data).await {
                        to_remove.push((id.clone(), *target_id));
                    }
                }
                for id in to_remove.into_iter() {
                    sent_guard.remove(&id);
                }
            }
        }
    }

    #[derive(Clone)]
    pub struct SenderWrapper {
        sender_ref: ModuleRef<Sender>,
        send_store: Arc<SendStore>,
        hmac_key: [u8; 64],
        target_range: usize,
    }

    impl SenderWrapper {
        pub async fn new(hmac_key: [u8; 64],
                         tcp_locations: &Vec<(String, u16)>,
                         system: &mut System) -> SenderWrapper {
            let mut sockets: Vec<tokio::sync::Mutex<Option<tokio::net::TcpStream>>> = vec![];
            for target in tcp_locations.iter() {
                let stream = match tokio::net::TcpStream::connect(target).await {
                    Ok(s) => { Some(s) }
                    Err(error) => {
                        log::warn!("Connection error encountered when connecting to {:?}. Error details: {}", target, error.to_string());
                        None
                    }
                };
                sockets.push(tokio::sync::Mutex::new(stream));
            }
            let send_store = Arc::new(SendStore::new().await);
            let target_range = tcp_locations.len();
            let sender = Sender {
                tcp_locations: tcp_locations.clone(),
                sockets,
                send_store: send_store.clone(),
            };

            let sender_ref = system.register_module(sender).await;
            system.request_tick(&sender_ref, Duration::new(TIMEOUT_SEC, TIMEOUT_NANOS)).await;

            SenderWrapper {
                sender_ref,
                send_store,
                hmac_key,
                target_range,
            }
        }
    }

    #[async_trait::async_trait]
    impl RegisterClient for SenderWrapper {
        async fn send(&self, msg: crate::Send) {
            let mut buffer: Vec<u8> = vec![];
            let command: RegisterCommand = RegisterCommand::System((*msg.cmd).clone());
            log::debug!("Sending to {} system command {:?}", msg.target, command);
            serialize_register_command(&command, &mut buffer, &self.hmac_key).await.unwrap();
            let data = Arc::new(buffer);
            let sender_cmd = SenderCmd {
                target: msg.target,
                data: data.clone(),
            };
            match &command {
                RegisterCommand::Client(_) => {}
                RegisterCommand::System(system_msg) => {
                    match system_msg.content {
                        SystemRegisterCommandContent::ReadProc => {
                            self.send_store.put(msg.cmd.header.msg_ident, msg.target, data).await;
                        }
                        SystemRegisterCommandContent::Value { .. } => {}
                        SystemRegisterCommandContent::WriteProc { .. } => {
                            self.send_store.put(msg.cmd.header.msg_ident, msg.target, data).await;
                        }
                        SystemRegisterCommandContent::Ack => {}
                    }
                }
            }
            self.sender_ref.send(sender_cmd).await;
        }

        async fn broadcast(&self, msg: Broadcast) {
            let mut buffer: Vec<u8> = vec![];
            let command: RegisterCommand = RegisterCommand::System((*msg.cmd).clone());
            serialize_register_command(&command, &mut buffer, &self.hmac_key).await.unwrap();
            let data = Arc::new(buffer);
            let targets = (1..(self.target_range + 1)).collect::<Vec<_>>();

            log::debug!("Broadcasting system command {:?}", command);

            match &command {
                RegisterCommand::Client(_) => {}
                RegisterCommand::System(system_msg) => {
                    match system_msg.content {
                        SystemRegisterCommandContent::ReadProc => {
                            self.send_store.put_multiple(msg.cmd.header.msg_ident, &*targets, data.clone()).await;
                        }
                        SystemRegisterCommandContent::Value { .. } => {}
                        SystemRegisterCommandContent::WriteProc { .. } => {
                            self.send_store.put_multiple(msg.cmd.header.msg_ident, &*targets, data.clone()).await;
                        }
                        SystemRegisterCommandContent::Ack => {}
                    }
                }
            }
            for target in targets {
                let sender_cmd = SenderCmd {
                    target,
                    data: data.clone(),
                };
                self.sender_ref.send(sender_cmd).await;
            }
        }
    }

    pub struct ListeningSocket {
        hmac_system_key: [u8; 64],
        hmac_client_key: [u8; 32],
        listener: tokio::net::TcpListener,
        storage: Arc<SendStore>,
        job_handler: Option<ModuleRef<JobHandler>>,
    }

    pub struct Accept();

    impl ListeningSocket {
        pub async fn init(hmac_system_key: [u8; 64],
                          hmac_client_key: [u8; 32],
                          tcp_location: (String, u16)) -> ListeningSocket {
            let listener = tokio::net::TcpListener::bind(&tcp_location).await.unwrap();
            let storage = Arc::new(SendStore::new().await);
            ListeningSocket {
                hmac_system_key,
                hmac_client_key,
                listener,
                storage,
                job_handler: None,
            }
        }
    }

    #[async_trait::async_trait]
    impl Handler<ModuleRef<JobHandler>> for ListeningSocket {
        async fn handle(&mut self, job_handler: ModuleRef<JobHandler>) {
            self.job_handler = Some(job_handler)
        }
    }

    #[async_trait::async_trait]
    impl Handler<Accept> for ListeningSocket {
        async fn handle(&mut self, _: Accept) {
            match self.listener.accept().await {
                Ok((stream, addr)) => {
                    log::debug!("New connection from {}", addr);
                    let (read_half, send_half) = stream.into_split();
                    let send_half_arc = Arc::new(tokio::sync::Mutex::new(Some(send_half)));
                    let receiver = RecvStream {
                        hmac_system_key: self.hmac_system_key,
                        hmac_client_key: self.hmac_client_key,
                        stream: Some(read_half),
                        send_stream: send_half_arc.clone(),
                        addr,
                        send_store: self.storage.clone(),
                        job_handler: self.job_handler.as_ref().unwrap().clone(),
                    };
                    let sender = SendStream {
                        hmac_client_key: self.hmac_client_key,
                        stream: send_half_arc,
                        addr,
                    };
                    self.job_handler.as_ref().unwrap().send((receiver, sender)).await;
                }
                Err(error) => {
                    log::warn!("Accepting socket got error: {}. Closing!", error.to_string());
                    return;
                }
            }
        }
    }

    pub struct RecvStream {
        hmac_system_key: [u8; 64],
        hmac_client_key: [u8; 32],
        stream: Option<tokio::net::tcp::OwnedReadHalf>,
        send_stream: Arc<tokio::sync::Mutex<Option<tokio::net::tcp::OwnedWriteHalf>>>,
        addr: SocketAddr,
        send_store: Arc<SendStore>,
        job_handler: ModuleRef<JobHandler>,
    }

    impl RecvStream {
        async fn read(&mut self) -> Option<(RegisterCommand, bool)> {
            return
                if let Some(stream) = &mut self.stream {
                    match deserialize_register_command(stream, &self.hmac_system_key, &self.hmac_client_key).await {
                        Ok((cmd, signed)) => {
                            match &cmd {
                                RegisterCommand::Client(_) => {}
                                RegisterCommand::System(system_cmd) => {
                                    match system_cmd.content {
                                        SystemRegisterCommandContent::ReadProc => {}
                                        SystemRegisterCommandContent::Value { .. } => {
                                            self.send_store.remove(system_cmd.header.msg_ident, (system_cmd.header.process_identifier) as usize).await;
                                        }
                                        SystemRegisterCommandContent::WriteProc { .. } => {}
                                        SystemRegisterCommandContent::Ack => {
                                            self.send_store.remove(system_cmd.header.msg_ident, (system_cmd.header.process_identifier) as usize).await;
                                        }
                                    }
                                }
                            }
                            Some((cmd, signed))
                        }
                        Err(error) => {
                            // Stream read error
                            log::warn!("Stream read error from {}. Error details: {}.", self.addr.to_string(), error.to_string());
                            let mut write_half_guard = self.send_stream.lock().await;
                            if let Some(write_half) = write_half_guard.take() {
                                let read_half = self.stream.take().unwrap();
                                if let Ok(mut stream) = read_half.reunite(write_half) {
                                    if let Err(error) = stream.shutdown().await {
                                        log::warn!("An error occurred when closing stream to {}, details: {}", self.addr, error.to_string());
                                    }
                                }
                            }
                            None
                        }
                    }
                } else {
                    log::warn!("Tried to use recv stream with closed socket with addr: {}", self.addr.to_string());
                    None
                };
            }
            fn is_dead(&self) -> bool {
                self.stream.is_none()
            }
        }

        #[async_trait::async_trait]
        impl Handler<StreamRequest> for RecvStream {
            async fn handle(&mut self, msg: StreamRequest) {
                match msg {
                    StreamRequest::Read(recv_stream, send_stream) => {
                        loop {
                            match self.read().await {
                                Some((cmd, bool)) => {
                                    log::debug!("Received message from process {}, msg: {:?}", self.addr, cmd);
                                    self.job_handler.send(StreamResponse::Return(recv_stream, send_stream, (cmd, bool))).await;
                                    return;
                                }
                                None => {
                                    if self.is_dead() {
                                        log::warn!("Could not realise send request - socket has been closed");
                                        self.job_handler.send(StreamResponse::ReadFail(recv_stream, send_stream)).await;
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        pub struct SendStream {
            hmac_client_key: [u8; 32],
            stream: Arc<tokio::sync::Mutex<Option<tokio::net::tcp::OwnedWriteHalf>>>,
            addr: SocketAddr,
        }


        #[async_trait::async_trait]
        impl Handler<ClientReply> for SendStream {
            async fn handle(&mut self, msg: ClientReply) {
                let mut stream_guard = self.stream.lock().await;
                if let Some(stream) = &mut *stream_guard {
                    match serialize_client_reply(&msg, stream, &self.hmac_client_key).await {
                        Ok(_) => {
                            log::debug!("Sent command {:?} to {}.", msg, self.addr);
                        }
                        Err(error) => {
                            log::warn!("Error while sending response to client {}, details: {}", self.addr.to_string(), error.to_string());
                        }
                    }
                } else {
                    log::warn!("Error while sending response to client {} - socket already closed", self.addr.to_string());
                };
            }
        }

        pub enum StreamRequest {
            Read(ModuleRef<RecvStream>, ModuleRef<SendStream>),
        }

        pub enum StreamResponse {
            Return(ModuleRef<RecvStream>, ModuleRef<SendStream>, (RegisterCommand, bool)),
            ReadFail(ModuleRef<RecvStream>, ModuleRef<SendStream>),
        }
    }

    pub mod netdata {
        use std::convert::TryInto;
        use std::io::Error;
        use std::mem;
        use bincode::Options;

        use hmac::{Hmac, Mac, NewMac};
        use serde::{Deserialize, Serialize};
        use serde_repr::{Deserialize_repr, Serialize_repr};
        use sha2::Sha256;
        use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
        use uuid::Uuid;

        use crate::domain::*;

        big_array! { BigArray; }

        const SHORTEST_MSG: usize = 56;
        pub(crate) const SECTOR_SIZE: usize = 4096;
        const LONGEST_MSG: usize = SECTOR_SIZE + 11 * 8;
        const HEADER_LEN: usize = 8;
        const HTAG_LEN: usize = 32;
        const MAGIC_INT: u32 = 1635017828;

        pub struct NetBuffer {
            buffer: [u8; LONGEST_MSG],
            buf_start: usize,
            buf_end: usize,
        }

        fn is_magic(buffer: &[u8]) -> bool {
            if buffer.len() != MAGIC_NUMBER.len() {
                panic!("Function must be called with buffer of length {}", MAGIC_NUMBER.len());
            }
            for i in 0..MAGIC_NUMBER.len() {
                if buffer[i] != MAGIC_NUMBER[i] {
                    return false;
                }
            }
            return true;
        }

        type HmacSha256 = Hmac<Sha256>;

        fn calculate_hmac_tag(message: &[u8], secret_key: &[u8]) -> [u8; 32] {
            // Initialize a new MAC instance from the secret key:
            let mut mac = HmacSha256::new_from_slice(secret_key).unwrap();

            // Calculate MAC for the data (one can provide it in multiple portions):
            mac.update(message);

            // Finalize the computations of MAC and obtain the resulting tag:
            let tag = mac.finalize().into_bytes();

            tag.into()
        }

        fn verify_hmac_tag(tag: &[u8], message: &[u8], secret_key: &[u8]) -> bool {
            // Initialize a new MAC instance from the secret key:
            let mut mac = HmacSha256::new_from_slice(secret_key).unwrap();

            // Calculate MAC for the data (one can provide it in multiple portions):
            mac.update(message);

            // Verify the tag:
            mac.verify(tag).is_ok()
        }

        impl NetBuffer {
            pub fn new() -> NetBuffer {
                return NetBuffer {
                    buffer: [0; LONGEST_MSG],
                    buf_start: 0,
                    buf_end: 0,
                };
            }

            fn move_to_start(&mut self) {
                self.buffer.copy_within(self.buf_start..self.buf_end, 0);
                self.buf_end = self.len();
                self.buf_start = 0
            }

            pub fn consume_exact(&mut self, len: usize) {
                if self.len() < len {
                    panic!("Cannot consume more than in buffer, buffer len: {}, tried to consume: {}", self.len(), len);
                }
                self.buf_start += len;
            }

            pub fn consume_until_magic(&mut self) -> bool {
                if self.len() < MAGIC_NUMBER.len() {
                    return false;
                }
                for i in self.buf_start..(self.buf_end + 1 - MAGIC_NUMBER.len()) {
                    if is_magic(&self.buffer[i..i + 4]) {
                        self.buf_start = i;
                        return true;
                    }
                }
                self.buf_start = self.buf_end;
                return false;
            }

            pub fn len(&self) -> usize {
                self.buf_end - self.buf_start
            }

            pub async fn read_to_buff(&mut self,
                                      len: usize,
                                      data: &mut (dyn AsyncRead + Send + Unpin)) -> Result<(), Error> {
                if LONGEST_MSG - self.len() < len {
                    log::error!("Cannot write to network buffer {} bytes, as it already contains {} bytes and this amount exceeds size by {} bytes.", len, self.len(), len - (LONGEST_MSG - self.len()));
                    panic!("Cannot write to network buffer {} bytes, as it already contains {} bytes and this amount exceeds size by {} bytes.", len, self.len(), len - (LONGEST_MSG - self.len()));
                }
                if LONGEST_MSG - self.buf_end < len {
                    self.move_to_start()
                }
                return match data.read_exact(&mut self.buffer[self.buf_end..self.buf_end + len]).await {
                    Ok(_) => {
                        self.buf_end += len;
                        Ok(())
                    }
                    Err(error) => {
                        log::error!("Unexpected error occurred when reading from socket stream: {}", error.to_string());
                        Err(error)
                    }
                };
            }

            pub async fn read_until_magic(&mut self,
                                          data: &mut (dyn AsyncRead + Send + Unpin)) -> Result<(), Error> {
                while !self.consume_until_magic() {
                    self.read_to_buff(SHORTEST_MSG - MAGIC_NUMBER.len() + 1, data).await?;
                }
                Ok(())
            }

            pub async fn read_header(&mut self,
                                     data: &mut (dyn AsyncRead + Send + Unpin)) -> Result<MsgHeader, Error> {
                if self.len() < HEADER_LEN {
                    self.read_to_buff(HEADER_LEN - self.len(), data).await?;
                }
                if !is_magic(&self.buffer[self.buf_start..self.buf_start + 4]) {
                    log::error!("Tried to read header without checking if magic numbers are present!");
                    panic!("Tried to read header without checking if magic numbers are present!")
                }
                match bincode::options().with_big_endian().with_fixint_encoding().deserialize::<MsgHeader>(&self.buffer[self.buf_start..self.buf_start + HEADER_LEN]) {
                    Ok(header) => { return Ok(header); }
                    Err(error) => {
                        log::error!("Unexpected error occurred when deserializing header: {}", error.to_string());
                        panic!("Unexpected error occurred when deserializing header: {}", error.to_string())
                    }
                }
            }

            async fn assure_buff_content(&mut self,
                                         len: usize,
                                         data: &mut (dyn AsyncRead + Send + Unpin)) -> Result<(), Error> {
                if LONGEST_MSG < len {
                    log::error!("Cannot write to network buffer {} bytes, as it exceeds maximum buffer size.", len);
                    panic!("Cannot write to network buffer {} bytes, as it exceeds maximum buffer size.", len);
                }
                if len <= self.len() {
                    return Ok(());
                };
                if LONGEST_MSG - self.buf_end < len {
                    self.move_to_start()
                }
                let to_read = len - self.len();
                data.read_exact(&mut self.buffer[self.buf_end..self.buf_end + to_read]).await?;
                self.buf_end += to_read;
                Ok(())
            }

            pub async fn read_msg(&mut self,
                                  header: &MsgHeader,
                                  client_secret_key: &[u8],
                                  system_secret_key: &[u8],
                                  data: &mut (dyn AsyncRead + Send + Unpin)) -> Result<(RegisterCommand, bool), ReadError> {
                let (command, len): (RegisterCommand, usize) = match &header.msg_type {
                    CommandID::ClientReadRequest => {
                        self.assure_buff_content(mem::size_of::<ClientReadRequest>() + HTAG_LEN, data).await.map_err(|e| ReadError::IOError(e))?;
                        let command = bincode::options().with_big_endian().with_fixint_encoding().deserialize::<ClientReadRequest>(&self.buffer[self.buf_start..self.buf_start + mem::size_of::<ClientReadRequest>()]).map_err(|err| ReadError::BincodeError(err))?;
                        (command.to_register_command(), mem::size_of::<ClientReadRequest>())
                    }
                    CommandID::ClientWriteRequest => {
                        self.assure_buff_content(mem::size_of::<ClientWriteRequest>() + HTAG_LEN, data).await.map_err(|e| ReadError::IOError(e))?;
                        let command = bincode::options().with_big_endian().with_fixint_encoding().deserialize::<ClientWriteRequest>(&self.buffer[self.buf_start..self.buf_start + mem::size_of::<ClientWriteRequest>()]).map_err(|err| ReadError::BincodeError(err))?;
                        (command.to_register_command(), mem::size_of::<ClientWriteRequest>())
                    }
                    CommandID::ReadProc => {
                        self.assure_buff_content(mem::size_of::<ReadProcCommand>() + HTAG_LEN, data).await.map_err(|e| ReadError::IOError(e))?;
                        let command = bincode::options().with_big_endian().with_fixint_encoding().deserialize::<ReadProcCommand>(&self.buffer[self.buf_start..self.buf_start + mem::size_of::<ReadProcCommand>()]).map_err(|err| ReadError::BincodeError(err))?;
                        (command.to_register_command(), mem::size_of::<ReadProcCommand>())
                    }
                    CommandID::Value => {
                        self.assure_buff_content(mem::size_of::<ValueCommand>() + HTAG_LEN, data).await.map_err(|e| ReadError::IOError(e))?;
                        let command = bincode::options().with_big_endian().with_fixint_encoding().deserialize::<ValueCommand>(&self.buffer[self.buf_start..self.buf_start + mem::size_of::<ValueCommand>()]).map_err(|err| ReadError::BincodeError(err))?;
                        (command.to_register_command(), mem::size_of::<ValueCommand>())
                    }
                    CommandID::WriteProc => {
                        self.assure_buff_content(mem::size_of::<WriteProcCommand>() + HTAG_LEN, data).await.map_err(|e| ReadError::IOError(e))?;
                        let command = bincode::options().with_big_endian().with_fixint_encoding().deserialize::<WriteProcCommand>(&self.buffer[self.buf_start..self.buf_start + mem::size_of::<WriteProcCommand>()]).map_err(|err| ReadError::BincodeError(err))?;
                        (command.to_register_command(), mem::size_of::<WriteProcCommand>())
                    }
                    CommandID::Ack => {
                        self.assure_buff_content(mem::size_of::<AckCommand>() + HTAG_LEN, data).await.map_err(|e| ReadError::IOError(e))?;
                        let command = bincode::options().with_big_endian().with_fixint_encoding().deserialize::<AckCommand>(&self.buffer[self.buf_start..self.buf_start + mem::size_of::<AckCommand>()]).map_err(|err| ReadError::BincodeError(err))?;
                        (command.to_register_command(), mem::size_of::<AckCommand>())
                    }
                    x => {
                        log::warn!("Received message of unknown type: {:?}", x);
                        return Err(ReadError::UnknownMessageID);
                    }
                };
                let msg_slice: &[u8] = &self.buffer[self.buf_start..self.buf_start + len];
                let received_htag = match bincode::options().with_big_endian().with_fixint_encoding().deserialize::<MsgHTAG>(&self.buffer[self.buf_start + len..self.buf_start + len + HTAG_LEN]) {
                    Ok(res) => { res }
                    Err(error) => {
                        log::error!("Unexpected error occurred when deserializing message htag: {}", error.to_string());
                        panic!("Unexpected error occurred when deserializing message htag: {}", error.to_string());
                    }
                };
                let hmac_key = match command {
                    RegisterCommand::Client(_) => { client_secret_key }
                    RegisterCommand::System(_) => { system_secret_key }
                };
                return Ok((command, verify_hmac_tag(&received_htag.htag, msg_slice, hmac_key)));
            }
        }

        async fn write_raw(data: Result<Vec<u8>, bincode::Error>,
                           writer: &mut (dyn AsyncWrite + Send + Unpin),
                           hmac_key: &[u8]) -> Result<(), Error> {
            let raw_cmd = match data {
                Ok(b) => { b }
                Err(error) => {
                    log::error!("Unexpected error occurred when serializing command: {}", error.to_string());
                    return Ok(());
                }
            };
            let hmac_section = MsgHTAG {
                htag: calculate_hmac_tag(&raw_cmd, hmac_key)
            };
            let raw_htag = match bincode::options().with_big_endian().with_fixint_encoding().serialize(&hmac_section) {
                Ok(b) => { b }
                Err(error) => {
                    log::error!("Unexpected error occurred when serializing command htag: {}", error.to_string());
                    return Ok(());
                }
            };
            writer.write(&raw_cmd).await?;
            writer.write(&raw_htag).await?;
            Ok(())
        }

        pub async fn serialize_msg(cmd: &RegisterCommand,
                                   writer: &mut (dyn AsyncWrite + Send + Unpin),
                                   hmac_key: &[u8]) -> Result<(), Error> {
            let raw_cmd_res = match cmd {
                RegisterCommand::Client(client_command) => {
                    match &client_command.content {
                        ClientRegisterCommandContent::Read => {
                            bincode::options().with_big_endian().with_fixint_encoding().serialize(&ClientReadRequest {
                                header: MsgHeader::new(CommandID::ClientReadRequest),
                                request_number: client_command.header.request_identifier,
                                sector_index: client_command.header.sector_idx,
                            })
                        }
                        ClientRegisterCommandContent::Write { data } => {
                            bincode::options().with_big_endian().with_fixint_encoding().serialize(&ClientWriteRequest {
                                header: MsgHeader::new(CommandID::ClientWriteRequest),
                                request_number: client_command.header.request_identifier,
                                sector_index: client_command.header.sector_idx,
                                command_content: data.0.clone().try_into().unwrap(),
                            })
                        }
                    }
                }
                RegisterCommand::System(system_command) => {
                    let header_builder = |id: CommandID| {
                        MsgHeader {
                            magic_number: MAGIC_INT,
                            padding: 0,
                            extra_field: system_command.header.process_identifier,
                            msg_type: id,
                        }
                    };
                    let sys_header = SysCommandHeader {
                        uuid: *system_command.header.msg_ident.as_bytes(),
                        rid: system_command.header.read_ident,
                        sector_id: system_command.header.sector_idx,
                    };
                    match &system_command.content {
                        SystemRegisterCommandContent::ReadProc => {
                            bincode::options().with_big_endian().with_fixint_encoding().serialize(&ReadProcCommand {
                                header: header_builder(CommandID::ReadProc),
                                sys_header,
                            })
                        }
                        SystemRegisterCommandContent::Value {
                            timestamp,
                            write_rank,
                            sector_data
                        } => {
                            bincode::options().with_big_endian().with_fixint_encoding().serialize(&ValueCommand {
                                header: header_builder(CommandID::Value),
                                sys_header,
                                timestamp: *timestamp,
                                padding: Default::default(),
                                write_rank: *write_rank,
                                sector_data: sector_data.0.clone().try_into().unwrap(),
                            })
                        }
                        SystemRegisterCommandContent::WriteProc {
                            timestamp,
                            write_rank,
                            data_to_write
                        } => {
                            bincode::options().with_big_endian().with_fixint_encoding().serialize(&WriteProcCommand {
                                header: header_builder(CommandID::WriteProc),
                                sys_header,
                                timestamp: *timestamp,
                                padding: Default::default(),
                                write_rank: *write_rank,
                                sector_data: data_to_write.0.clone().try_into().unwrap(),
                            })
                        }
                        SystemRegisterCommandContent::Ack => {
                            bincode::options().with_big_endian().with_fixint_encoding().serialize(&AckCommand {
                                header: header_builder(CommandID::Ack),
                                sys_header,
                            })
                        }
                    }
                }
            };
            write_raw(raw_cmd_res, writer, hmac_key).await
        }

        pub async fn serialize_client_reply(cmd: &ClientReply,
                                            writer: &mut (dyn AsyncWrite + Send + Unpin),
                                            hmac_key: &[u8]) -> Result<(), Error> {
            let raw_cmd_res = match cmd {
                ClientReply::Bare(read_cmd) => {
                    bincode::options().with_big_endian().with_fixint_encoding().serialize(&read_cmd)
                }
                ClientReply::WithContent(write_cmd) => {
                    bincode::options().with_big_endian().with_fixint_encoding().serialize(&write_cmd)
                }
            };
            write_raw(raw_cmd_res, writer, hmac_key).await
        }

        #[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug, Clone)]
        #[repr(u8)]
        pub enum CommandID {
            ClientReadRequest = 0x01,
            ClientReadReply = 0x41,
            ClientWriteRequest = 0x02,
            ClientWriteReply = 0x42,
            ReadProc = 0x03,
            Value = 0x04,
            WriteProc = 0x05,
            Ack = 0x06,
        }

        trait RawCommand {
            fn to_register_command(self) -> RegisterCommand;
        }

        pub enum ReadError {
            IOError(Error),
            BincodeError(bincode::Error),
            UnknownMessageID,
        }

        #[derive(Serialize, Deserialize, Debug, Clone)]
        pub struct MsgHeader {
            magic_number: u32,
            padding: u16,
            extra_field: u8,
            msg_type: CommandID,
        }

        impl MsgHeader {
            fn new(msg_type: CommandID) -> MsgHeader {
                return MsgHeader {
                    magic_number: MAGIC_INT,
                    padding: 0,
                    extra_field: 0,
                    msg_type,
                };
            }

            pub fn new_with_extra(msg_type: CommandID, extra_field: u8) -> MsgHeader {
                return MsgHeader {
                    magic_number: MAGIC_INT,
                    padding: 0,
                    extra_field,
                    msg_type,
                };
            }
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct MsgHTAG {
            htag: [u8; 32],
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct ClientReadRequest {
            header: MsgHeader,
            request_number: u64,
            sector_index: u64,
        }

        impl RawCommand for ClientReadRequest {
            fn to_register_command(self) -> RegisterCommand {
                RegisterCommand::Client(
                    ClientRegisterCommand {
                        header: ClientCommandHeader {
                            request_identifier: self.request_number,
                            sector_idx: self.sector_index,
                        },
                        content: ClientRegisterCommandContent::Read,
                    }
                )
            }
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct ClientWriteRequest {
            header: MsgHeader,
            request_number: u64,
            sector_index: u64,
            #[serde(with = "BigArray")]
            command_content: [u8; SECTOR_SIZE],
        }

        impl RawCommand for ClientWriteRequest {
            fn to_register_command(self) -> RegisterCommand {
                RegisterCommand::Client(
                    ClientRegisterCommand {
                        header: ClientCommandHeader {
                            request_identifier: self.request_number,
                            sector_idx: self.sector_index,
                        },
                        content: ClientRegisterCommandContent::Write {
                            data: SectorVec(Vec::from(self.command_content))
                        },
                    }
                )
            }
        }

        #[derive(Clone, Debug)]
        pub enum ClientReply {
            WithContent(ClientContentReply),
            Bare(ClientBareReply),
        }

        #[derive(Serialize, Deserialize, Debug, Clone)]
        pub struct ClientContentReply {
            header: MsgHeader,
            request_number: u64,
            #[serde(with = "BigArray")]
            command_content: [u8; SECTOR_SIZE],
        }

        impl ClientContentReply {
            pub(crate) fn new(header: MsgHeader,
                              request_number: u64,
                              command_content: [u8; SECTOR_SIZE], ) -> ClientContentReply {
                ClientContentReply {
                    header,
                    request_number,
                    command_content,
                }
            }
        }

        #[derive(Serialize, Deserialize, Debug, Clone)]
        pub struct ClientBareReply {
            header: MsgHeader,
            request_number: u64,
        }

        impl ClientBareReply {
            pub(crate) fn new(header: MsgHeader,
                              request_number: u64) -> ClientBareReply {
                ClientBareReply {
                    header,
                    request_number,
                }
            }
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct SysCommandHeader {
            uuid: [u8; 16],
            rid: u64,
            sector_id: u64,
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct ReadProcCommand {
            header: MsgHeader,
            sys_header: SysCommandHeader,
        }

        impl RawCommand for ReadProcCommand {
            fn to_register_command(self) -> RegisterCommand {
                RegisterCommand::System(SystemRegisterCommand {
                    header: SystemCommandHeader {
                        process_identifier: self.header.extra_field,
                        msg_ident: Uuid::from_bytes(self.sys_header.uuid),
                        read_ident: self.sys_header.rid,
                        sector_idx: self.sys_header.sector_id,
                    },
                    content: SystemRegisterCommandContent::ReadProc,
                })
            }
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct ValueCommand {
            header: MsgHeader,
            sys_header: SysCommandHeader,
            timestamp: u64,
            padding: [u8; 7],
            write_rank: u8,
            #[serde(with = "BigArray")]
            sector_data: [u8; SECTOR_SIZE],
        }

        impl RawCommand for ValueCommand {
            fn to_register_command(self) -> RegisterCommand {
                RegisterCommand::System(SystemRegisterCommand {
                    header: SystemCommandHeader {
                        process_identifier: self.header.extra_field,
                        msg_ident: Uuid::from_bytes(self.sys_header.uuid),
                        read_ident: self.sys_header.rid,
                        sector_idx: self.sys_header.sector_id,
                    },
                    content: SystemRegisterCommandContent::Value {
                        timestamp: self.timestamp,
                        write_rank: self.write_rank,
                        sector_data: SectorVec(Vec::from(self.sector_data)),
                    },
                })
            }
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct WriteProcCommand {
            header: MsgHeader,
            sys_header: SysCommandHeader,
            timestamp: u64,
            padding: [u8; 7],
            write_rank: u8,
            #[serde(with = "BigArray")]
            sector_data: [u8; SECTOR_SIZE],
        }

        impl RawCommand for WriteProcCommand {
            fn to_register_command(self) -> RegisterCommand {
                RegisterCommand::System(SystemRegisterCommand {
                    header: SystemCommandHeader {
                        process_identifier: self.header.extra_field,
                        msg_ident: Uuid::from_bytes(self.sys_header.uuid),
                        read_ident: self.sys_header.rid,
                        sector_idx: self.sys_header.sector_id,
                    },
                    content: SystemRegisterCommandContent::WriteProc {
                        timestamp: self.timestamp,
                        write_rank: self.write_rank,
                        data_to_write: SectorVec(Vec::from(self.sector_data)),
                    },
                })
            }
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct AckCommand {
            header: MsgHeader,
            sys_header: SysCommandHeader,
        }

        impl RawCommand for AckCommand {
            fn to_register_command(self) -> RegisterCommand {
                RegisterCommand::System(SystemRegisterCommand {
                    header: SystemCommandHeader {
                        process_identifier: self.header.extra_field,
                        msg_ident: Uuid::from_bytes(self.sys_header.uuid),
                        read_ident: self.sys_header.rid,
                        sector_idx: self.sys_header.sector_id,
                    },
                    content: SystemRegisterCommandContent::Ack,
                })
            }
        }
    }
