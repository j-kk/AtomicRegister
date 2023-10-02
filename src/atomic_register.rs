pub mod atomic_register_impl {
    use std::cmp::Ordering;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use serde::Serialize;
    use serde::de::DeserializeOwned;
    use uuid::Uuid;

    use crate::{AtomicRegister, Broadcast, ClientRegisterCommand, ClientRegisterCommandContent, OperationComplete, OperationReturn, ReadReturn, RegisterClient, SectorsManager, SectorVec, StableStorage, StatusCode, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent};

    pub struct AtomicReg {
        process_identifier: u8,
        metadata: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: usize,
        readlist: Vec<Option<RegisterOp>>,
        acklist: Vec<bool>,
        reading: bool,
        writing: bool,
        writeval: Option<SectorVec>,
        readval: Option<SectorVec>,
        write_phase: bool,
        rid: u64,
        operation_complete: Option<Box<dyn FnOnce(OperationComplete) -> Pin<Box<dyn Future<Output=()> + Send>> + Send + Sync>>,
        request_identifier: Option<u64>
    }

    #[derive(Debug, Clone)]
    struct RegisterOp(u64, u8, SectorVec);

    impl Eq for RegisterOp {}

    impl PartialEq<Self> for RegisterOp {
        fn eq(&self, other: &Self) -> bool {
            return self.0 == other.0 && self.1 == other.1;
        }
    }

    impl PartialOrd<Self> for RegisterOp {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            return (self.0, self.1).partial_cmp(&(other.0, other.1));
        }
    }

    impl Ord for RegisterOp {
        fn cmp(&self, other: &Self) -> Ordering {
            return (self.0, self.1).cmp(&(other.0, other.1));
        }
    }

    async fn store<T: Serialize>(storage: &mut Box<dyn StableStorage>, key: &str, meta: T) {
        let data: Vec<u8> = match bincode::serialize(&meta) {
            Ok(data) => { data }
            Err(error) => {
                log::error!("Unexpected error occurred when serializing register metadata: {}", error.to_string());
                panic!("Unexpected error occurred when serializing register metadata: {}", error.to_string())
            }
        };
        storage.put(key, &*data).await.unwrap();
    }

    async fn restore<T: DeserializeOwned>(storage: &Box<dyn StableStorage>, key: &str) -> Option<T> {
        let data = storage.get(key).await?;
        let maybe_v = bincode::deserialize::<T>(&data);
        let v: T = match maybe_v {
            Ok(meta) => { meta }
            Err(error) => {
                log::error!("Unexpected error occurred when deserializing register metadata: {}", error.to_string());
                panic!("Unexpected error occurred when deserializing register metadata: {}", error.to_string())
            }
        };
        return Some(v);
    }

    impl AtomicReg {
        async fn store<T: Serialize>(&mut self, key: &str, meta: T) {
            store(&mut self.metadata, key, meta).await;
        }

        pub async fn new(process_identifier: u8,
                         metadata: Box<dyn StableStorage>,
                         register_client: Arc<dyn RegisterClient>,
                         sectors_manager: Arc<dyn SectorsManager>,
                         processes_count: usize) -> AtomicReg {
            let mut readlist: Vec<Option<RegisterOp>> = vec![];
            readlist.resize_with(processes_count, || None);
            let rid = restore(&metadata, "rid").await.unwrap_or(0);
            let mut reg = AtomicReg {
                process_identifier,
                metadata,
                register_client,
                sectors_manager,
                processes_count,
                readlist,
                acklist: vec![false; processes_count],
                reading: false,
                writing: false,
                writeval: None,
                readval: None,
                write_phase: false,
                rid,
                operation_complete: None,
                request_identifier: None
            };
            reg.store("rid", reg.rid).await;
            return reg;
        }
    }

    #[async_trait::async_trait]
    impl AtomicRegister for AtomicReg {
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            operation_complete: Box<dyn FnOnce(OperationComplete) -> Pin<Box<dyn Future<Output=()> + Send>> + Send + Sync>) {
            match cmd {
                ClientRegisterCommand {
                    header: cmd_header,
                    content: ClientRegisterCommandContent::Read
                } => {
                    if self.reading {
                        log::debug!("Atomic register received READ command while already reading!");
                        return;
                    };
                    if self.writing {
                        log::debug!("Atomic register received READ command while already writing!");
                        return;
                    }
                    // rid := rid + 1;
                    self.rid += 1;
                    // store(rid);
                    self.store("rid", self.rid).await;
                    // readlist := [ _ ] `of length` N;
                    self.readlist.iter_mut().for_each(|x| *x = None);
                    // acklist := [ _ ] `of length` N;
                    self.acklist.iter_mut().for_each(|x| *x = false);
                    // reading := TRUE;
                    self.reading = true;

                    // trigger < sbeb, Broadcast | [READ_PROC, rid] >;
                    let cmd_to_send = Arc::new(SystemRegisterCommand {
                        header: SystemCommandHeader {
                            process_identifier: self.process_identifier,
                            msg_ident: Uuid::new_v4(),
                            read_ident: self.rid,
                            sector_idx: cmd_header.sector_idx,
                        },
                        content: SystemRegisterCommandContent::ReadProc,
                    });
                    self.register_client.broadcast(Broadcast { cmd: cmd_to_send }).await;
                    self.operation_complete = Some(operation_complete);
                    self.request_identifier = Some(cmd_header.request_identifier);
                }
                ClientRegisterCommand {
                    header: cmd_header,
                    content: ClientRegisterCommandContent::Write { data }
                } => {
                    if self.reading {
                        log::debug!("Atomic register received WRITE command while already reading!");
                        return;
                    };
                    if self.writing {
                        log::debug!("Atomic register received WRITE command while already writing!");
                        return;
                    }
                    // rid := rid + 1;
                    self.rid += 1;
                    // writeval := v;
                    self.writeval = Some(data);
                    // acklist := [ _ ] `of length` N;
                    self.readlist.iter_mut().for_each(|x| *x = None);
                    // readlist := [ _ ] `of length` N;
                    self.acklist.iter_mut().for_each(|x| *x = false);
                    // writing := TRUE;
                    self.writing = true;
                    // store(rid);
                    self.store("rid", self.rid).await;
                    // trigger < sbeb, Broadcast | [READ_PROC, rid] >;
                    let cmd_to_send = Arc::new(SystemRegisterCommand {
                        header: SystemCommandHeader {
                            process_identifier: self.process_identifier,
                            msg_ident: Uuid::new_v4(),
                            read_ident: self.rid,
                            sector_idx: cmd_header.sector_idx,
                        },
                        content: SystemRegisterCommandContent::ReadProc,
                    });
                    self.register_client.broadcast(Broadcast { cmd: cmd_to_send }).await;
                    self.operation_complete = Some(operation_complete);
                    self.request_identifier = Some(cmd_header.request_identifier);
                }
            };
        }

        async fn system_command(&mut self, cmd: SystemRegisterCommand) {
            match cmd {
                SystemRegisterCommand {
                    header,
                    content: SystemRegisterCommandContent::ReadProc,
                } => {
                    //trigger < sl, Send | p, [VALUE, r, ts, wr, val] >;
                    let (ts, wr) = self.sectors_manager.read_metadata(header.sector_idx).await;
                    let data = self.sectors_manager.read_data(header.sector_idx).await;
                    let cmd_to_send = Arc::new(SystemRegisterCommand {
                        header: SystemCommandHeader {
                            process_identifier: self.process_identifier,
                            msg_ident: header.msg_ident,
                            read_ident: header.read_ident,
                            sector_idx: header.sector_idx,
                        },
                        content: SystemRegisterCommandContent::Value {
                            timestamp: ts,
                            write_rank: wr,
                            sector_data: data,
                        },
                    });
                    self.register_client.send(crate::register_client_public::Send {
                        cmd: cmd_to_send,
                        target: usize::from(header.process_identifier),
                    }).await;
                }
                SystemRegisterCommand {
                    header,
                    content: SystemRegisterCommandContent::WriteProc {
                        timestamp,
                        write_rank,
                        data_to_write
                    }
                } => {
                    let (ts, wr) = self.sectors_manager.read_metadata(header.sector_idx).await;
                    // if (ts', wr') > (ts, wr) then
                    if (timestamp, write_rank) > (ts, wr) {
                        // (ts, wr, val) := (ts', wr', v');
                        // store(ts, wr, val);
                        self.sectors_manager.write(header.sector_idx, &(data_to_write, timestamp, write_rank)).await;
                    }
                    // trigger < sl, Send | p, [ACK, r] >;
                    let cmd_to_send = Arc::new(SystemRegisterCommand {
                        header: SystemCommandHeader {
                            process_identifier: self.process_identifier,
                            msg_ident: Uuid::new_v4(),
                            read_ident: header.read_ident,
                            sector_idx: header.sector_idx,
                        },
                        content: SystemRegisterCommandContent::Ack,
                    });
                    self.register_client.send(crate::register_client_public::Send {
                        cmd: cmd_to_send,
                        target: usize::from(header.process_identifier),
                    }).await;
                }

                SystemRegisterCommand {
                    header,
                    content: SystemRegisterCommandContent::Value {
                        timestamp,
                        write_rank,
                        sector_data
                    }
                } => {
                    // <sl, Deliver | q, [VALUE, r, ts', wr', v'] > such that r == rid and !write_phase do
                    if header.read_ident != self.rid {
                        log::debug!("Atomic register received VALUE with incorrect rid!");
                        return;
                    }
                    if self.write_phase {
                        log::debug!("Atomic register received VALUE command while being in write_phase!");
                        return;
                    }
                    // readlist[q] := (ts', wr', v');
                    self.readlist[usize::from(header.process_identifier) - 1] = Some(RegisterOp(timestamp, write_rank, sector_data));
                    // if #(readlist) > N / 2 and (reading or writing) then
                    if self.readlist.iter().filter(|&x| Option::is_some(x)).count() > self.processes_count / 2 && (self.reading || self.writing) {
                        // readlist[self] := (ts, wr, val);
                        let (ts, wr) = self.sectors_manager.read_metadata(header.sector_idx).await;
                        let value = self.sectors_manager.read_data(header.sector_idx).await;
                        self.readlist[usize::from(self.process_identifier) - 1] = Some(RegisterOp(
                            ts,
                            wr,
                            value,
                        ));
                        // (maxts, rr, readval) := highest(readlist);
                        let filtered_readlist: Vec<RegisterOp> = self.readlist.clone().into_iter().flatten().collect();
                        let max_op = filtered_readlist.iter().max().unwrap(); // We know that readlist has more than one element
                        self.readval = Some(max_op.2.clone());
                        // readlist := [ _ ] `of length` N;
                        self.readlist.iter_mut().for_each(|x| *x = None);
                        // acklist := [ _ ] `of length` N;
                        self.acklist.iter_mut().for_each(|x| *x = false);
                        // write_phase := TRUE;
                        self.write_phase = true;
                        // if reading = TRUE then
                        if self.reading {
                            // trigger < sbeb, Broadcast | [WRITE_PROC, rid, maxts, rr, readval] >;
                            let cmd_to_send = Arc::new(SystemRegisterCommand {
                                header: SystemCommandHeader {
                                    process_identifier: self.process_identifier,
                                    msg_ident: header.msg_ident,
                                    read_ident: header.read_ident,
                                    sector_idx: header.sector_idx,
                                },
                                content: SystemRegisterCommandContent::WriteProc {
                                    timestamp: max_op.0,
                                    write_rank: max_op.1,
                                    data_to_write: max_op.2.clone(),
                                },
                            });
                            self.register_client.broadcast(Broadcast {
                                cmd: cmd_to_send,
                            }).await;
                        } else {
                            // else
                            // (ts, wr, val) := (maxts + 1, rank(self), writeval);
                            // store(ts, wr, val);
                            let writeval = self.writeval.clone().unwrap();
                            self.sectors_manager.write(header.sector_idx, &(writeval.clone(), max_op.0 + 1, self.process_identifier)).await;
                            // trigger < sbeb, Broadcast | [WRITE_PROC, rid, maxts + 1, rank(self), writeval] >;
                            let cmd_to_send = Arc::new(SystemRegisterCommand {
                                header: SystemCommandHeader {
                                    process_identifier: self.process_identifier,
                                    msg_ident: header.msg_ident,
                                    read_ident: header.read_ident,
                                    sector_idx: header.sector_idx,
                                },
                                content: SystemRegisterCommandContent::WriteProc {
                                    timestamp: max_op.0 + 1,
                                    write_rank: self.process_identifier,
                                    data_to_write: writeval,
                                },
                            });
                            self.register_client.broadcast(Broadcast {
                                cmd: cmd_to_send,
                            }).await;
                        }
                    }
                }

                SystemRegisterCommand {
                    header,
                    content: SystemRegisterCommandContent::Ack
                } => {
                    // event < sl, Deliver | q, [ACK, r] > such that r == rid and write_phase do
                    if header.read_ident != self.rid {
                        log::debug!("Atomic register received ACK with incorrect rid!");
                        return;
                    }
                    if !self.write_phase {
                        log::debug!("Atomic register received ACK command while not being in write_phase!");
                        return;
                    }
                    // acklist[q] := Ack;
                    self.acklist[usize::from(header.process_identifier) - 1] = true;
                    // if #(acklist) > N / 2 and (reading or writing) then
                    if self.acklist.iter().filter(|&x| *x).count() > self.processes_count / 2 && (self.reading || self.writing) {
                        // acklist := [ _ ] `of length` N;
                        self.acklist.iter_mut().for_each(|x| *x = false);
                        // write_phase := FALSE;    
                        self.write_phase = false;
                        // if reading = TRUE then
                        log::debug!("Request {} got enough acks!", header.read_ident);
                        if self.reading {
                            log::debug!("Request {} reading {}!", header.read_ident, self.reading);
                            // reading := FALSE;
                            self.reading = false;
                            // trigger < nnar, ReadReturn | readval >;
                            let op_result = OperationComplete {
                                status_code: StatusCode::Ok,
                                request_identifier: self.request_identifier.take().unwrap(),
                                op_return: OperationReturn::Read(ReadReturn { read_data: self.readval.clone() }),
                            };
                            if let Some(complete_callback) = self.operation_complete.take() {
                                complete_callback(op_result).await;
                            }
                        } else {
                            log::debug!("Request {} writing {}!", header.read_ident, self.writing);
                            // else
                            // writing := FALSE;
                            self.writing = false;
                            // trigger < nnar, WriteReturn >;
                            let op_result = OperationComplete {
                                status_code: StatusCode::Ok,
                                request_identifier: self.request_identifier.take().unwrap(),
                                op_return: OperationReturn::Write,
                            };
                            log::debug!("Request {} calling callback!", header.read_ident);
                            if let Some(complete_callback) = self.operation_complete.take() {
                                log::debug!("Request {} found callback!", header.read_ident);
                                complete_callback(op_result).await;
                            }
                        }
                    }
                }
            }
        }
    }
}