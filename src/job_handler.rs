pub mod jobs {
    use std::collections::{HashMap, VecDeque};
    use std::convert::TryInto;
    use std::sync::Arc;

    use crate::{AtomicRegister, ClientRegisterCommand, OperationComplete, OperationReturn, ReadReturn, RegisterCommand, StatusCode, SystemRegisterCommand, SystemRegisterCommandContent};
    use crate::executors::{Handler, ModuleRef, System};
    use crate::network::netdata::{ClientBareReply, ClientContentReply, ClientReply, CommandID, MsgHeader};
    use crate::network::network::{Accept, ListeningSocket, RecvStream, SendStream, StreamRequest, StreamResponse};

    pub struct RegisterWorker {
        register_id: usize,
        register: Box<dyn AtomicRegister>,
        job_handler: ModuleRef<JobHandler>,
        current_job: Arc<tokio::sync::Mutex<Option<u64>>>,
        is_done: Arc<tokio::sync::Mutex<bool>>,
    }

    enum RegisterMsg {
        HandleClientCommand(ClientRegisterCommand, ModuleRef<SendStream>),
        HandleSystemCommand(SystemRegisterCommand),
    }

    #[async_trait::async_trait]
    impl Handler<RegisterMsg> for RegisterWorker {
        async fn handle(&mut self, msg: RegisterMsg) {
            match msg {
                RegisterMsg::HandleClientCommand(client_command, stream) => {
                    let is_done_ref = self.is_done.clone();
                    let current_job_ref = self.current_job.clone();
                    let job_handler_ref = self.job_handler.clone();
                    let register_id = self.register_id;
                    let sector_id = client_command.header.sector_idx;
                    {
                        let mut current_job_guard = self.current_job.lock().await;
                        *current_job_guard = Some(client_command.header.sector_idx);
                    }
                    self.register.client_command(client_command, Box::new(move |operation_complete: OperationComplete| {
                        Box::pin(async move {
                            log::debug!("Performing callback for operation {}", operation_complete.request_identifier);
                            let response: ClientReply = match operation_complete.op_return {
                                OperationReturn::Read(ReadReturn { read_data: Some(data) }) => {
                                    ClientReply::WithContent(ClientContentReply::new(
                                        MsgHeader::new_with_extra(CommandID::ClientReadReply, operation_complete.status_code as u8),
                                        operation_complete.request_identifier,
                                        (*data.0).try_into().unwrap(),
                                    ))
                                }
                                OperationReturn::Read(ReadReturn { read_data: None }) => {
                                    panic!("Atomic register return no data for read! Request id: {}", operation_complete.request_identifier)
                                }
                                OperationReturn::Write => {
                                    ClientReply::Bare(ClientBareReply::new(
                                        MsgHeader::new_with_extra(CommandID::ClientWriteReply, operation_complete.status_code as u8),
                                        operation_complete.request_identifier,
                                    ))
                                }
                            };
                            stream.send(response).await;
                            {
                                let mut current_job_guard = current_job_ref.lock().await;
                                *current_job_guard = None;
                            }
                            *is_done_ref.lock().await = true;
                            job_handler_ref.send(JobMsg::ReadyForClientMsg(register_id, sector_id)).await;
                        })
                    })).await;
                    self.job_handler.send(JobMsg::ReadyForSystemMsg(self.register_id, true)).await;
                }
                RegisterMsg::HandleSystemCommand(sys_command) => {
                    self.register.system_command(sys_command).await;
                    let is_done = {
                        let mut is_done_guard = self.is_done.lock().await;
                        let is_done_temp = *is_done_guard;
                        *is_done_guard = false;
                        is_done_temp
                    };
                    if !is_done {
                        self.job_handler.send(JobMsg::ReadyForSystemMsg(self.register_id, false)).await;
                    }
                }
            }
        }
    }

    pub struct JobHandler {
        proc_id: u8,
        register_workers: Vec<ModuleRef<RegisterWorker>>,
        listening_module: ModuleRef<ListeningSocket>,
        queued_client_streams: VecDeque<(ModuleRef<RecvStream>, ModuleRef<SendStream>)>,
        queued_system_streams: VecDeque<(ModuleRef<RecvStream>, ModuleRef<SendStream>)>,
        queued_system_jobs: VecDeque<SystemRegisterCommand>,
        system: Arc<tokio::sync::Mutex<System>>,
        scheduled_system_jobs: Vec<usize>,
        is_register_client_handling: Vec<bool>,
        pending_client_commands_sectors: HashMap<crate::SectorIdx, usize>,
        queued_client_commands_dedicated: Vec<VecDeque<(ClientRegisterCommand, ModuleRef<RecvStream>, ModuleRef<SendStream>)>>,
        queued_client_commands: VecDeque<(ClientRegisterCommand, ModuleRef<RecvStream>, ModuleRef<SendStream>)>,
        max_sector: u64,
        max_proc: u8,
    }

    pub enum JobMsg {
        Init(Vec<ModuleRef<RegisterWorker>>, ModuleRef<JobHandler>),
        ReadyForClientMsg(usize, crate::SectorIdx),
        ReadyForSystemMsg(usize, bool),
    }

    impl JobHandler {
        pub async fn init(proc_id: u8,
                          system: System,
                          atomic_registers: Vec<Box<dyn AtomicRegister>>,
                          max_sector: u64,
                          listening_module: ModuleRef<ListeningSocket>,
                          max_proc: u8) -> ModuleRef<JobHandler> {
            let registers_count = atomic_registers.len();
            let system = Arc::new(tokio::sync::Mutex::new(system));
            let mut system_guard = system.lock().await;
            let job_handler = JobHandler {
                proc_id,
                register_workers: vec![],
                listening_module,
                queued_client_streams: VecDeque::new(),
                queued_system_streams: VecDeque::new(),
                queued_system_jobs: VecDeque::new(),
                system: system.clone(),
                is_register_client_handling: vec![false; registers_count],
                pending_client_commands_sectors: HashMap::new(),
                queued_client_commands_dedicated: vec![VecDeque::new(); registers_count],
                queued_client_commands: VecDeque::new(),
                max_sector,
                scheduled_system_jobs: vec![0; registers_count],
                max_proc,
            };
            let job_handler_module = system_guard.register_module(job_handler).await;

            let mut register_modules = vec![];
            let mut register_id = 0;
            for atomic_register in atomic_registers.into_iter() {
                let register_worker = RegisterWorker {
                    register_id,
                    register: atomic_register,
                    job_handler: job_handler_module.clone(),
                    is_done: Arc::new(tokio::sync::Mutex::new(false)),
                    current_job: Arc::new(tokio::sync::Mutex::new(None)),
                };
                let register_module = system_guard.register_module(register_worker).await;
                register_modules.push(register_module);
                register_id += 1;
            };

            job_handler_module.send(JobMsg::Init(register_modules, job_handler_module.clone())).await;
            job_handler_module
        }

        async fn dispatch_client_task(&mut self, client_cmd: ClientRegisterCommand, recv_stream: ModuleRef<RecvStream>, send_stream: ModuleRef<SendStream>) {
            if let Some(register_id) = self.pending_client_commands_sectors.get(&client_cmd.header.sector_idx) {
                log::debug!("{}: Enqueueing client_cmd {:?} with req id {} to register_id {}", self.proc_id, client_cmd.header, client_cmd.header.request_identifier, register_id);
                self.queued_client_commands_dedicated[*register_id].push_back((client_cmd, recv_stream, send_stream));
            } else {
                for (register_id, is_handling) in self.is_register_client_handling.iter().enumerate() {
                    if !is_handling {
                        self.pending_client_commands_sectors.insert(client_cmd.header.sector_idx.clone(), register_id);
                        self.is_register_client_handling[register_id] = true;
                        recv_stream.send(StreamRequest::Read(recv_stream.clone(), send_stream.clone())).await;
                        log::debug!("{}: Dispatching client_cmd {:?} with req id {} to register_id {}", self.proc_id, client_cmd.header, client_cmd.header.request_identifier, register_id);
                        self.register_workers[register_id].send(RegisterMsg::HandleClientCommand(client_cmd, send_stream)).await;
                        return;
                    }
                }
                // No register is free, enqueue
                log::debug!("{}: Enqueueing client_cmd {:?}", self.proc_id, client_cmd.header);
                self.queued_client_commands.push_back((client_cmd, recv_stream, send_stream));
            }
        }
    }

    #[async_trait::async_trait]
    impl Handler<JobMsg> for JobHandler {
        async fn handle(&mut self, msg: JobMsg) {
            match msg {
                JobMsg::Init(register_modules, job_handler) => {
                    self.register_workers = register_modules;
                    self.listening_module.send(job_handler).await;
                    self.listening_module.send(Accept {}).await;
                }
                JobMsg::ReadyForClientMsg(register_id, sector_id) => {
                    log::debug!("{}: ReadyForClientMsg {}", self.proc_id, register_id);
                    self.scheduled_system_jobs[register_id] -= 1;
                    log::debug!("{}: Register {} finished job", self.proc_id, register_id);
                    if let Some((cmd, recv_stream, send_stream)) = self.queued_client_commands_dedicated[register_id].pop_front() {
                        // Claim dedicated message
                        log::debug!("{}: Register {} claims dedicated command id {:?} with req id {}", self.proc_id, register_id, cmd, cmd.header.request_identifier);
                        self.register_workers[register_id].send(RegisterMsg::HandleClientCommand(cmd, send_stream.clone())).await;
                        self.queued_client_streams.push_back((recv_stream, send_stream));
                    } else if let Some((cmd, recv_stream, send_stream)) = self.queued_client_commands.pop_front() {
                        log::debug!("{}: Register {} claims command id {:?} with req id {}", self.proc_id, register_id, cmd, cmd.header.request_identifier);
                        self.pending_client_commands_sectors.remove(&sector_id);
                        self.is_register_client_handling[register_id] = true;
                        self.pending_client_commands_sectors.insert(cmd.header.sector_idx, register_id);
                        self.register_workers[register_id].send(RegisterMsg::HandleClientCommand(cmd, send_stream.clone())).await;
                        self.queued_client_streams.push_back((recv_stream, send_stream));
                    } else {
                        log::debug!("{}: No client cmd enqueued. Waiting", self.proc_id);
                        self.pending_client_commands_sectors.remove(&sector_id);
                        self.is_register_client_handling[register_id] = false;
                        while let Some((recv_stream, send_stream)) = self.queued_client_streams.pop_front() {
                            recv_stream.send(StreamRequest::Read(recv_stream.clone(), send_stream)).await;
                        }
                    }
                }
                JobMsg::ReadyForSystemMsg(register_id, from_client) => {
                    if !from_client {
                        self.scheduled_system_jobs[register_id] -= 1; // todo may be optimized
                    }
                    log::debug!("{}: Register {} reports {} in queue", self.proc_id, register_id, self.scheduled_system_jobs[register_id]);
                    if self.scheduled_system_jobs[register_id] == 0 {
                        log::debug!("{}: Register {} has no enqueued system commands", self.proc_id, register_id);
                        if let Some(system_cmd) = self.queued_system_jobs.pop_front() {
                            self.scheduled_system_jobs[register_id] += 1;
                            log::debug!("{}: Register {} claims system command {:?}", self.proc_id, register_id, system_cmd);
                            self.register_workers[register_id].send(RegisterMsg::HandleSystemCommand(system_cmd)).await;
                        } else {
                            log::debug!("{}: No enqueued system commands. Awaiting for new system commands", self.proc_id);
                            while let Some((recv_stream, send_stream)) = self.queued_system_streams.pop_front() {
                                recv_stream.send(StreamRequest::Read(recv_stream.clone(), send_stream)).await;
                            }
                        }
                    }
                }
            }
        }
    }

    #[async_trait::async_trait]
    impl Handler<(RecvStream, SendStream)> for JobHandler {
        async fn handle(&mut self, (recv_stream, send_stream): (RecvStream, SendStream)) {
            let mut system_lock = self.system.lock().await;
            let recv_module = system_lock.register_module(recv_stream).await;
            let send_module = system_lock.register_module(send_stream).await;
            drop(system_lock);
            // As we don't know if it's client or system stream we push it to general queue
            recv_module.send(StreamRequest::Read(recv_module.clone(), send_module)).await;
            self.listening_module.send(Accept {}).await;
        }
    }

    #[async_trait::async_trait]
    impl Handler<StreamResponse> for JobHandler {
        async fn handle(&mut self, msg: StreamResponse) {
            match msg {
                StreamResponse::Return(recv_stream, send_stream, (RegisterCommand::System(system_cmd), is_signed)) => {
                    if is_signed && system_cmd.header.process_identifier <= self.max_proc {
                        // Check if system command must be sent to exact register
                        let mut target: Option<usize> = None;
                        // If there is register waiting for message -> send to it
                        match system_cmd.content {
                            SystemRegisterCommandContent::ReadProc => {}
                            SystemRegisterCommandContent::Value { .. } => {
                                if let Some(register_id) = self.pending_client_commands_sectors.get(&system_cmd.header.sector_idx) {
                                    log::debug!("{}: Dispatching command {:?} to register {}", self.proc_id, system_cmd, *register_id);
                                    target = Some(*register_id);
                                } else {
                                    // Received response to non existing client task - discard and ask for next
                                    log::debug!("{}: Received unexpected message VALUE to message with sector id {}", self.proc_id, system_cmd.header.sector_idx.to_string());
                                    recv_stream.send(StreamRequest::Read(recv_stream.clone(), send_stream)).await;
                                    return;
                                }
                            }
                            SystemRegisterCommandContent::WriteProc { .. } => {}
                            SystemRegisterCommandContent::Ack => {
                                if let Some(register_id) = self.pending_client_commands_sectors.get(&system_cmd.header.sector_idx) {
                                    log::debug!("{}: Dispatching command {:?} to register {}", self.proc_id, system_cmd, *register_id);
                                    target = Some(*register_id);
                                } else {
                                    // Received response to non existing client task - discard and ask for next
                                    log::debug!("{}: Received unexpected message ACK to message with sector id {}", self.proc_id, system_cmd.header.sector_idx.to_string());
                                    recv_stream.send(StreamRequest::Read(recv_stream.clone(), send_stream)).await;
                                    return;
                                }
                            }
                        };
                        // Otherwise claim free register
                        let mut free_registers: usize = 0;
                        if target.is_none() {
                            for (register_id, job_amount) in self.scheduled_system_jobs.iter_mut().enumerate() {
                                if *job_amount == 0 {
                                    if target.is_none() {
                                        target = Some(register_id);
                                        log::debug!("{}: Dispatching command {:?} to register {}", self.proc_id, system_cmd, register_id);
                                    } else {
                                        free_registers += 1;
                                    }
                                }
                            }
                        }
                        match target {
                            None => {
                                // Otherwise put to the queue
                                log::debug!("{}: Dispatching command {:?} to queue", self.proc_id, system_cmd);
                                self.queued_system_jobs.push_back(system_cmd);
                            }
                            Some(register_id) => {
                                self.scheduled_system_jobs[register_id] += 1;
                                self.register_workers[register_id].send(RegisterMsg::HandleSystemCommand(system_cmd)).await;
                                if free_registers > 0 {
                                    recv_stream.send(StreamRequest::Read(recv_stream.clone(), send_stream)).await;
                                    while let Some((recv_stream, send_stream)) = self.queued_system_streams.pop_front() {
                                        recv_stream.send(StreamRequest::Read(recv_stream.clone(), send_stream)).await;
                                    }
                                } else {
                                    self.queued_system_streams.push_back((recv_stream, send_stream));
                                }
                            }
                        }
                    } else {
                        if !is_signed {
                            log::warn!("{}: Received message with invalid HMAC from process with ID {}", self.proc_id, system_cmd.header.process_identifier);
                            recv_stream.send(StreamRequest::Read(recv_stream.clone(), send_stream)).await;
                        } else {
                            log::warn!("{}: Received message with invalid proc_id  {}", self.proc_id, system_cmd.header.process_identifier);
                            recv_stream.send(StreamRequest::Read(recv_stream.clone(), send_stream)).await;
                        }
                    }
                }
                StreamResponse::Return(recv_stream, send_stream, (RegisterCommand::Client(client_cmd), is_signed)) => {
                    // CLIENT COMMAND
                    // Check if signed
                    if is_signed {
                        if client_cmd.header.sector_idx >= self.max_sector {
                            // Invalid sector
                            let response = ClientReply::Bare(ClientBareReply::new(
                                MsgHeader::new_with_extra(CommandID::ClientReadReply, StatusCode::InvalidSectorIndex as u8),
                                client_cmd.header.request_identifier,
                            ));
                            send_stream.send(response).await;
                            recv_stream.send(StreamRequest::Read(recv_stream.clone(), send_stream.clone())).await;
                        } else {
                            // Ok
                            self.dispatch_client_task(client_cmd, recv_stream, send_stream).await;
                        }
                    } else {
                        // Invalid htag
                        let response = ClientReply::Bare(ClientBareReply::new(
                            MsgHeader::new_with_extra(CommandID::ClientReadReply, StatusCode::AuthFailure as u8),
                            client_cmd.header.request_identifier,
                        ));
                        send_stream.send(response).await;
                        recv_stream.send(StreamRequest::Read(recv_stream.clone(), send_stream.clone())).await;
                    }
                }
                StreamResponse::ReadFail(recv_stream, send_stream) => {
                    let mut system_guard = self.system.lock().await;
                    system_guard.deregister_module(&recv_stream).await;
                    system_guard.deregister_module(&send_stream).await;
                }
            }
        }
    }
}