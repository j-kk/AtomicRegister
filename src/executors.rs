use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_channel::{Receiver, Sender, unbounded};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub trait Message: Send + 'static {}

impl<T: Send + 'static> Message for T {}

/// A trait for modules capable of handling messages of type `M`.
#[async_trait::async_trait]
pub trait Handler<M: Message>
    where
        M: Message,
{
    /// Handles the message.
    async fn handle(&mut self, msg: M);
}

/// The message sent as a result of calling `System::request_tick()`.
#[derive(Debug, Clone)]
pub struct Tick {}

// You can add fields to this struct.
pub struct System {
    is_closed: AtomicBool,
    tasks: HashMap<Uuid, Vec<JoinHandle<()>>>,
    to_close: HashMap<Uuid, (Box<dyn Closeable>, Box<dyn Closeable>)>,
}

#[allow(dead_code)]
impl System {
    /// Schedules a `Tick` message to be sent to the given module periodically
    /// with the given interval. The first tick is sent immediately.
    pub async fn request_tick<T: Handler<Tick> + Send>(
        &mut self,
        requester: &ModuleRef<T>,
        delay: Duration,
    ) {
        if !self.tasks.contains_key(&requester.uuid) {
            log::error!("Tried to request tick on disabled module!");
        }
        let tick_sender = requester.tick_sender.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(delay);
            loop {
                interval.tick().await;
                let result = tick_sender.send(Box::new(Tick {})).await;
                if result.is_err() {
                    return;
                }
            }
        });
        self.tasks.get_mut(&requester.uuid).unwrap().push(task);
    }

    /// Registers the module in the system.
    /// Returns a `ModuleRef`, which can be used then to send messages to the module.
    pub async fn register_module<T: Send + 'static>(&mut self, module: T) -> ModuleRef<T> {
        if self.is_closed.load(Ordering::Relaxed) {
            panic!("System is closed!")
        }
        let (msg_sender, msg_receiver): (Sender<Box<dyn Handlee<T>>>, Receiver<Box<dyn Handlee<T>>>)
            = unbounded();
        let (tick_sender, tick_receiver): (Sender<Box<dyn Handlee<T>>>, Receiver<Box<dyn Handlee<T>>>) = unbounded();

        let msg_module: Arc<Mutex<T>> = Arc::new(Mutex::new(module));
        let tick_module: Arc<Mutex<T>> = Arc::clone(&msg_module);
        let module_uuid = Uuid::new_v4();
        self.to_close.insert(module_uuid, (Box::new(msg_receiver.clone()), Box::new(tick_receiver.clone())));

        let msg_task = tokio::spawn(async move {
            loop {
                if msg_receiver.is_closed() { return; }
                let message = msg_receiver.recv().await;
                match message {
                    Ok(message) => {
                        let mut module_lock = msg_module.lock().await;
                        let mut claimed_module = module_lock.deref_mut();
                        message.get_handled(&mut claimed_module).await
                    }
                    Err(_) => return
                }
            }
        });

        let tick_task = tokio::spawn(async move {
            loop {
                if tick_receiver.is_closed() { return; }
                let tick_result = tick_receiver.recv().await;
                match tick_result {
                    Ok(tick) => {
                        let mut module_lock = tick_module.lock().await;
                        let mut claimed_module = module_lock.deref_mut();
                        tick.get_handled(&mut claimed_module).await
                    }
                    Err(_) => return
                }
            }
        });
        let tasks = vec![msg_task, tick_task];
        self.tasks.insert(module_uuid, tasks);

        return ModuleRef {
            msg_sender,
            tick_sender,
            uuid: module_uuid,
        };
    }
    /// Deregisters module. Does nothing if module is already deregistered
    pub async fn deregister_module<T: Send + 'static>(&mut self, module: &ModuleRef<T>) {
        if let Some((channel_msg, channel_tick)) = self.to_close.remove(&module.uuid) {
            channel_msg.close().await;
            channel_tick.close().await;
        }
        if let Some(mut tasks) = self.tasks.remove(&module.uuid) {
            for task in tasks.iter_mut() {
                match task.await {
                    Ok(_) => (),
                    Err(error) => log::debug!("An error occurred when joining task: {:?}", error)
                }
            }
        }
    }

    /// Creates and starts a new instance of the system.
    pub async fn new() -> Self {
        return System {
            is_closed: AtomicBool::new(false),
            tasks: HashMap::new(),
            to_close: HashMap::new(),
        };
    }

    /// Gracefully shuts the system down.
    pub async fn shutdown(&mut self) {
        self.is_closed.store(true, Ordering::Relaxed);
        for (_, (channel_msg, channel_tick)) in self.to_close.iter() {
            channel_msg.close().await;
            channel_tick.close().await;
        }

        for (_, tasks) in self.tasks.iter_mut() {
            for task in tasks.iter_mut() {
                match task.await {
                    Ok(_) => (),
                    Err(error) => log::debug!("An error occurred when joining task: {:?}", error)
                }
            }
        }
    }
}

#[async_trait::async_trait]
trait Closeable: Send + Sync + 'static
{
    async fn close(&self) -> bool;
}

#[async_trait::async_trait]
impl<T> Closeable for Receiver<T>
    where
        T: Send + 'static,
{
    async fn close(&self) -> bool {
        self.close()
    }
}

#[async_trait::async_trait]
trait Handlee<T>: Send + 'static
    where
        T: Send,
{
    async fn get_handled(self: Box<Self>, module: &mut T);
}

#[async_trait::async_trait]
impl<M, T> Handlee<T> for M
    where
        T: Handler<M> + Send,
        M: Message,
{
    async fn get_handled(self: Box<Self>, module: &mut T) {
        module.handle(*self).await
    }
}

/// A reference to a module used for sending messages.
pub struct ModuleRef<T: Send + 'static> {
    msg_sender: Sender<Box<dyn Handlee<T>>>,
    tick_sender: Sender<Box<dyn Handlee<T>>>,
    uuid: Uuid,
}

impl<T: Send> ModuleRef<T> {
    /// Sends the message to the module.
    pub async fn send<M: Message>(&self, msg: M)
        where
            T: Handler<M>,
    {
        let _ = self.msg_sender.send(Box::new(msg)).await;
    }
}

impl<T: Send> Clone for ModuleRef<T> {
    /// Creates a new reference to the same module.
    fn clone(&self) -> Self {
        ModuleRef {
            msg_sender: self.msg_sender.clone(),
            tick_sender: self.tick_sender.clone(),
            uuid: self.uuid.clone()
        }
    }
}
