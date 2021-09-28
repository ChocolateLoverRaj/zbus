use async_broadcast::{broadcast, InactiveReceiver, Sender as Broadcaster};
use async_io::Timer;
use async_lock::Mutex;
use async_recursion::async_recursion;
use async_task::Task;
use event_listener::Event;
use futures_core::{future::BoxFuture, stream};
use futures_util::{future::select, stream::StreamExt, TryStreamExt};
use once_cell::sync::OnceCell;
use slotmap::{new_key_type, SlotMap};
use static_assertions::assert_impl_all;
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    pin::Pin,
    sync::{Arc, Mutex as SyncMutex, RwLock},
    task::{Context, Poll},
    time::Duration,
};

use zbus_names::{
    BusName, InterfaceName, MemberName, OwnedUniqueName, OwnedWellKnownName, UniqueName,
    WellKnownName,
};
use zvariant::{ObjectPath, Optional, OwnedValue, Value};

use crate::{
    fdo::{self, IntrospectableProxy, PropertiesProxy},
    Connection, Error, Message, MessageStream, MessageType, ProxyBuilder, Result,
};

type SignalHandler = Box<dyn for<'msg> FnMut(&'msg Message) -> BoxFuture<'msg, ()> + Send>;

new_key_type! {
    /// The ID for a registered signal handler.
    pub struct SignalHandlerId;
}

assert_impl_all!(SignalHandlerId: Send, Sync, Unpin);

struct SignalHandlerInfo {
    signal_name: MemberName<'static>,
    handler: SignalHandler,
}

type PropertyChangedEvent = Arc<(String, Option<OwnedValue>)>;

type PropertyChangedHandler =
    Box<dyn for<'v> FnMut(Option<&'v Value<'_>>) -> BoxFuture<'v, ()> + Send>;

new_key_type! {
    /// The ID for a registered proprety changed handler.
    pub struct PropertyChangedHandlerId;
}

pub(crate) struct PropertyChangedHandlerInfo {
    property_name: &'static str,
    handler: PropertyChangedHandler,
}

// Hold proxy properties related data.
pub(crate) struct ProxyProperties<'a> {
    pub(crate) proxy: OnceCell<PropertiesProxy<'a>>,
    pub(crate) values: SyncMutex<HashMap<String, OwnedValue>>,
    task: OnceCell<Task<()>>,
    pub(crate) changed_handlers:
        Mutex<SlotMap<PropertyChangedHandlerId, PropertyChangedHandlerInfo>>,
    broadcaster: Broadcaster<PropertyChangedEvent>,
    receiver: InactiveReceiver<PropertyChangedEvent>,
}

impl<'a> std::fmt::Debug for ProxyProperties<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyProperties")
            .field("values", &self.values)
            .finish_non_exhaustive()
    }
}

/// A client-side interface proxy.
///
/// A `Proxy` is a helper to interact with an interface on a remote object.
///
/// # Example
///
/// ```
/// use std::result::Result;
/// use std::error::Error;
/// use async_io::block_on;
/// use zbus::{Connection, Proxy};
///
/// fn main() -> Result<(), Box<dyn Error>> {
///     block_on(run())
/// }
///
/// async fn run() -> Result<(), Box<dyn Error>> {
///     let connection = Connection::session().await?;
///     let p = Proxy::new(
///         &connection,
///         "org.freedesktop.DBus",
///         "/org/freedesktop/DBus",
///         "org.freedesktop.DBus",
///     ).await?;
///     // owned return value
///     let _id: String = p.call("GetId", &()).await?;
///     // borrowed return value
///     let _id: &str = p.call_method("GetId", &()).await?.body()?;
///
///     Ok(())
/// }
/// ```
///
/// # Note
///
/// It is recommended to use the [`dbus_proxy`] macro, which provides a more convenient and
/// type-safe *façade* `Proxy` derived from a Rust trait.
///
/// ## Current limitations:
///
/// At the moment, `Proxy` doesn't:
///
/// * cache properties
/// * track the current name owner
/// * prevent auto-launching
///
/// [`futures` crate]: https://crates.io/crates/futures
/// [`dbus_proxy`]: attr.dbus_proxy.html
#[derive(Clone, Debug)]
pub struct Proxy<'a> {
    pub(crate) inner: Arc<ProxyInner<'a>>,
    // Use a 'static as we can't self-reference ProxyInner fields
    // eventually, we could make destination/path inside an Arc
    // but then we would have other issues with async 'static closures
    pub(crate) properties: Arc<ProxyProperties<'static>>,
}

assert_impl_all!(Proxy<'_>: Send, Sync, Unpin);

#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub(crate) struct ProxyInner<'a> {
    #[derivative(Debug = "ignore")]
    pub(crate) conn: Connection,
    pub(crate) destination: BusName<'a>,
    pub(crate) path: ObjectPath<'a>,
    pub(crate) interface: InterfaceName<'a>,
    // Keep it in an Arc so that dest_name_update_task can keep its own ref to it.
    dest_unique_name: Arc<RwLock<Option<OwnedUniqueName>>>,
    #[derivative(Debug = "ignore")]
    // Keep it in an Arc so that sign_handler_task can keep its own ref to it.
    sig_handlers: Arc<Mutex<SlotMap<SignalHandlerId, SignalHandlerInfo>>>,
    sig_handler_task: OnceCell<Task<()>>,
    #[derivative(Debug = "ignore")]
    signal_msg_stream: OnceCell<Mutex<MessageStream>>,
    dest_name_update_task: OnceCell<Task<()>>,
    dest_name_update_event: Arc<Event>,
}

pub struct PropertyStream<'a, T> {
    name: &'a str,
    stream: stream::BoxStream<'static, PropertyChangedEvent>,
    phantom: std::marker::PhantomData<T>,
}

impl<'a, T> stream::Stream for PropertyStream<'a, T>
where
    T: TryFrom<zvariant::OwnedValue> + Unpin,
{
    type Item = Option<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let m = self.get_mut();
        let (name, stream) = (m.name, m.stream.as_mut());
        // there must be a way to simplify the following code..
        match futures_core::ready!(stream::Stream::poll_next(stream, cx)) {
            Some(item) => {
                if item.0 == name {
                    if let Some(Ok(v)) = item.1.clone().map(T::try_from) {
                        Poll::Ready(Some(Some(v)))
                    } else {
                        Poll::Ready(Some(None))
                    }
                } else {
                    Poll::Pending
                }
            }
            None => Poll::Ready(None),
        }
    }
}

impl<'a> ProxyProperties<'a> {
    pub(crate) fn new() -> Self {
        // note: do we need to make this configurable?
        let (mut sender, receiver) = broadcast(64);
        sender.set_overflow(true);
        let receiver = receiver.deactivate();

        Self {
            proxy: Default::default(),
            values: Default::default(),
            task: Default::default(),
            changed_handlers: Default::default(),
            broadcaster: sender,
            receiver,
        }
    }

    fn update_cache(&self, args: &fdo::PropertiesChangedArgs<'_>) {
        let mut values = self.values.lock().expect("lock poisoned");

        for inval in args.invalidated_properties() {
            values.remove(*inval);
        }

        for (property_name, value) in args
            .changed_properties()
            .iter()
            .map(|(k, v)| (k.to_string(), OwnedValue::from(v)))
        {
            values.insert(property_name, value);
        }
    }

    async fn changed(&self, property_name: &str, value: Option<&Value<'_>>) {
        if self.broadcaster.receiver_count() > 0 {
            // Ignore event errors.
            // TODO: We should still log in case of error when we've logging.
            let _res = self
                .broadcaster
                .broadcast(Arc::new((
                    property_name.to_string(),
                    value.map(OwnedValue::from),
                )))
                .await;
        }

        let mut handlers = self.changed_handlers.lock().await;
        for info in handlers
            .values_mut()
            .filter(|info| info.property_name == property_name)
        {
            (*info.handler)(value).await;
        }
    }
}

impl<'a> ProxyInner<'a> {
    pub(crate) fn new(
        conn: Connection,
        destination: BusName<'a>,
        path: ObjectPath<'a>,
        interface: InterfaceName<'a>,
    ) -> Self {
        Self {
            conn,
            destination,
            path,
            interface,
            dest_unique_name: Arc::new(RwLock::new(None)),
            sig_handlers: Arc::new(Mutex::new(SlotMap::with_key())),
            sig_handler_task: OnceCell::new(),
            signal_msg_stream: OnceCell::new(),
            dest_name_update_task: OnceCell::new(),
            dest_name_update_event: Arc::new(Event::new()),
        }
    }

    async fn matching_signal<'m>(&self, m: &'m Message) -> Result<Option<MemberName<'m>>> {
        if m.message_type() != MessageType::Signal {
            return Ok(None);
        }

        if m.interface() == Ok(Some(self.interface.as_ref()))
            && m.path() == Ok(Some(self.path.as_ref()))
        {
            for _ in 0..2 {
                let listener = self.dest_name_update_event.listen();
                if m.header()?.sender()?
                    == self
                        .dest_unique_name
                        .read()
                        .expect("lock poisoned")
                        .as_deref()
                {
                    return Ok(m.member().ok().flatten());
                }

                // Due to signal and task run not being orderered (see issue#190), we can easily end
                // up with handling a signal here **before** the destination's name ownership signal
                // from the bus is handled. Therefore, if all other parameters of the signal match
                // except for sender, we wait a bit for the possible name update before calling it
                // a non-match.
                select(listener, Timer::after(Duration::from_millis(100))).await;
            }
        }

        Ok(None)
    }

    /// Resolves the destination name to the associated unique connection name and watches for any changes.
    ///
    /// Typically you would want to create the [`Proxy`] with the well-known name of the destination
    /// service but signal messages only specify the unique name of the peer (except for signals
    /// from `org.freedesktop.DBus` service). This means we have no means to check the sender of
    /// the message. While in most cases this will not be a problem, it becomes a problem if you
    /// need to communicate with multiple services exposing the same interface, over the same
    /// connection. Hence the need for this method.
    ///
    /// This is only called when the user show interest in receiving a signal so that we don't end up
    /// doing all this needlessly.
    pub(crate) async fn destination_unique_name(&self) -> Result<()> {
        if !self.conn.is_bus() {
            // Names don't mean much outside the bus context.
            return Ok(());
        }

        let destination = &self.destination;
        match destination {
            BusName::Unique(name) => {
                if self
                    .dest_unique_name
                    .read()
                    .expect("lock poisoned")
                    .is_none()
                {
                    *self.dest_unique_name.write().expect("lock poisoned") =
                        Some(name.to_owned().into());
                    self.dest_name_update_event.notify(usize::MAX);
                }
            }
            BusName::WellKnown(well_known_name) => {
                if self.dest_name_update_task.get().is_some() {
                    // Already watching over the bus for any name updates so nothing to do here.
                    return Ok(());
                }

                let mut conn = self.conn.clone();
                let dest_unique_name = self.dest_unique_name.clone();
                let dest_name_update_event = self.dest_name_update_event.clone();
                let well_known_name = OwnedWellKnownName::from(well_known_name.to_owned());
                // We've to use low-level API here to avoid infinite cycles. Otherwise, we'll
                // get a complicated error from MIR, w/ this repeated multiple times:
                // note: ...which requires building MIR for `proxy::<impl at zbus/src/azync/proxy.rs:338:1: 886:2>::receive_signal`...
                let subscription_id = conn
                    .subscribe_signal(
                        "org.freedesktop.DBus",
                        "/org/freedesktop/DBus",
                        "org.freedesktop.DBus",
                        "NameOwnerChanged",
                    )
                    .await?;
                let task = self.conn.executor().spawn(async move {
                    struct Subcription<'c> {
                        id: u64,
                        conn: &'c mut Connection,
                    }
                    impl Drop for Subcription<'_> {
                        fn drop(&mut self) {
                            self.conn.queue_unsubscribe_signal(self.id);
                        }
                    }
                    let mut stream = MessageStream::from(conn.clone());
                    let _subscription = Subcription {
                        id: subscription_id,
                        conn: &mut conn,
                    };
                    while let Some(Ok(msg)) = stream.next().await {
                        let header = match msg.header() {
                            Ok(h) => h,
                            Err(_) => continue,
                        };
                        if header.sender()
                            != Ok(Some(&UniqueName::from_str_unchecked(
                                "org.freedesktop.DBus",
                            )))
                            || header.path()
                                != Ok(Some(&ObjectPath::from_str_unchecked(
                                    "/org/freedesktop/DBus",
                                )))
                            || header.interface()
                                != Ok(Some(&InterfaceName::from_str_unchecked(
                                    "org.freedesktop.DBus",
                                )))
                            || header.member()
                                != Ok(Some(&MemberName::from_str_unchecked("NameOwnerChanged")))
                        {
                            continue;
                        }
                        if let Ok((name, _, new_owner)) = msg.body::<(
                            WellKnownName<'_>,
                            Optional<UniqueName<'_>>,
                            Optional<UniqueName<'_>>,
                        )>() {
                            if name == well_known_name {
                                let unique_name = new_owner.as_ref().map(|n| n.to_owned().into());
                                *dest_unique_name.write().expect("lock poisoned") = unique_name;
                                dest_name_update_event.notify(usize::MAX);
                            }
                        }
                    }
                });
                self.dest_name_update_task
                    .set(task)
                    .expect("Attempted to set destination update task twice");

                let unique_name = match fdo::DBusProxy::new(&self.conn)
                    .await?
                    .get_name_owner(destination.as_ref())
                    .await
                {
                    // That's ok. The destination isn't available right now.
                    Err(fdo::Error::NameHasNoOwner(_)) => None,
                    res => Some(res?),
                };

                *self.dest_unique_name.write().expect("lock poisoned") = unique_name;
                self.dest_name_update_event.notify(usize::MAX);
            }
        }

        Ok(())
    }

    /// Handle the provided signal message.
    ///
    /// Call any handlers registered through the [`Self::connect_signal`] method for the provided
    /// signal message.
    ///
    /// If no errors are encountered, `Ok(true)` is returned if any handlers where found and called for,
    /// the signal; `Ok(false)` otherwise.
    pub async fn handle_signal(&self, msg: &Message) -> Result<bool> {
        let mut handlers = self.sig_handlers.lock().await;
        if handlers.is_empty() {
            return Ok(false);
        }

        let signal_name = match self.matching_signal(msg).await? {
            Some(signal) => signal,
            _ => return Ok(false),
        };

        let mut handled = false;
        for info in handlers
            .values_mut()
            .filter(|info| info.signal_name == *signal_name)
        {
            (*info.handler)(msg).await;
            handled = true;
        }

        Ok(handled)
    }
}

impl<'a> Proxy<'a> {
    /// Create a new `Proxy` for the given destination/path/interface.
    pub async fn new<D, P, I>(
        conn: &Connection,
        destination: D,
        path: P,
        interface: I,
    ) -> Result<Proxy<'a>>
    where
        D: TryInto<BusName<'a>>,
        P: TryInto<ObjectPath<'a>>,
        I: TryInto<InterfaceName<'a>>,
        D::Error: Into<Error>,
        P::Error: Into<Error>,
        I::Error: Into<Error>,
    {
        ProxyBuilder::new_bare(conn)
            .destination(destination)?
            .path(path)?
            .interface(interface)?
            .build()
            .await
    }

    /// Create a new `Proxy` for the given destination/path/interface, taking ownership of all
    /// passed arguments.
    pub async fn new_owned<D, P, I>(
        conn: Connection,
        destination: D,
        path: P,
        interface: I,
    ) -> Result<Proxy<'a>>
    where
        D: TryInto<BusName<'static>>,
        P: TryInto<ObjectPath<'static>>,
        I: TryInto<InterfaceName<'static>>,
        D::Error: Into<Error>,
        P::Error: Into<Error>,
        I::Error: Into<Error>,
    {
        ProxyBuilder::new_bare(&conn)
            .destination(destination)?
            .path(path)?
            .interface(interface)?
            .build()
            .await
    }

    /// Register a changed handler for the property named `property_name`.
    ///
    /// A unique ID for the handler is returned, which can be used to deregister this handler
    /// using [`Self::disconnect_property_changed`] method.
    ///
    /// *Note:* The signal handler will be called by the executor thread of the [`Connection`].
    /// See the [`Connection::executor`] documentation for an example of how you can run the
    /// executor (and in turn all the signal handlers called) in your own thread.
    ///
    /// # Errors
    ///
    /// The current implementation requires cached properties. It returns an [`Error::Unsupported`]
    /// if the proxy isn't setup with cache.
    pub async fn connect_property_changed<H>(
        &self,
        property_name: &'static str,
        handler: H,
    ) -> Result<PropertyChangedHandlerId>
    where
        for<'v> H: FnMut(Option<&'v Value<'_>>) -> BoxFuture<'v, ()> + Send + 'static,
    {
        if !self.has_cached_properties() {
            return Err(Error::Unsupported);
        }

        let id = self
            .properties
            .changed_handlers
            .lock()
            .await
            .insert(PropertyChangedHandlerInfo {
                property_name,
                handler: Box::new(handler),
            });
        Ok(id)
    }

    /// Deregister the property handler with the ID `handler_id`.
    ///
    /// This method returns `Ok(true)` if a handler with the id `handler_id` is found and removed;
    /// `Ok(false)` otherwise.
    pub async fn disconnect_property_changed(
        &self,
        handler_id: PropertyChangedHandlerId,
    ) -> Result<bool> {
        Ok(self
            .properties
            .changed_handlers
            .lock()
            .await
            .remove(handler_id)
            .is_some())
    }

    /// Get a reference to the associated connection.
    pub fn connection(&self) -> &Connection {
        &self.inner.conn
    }

    /// Get a reference to the destination service name.
    pub fn destination(&self) -> &BusName<'_> {
        &self.inner.destination
    }

    /// Get a reference to the object path.
    pub fn path(&self) -> &ObjectPath<'_> {
        &self.inner.path
    }

    /// Get a reference to the interface.
    pub fn interface(&self) -> &InterfaceName<'_> {
        &self.inner.interface
    }

    /// Introspect the associated object, and return the XML description.
    ///
    /// See the [xml](xml/index.html) module for parsing the result.
    pub async fn introspect(&self) -> fdo::Result<String> {
        let proxy = IntrospectableProxy::builder(&self.inner.conn)
            .destination(&self.inner.destination)?
            .path(&self.inner.path)?
            .build()
            .await?;

        proxy.introspect().await
    }

    #[async_recursion]
    async fn properties_proxy(&self) -> Result<&PropertiesProxy<'static>> {
        match self.properties.proxy.get() {
            Some(proxy) => Ok(proxy),
            None => {
                let proxy = PropertiesProxy::builder(&self.inner.conn)
                    // Safe because already checked earlier
                    .destination(self.inner.destination.to_owned())
                    .unwrap()
                    // Safe because already checked earlier
                    .path(self.inner.path.to_owned())
                    .unwrap()
                    // does not have properties and do not recurse!
                    .cache_properties(false)
                    .build()
                    .await?;
                // doesn't matter if another thread sets it before
                let _ = self.properties.proxy.set(proxy);
                // but we must have a Ok() here
                self.properties.proxy.get().ok_or_else(|| panic!())
            }
        }
    }

    pub(crate) async fn cache_properties(&self) -> Result<()> {
        let proxy = self.properties_proxy().await?;

        let mut stream = proxy.receive_properties_changed().await?;
        let properties = Arc::downgrade(&self.properties);
        let task = self.inner.conn.executor().spawn(async move {
            while let Some(changed) = stream.next().await {
                if let Ok(args) = changed.args() {
                    let properties = match properties.upgrade() {
                        Some(p) => p,
                        None => break,
                    };
                    properties.update_cache(&args);

                    for inval in args.invalidated_properties() {
                        properties.changed(inval, None).await;
                    }

                    for (property_name, value) in args.changed_properties() {
                        properties.changed(property_name, Some(value)).await;
                    }
                }
            }
        });
        self.properties.task.set(task).unwrap();

        if let Ok(values) = proxy.get_all(self.inner.interface.as_ref()).await {
            self.properties
                .values
                .lock()
                .expect("lock poisoned")
                .extend(values);
        }

        Ok(())
    }

    /// Get the cached value of the property `property_name`.
    ///
    /// This returns `None` if the property is not in the cache.  This could be because the cache
    /// was invalidated by an update, because caching was disabled for this property or proxy, or
    /// because the cache has not yet been populated.  Use `get_property` to fetch the value from
    /// the peer.
    pub fn get_cached_property<T>(&self, property_name: &str) -> fdo::Result<Option<T>>
    where
        T: TryFrom<OwnedValue>,
    {
        self.properties
            .values
            .lock()
            .expect("lock poisoned")
            .get(property_name)
            .cloned()
            .map(T::try_from)
            .transpose()
            .map_err(|_| Error::InvalidReply.into())
    }

    fn set_cached_property(&self, property_name: String, value: Option<OwnedValue>) {
        let mut values = self.properties.values.lock().expect("lock poisoned");
        if let Some(value) = value {
            values.insert(property_name, value);
        } else {
            values.remove(&property_name);
        }
    }

    async fn get_proxy_property(&self, property_name: &str) -> Result<OwnedValue> {
        Ok(self
            .properties_proxy()
            .await?
            .get(self.inner.interface.as_ref(), property_name)
            .await?)
    }

    fn has_cached_properties(&self) -> bool {
        self.properties.task.get().is_some()
    }

    /// Get the property `property_name`.
    ///
    /// Get the property value from the cache (if caching is enabled on this proxy) or call the
    /// `Get` method of the `org.freedesktop.DBus.Properties` interface.
    pub async fn get_property<T>(&self, property_name: &str) -> fdo::Result<T>
    where
        T: TryFrom<OwnedValue>,
    {
        let value = if self.has_cached_properties() {
            if let Some(value) = self.get_cached_property(property_name)? {
                return Ok(value);
            } else {
                let value = self.get_proxy_property(property_name).await?;
                self.set_cached_property(property_name.to_string(), Some(value.clone()));
                value
            }
        } else {
            self.get_proxy_property(property_name).await?
        };

        value.try_into().map_err(|_| Error::InvalidReply.into())
    }

    /// Set the property `property_name`.
    ///
    /// Effectively, call the `Set` method of the `org.freedesktop.DBus.Properties` interface.
    pub async fn set_property<'t, T: 't>(&self, property_name: &str, value: T) -> fdo::Result<()>
    where
        T: Into<Value<'t>>,
    {
        self.properties_proxy()
            .await?
            .set(self.inner.interface.as_ref(), property_name, &value.into())
            .await
    }

    /// Call a method and return the reply.
    ///
    /// Typically, you would want to use [`call`] method instead. Use this method if you need to
    /// deserialize the reply message manually (this way, you can avoid the memory
    /// allocation/copying, by deserializing the reply to an unowned type).
    ///
    /// [`call`]: struct.Proxy.html#method.call
    pub async fn call_method<'m, M, B>(&self, method_name: M, body: &B) -> Result<Arc<Message>>
    where
        M: TryInto<MemberName<'m>>,
        M::Error: Into<Error>,
        B: serde::ser::Serialize + zvariant::DynamicType,
    {
        self.inner
            .conn
            .call_method(
                Some(&self.inner.destination),
                self.inner.path.as_str(),
                Some(&self.inner.interface),
                method_name,
                body,
            )
            .await
    }

    /// Call a method and return the reply body.
    ///
    /// Use [`call_method`] instead if you need to deserialize the reply manually/separately.
    ///
    /// [`call_method`]: struct.Proxy.html#method.call_method
    pub async fn call<'m, M, B, R>(&self, method_name: M, body: &B) -> Result<R>
    where
        M: TryInto<MemberName<'m>>,
        M::Error: Into<Error>,
        B: serde::ser::Serialize + zvariant::DynamicType,
        R: serde::de::DeserializeOwned + zvariant::Type,
    {
        let reply = self.call_method(method_name, body).await?;

        Ok(reply.body()?)
    }

    /// Create a stream for signal named `signal_name`.
    ///
    /// # Errors
    ///
    /// Apart from general I/O errors that can result from socket communications, calling this
    /// method will also result in an error if the destination service has not yet registered its
    /// well-known name with the bus (assuming you're using the well-known name as destination).
    pub async fn receive_signal<M>(&self, signal_name: M) -> Result<SignalStream<'a>>
    where
        M: TryInto<MemberName<'static>>,
        M::Error: Into<Error>,
    {
        // Time to try & resolve the destination name & track changes to it.
        self.inner.destination_unique_name().await?;

        let signal_name = signal_name.try_into().map_err(Into::into)?;
        let subscription_id = if self.inner.conn.is_bus() {
            let id = self
                .inner
                .conn
                .subscribe_signal(
                    self.destination(),
                    self.path().clone(),
                    self.interface(),
                    signal_name.as_ref(),
                )
                .await?;

            Some(id)
        } else {
            None
        };

        let proxy = self.inner.clone();
        let stream = MessageStream::from(&self.inner.conn)
            .try_filter_map(move |msg| {
                let proxy = proxy.clone();
                let signal_name = signal_name.clone();

                async move {
                    Ok(if proxy.matching_signal(&msg).await? == Some(signal_name) {
                        Some(msg)
                    } else {
                        None
                    })
                }
            })
            .filter_map(|msg| async move { msg.ok() });

        Ok(SignalStream {
            stream: stream.boxed(),
            conn: self.inner.conn.clone(),
            subscription_id,
        })
    }

    /// Register a handler for signal named `signal_name`.
    ///
    /// A unique ID for the handler is returned, which can be used to deregister this handler using
    /// [`Self::disconnect_signal`] method.
    ///
    /// *Note:* The signal handler will be called by the executor thread of the [`Connection`].
    /// See the [`Connection::executor`] documentation for an example of how you can run the
    /// executor (and in turn all the signal handlers called) in your own thread.
    ///
    /// ### Errors
    ///
    /// This method can fail if addition of the relevant match rule on the bus fails. You can
    /// safely `unwrap` the `Result` if you're certain that associated connection is not a bus
    /// connection.
    pub async fn connect_signal<M, H>(
        &self,
        signal_name: M,
        handler: H,
    ) -> fdo::Result<SignalHandlerId>
    where
        M: TryInto<MemberName<'static>>,
        M::Error: Into<Error>,
        for<'msg> H: FnMut(&'msg Message) -> BoxFuture<'msg, ()> + Send + 'static,
    {
        // Ensure the stream.
        self.msg_stream().await;

        // Start the dispatch task.
        self.start_signal_dispatch_task();

        // Time to try resolve the destination name & track changes to it.
        self.inner.destination_unique_name().await?;

        let signal_name = signal_name.try_into().map_err(Into::into)?;
        let id = self
            .inner
            .sig_handlers
            .lock()
            .await
            .insert(SignalHandlerInfo {
                signal_name: signal_name.clone(),
                handler: Box::new(handler),
            });

        if self.inner.conn.is_bus() {
            let _ = self
                .inner
                .conn
                .subscribe_signal(
                    self.destination(),
                    self.path().clone(),
                    self.interface(),
                    signal_name,
                )
                .await?;
        }

        Ok(id)
    }

    /// Deregister the signal handler with the ID `handler_id`.
    ///
    /// This method returns `Ok(true)` if a handler with the id `handler_id` is found and removed;
    /// `Ok(false)` otherwise.
    ///
    /// ### Errors
    ///
    /// This method can fail if removal of the relevant match rule on the bus fails. You can
    /// safely `unwrap` the `Result` if you're certain that associated connection is not a bus
    /// connection.
    pub async fn disconnect_signal(&self, handler_id: SignalHandlerId) -> fdo::Result<bool> {
        match self.inner.sig_handlers.lock().await.remove(handler_id) {
            Some(handler_info) => {
                if self.inner.conn.is_bus() {
                    let _ = self
                        .inner
                        .conn
                        .unsubscribe_signal(
                            self.destination(),
                            self.path().clone(),
                            self.interface(),
                            handler_info.signal_name,
                        )
                        .await?;
                }

                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn msg_stream(&self) -> &Mutex<MessageStream> {
        match self.inner.signal_msg_stream.get() {
            Some(stream) => stream,
            None => {
                let stream = self.inner.conn.clone().into();
                self.inner
                    .signal_msg_stream
                    .set(Mutex::new(stream))
                    .unwrap_or_else(|_| panic!("Attempted to set stream twice"));

                // Safety: We just set it in the previous line.
                self.inner
                    .signal_msg_stream
                    .get()
                    .expect("message stream not set")
            }
        }
    }

    fn start_signal_dispatch_task(&self) {
        self.inner.sig_handler_task.get_or_init(|| {
            let inner = &self.inner;
            // Clone of inner with 'static lifetime.
            let inner = ProxyInner {
                conn: inner.conn.clone(),
                destination: inner.destination.to_owned(),
                path: inner.path.to_owned(),
                interface: inner.interface.to_owned(),
                dest_unique_name: inner.dest_unique_name.clone(),
                sig_handlers: inner.sig_handlers.clone(),
                // We'll not need the next 3 and so these will remain uninitialized.
                sig_handler_task: OnceCell::new(),
                signal_msg_stream: OnceCell::new(),
                dest_name_update_task: OnceCell::new(),
                // Won't need this either but doesn't hurt keeping the clone around.
                dest_name_update_event: inner.dest_name_update_event.clone(),
            };
            let mut stream = MessageStream::from(self.inner.conn.clone());
            self.inner.conn.executor().spawn(async move {
                // TODO: Log errors when we've logging.
                while let Some(msg) = stream.next().await.and_then(|m| m.ok()) {
                    let _ = inner.handle_signal(&msg).await;
                }
            })
        });
    }

    /// Get a stream to receive property changed events.
    ///
    /// Note that zbus doesn't queue the updates. If the listener is slower than the receiver, it
    /// will only receive the last update.
    pub async fn receive_property_stream<'n, T>(&self, name: &'n str) -> PropertyStream<'n, T> {
        PropertyStream {
            name,
            stream: self.properties.receiver.activate_cloned().boxed(),
            phantom: std::marker::PhantomData,
        }
    }
}

/// A [`stream::Stream`] implementation that yields signal [messages](`Message`).
///
/// Use [`Proxy::receive_signal`] to create an instance of this type.
#[derive(derivative::Derivative)]
#[derivative(Debug)]
pub struct SignalStream<'s> {
    #[derivative(Debug = "ignore")]
    stream: stream::BoxStream<'s, Arc<Message>>,
    conn: Connection,
    subscription_id: Option<u64>,
}

assert_impl_all!(SignalStream<'_>: Send, Unpin);

impl stream::Stream for SignalStream<'_> {
    type Item = Arc<Message>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        stream::Stream::poll_next(self.get_mut().stream.as_mut(), cx)
    }
}

impl std::ops::Drop for SignalStream<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.subscription_id.take() {
            self.conn.queue_unsubscribe_signal(id);
        }
    }
}

impl<'a> From<crate::blocking::Proxy<'a>> for Proxy<'a> {
    fn from(proxy: crate::blocking::Proxy<'a>) -> Self {
        proxy.into_inner()
    }
}

#[cfg(test)]
mod tests {
    use event_listener::Event;
    use zbus_names::UniqueName;

    use super::*;
    use async_io::block_on;
    use futures_util::{future::FutureExt, join};
    use ntest::timeout;
    use std::{future::ready, sync::Arc};
    use test_env_log::test;
    use zvariant::Optional;

    #[test]
    #[timeout(15000)]
    fn signal_stream() {
        block_on(test_signal_stream()).unwrap();
    }

    async fn test_signal_stream() -> Result<()> {
        // Register a well-known name with the session bus and ensure we get the appropriate
        // signals called for that.
        let conn = Connection::session().await?;
        let unique_name = conn.unique_name().unwrap();

        let proxy = fdo::DBusProxy::new(&conn).await?;

        let well_known = "org.freedesktop.zbus.async.ProxySignalStreamTest";
        let owner_changed_stream = proxy
            .receive_signal("NameOwnerChanged")
            .await?
            .filter(|msg| {
                if let Ok((name, _, new_owner)) = msg.body::<(
                    BusName<'_>,
                    Optional<UniqueName<'_>>,
                    Optional<UniqueName<'_>>,
                )>() {
                    ready(match &*new_owner {
                        Some(new_owner) => *new_owner == *unique_name && name == well_known,
                        None => false,
                    })
                } else {
                    ready(false)
                }
            });

        let name_acquired_stream = proxy.receive_signal("NameAcquired").await?.filter(|msg| {
            if let Ok(name) = msg.body::<BusName<'_>>() {
                return ready(name == well_known);
            }

            ready(false)
        });

        let _prop_stream =
            proxy
                .receive_property_stream("SomeProp")
                .await
                .filter(|v: &Option<u32>| {
                    dbg!(v);
                    ready(false)
                });

        let reply = proxy
            .request_name(
                well_known.try_into()?,
                fdo::RequestNameFlags::ReplaceExisting.into(),
            )
            .await?;
        assert_eq!(reply, fdo::RequestNameReply::PrimaryOwner);

        let (changed_signal, acquired_signal) = futures_util::join!(
            owner_changed_stream.into_future(),
            name_acquired_stream.into_future(),
        );

        let changed_signal = changed_signal.0.unwrap();
        let (acquired_name, _, new_owner) = changed_signal
            .body::<(
                BusName<'_>,
                Optional<UniqueName<'_>>,
                Optional<UniqueName<'_>>,
            )>()
            .unwrap();
        assert_eq!(acquired_name, well_known);
        assert_eq!(*new_owner.as_ref().unwrap(), *unique_name);

        let acquired_signal = acquired_signal.0.unwrap();
        assert_eq!(acquired_signal.body::<&str>().unwrap(), well_known);

        Ok(())
    }

    #[test]
    #[timeout(15000)]
    fn signal_connect() {
        block_on(test_signal_connect()).unwrap();
    }

    async fn test_signal_connect() -> Result<()> {
        // Register a well-known name with the session bus and ensure we get the appropriate
        // signals called for that.
        let conn = Connection::session().await?;

        let owner_change_signaled = Arc::new(Event::new());
        let owner_change_listener = owner_change_signaled.listen();

        let name_acquired_signaled = Arc::new(Event::new());
        let name_acquired_listener = name_acquired_signaled.listen();

        let name_acquired_signaled2 = Arc::new(Event::new());
        let name_acquired_listener2 = name_acquired_signaled2.listen();

        let proxy = fdo::DBusProxy::new(&conn).await?;
        let well_known = "org.freedesktop.zbus.async.ProxySignalConnectTest";
        let unique_name = conn.unique_name().unwrap().clone();
        let name_owner_changed_id = {
            proxy
                .connect_signal("NameOwnerChanged", move |m| {
                    let unique_name = unique_name.clone();
                    let signaled = owner_change_signaled.clone();

                    async move {
                        let (name, _, new_owner) = m
                            .body::<(
                                BusName<'_>,
                                Optional<UniqueName<'_>>,
                                Optional<UniqueName<'_>>,
                            )>()
                            .unwrap();
                        if name != well_known {
                            // Meant for the other testcase then
                            return;
                        }
                        assert_eq!(*new_owner.as_ref().unwrap(), *unique_name);
                        signaled.notify(1);
                    }
                    .boxed()
                })
                .await?
        };
        // `NameAcquired` is emitted twice, first when the unique name is assigned on
        // connection and secondly after we ask for a specific name.
        let name_acquired_id = proxy
            .connect_signal("NameAcquired", move |m| {
                let signaled = name_acquired_signaled.clone();
                async move {
                    if m.body::<&str>().unwrap() == well_known {
                        signaled.notify(1);
                    }
                }
                .boxed()
            })
            .await?;
        // Test multiple handers for the same signal
        let name_acquired_id2 = proxy
            .connect_signal("NameAcquired", move |m| {
                let signaled = name_acquired_signaled2.clone();
                async move {
                    if m.body::<&str>().unwrap() == well_known {
                        signaled.notify(1);
                    }
                }
                .boxed()
            })
            .await?;

        crate::blocking::fdo::DBusProxy::new(&crate::blocking::Connection::from(conn))?
            .request_name(
                well_known.try_into()?,
                fdo::RequestNameFlags::ReplaceExisting.into(),
            )
            .unwrap();

        join!(
            owner_change_listener,
            name_acquired_listener,
            name_acquired_listener2,
        );

        assert!(proxy.disconnect_signal(name_owner_changed_id).await?);
        assert!(!proxy.disconnect_signal(name_owner_changed_id).await?);
        assert!(proxy.disconnect_signal(name_acquired_id).await?);
        assert!(proxy.disconnect_signal(name_acquired_id2).await?);

        Ok(())
    }
}
