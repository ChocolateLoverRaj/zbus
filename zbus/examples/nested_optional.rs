use std::thread;
use zbus::{object_server::SignalContext, connection::Builder, interface, fdo, Result};

use event_listener::{Event, Listener};
use zvariant::Optional;

struct Greeter {}

#[interface(name = "org.zbus.MyGreeter1")]
impl Greeter {
    async fn my_fn(&self) -> Optional<Optional<u32>> {
        // Some(None.into()).into()
        Some(Some(0).into()).into()
    }
}
    async fn my_fn(&self) -> Optional<Optional<u32>> {

// Although we use `async-std` here, you can use any async runtime of choice.
#[async_std::main]
async fn main() -> Result<()> {
    let greeter = Greeter {};
    let _connection = Builder::session()?
        .name("org.zbus.MyGreeter")?
        .serve_at("/org/zbus/MyGreeter", greeter)?
        .build()
        .await?;

    loop {
        thread::park();
    }
}
