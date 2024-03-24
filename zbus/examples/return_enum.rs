use event_listener::{Event, Listener};
use serde::{Deserialize, Serialize};

use zbus::{connection::Builder, fdo, interface, Result};
use zvariant::{OwnedValue, Type, Value};

#[derive(Deserialize, Serialize, Type, PartialEq, Debug, Value, OwnedValue)]
#[repr(u8)]
enum ExampleEnum {
    A = 0,
    B = 1,
    C = 2,
}

struct ReturnEnum {
    done: Event,
}

#[interface(name = "org.zbus.ReturnEnum")]
impl ReturnEnum {
    async fn get_something(
        &self
    ) -> fdo::Result<ExampleEnum> {
        Ok(ExampleEnum::A)
    }
}

// Although we use `async-std` here, you can use any async runtime of choice.
#[async_std::main]
async fn main() -> Result<()> {
    let interface = ReturnEnum {
        done: event_listener::Event::new(),
    };
    let done_listener = interface.done.listen();
    let _connection = Builder::session()?
        .name("org.zbus.ReturnEnum")?
        .serve_at("/org/zbus/ReturnEnum", interface)?
        .build()
        .await?;

    done_listener.wait();

    Ok(())
}
