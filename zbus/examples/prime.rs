use std::thread;

use futures_util::TryFutureExt;
use prime_checker::is_prime;

use zbus::{connection::Builder, fdo, interface};

struct Greeter {}

const PRIME_NUMBER_FILE: &str = "/etc/prime-number";

#[interface(name = "org.zbus.PrimeFile")]
impl Greeter {
    #[zbus(property)]
    async fn prime_number(&self) -> fdo::Result<u32> {
        Ok(String::from_utf8(
            async_std::fs::read(PRIME_NUMBER_FILE)
                .map_err(|e| fdo::Error::IOError(e.to_string()))
                .await?,
        )
        .map_err(|e| fdo::Error::Failed(e.to_string()))?
        .trim()
        .parse::<u32>()
        .map_err(|e| fdo::Error::Failed(e.to_string()))?)
    }

    #[zbus(property)]
    async fn set_prime_number(&mut self, prime_number: u32) -> fdo::Result<()> {
        let (is_prime, factors) = is_prime(prime_number as u64);
        if is_prime {
            async_std::fs::write(PRIME_NUMBER_FILE, format!("{}\n", prime_number))
                .await
                .map_err(|e| fdo::Error::IOError(e.to_string()))?;
            Ok(())
        } else {
            Err(fdo::Error::InvalidArgs(format!(
                "The number {:#?} is not a prime number because it has the factors {:?}",
                prime_number, factors
            )))
        }
    }
}

// Although we use `async-std` here, you can use any async runtime of choice.
#[async_std::main]
async fn main() -> zbus::Result<()> {
    let _connection = Builder::system()?
        .name("org.zbus.PrimeFile")?
        .serve_at("/org/zbus/PrimeFile", Greeter {})?
        .build()
        .await?;

    loop {
        thread::park();
    }
}
