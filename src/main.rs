#[macro_use]
extern crate log;

// #[macro_use]
// extern crate commodity_exchange_zh;

mod cli;
use commodity_exchange_zh::{util::init_log, Result, Str};

fn main() -> Result<()> {
    color_eyre::install()?;
    init_log()?;
    let args: cli::Args = argh::from_env();
    args.run()?;
    Ok(())
}
