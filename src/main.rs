#[macro_use]
extern crate log;

mod cli;
use commodity_exchange_zh::{util::init_log, Result, Str};

fn main() -> Result<()> {
    init_log()?;
    let args: cli::Args = argh::from_env();
    args.run()?;
    Ok(())
}
