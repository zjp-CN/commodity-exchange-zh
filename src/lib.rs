#![allow(unused)]

#[macro_use]
extern crate log;

/// 郑州商品交易所
mod czce;
/// 大连商品交易所
mod dce;

/// 辅助
pub mod util;

pub type Error = Box<dyn std::error::Error>;
pub type Result<T, E = Error> = std::result::Result<T, E>;
pub type Str = compact_str::CompactString;
