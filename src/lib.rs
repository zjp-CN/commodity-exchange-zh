// #![allow(unused)]

#[macro_use]
extern crate log;

#[macro_use]
mod macros {
    #[macro_export]
    macro_rules! bail {
        ($($t:tt)*) => { ::color_eyre::eyre::bail!($($t)*) };
    }
    #[macro_export]
    macro_rules! eyre {
        ($($t:tt)*) => { ::color_eyre::eyre::eyre!($($t)*) };
    }
    #[macro_export]
    macro_rules! ensure {
        ($($t:tt)*) => { ::color_eyre::eyre::ensure!($($t)*) };
    }
}

/// 郑州商品交易所
pub mod czce;
/// 大连商品交易所
pub mod dce;

/// 辅助
pub mod util;

pub use color_eyre::eyre::Result;
pub type Str = compact_str::CompactString;

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy)]
pub enum Exchange {
    czce,
    dce,
}

impl Exchange {
    pub fn run(self, year: u16) -> Result<()> {
        match self {
            Exchange::czce => czce::run(year),
            Exchange::dce => todo!(),
        }
    }
}

impl std::str::FromStr for Exchange {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(match s {
            "czce" | "CZCE" | "郑州" => Exchange::czce,
            "dce" | "DCE" | "大连" => Exchange::dce,
            _ => return Err(format!("{s} 不是商品期货交易所，只支持 czce/dce")),
        })
    }
}
