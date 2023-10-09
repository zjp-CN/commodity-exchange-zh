use crate::{Result, Str};
use commodity_exchange_zh::Exchange;
use regex::Regex;

/// 下载、解析和保存商品期货交易所数据。推荐命令：
/// * -e czce -a：下载郑州交易所所有合约数据
/// * -e dce -k C：下载大连交易所玉米合约数据
#[derive(argh::FromArgs, Debug)]
pub struct Args {
    /// 交易所。此参数只支持指定单个交易所，且输入的合约或品种必须都为这个交易所，如
    /// `-e czce -k MA` 或 `-e czce -k MA -k V`
    #[argh(option, short = 'e')]
    exchange: Exchange,

    /// 年份：xxxx 年或者 xxxx..xxxx 年。如 `-y 2022` 或者等价的 `-y 2022..2023` （前开后闭）
    #[argh(option, short = 'y')]
    year: Year,

    /// 是否全部合约。默认为否，使用 `--all` 或 `-a` 下载所有合约（仅对支持的交易所有效）。比如
    /// czce 支持一次性下载一年内所有合约，而 dce 不支持。
    #[argh(switch, short = 'a')]
    all: bool,

    /// 品种代码。可指定多个。
    #[argh(option, short = 'k')]
    kind: Vec<Str>,
    // /// 合约代码（一般为品种代码+交割月）。可指定多个。
    // #[argh(option, short = 'c')]
    // contract: Vec<Str>,
}

impl Args {
    fn check(&self) -> Result<()> {
        match self.exchange {
            Exchange::dce if self.all => {
                Err("大连交易所 (dce) 不支持下载所有合约，请使用 -k 指定品种")?
            }
            _ => (),
        }
        debug!("args = {self:?}");
        Ok(())
    }

    pub fn run(self) -> Result<()> {
        self.check()?;

        match self.year {
            Year::Single(y) => self.exchange.run(y)?,
            Year::Range { start, end } => {
                for y in start..end {
                    self.exchange.run(y)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
enum Year {
    Single(u16),
    /// start..end
    Range {
        start: u16,
        end: u16,
    },
}

impl std::str::FromStr for Year {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = |err: std::num::ParseIntError| format!("{s} 无法解析为 u16: {err}");
        let pattern = r"^((?P<range>(?P<start>\d{4})\.\.(?P<end>\d{4}))|(?P<single>\d{4}))$";
        let re = Regex::new(pattern).unwrap();
        let cap = re
            .captures(s)
            .ok_or_else(|| format!(r"{s} 不是年份，应输入 \d{{4}} 或者 \d{{4}}..\d{{4}}"))?;
        let year = if cap.name("range").is_some() {
            Year::Range {
                start: cap.name("start").unwrap().as_str().parse().map_err(err)?,
                end: cap.name("end").unwrap().as_str().parse().map_err(err)?,
            }
        } else {
            Year::Single(cap.name("single").unwrap().as_str().parse().map_err(err)?)
        };
        Ok(year)
    }
}
