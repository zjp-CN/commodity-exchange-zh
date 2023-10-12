use crate::{Result, Str};
use commodity_exchange_zh::Exchange;
use regex::Regex;

#[doc = "\
下载、解析和保存商品期货交易所数据。基本命令：

* -e czce -y 2010..2023：下载郑州交易所 2010 至 2022 年所有合约数据
* -e dce -y 2022 -k C -k M：下载大连交易所玉米和豆粕两个品种的数据

注意：
1. `-e` 和 `-y` 用来指定交易所和年份，为必填项
2. `k` 用于无法下载全部合约时指定品种，目前 dce （大连交易所）需要此参数来选择品种
3. “品种” 不等于 “合约”\
"]
#[derive(argh::FromArgs, Debug)]
pub struct Args {
    /// 交易所。此参数只支持指定单个交易所，且输入的合约或品种必须都为这个交易所。
    #[argh(option, short = 'e')]
    exchange: Exchange,

    /// 年份：xxxx 年或者 xxxx..xxxx 年。如 `-y 2022` 或者等价的 `-y 2022..2023`。
    #[argh(option, short = 'y')]
    year: Year,

    /// 品种代码。可指定多个或者不指定（支持下载所有合约的交易所无需指定）。
    #[argh(option, short = 'k')]
    kind: Vec<Str>,
}

impl Args {
    fn check(&self) -> Result<()> {
        match self.exchange {
            Exchange::dce if self.kind.is_empty() => {
                bail!("大连交易所 (dce) 不支持下载所有合约，请使用 -k 指定品种");
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
        let pattern = r"^((?P<range>(?P<start>\d{4})\.\.(?P<end>\d{4}))|(?P<single>\d{4}))$";
        let re = Regex::new(pattern).unwrap();
        let cap = re
            .captures(s)
            .ok_or_else(|| format!(r"{s} 不是年份，应输入 \d{{4}} 或者 \d{{4}}..\d{{4}}"))?;
        let parse = |key: &str| {
            let res = cap.name(key).unwrap().as_str().parse::<u16>();
            res.map_err(|err| format!("{s} 无法解析为 u16: {err}"))
        };
        let year = if cap.name("range").is_some() {
            Year::Range {
                start: parse("start")?,
                end: parse("end")?,
            }
        } else {
            Year::Single(parse("single")?)
        };
        Ok(year)
    }
}
