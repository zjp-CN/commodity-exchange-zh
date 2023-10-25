use crate::{Result, Str};
use argh::FromArgs;
use commodity_exchange_zh::{czce, dce};
use regex::Regex;

#[doc = "\
下载、解析和保存商品期货交易所数据。子命令示例：

* czce -y 2010..2023：下载郑州交易所 2010 至 2022 年所有合约数据
* dce -y 2020..=2023 C M：下载大连交易所 2019 至 2022 年玉米和豆粕两个品种的数据
* dce -s：交互式选择大连交易所年份和品种
"]
#[derive(FromArgs, Debug)]
pub struct Args {
    #[argh(subcommand)]
    exchange: Exchange,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum Exchange {
    Czce(Czce),
    Dce(Dce),
}

/// 大连交易所
#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "dce")]
struct Dce {
    /// 交互式选择年份和品种。该参数与其他参数互斥。
    #[argh(switch, short = 's')]
    select: bool,

    /// 年份：xxxx 年或者 xxxx..xxxx 年。如 `-y 2022` 或者等价的 `-y 2022..2023`。
    #[argh(option, short = 'y')]
    year: Option<Year>,

    /// 品种代码。可指定多个或者不指定（支持下载所有合约的交易所无需指定）。
    #[argh(positional, greedy)]
    kinds: Vec<Str>,
}

/// 郑州交易所
#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand, name = "czce")]
struct Czce {
    /// 年份：xxxx 年或者 xxxx..xxxx 年。如 `-y 2022` 或者等价的 `-y 2022..2023`。
    #[argh(option, short = 'y')]
    year: Year,
}

impl Args {
    pub fn run(self) -> Result<()> {
        debug!("Args = {self:?}");
        match self.exchange {
            Exchange::Czce(Czce { year }) => year.for_each_year(czce::run)?,
            Exchange::Dce(d) => {
                if d.select {
                } else if let Some(year) = d.year {
                    year.for_each_year(|y| {
                        for kind in &d.kinds {
                            let link = dce::get_download_link(y, kind)?;
                            info!("{link}");
                        }
                        Ok(())
                    })?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
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

impl Year {
    fn for_each_year(self, mut f: impl FnMut(u16) -> Result<()>) -> Result<()> {
        match self {
            Year::Single(year) => f(year),
            Year::Range { start, end } => (start..end).try_for_each(f),
        }
    }
}
