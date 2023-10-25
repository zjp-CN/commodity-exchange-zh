use crate::{util::init_data, Result, Str};
use bincode::{Decode, Encode};
use calamine::{DataType, Reader};
use color_eyre::eyre::{Context, ContextCompat};
use indexmap::{Equivalent, IndexMap};
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek};
use time::Date;

mod parse;
pub use parse::parse_download_links;
pub mod select;

pub static DOWNLOAD_LINKS: &[u8] = include_bytes!("../../tests/dce.bincode");
pub const URL_PREFIX: &str = "http://www.dce.com.cn";

#[derive(Debug, Decode, Encode, PartialEq, Eq)]
pub struct DownloadLinks(#[bincode(with_serde)] IndexMap<Key, String>);

impl DownloadLinks {
    pub fn new_static() -> Result<DownloadLinks> {
        let d = bincode::decode_from_slice::<DownloadLinks, _>(
            DOWNLOAD_LINKS,
            bincode::config::standard(),
        )?;
        Ok(d.0)
    }
    pub fn iter(&self) -> impl Iterator<Item = (&Key, &String)> {
        self.0.iter()
    }
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "tabled", derive(tabled::Tabled))]
pub struct Key {
    pub year: u16,
    pub name: Str,
}

impl Equivalent<Key> for (u16, &str) {
    fn equivalent(&self, key: &Key) -> bool {
        self.0 == key.year && self.1 == key.name
    }
}

pub fn get_download_link(year: u16, name: &str) -> Result<String> {
    let index_map = &init_data().links_dce.0;
    let postfix = index_map
        .get(&(year, name))
        .with_context(|| format!("无法找到 {year} 年 {name} 品种的下载链接"))?;
    Ok(format!("{URL_PREFIX}{postfix}"))
}

/// 读取 xlsx 文件，并处理解析过的每行数据
pub fn read_dce_xlsx<R: Read + Seek>(
    mut wb: calamine::Xlsx<R>,
    mut handle: impl FnMut(Data) -> Result<()>,
) -> Result<()> {
    let sheet = match wb.worksheet_range_at(0) {
        Some(Ok(sheet)) => sheet,
        Some(Err(err)) => bail!("无法读取第 0 个表，因为 {err:?}"),
        None => bail!("无法读取第 0 个表"),
    };
    let mut rows = sheet.rows();
    let header = rows.next().context("无法读取第一行")?;
    let pos = parse::parse_xslx_header(header)?;
    for row in rows {
        handle(Data::new(row, &pos)?)?;
    }
    Ok(())
}

#[derive(Debug, Encode)]
#[cfg_attr(feature = "tabled", derive(tabled::Tabled))]
pub struct Data {
    /// 合约代码
    #[bincode(with_serde)]
    pub code: Str,
    /// 交易日期
    #[bincode(with_serde)]
    pub date: Date,
    /// 昨结算
    pub prev: f32,
    /// 今开盘
    pub open: f32,
    /// 最高价
    pub high: f32,
    /// 最低价
    pub low: f32,
    /// 今收盘
    pub close: f32,
    /// 今结算
    pub settle: f32,
    /// 涨跌1：涨幅百分数??
    pub zd1: f32,
    /// 涨跌2：涨跌数??
    pub zd2: f32,
    /// 成交量
    pub vol: u32,
    /// 交易额
    pub amount: u32,
    /// 持仓量
    pub position: u32,
}

impl Data {
    pub fn new(row: &[DataType], pos: &[usize]) -> Result<Data> {
        use parse::{as_date, as_f32, as_str, as_u32, LEN};

        ensure!(pos.len() == LEN, "xlsx 的表头有效列不足 {LEN}：{pos:?}");
        let err = |n: usize| format!("{row:?} 无法获取到第 {n} 个单元格数据");
        Ok(Data {
            code: as_str(row.get(pos[0]).with_context(|| err(0))?)?,
            date: as_date(row.get(pos[1]).with_context(|| err(1))?)?,
            prev: as_f32(row.get(pos[2]).with_context(|| err(2))?)?,
            open: as_f32(row.get(pos[3]).with_context(|| err(3))?)?,
            high: as_f32(row.get(pos[4]).with_context(|| err(4))?)?,
            low: as_f32(row.get(pos[5]).with_context(|| err(5))?)?,
            close: as_f32(row.get(pos[6]).with_context(|| err(6))?)?,
            settle: as_f32(row.get(pos[7]).with_context(|| err(7))?)?,
            zd1: as_f32(row.get(pos[8]).with_context(|| err(8))?)?,
            zd2: as_f32(row.get(pos[9]).with_context(|| err(9))?)?,
            vol: as_u32(row.get(pos[10]).with_context(|| err(10))?)?,
            amount: as_u32(row.get(pos[11]).with_context(|| err(11))?)?,
            position: as_u32(row.get(pos[12]).with_context(|| err(12))?)?,
        })
    }
}
