use crate::{util::init_data, Result, Str};
use bincode::{Decode, Encode};
use calamine::{DataType, Reader};
use color_eyre::eyre::{Context, ContextCompat};
use indexmap::{Equivalent, IndexMap};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{Read, Seek},
};
use time::Date;

pub static DOWNLOAD_LINKS: &[u8] = include_bytes!("../tests/dce.bincode");
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

pub fn parse_download_links(html: &str) -> Result<DownloadLinks> {
    fn query_err(s: &str) -> String {
        format!("无法根据 `{s}` 搜索到内容")
    }
    fn as_tag_err(s: &str) -> String {
        format!("`{s}` 搜索到的内容无法识别为标签")
    }
    fn get_err(s: &str) -> String {
        format!("无法解析 `{s}`")
    }
    fn attribute_err(s: &str) -> String {
        format!("`{s}` attribute 没有值")
    }
    const UL: &str = r#"ul.cate_sel.clearfix[opentype="page"]"#;
    const LABEL: &str = "label";
    const INPUT: &str = r#"input[type="radio"][name="hisItem"]"#;
    const REL: &str = "rel";
    const OPTION: &str = "option";
    const OPT_VALUE: &str = "value";
    let dom = tl::parse(html, Default::default())?;
    let parser = dom.parser();
    let uls: Vec<_> = dom
        .query_selector(UL)
        .with_context(|| query_err(UL))?
        .collect();
    let options: Vec<_> = dom
        .query_selector(OPTION)
        .with_context(|| query_err(OPTION))?
        .collect();
    let (uls_len, options_len) = (uls.len(), options.len());
    ensure!(
        uls_len == options_len,
        "年份数量 {options_len} 与列表数量 {uls_len} 不相等，需检查 HTML"
    );
    let mut data = IndexMap::with_capacity(options_len);
    for (option, ul) in options.into_iter().zip(uls) {
        let year_str = option
            .get(parser)
            .with_context(|| get_err(OPTION))?
            .as_tag()
            .with_context(|| as_tag_err(OPTION))?
            .attributes()
            .get(OPT_VALUE)
            .with_context(|| get_err(OPT_VALUE))?
            .with_context(|| attribute_err(OPT_VALUE))?
            .as_utf8_str();
        let year = year_str
            .parse::<u16>()
            .with_context(|| format!("年份 `{year_str}` 无法解析为 u16"))?;
        let labels = ul
            .get(parser)
            .with_context(|| get_err(UL))?
            .as_tag()
            .with_context(|| as_tag_err(UL))?
            .query_selector(parser, LABEL)
            .with_context(|| query_err(LABEL))?;
        for label in labels {
            let label = label.get(parser).with_context(|| get_err(LABEL))?;
            let input = label
                .as_tag()
                .with_context(|| as_tag_err(LABEL))?
                .query_selector(parser, INPUT)
                .with_context(|| query_err(INPUT))?
                .next()
                .with_context(|| query_err(INPUT))?
                .get(parser)
                .with_context(|| get_err(INPUT))?
                .as_tag()
                .with_context(|| as_tag_err(INPUT))?;
            data.insert(
                Key {
                    year,
                    name: label.inner_text(parser).into(),
                },
                input
                    .attributes()
                    .get(REL)
                    .with_context(|| get_err(REL))?
                    .with_context(|| attribute_err(REL))?
                    .as_utf8_str()
                    .into_owned(),
            );
        }
    }
    data.sort_keys();
    Ok(DownloadLinks(data))
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
    let pos = parse_xslx_header(header)?;
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

pub fn as_str(cell: &DataType) -> Result<Str> {
    cell.get_string()
        .map(Str::from)
        .with_context(|| format!("{cell:?} 无法读取为 &str"))
}

pub fn as_date(cell: &DataType) -> Result<Date> {
    let u = as_u32(cell)?;
    let year = u / 10000;
    let minus_year = u - year * 10000;
    let month = (minus_year) / 100;
    let day = minus_year - month * 100;
    Ok(Date::from_calendar_date(
        year.try_into()
            .with_context(|| format!("{year} 无法转成 i32"))?,
        u8::try_from(month)
            .with_context(|| format!("{month} 无法转成 u8"))?
            .try_into()
            .with_context(|| format!("{month} 无法转成月份"))?,
        u8::try_from(day).with_context(|| format!("{day} 无法转成 u8"))?,
    )?)
}

pub fn as_f32(cell: &DataType) -> Result<f32> {
    cell.get_float()
        .map(|f| f as f32)
        .with_context(|| format!("{cell:?} 无法读取为 f32"))
}

pub fn as_u32(cell: &DataType) -> Result<u32> {
    if let Some(f) = cell.get_float() {
        Ok(f as u32)
    } else if let Some(int) = cell.get_int() {
        int.try_into()
            .map_err(|err| eyre!("{int}i64 无法转化为 u32：{err:?}"))
    } else {
        bail!("{cell:?} 无法读取为 u32")
    }
}

/// 注意：源数据中多了一列“前收盘价”，但尚未研究它；由于 czce 数据不具备它，所以舍弃。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Field {
    合约,
    日期,
    // 前收盘价,
    前结算价,
    开盘价,
    最高价,
    最低价,
    收盘价,
    结算价,
    涨跌1,
    涨跌2,
    成交量,
    成交额,
    持仓量,
}
const LEN: usize = 13;

/// Xlsx 中的数据的正确位置（不同年份具有不同的表头），因此需要先识别表头。
pub fn parse_xslx_header(header: &[DataType]) -> Result<Vec<usize>> {
    use Field::*;
    let mut pos = HashMap::with_capacity(LEN);
    for (idx, h) in header.iter().enumerate() {
        match h
            .get_string()
            .with_context(|| format!("无法按照字符串读取第一行：{header:?}"))?
        {
            "合约" => {
                pos.insert(合约, idx);
            }
            "日期" => {
                pos.insert(日期, idx);
            }
            // "前收盘价" => {
            //     pos.insert(前收盘价, idx);
            // }
            "前结算价" => {
                pos.insert(前结算价, idx);
            }
            "开盘价" => {
                pos.insert(开盘价, idx);
            }
            "最高价" => {
                pos.insert(最高价, idx);
            }
            "最低价" => {
                pos.insert(最低价, idx);
            }
            "收盘价" => {
                pos.insert(收盘价, idx);
            }
            "结算价" => {
                pos.insert(结算价, idx);
            }
            "涨跌1" => {
                pos.insert(涨跌1, idx);
            }
            "涨跌2" => {
                pos.insert(涨跌2, idx);
            }
            "成交量" => {
                pos.insert(成交量, idx);
            }
            "成交额" => {
                pos.insert(成交额, idx);
            }
            "持仓量" => {
                pos.insert(持仓量, idx);
            }
            _ => (),
        }
    }
    ensure!(pos.len() == LEN, "xlsx 的表头有效列不足 {LEN}：{pos:?}");
    // 通过 HashMap 确定所有字段在第几列，并按字段顺序解析
    // 所以 Field 的变体顺序与 Data 的字段顺序必须一致
    let mut field_pos: Vec<_> = pos.into_iter().collect();
    field_pos.sort_by_key(|v| v.0);
    Ok(field_pos.into_iter().map(|v| v.1).collect())
}
