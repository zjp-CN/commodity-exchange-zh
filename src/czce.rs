use crate::{util, Result, Str};
use serde::Deserialize;
use time::Date;

const MEMO: &str = "自2020年1月1日起，成交量、持仓量、成交额、行权量均为单边计算";

/// 注意：
/// 2010..=2014 使用 http://www.czce.com.cn/cn/exchange/datahistory2010.zip
/// 2015..=2019 使用 http://www.czce.com.cn/cn/DFSStaticFiles/Future/2019/FutureDataHistory.zip
/// 2020..      使用 http://www.czce.com.cn/cn/DFSStaticFiles/Future/2023/ALLFUTURES2023.zip
pub fn get_url(year: u16) -> Result<String> {
    let this_year = util::init_data().this_year;
    let url = match year {
        2010..=2014 => format!("http://www.czce.com.cn/cn/exchange/datahistory{year}.zip"),
        2015..=2019 => {
            format!("http://www.czce.com.cn/cn/DFSStaticFiles/Future/{year}/FutureDataHistory.zip")
        }
        2020.. if year <= this_year => {
            format!("http://www.czce.com.cn/cn/DFSStaticFiles/Future/{year}/ALLFUTURES{year}.zip")
        }
        _ => bail!("{year} 必须在 2010..={this_year} 范围内"),
    };
    Ok(url)
}

#[derive(Deserialize, Debug)]
#[cfg_attr(feature = "tabled", derive(tabled::Tabled))]
pub struct Data {
    /// 交易日期
    #[serde(deserialize_with = "crate::util::parse_date_czce")]
    #[cfg_attr(feature = "tabled", tabled(rename = "交易日期"))]
    pub date: Date,
    /// 合约代码
    #[cfg_attr(feature = "tabled", tabled(rename = "合约代码"))]
    pub code: Str,
    /// 昨结算
    #[cfg_attr(feature = "tabled", tabled(rename = "昨结算"))]
    pub prev: f32,
    /// 今开盘
    #[cfg_attr(feature = "tabled", tabled(rename = "今开盘"))]
    pub open: f32,
    /// 最高价
    #[cfg_attr(feature = "tabled", tabled(rename = "最高价"))]
    pub high: f32,
    /// 最低价
    #[cfg_attr(feature = "tabled", tabled(rename = "最低价"))]
    pub low: f32,
    /// 今收盘
    #[cfg_attr(feature = "tabled", tabled(rename = "今收盘"))]
    pub close: f32,
    /// 今结算
    #[cfg_attr(feature = "tabled", tabled(rename = "今结算"))]
    pub settle: f32,
    /// 涨跌1：涨幅百分数??
    #[cfg_attr(feature = "tabled", tabled(rename = "涨跌1"))]
    pub zd1: f32,
    /// 涨跌2：涨跌数??
    #[cfg_attr(feature = "tabled", tabled(rename = "涨跌2"))]
    pub zd2: f32,
    /// 成交量
    #[cfg_attr(feature = "tabled", tabled(rename = "成交量"))]
    pub vol: u32,
    /// 持仓量
    #[serde(deserialize_with = "crate::util::parse_u32_from_f32")]
    #[cfg_attr(feature = "tabled", tabled(rename = "持仓量"))]
    pub position: u32,
    /// 增减量
    #[cfg_attr(feature = "tabled", tabled(rename = "增减量"))]
    pub pos_delta: i32,
    /// 交易额（万）
    #[cfg_attr(feature = "tabled", tabled(rename = "交易额（万）"))]
    pub amount: f32,
    /// 交割结算价
    #[serde(deserialize_with = "crate::util::parse_option_f32")]
    #[cfg_attr(
        feature = "tabled",
        tabled(display_with = "crate::util::display_option", rename = "交割结算价")
    )]
    pub dsp: Option<f32>,
}

pub fn run(year: u16) -> Result<()> {
    // NOTE: GBK 编码的表头与现有 UTF8 的表头和内容不一致：
    // * 空盘量（GBK） -> 持仓量（UTF8)
    // * 换行符是 CRLF -> LF
    // * 换行符前为 `交割结算价|` -> `交割结算价`
    // * 交割结算价的若无实际数据则为 0 -> 空
    //  （从而 SQL 需要把 dsp 为 0 替换成 NULL）
    // TODO: 该函数返回一个状态来在录入当年数据后替换 dsp 的 SQL 语句
    util::fetch_zip(&get_url(year)?, |raw, fname| {
        let (txt, encoding) = util::read_txt(&raw, &fname)?;
        let csv_content = parse_txt(&txt, None::<fn(_) -> _>)?;
        let fname = format!("czce-{fname}");
        util::save_to_csv_and_clickhouse(
            || util::save_csv(csv_content.trim().as_bytes(), fname),
            || {
                util::clickhouse::execute(include_str!("./sql/czce.sql"))?;
                const TABLE: &str = "qihuo.czce";
                util::clickhouse::insert_with_count_reported(TABLE, csv_content.as_bytes())?;
                if matches!(encoding, util::Encoding::GBK) {
                    util::clickhouse::execute(&format!(
                        "ALTER TABLE qihuo.czce UPDATE dsp=Null \
                         WHERE dsp==0 AND year(date)=={year};"
                    ))?;
                    info!("{TABLE} 由于源数据不规范，需要将 dsp 为 0 的数据修改为 Null");
                }
                Ok(())
            },
        )?;
        info!("成功获取 {year} 年的数据\n来自【郑州交易所】的数据备注：{MEMO}");
        Ok(())
    })
}

pub fn parse_txt(raw: &str, f: Option<impl FnMut(Data)>) -> Result<String> {
    let mut start = 0;
    // 跳过前两行
    for head in raw.lines().take(2) {
        info!("{head}");
        start += head.len();
    }
    start += 2;
    // 删除所有数字千位分隔符和单元格内的空格
    let stripped = util::init_data()
        .regex_czce
        .replace_all(raw[start..].trim(), "")
        .into_owned();
    let Some(f) = f else { return Ok(stripped) };
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'|')
        .from_reader(stripped.as_bytes());
    reader
        .records()
        .filter_map(|record| match &record {
            Ok(line) => match line.deserialize::<Data>(None) {
                Ok(data) => Some(data),
                Err(err) => {
                    error!("反序列化 {line:?} 出错：{err:?}");
                    None
                }
            },
            Err(err) => {
                error!("解析 {record:?} 出错：{err:?}");
                None
            }
        })
        .for_each(f);
    Ok(stripped)
}
