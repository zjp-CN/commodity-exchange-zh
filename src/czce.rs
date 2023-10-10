use crate::{
    util::{fetch, init_data, save_csv},
    Result, Str,
};
use bytesize::ByteSize;
use serde::Deserialize;
use std::{io, path::PathBuf};
use time::Date;

const MEMO: &str = "自2020年1月1日起，成交量、持仓量、成交额、行权量均为单边计算";

/// 注意：
/// 2010..=2014 使用 http://www.czce.com.cn/cn/exchange/datahistory2010.zip
/// 2015..=2019 使用 http://www.czce.com.cn/cn/DFSStaticFiles/Future/2019/FutureDataHistory.zip
/// 2020..      使用 http://www.czce.com.cn/cn/DFSStaticFiles/Future/2023/ALLFUTURES2023.zip
pub fn get_url(year: u16) -> Result<String> {
    let this_year = init_data().this_year;
    let url = match year {
        2010..=2014 => format!("http://www.czce.com.cn/cn/exchange/datahistory{year}.zip"),
        2015..=2019 => {
            format!("http://www.czce.com.cn/cn/DFSStaticFiles/Future/{year}/FutureDataHistory.zip")
        }
        2020.. if year <= this_year => {
            format!("http://www.czce.com.cn/cn/DFSStaticFiles/Future/{year}/FutureDataAllHistory/ALLFUTURES{year}.xls")
        }
        _ => return Err(format!("{year} 必须在 2010..={this_year} 范围内").into()),
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
    fetch_txt(year, |raw, fname| {
        let csv_content = parse_txt(raw, None::<fn(_) -> _>)?;
        save_csv(&csv_content, &fname)?;
        info!("来自【郑州交易所】的数据备注：{MEMO}");
        Ok(())
    })
}

pub fn fetch_txt(
    year: u16,
    mut handle_unzipped: impl FnMut(&str, PathBuf) -> Result<()>,
) -> Result<()> {
    let url = get_url(year)?;
    let fetched = fetch(&url)?;
    let mut zipped = zip::ZipArchive::new(fetched)?;
    for i in 0..zipped.len() {
        let mut unzipped = zipped.by_index(i)?;
        if unzipped.is_file() {
            let unzipped_path = unzipped
                .enclosed_name()
                .ok_or_else(|| format!("`{}` 无法转成 &Path", unzipped.name()))?;
            let size = unzipped.size();
            info!(
                "{url} 获取的第 {i} 个文件：{} ({} => {})",
                unzipped_path.display(),
                ByteSize(unzipped.compressed_size()),
                ByteSize(size),
            );
            let file_name = unzipped_path
                .file_name()
                .and_then(|fname| Some(format!("郑州-{}", fname.to_str()?)))
                .ok_or_else(|| format!("无法从 {unzipped_path:?} 中获取文件名"))?;
            let mut buf = Vec::with_capacity(size as usize);
            io::copy(&mut unzipped, &mut buf)?;
            handle_unzipped(std::str::from_utf8(&buf)?, file_name.into())?;
        } else {
            return Err(format!("{} 还未实现解压成文件夹", unzipped.name()).into());
        }
    }
    Ok(())
}

pub fn parse_txt(raw: &str, f: Option<impl FnMut(Data)>) -> Result<String> {
    let mut start = 0;
    // 跳过前两行
    for head in raw.lines().take(2) {
        info!("{head}");
        start += head.len();
    }
    start += 1;
    // 删除所有数字千位分隔符和单元格内的空格
    let stripped = init_data()
        .regex_czce
        .replace_all(&raw[start..], "")
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

#[test]
fn test_clickhouse() -> Result<()> {
    crate::util::init_test_log();
    // let content = std::fs::read_to_string("../cache/郑州-ALLFUTURES2022.csv")?;
    const SQL: &str = include_str!("./sql/czce.sql");
    let mut cmd = std::process::Command::new("clickhouse-client");
    cmd.args(["--multiquery", SQL]);
    let cmd_string = format!("{cmd:?}");
    let output = cmd.output()?;
    if !output.status.success() {
        return Err(format!(
            "保存至 clickhouse 失败；运行 {cmd_string} 的结果为：\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    } else {
        info!("成功将数据保存至 clickhouse");
    }
    Ok(())
}
