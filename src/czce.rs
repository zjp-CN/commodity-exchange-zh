use crate::{
    util::{fetch, init_data, Response},
    Result, Str,
};
use bytesize::ByteSize;
use calamine::{Reader, Xls, XlsOptions};
use csv::StringRecordsIter;
use regex::Regex;
use serde::Deserialize;
use std::{
    fs::File,
    io::{self, BufRead, BufReader, Cursor, Read},
};
use time::Date;

const XLS: &str =
    "http://www.czce.com.cn/cn/DFSStaticFiles/Future/2023/FutureDataAllHistory/ALLFUTURES2023.xls";

const URL: &str = "http://www.czce.com.cn/cn/DFSStaticFiles/Future/2023/ALLFUTURES2023.zip";

pub fn fetch_xls(year: u16) -> Response {
    let url = format!(
        "http://www.czce.com.cn/cn/DFSStaticFiles/\
         Future/{year}/FutureDataAllHistory/ALLFUTURES{year}.xls",
    );
    fetch(&url)
}

fn fetch_parse_xls() -> Result<()> {
    // let init = crate::util::init_log();
    let reader = fetch_xls(2023)?;
    let workbook = Xls::new_with_options(reader, XlsOptions::default())?;
    Ok(())
}

#[derive(Deserialize, Debug)]
#[cfg_attr(feature = "tabled", derive(tabled::Tabled))]
pub struct Data {
    /// 交易日期
    #[serde(deserialize_with = "crate::util::parse_date_czce")]
    #[cfg_attr(feature = "tabled", tabled(rename = "交易日期"))]
    date: Date,
    /// 合约代码
    #[cfg_attr(feature = "tabled", tabled(rename = "合约代码"))]
    code: Str,
    /// 昨结算
    #[cfg_attr(feature = "tabled", tabled(rename = "昨结算"))]
    prev: f32,
    /// 今开盘
    #[cfg_attr(feature = "tabled", tabled(rename = "今开盘"))]
    open: f32,
    /// 最高价
    #[cfg_attr(feature = "tabled", tabled(rename = "最高价"))]
    high: f32,
    /// 最低价
    #[cfg_attr(feature = "tabled", tabled(rename = "最低价"))]
    low: f32,
    /// 今收盘
    #[cfg_attr(feature = "tabled", tabled(rename = "今收盘"))]
    close: f32,
    /// 今结算
    #[cfg_attr(feature = "tabled", tabled(rename = "今结算"))]
    settle: f32,
    /// 涨跌1：涨幅百分数??
    #[cfg_attr(feature = "tabled", tabled(rename = "涨跌1"))]
    zd1: f32,
    /// 涨跌2：涨跌数??
    #[cfg_attr(feature = "tabled", tabled(rename = "涨跌2"))]
    zd2: f32,
    /// 成交量
    #[cfg_attr(feature = "tabled", tabled(rename = "成交量"))]
    vol: u32,
    /// 持仓量
    #[serde(deserialize_with = "crate::util::parse_u32_from_f32")]
    #[cfg_attr(feature = "tabled", tabled(rename = "持仓量"))]
    position: u32,
    /// 增减量
    #[cfg_attr(feature = "tabled", tabled(rename = "增减量"))]
    pos_delta: i32,
    /// 交易额（万）
    #[cfg_attr(feature = "tabled", tabled(rename = "交易额（万）"))]
    amount: f32,
    /// 交割结算价
    #[serde(deserialize_with = "crate::util::parse_option_f32")]
    #[cfg_attr(
        feature = "tabled",
        tabled(display_with = "crate::util::display_option", rename = "交割结算价")
    )]
    dsp: Option<f32>,
}

pub fn fetch_txt(year: u16) -> Response {
    fetch(&format!(
        "http://www.czce.com.cn/cn/DFSStaticFiles/Future/{year}/ALLFUTURES{year}.zip"
    ))
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
fn parse_xls() -> Result<()> {
    let mut xls = calamine::open_workbook_auto("./cache/c.xlsx")?;
    // let mut opts = XlsOptions::default();
    // opts.force_codepage = Some(1201);
    // let mut xls = Xls::new_with_options(File::open("./cache/ALLFUTURES2023.xls")?, opts)?;
    info!("Reading {:?} in c.xlsx", xls.sheet_names());
    let sheet = xls
        .worksheet_range_at(0)
        .ok_or_else(|| format!("无法获取到第 0 个表，所有表为：{:?}", xls.sheet_names()))??;
    for row in sheet.rows().take(3) {
        println!("{row:#?}");
    }
    Ok(())
}

#[test]
fn fetch_parse() -> Result<()> {
    let init = crate::util::init_test_log();
    let resp = minreq::get(URL).send()?;
    let bytes = resp.as_bytes();
    info!("{URL} 获取的字节数：{}", ByteSize(bytes.len() as u64));
    let mut zipped = zip::ZipArchive::new(Cursor::new(bytes))?;
    for i in 0..zipped.len() {
        let mut unzipped = zipped.by_index(i)?;
        if unzipped.is_file() {
            let file_name = unzipped
                .enclosed_name()
                .ok_or_else(|| format!("`{}` 无法转成 &Path", unzipped.name()))?;
            info!(
                "{URL} 获取的第 {i} 个文件：{} ({} => {})",
                file_name.display(),
                ByteSize(unzipped.compressed_size()),
                ByteSize(unzipped.size()),
            );
            let cached_file = init.cache_dir.join(file_name);
            let mut file = File::create(&cached_file)?;
            io::copy(&mut unzipped, &mut file)?;
            info!("已解压至 {}", cached_file.display());
        } else {
            return Err(format!("{} 还未实现解压成文件夹", unzipped.name()).into());
        }
    }
    Ok(())
}
