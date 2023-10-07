use crate::{Result, Str};
use bytesize::ByteSize;
use calamine::{Reader, Xls, XlsOptions};
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

pub fn fetch_xls(year: u16) -> Result<Cursor<Vec<u8>>> {
    let url = format!(
        "http://www.czce.com.cn/cn/DFSStaticFiles/\
         Future/{year}/FutureDataAllHistory/ALLFUTURES{year}.xls",
    );
    let bytes = minreq::get(url).send()?.into_bytes();
    info!("{URL} 获取的字节数：{}", ByteSize(bytes.len() as u64));
    Ok(Cursor::new(bytes))
}

fn fetch_parse_xls() -> Result<()> {
    // let init = crate::util::init_log();
    let reader = fetch_xls(2023)?;
    let workbook = Xls::new_with_options(reader, XlsOptions::default())?;
    Ok(())
}

#[derive(Deserialize, Debug)]
pub struct Data {
    /// 交易日期
    #[serde(deserialize_with = "crate::util::parse_date_czce")]
    date: Date,
    /// 合约代码
    code: Str,
    /// 昨结算
    prev: f32,
    /// 今开盘
    open: f32,
    /// 最高价
    high: f32,
    /// 最低价
    low: f32,
    /// 今收盘
    close: f32,
    /// 今结算
    settle: f32,
    /// 涨跌1：涨幅百分数??
    zd1: f32,
    /// 涨跌2：涨跌数??
    zd2: f32,
    /// 成交量
    vol: u32,
    /// 持仓量
    #[serde(deserialize_with = "crate::util::parse_u32_from_f32")]
    position: u32,
    /// 增减量
    pos_delta: i32,
    /// 交易额（万）
    amount: f32,
    /// 交割结算价
    #[serde(deserialize_with = "crate::util::parse_option_f32")]
    dsp: Option<f32>,
}

#[test]
fn parse_txt() -> Result<()> {
    let init = crate::util::init_log();
    let mut file = BufReader::new(File::open(init.cache_dir.join("ALLFUTURES2023.txt"))?);
    let capacity = file.get_ref().metadata()?.len() as usize;
    let mut buf = String::with_capacity(capacity);
    file.read_to_string(&mut buf)?;
    let mut lines = buf.lines();
    let mut start = 0;
    for head in lines.by_ref().take(2) {
        info!("{head}");
        start += head.len();
    }
    start += 1;
    info!("buf = {}...", &buf[start..start + 10]);
    let buf = Regex::new(",| ")?.replace_all(&buf[start..], "");
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'|')
        .from_reader(buf.as_bytes());
    for line in reader.records().take(3) {
        let line = line?;
        info!("line = {line:?}");
        let data: Data = line.deserialize(None)?;
        info!("data = {data:?}");
    }
    Ok(())
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
    let init = crate::util::init_log();
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
