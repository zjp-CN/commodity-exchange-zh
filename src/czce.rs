use crate::{
    util::{fetch, init_data, save_csv},
    Result, Str,
};
use bytesize::ByteSize;
use serde::Deserialize;
use std::{
    io,
    path::PathBuf,
    process::{Command, Output, Stdio},
};
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
    fetch_txt(year, |raw, fname, status| {
        let csv_content = parse_txt(raw, None::<fn(_) -> _>)?;
        std::thread::scope(|s| {
            let task1 = s.spawn(|| save_csv(&csv_content, fname));
            let task2 = s.spawn(|| -> Result<()> {
                clickhouse_execute(include_str!("./sql/czce.sql"))?;
                const TABLE: &str = "qihuo.czce";
                let sql_count = format!("SELECT count(*) FROM {TABLE}");
                let count_old = clickhouse_execute(&sql_count)?;
                info!("{TABLE} 现有数据 {count_old} 条");
                info!("插入\n{}", &csv_content[..100]);
                let sql_insert_csv =
                    format!("SET format_csv_delimiter = '|'; INSERT INTO {TABLE} FORMAT CSV");
                clickhouse_insert(&sql_insert_csv, io::Cursor::new(&csv_content))?;
                let count_new = clickhouse_execute(&sql_count)?;
                let added = count_new
                    .parse::<u64>()
                    .ok()
                    .and_then(|x| {
                        x.checked_sub(count_old.parse::<u64>().ok()?)
                            .map(|r| r.to_string())
                    })
                    .unwrap_or_default();
                info!("{TABLE} 现有数据 {count_new} 条（增加了 {added} 条）");
                if status.change_0_to_null {
                    clickhouse_execute(&format!(
                        "ALTER TABLE qihuo.czce UPDATE dsp=Null \
                         WHERE dsp==0 AND year(date)=={year};"
                    ))?;
                    info!("{TABLE} 由于源数据不规范，需要将 dsp 为 0 的数据修改为 Null");
                }
                Ok(())
            });

            match task1.join() {
                Ok(res) => _ = res?,
                Err(err) => bail!("save_csv 运行失败：{err:?}"),
            }
            match task2.join() {
                Ok(res) => res?,
                Err(err) => bail!("保存到 clickhouse 运行失败：{err:?}"),
            }
            Ok(())
        })?;
        info!("成功获取 {year} 年的数据\n来自【郑州交易所】的数据备注：{MEMO}");
        Ok(())
    })
}

#[derive(Default)]
pub struct Status {
    change_0_to_null: bool,
}

pub fn fetch_txt(
    year: u16,
    mut handle_unzipped: impl FnMut(&str, PathBuf, &Status) -> Result<()>,
) -> Result<()> {
    let url = get_url(year)?;
    let fetched = fetch(&url)?;
    let mut zipped = zip::ZipArchive::new(fetched)?;
    for i in 0..zipped.len() {
        let mut unzipped = zipped.by_index(i)?;
        if unzipped.is_file() {
            let unzipped_path = unzipped
                .enclosed_name()
                .ok_or_else(|| eyre!("`{}` 无法转成 &Path", unzipped.name()))?;
            let size = unzipped.size();
            let unzipped_path_display = unzipped_path.display().to_string();
            info!(
                "{url} 获取的第 {i} 个文件：{unzipped_path_display} ({} => {})",
                ByteSize(unzipped.compressed_size()),
                ByteSize(size),
            );
            let file_name = unzipped_path
                .file_name()
                .and_then(|fname| Some(format!("郑州-{}", fname.to_str()?)))
                .ok_or_else(|| eyre!("无法从 {unzipped_path:?} 中获取文件名"))?;
            let mut buf = Vec::with_capacity(size as usize);
            io::copy(&mut unzipped, &mut buf)?;
            let mut status = Status::default();
            let content = match std::str::from_utf8(&buf) {
                Ok(s) => s.into(),
                Err(_) => {
                    let gbk = encoding_rs::GBK;
                    info!("{unzipped_path_display} 不是 UTF8 编码的，尝试使用 GBK 解码");
                    let (cow, encoding, err) = gbk.decode(&buf);
                    if err {
                        bail!("{unzipped_path_display} 不是 GBK 编码的，需要手动确认编码");
                    } else if encoding != gbk {
                        bail!("{unzipped_path_display} GBK/{encoding:?} 解码失败");
                    }
                    // NOTE: GBK 编码的表头与现有 UTF8 的表头和内容不一致：
                    // * 空盘量（GBK） -> 持仓量（UTF8)
                    // * 换行符是 CRLF -> LF
                    // * 换行符前为 `交割结算价|` -> `交割结算价`
                    // * 交割结算价的若无实际数据则为 0 -> 空
                    //  （从而 SQL 需要把 dsp 为 0 替换成 NULL）
                    // TODO: 该函数返回一个状态来在录入当年数据后替换 dsp 的 SQL 语句
                    status.change_0_to_null = true;
                    cow
                }
            };
            handle_unzipped(&content, file_name.into(), &status)?;
        } else {
            bail!("{} 还未实现解压成文件夹", unzipped.name());
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
    start += 2;
    // 删除所有数字千位分隔符和单元格内的空格
    let stripped = init_data()
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

fn clickhouse_output(output: Output, cmd: String) -> Result<String> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stdout = stdout.trim();
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim();
    if output.status.success() {
        struct StdOutErr<'s> {
            stdout: &'s str,
            stderr: &'s str,
        }
        impl std::fmt::Display for StdOutErr<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let StdOutErr { stdout, stderr } = self;
                if !stdout.is_empty() {
                    writeln!(f, "stdout:\n{stdout}")?;
                }
                if !stderr.is_empty() {
                    write!(f, "stderr:\n{stderr}")?;
                }
                Ok(())
            }
        }
        info!(
            "成功运行命令：{}\n{}",
            regex::Regex::new("\n")
                .unwrap()
                .find_iter(&cmd)
                .nth(3)
                .map(|cap| format!("{} ...\"", &cmd[..cap.start()]))
                .unwrap_or(cmd),
            StdOutErr { stdout, stderr }
        );
        Ok(stdout.to_owned())
    } else {
        bail!("{cmd} 运行失败\nstdout:\n{stdout}\nstderr:\n{stderr}")
    }
}

pub fn clickhouse_execute(sql: &str) -> Result<String> {
    const MULTI: &str = "--multiquery";
    let mut cmd = Command::new("clickhouse-client");
    cmd.args([MULTI, sql]);
    let cmd_string = format!(r#"clickhouse-client "{MULTI}" "{sql}""#);
    let output = cmd.output()?;
    clickhouse_output(output, cmd_string)
}

pub fn clickhouse_insert(sql: &str, reader: impl io::Read + io::Seek) -> Result<()> {
    use io::Seek;
    const MULTI: &str = "--multiquery";
    let mut cmd = Command::new("clickhouse-client");
    cmd.stdin(Stdio::piped());
    cmd.args([MULTI, sql]);
    let cmd_string = format!(r#"clickhouse-client "{MULTI}" "{sql}""#);
    let mut child = cmd.spawn()?;
    if let Some(stdin) = child.stdin.as_mut() {
        let mut buf = io::BufReader::new(reader);
        let start = buf.stream_position().unwrap_or(0);
        io::copy(&mut buf, stdin)?;
        let end = buf.stream_position().unwrap_or(start);
        info!("成功向 clickhouse 插入了 {} 数据", ByteSize(end - start));
    } else {
        bail!("无法打开 stdin 来传输 clickhouse 所需的数据");
    }
    clickhouse_output(child.wait_with_output()?, cmd_string)?;
    Ok(())
}
