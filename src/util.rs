use crate::{dce, Result};
use bytesize::ByteSize;
use regex::Regex;
use serde::{Deserialize, Deserializer};
use simplelog::{
    ColorChoice, Config, ConfigBuilder, LevelFilter, SimpleLogger, TermLogger, TerminalMode,
};
use std::{
    borrow::Cow,
    fs::File,
    io::{self, Cursor, ErrorKind, Write},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::OnceLock,
};
use time::{format_description::FormatItem, macros::format_description, Date, OffsetDateTime};

/// 开启日志
pub fn init_log() -> Result<()> {
    let level = std::env::var("LOG").map_or_else(
        |_| LevelFilter::Info,
        |l| l.parse().unwrap_or(LevelFilter::Off),
    );
    let mut config = ConfigBuilder::new();
    config.set_time_offset(time::UtcOffset::from_hms(8, 0, 0)?);
    TermLogger::init(
        level,
        config.build(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )?;
    Ok(())
}

/// 测试函数的日志
#[doc(hidden)]
pub fn init_test_log() -> &'static Init {
    let level = std::env::var("TEST_LOG").map_or_else(
        |_| LevelFilter::Off,
        |l| l.parse().unwrap_or(LevelFilter::Off),
    );
    if SimpleLogger::init(level, Config::default()).is_err() {
        error!("日志开启失败，或许已经设置了日志");
    }
    init_data()
}

#[cfg(feature = "tabled")]
pub fn display_option<T: std::fmt::Display>(t: &Option<T>) -> String {
    match t {
        Some(val) => val.to_string(),
        None => String::new(),
    }
}

pub struct Init {
    pub cache_dir: PathBuf,
    pub regex_czce: Regex,
    pub this_year: u16,
    pub links_dce: dce::DownloadLinks,
}

pub fn init_data() -> &'static Init {
    static DATA: OnceLock<Init> = OnceLock::new();
    DATA.get_or_init(|| Init {
        cache_dir: cache_dir().unwrap(),
        regex_czce: Regex::new(",| ").unwrap(),
        this_year: OffsetDateTime::now_utc()
            .to_offset(time::macros::offset!(+8))
            .year()
            .try_into()
            .unwrap(),
        links_dce: dce::DownloadLinks::new_static().unwrap(),
    })
}

pub type Response = Result<Cursor<Vec<u8>>>;

pub fn fetch(url: &str) -> Response {
    let mut buf = Vec::with_capacity(1024 * 1024 * 4);
    ureq::get(url).call()?.into_reader().read_to_end(&mut buf)?;
    info!("{url} 获取的字节数：{}", ByteSize(buf.len() as u64));
    Ok(Cursor::new(buf))
}

pub fn parse_date_czce<'de, D: Deserializer<'de>>(d: D) -> Result<Date, D::Error> {
    const FMT: &[FormatItem<'static>] = format_description!("[year]-[month]-[day]");
    let s = <&str>::deserialize(d)?;
    Ok(Date::parse(s, FMT)
        .map_err(|err| format!("{s:?} 无法解析成日期：{err:?}"))
        .unwrap())
}

pub fn parse_option_f32<'de, D: Deserializer<'de>>(d: D) -> Result<Option<f32>, D::Error> {
    let s = <&str>::deserialize(d)?;
    if s.is_empty() {
        Ok(None)
    } else {
        let float = s
            .parse()
            .map_err(|err| format!("{s:?} 无法解析为 f32：{err:?}"))
            .unwrap();
        Ok(Some(float))
    }
}

pub fn parse_u32_from_f32<'de, D: Deserializer<'de>>(d: D) -> Result<u32, D::Error> {
    let s = <&str>::deserialize(d)?;
    let float: f32 = s
        .parse()
        .map_err(|_| format!("{s} 无法解析为 f32"))
        .unwrap();
    if float < 0.0 {
        panic!("{s} 无法从 f32 转化为 u32")
    } else {
        Ok(float as _)
    }
}

pub enum Encoding {
    UTF8,
    GBK,
}

pub fn fetch_zip(
    url: &str,
    mut handle_unzipped: impl FnMut(Vec<u8>, String) -> Result<()>,
) -> Result<()> {
    let mut fetched = fetch(url)?;
    let mut zipped = match zip::ZipArchive::new(&mut fetched) {
        Ok(data) => data,
        Err(err) => {
            let file = init_data().cache_dir.join("failed");
            File::create(&file)?.write_all(fetched.get_ref())?;
            bail!(
                "无法解析 zip 文件，下载的内容保存在 {}：{err:?}",
                file.display()
            );
        }
    };
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
                .and_then(|fname| Some(fname.to_str()?.to_owned()))
                .ok_or_else(|| eyre!("无法从 {unzipped_path:?} 中获取文件名"))?;
            let mut buf = Vec::with_capacity(size as usize);
            io::copy(&mut unzipped, &mut buf)?;
            handle_unzipped(buf, file_name)?;
        } else {
            bail!("{} 还未实现解压成文件夹", unzipped.name());
        }
    }
    Ok(())
}

/// 处理编码
pub fn read_txt<'a>(buf: &'a [u8], src: &str) -> Result<(Cow<'a, str>, Encoding)> {
    let content_encoding = match std::str::from_utf8(buf) {
        Ok(s) => (s.into(), Encoding::UTF8),
        Err(_) => {
            let gbk = encoding_rs::GBK;
            info!("{src} 不是 UTF8 编码的，尝试使用 GBK 解码");
            let (cow, encoding, err) = gbk.decode(buf);
            if err {
                bail!("{src} 不是 GBK 编码的，需要手动确认编码");
            } else if encoding != gbk {
                bail!("{src} GBK/{encoding:?} 解码失败");
            }
            (cow, Encoding::GBK)
        }
    };
    Ok(content_encoding)
}

/// 缓存目录
pub fn cache_dir() -> Result<PathBuf> {
    const CACHE: &str = "cache";
    let dir = PathBuf::from(CACHE);
    match std::fs::create_dir(&dir) {
        Ok(_) => debug!("成功创建 {CACHE} 目录"),
        Err(err) => {
            if matches!(err.kind(), ErrorKind::AlreadyExists) {
                debug!("{CACHE} 已存在");
            } else {
                bail!("无法创建 {CACHE}，因为 {err:?}");
            }
        }
    }
    Ok(dir)
}

pub fn save_csv(bytes: &[u8], filename: impl AsRef<Path>) -> Result<PathBuf> {
    let fname = filename.as_ref();
    let mut path = init_data().cache_dir.join(fname);
    if !path.set_extension("csv") {
        error!("{} 无法设置 csv 文件名后缀", fname.display());
    }
    File::create(&path)?.write_all(bytes)?;
    info!(
        "{} 已被写入 ({})",
        path.display(),
        ByteSize(bytes.len() as _)
    );
    Ok(path)
}

pub fn save_to_csv_and_clickhouse<F, G>(csv: F, ch: G) -> Result<()>
where
    F: Send + FnOnce() -> Result<PathBuf>,
    G: Send + FnOnce() -> Result<()>,
{
    std::thread::scope(|s| {
        let task1 = s.spawn(csv);
        let task2 = s.spawn(ch);
        match task1.join() {
            Ok(res) => _ = res?,
            Err(err) => bail!("save_csv 运行失败：{err:?}"),
        }
        match task2.join() {
            Ok(res) => res?,
            Err(err) => bail!("保存到 clickhouse 运行失败：{err:?}"),
        }
        Ok(())
    })
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

pub fn clichouse_insert_with_count_reported(table: &str, bytes: &[u8]) -> Result<()> {
    let sql_count = format!("SELECT count(*) FROM {table}");
    let count_old = clickhouse_execute(&sql_count)?;
    info!("{table} 现有数据 {count_old} 条");
    let sql_insert_csv = format!("INSERT INTO {table} FORMAT CSV");
    clickhouse_insert(&sql_insert_csv, io::Cursor::new(bytes))?;
    clickhouse_execute(&format!("OPTIMIZE TABLE {table} DEDUPLICATE BY date, code"))?;
    info!("{table} 已去重");
    let count_new = clickhouse_execute(&sql_count)?;
    let added = count_new
        .parse::<u64>()
        .ok()
        .zip(count_old.parse::<u64>().ok())
        .and_then(|(new, old)| new.checked_sub(old).map(|r| r.to_string()))
        .unwrap_or_default();
    info!("{table} 现有数据 {count_new} 条（增加了 {added} 条）");
    Ok(())
}
