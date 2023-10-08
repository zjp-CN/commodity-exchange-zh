use crate::Result;
use bytesize::ByteSize;
use regex::Regex;
use serde::{Deserialize, Deserializer};
use std::{
    io::{Cursor, ErrorKind},
    path::PathBuf,
    sync::OnceLock,
};
use time::{format_description::FormatItem, macros::format_description, Date};

/// 测试函数的日志
#[cfg(test)]
pub fn init_log() -> &'static Init {
    use simplelog::{Config, LevelFilter, SimpleLogger};
    let level = std::env::var("TEST_LOG").map_or_else(
        |_| LevelFilter::Off,
        |l| l.parse().unwrap_or(LevelFilter::Off),
    );
    SimpleLogger::init(level, Config::default()).expect("logger initialization failed");
    init_data()
}

pub struct Init {
    pub cache_dir: PathBuf,
    pub regex_czce: Regex,
}

pub fn init_data() -> &'static Init {
    static DATA: OnceLock<Init> = OnceLock::new();
    DATA.get_or_init(|| Init {
        cache_dir: cache_dir().unwrap(),
        regex_czce: Regex::new(",| ").unwrap(),
    })
}

pub type Response = Result<Cursor<Vec<u8>>>;

pub fn fetch(url: &str) -> Response {
    let bytes = minreq::get(url).send()?.into_bytes();
    info!("{url} 获取的字节数：{}", ByteSize(bytes.len() as u64));
    Ok(Cursor::new(bytes))
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
            .map_err(|err| format!("{s:?} 无法解析为 f32"))
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
                return Err(format!("无法创建 {CACHE}，因为 {err:?}").into());
            }
        }
    }
    Ok(dir)
}
