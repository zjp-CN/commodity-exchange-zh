use crate::Result;
use std::{io::ErrorKind, path::PathBuf, sync::OnceLock};

/// 测试函数的日志
#[cfg(test)]
pub fn init_log() -> &'static Init {
    use simplelog::{Config, LevelFilter, SimpleLogger};
    SimpleLogger::init(LevelFilter::Info, Config::default()).expect("logger initialization failed");
    init_data()
}

pub struct Init {
    pub cache_dir: PathBuf,
}

fn init_data() -> &'static Init {
    static DATA: OnceLock<Init> = OnceLock::new();
    DATA.get_or_init(|| Init {
        cache_dir: cache_dir().unwrap(),
    })
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




#[test]
fn parse_float() {
    dbg!(lexical::parse::<f64, _>("3,000.5"));
}
