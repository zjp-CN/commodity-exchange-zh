use crate::{util::clickhouse, Result};
use color_eyre::eyre::Context;

pub fn run() -> Result<()> {
    let count = clickhouse::execute(include_str!("./sql/ce.sql"))?;
    let count = count
        .trim()
        .parse::<u32>()
        .with_context(|| format!("{count} 无法解析为 u32"))?;
    info!("qihuo.ce: 重新录入 {count} 条数据",);
    Ok(())
}
