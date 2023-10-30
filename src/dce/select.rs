use super::{get_url, run, Key, Result};
use crate::util::init_data;
use inquire::{InquireError, MultiSelect};

/// `Ok(Some(()))` 表示正常运行；
/// `Ok(None)`     表示被中断；
/// `Err(...)`     表示交互问题。
pub fn select(with_options: bool) -> Result<Option<()>> {
    let year_name = init_data().links_dce.iter();
    let options: Vec<_> = if with_options {
        year_name.map(|(k, _)| k).collect()
    } else {
        year_name
            .filter(|(k, link)| !(k.name.contains("期权") || link.ends_with(".zip")))
            .map(|(k, _)| k)
            .collect()
    };
    let msg = format!(
        "请从大连交易所的 {} 条链接中选择 (年份, 品种)",
        options.len()
    );
    let keys = match MultiSelect::new(&msg, options).prompt() {
        Ok(keys) => keys,
        // <Ctrl-c> or <Esc>
        Err(InquireError::OperationInterrupted | InquireError::OperationCanceled) => {
            return Ok(None)
        }
        Err(err) => bail!("交互出现问题：{err:?}"),
    };
    for &Key { year, ref name } in keys {
        info!("正在从 {} 下载文件", get_url(year, name)?);
        run(year, name)?;
    }
    Ok(Some(()))
}
