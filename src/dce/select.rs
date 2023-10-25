use super::{get_url, Key, Result};
use crate::util::init_data;
use inquire::{InquireError, MultiSelect};

pub fn select(with_options: bool) -> Result<()> {
    let year_name = init_data().links_dce.iter().map(|(k, _)| k);
    let options: Vec<_> = if with_options {
        year_name.collect()
    } else {
        year_name.filter(|k| !k.name.contains("期权")).collect()
    };
    let msg = format!(
        "请从大连交易所的 {} 条链接中选择 (年份, 品种)",
        options.len()
    );
    let keys = match MultiSelect::new(&msg, options).prompt() {
        Ok(keys) => keys,
        // <Ctrl-c> or <Esc>
        Err(InquireError::OperationInterrupted | InquireError::OperationCanceled) => return Ok(()),
        Err(err) => bail!("交互出现问题：{err:?}"),
    };
    for Key { year, name } in keys {
        println!("{}", get_url(*year, name)?);
    }
    Ok(())
}
