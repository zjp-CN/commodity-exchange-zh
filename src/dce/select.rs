use super::{get_download_link, Key, Result};
use crate::util::init_data;
use inquire::MultiSelect;

pub fn select(with_options: bool) -> Result<()> {
    let year_name = init_data().links_dce.iter().map(|(k, _)| k);
    let options: Vec<_> = if with_options {
        year_name.collect()
    } else {
        year_name.filter(|k| !k.name.contains("期权")).collect()
    };
    let msg = format!(
        "请从大连交易所下的 {} 条链接中选择获取的 (年份, 品种)",
        options.len()
    );
    for Key { year, name } in MultiSelect::new(&msg, options).prompt()? {
        println!("{}", get_download_link(*year, name)?);
    }
    Ok(())
}
