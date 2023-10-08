use commodity_exchange_zh::{czce::parse_txt, util, Result};
use insta::assert_display_snapshot as shot;
use regex::Regex;
use std::{
    fs::File,
    io::{BufReader, Read},
};
use tabled::Table;

#[test]
fn test_parse_txt() -> Result<()> {
    let init = util::init_test_log();
    let mut file = BufReader::new(File::open(init.cache_dir.join("ALLFUTURES2023.txt"))?);
    let capacity = file.get_ref().metadata()?.len() as usize;
    let mut buf = String::with_capacity(capacity);
    file.read_to_string(&mut buf)?;
    let pos = Regex::new("\n")?.find_iter(&buf).nth(5).unwrap().end();
    let mut v = Vec::new();
    parse_txt(&buf[..pos], Some(|data| v.push(data)))?;
    shot!(Table::new(v), @r###"
    +------------+----------+--------+--------+--------+--------+--------+--------+-------+-------+--------+--------+--------+--------------+------------+
    | 交易日期   | 合约代码 | 昨结算 | 今开盘 | 最高价 | 最低价 | 今收盘 | 今结算 | 涨跌1 | 涨跌2 | 成交量 | 持仓量 | 增减量 | 交易额（万） | 交割结算价 |
    +------------+----------+--------+--------+--------+--------+--------+--------+-------+-------+--------+--------+--------+--------------+------------+
    | 2023-01-03 | AP303    | 8284   | 8305   | 8586   | 8300   | 8586   | 8486   | 302   | 202   | 8129   | 29539  | -2084  | 68985.41     |            |
    +------------+----------+--------+--------+--------+--------+--------+--------+-------+-------+--------+--------+--------+--------------+------------+
    | 2023-01-03 | AP304    | 8058   | 8058   | 8484   | 8058   | 8484   | 8376   | 426   | 318   | 2342   | 21181  | -859   | 19616.25     |            |
    +------------+----------+--------+--------+--------+--------+--------+--------+-------+-------+--------+--------+--------+--------------+------------+
    | 2023-01-03 | AP305    | 7872   | 7920   | 8358   | 7915   | 8351   | 8226   | 479   | 354   | 269484 | 206088 | 34044  | 2216829.3    |            |
    +------------+----------+--------+--------+--------+--------+--------+--------+-------+-------+--------+--------+--------+--------------+------------+
    "###);
    Ok(())
}
