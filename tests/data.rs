use calamine::Reader;
use commodity_exchange_zh::{
    czce::{clickhouse_execute, clickhouse_insert, parse_txt},
    dce::{parse_download_links, read_dce_xlsx, DownloadLinks, DOWNLOAD_LINKS},
    ensure, util, Result,
};
use insta::assert_display_snapshot as shot;
use regex::Regex;
use std::{
    fs::File,
    io::{BufReader, Read, Write},
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

#[test]
fn test_clickhouse() -> Result<()> {
    color_eyre::install()?;
    util::init_test_log();
    const TABLE: &str = "qihuo._testing_czce";

    let mut sql = format!(
        "\
DROP TABLE IF EXISTS {TABLE};
CREATE TABLE IF NOT EXISTS {TABLE} (
  date Date COMMENT '日期',
  code String COMMENT '合约代码',
  prev Float32 COMMENT '昨结算',
  open Float32 COMMENT '开盘价',
  high Float32 COMMENT '最高价',
  low Float32 COMMENT '最低价',
  close Float32 COMMENT '收盘价',
  settle Float32 COMMENT '结算价',
  zd1 Float32 COMMENT '涨跌1：涨幅百分数??',
  zd2 Float32 COMMENT '涨跌2：涨跌数??',
  vol UInt32 COMMENT '成交量',
  position UInt32 COMMENT '持仓量',
  pos_delta Int32 COMMENT '增减量',
  amount Float32 COMMENT '交易额（万）',
  dsp Nullable(Float32) COMMENT '交割结算价'
) ENGINE = ReplacingMergeTree
PRIMARY KEY(date, code)
ORDER BY (date, code);
"
    );
    clickhouse_execute(&sql)?;

    sql = format!("SET format_csv_delimiter = '|'; INSERT INTO {TABLE} FORMAT CSV");
    clickhouse_insert(&sql, File::open("cache/郑州-ALLFUTURES2022.csv")?)?;

    sql = format!("SELECT count(*) FROM {TABLE}; DROP TABLE IF EXISTS {TABLE}");
    shot!(clickhouse_execute(&sql)?, @"47916");

    Ok(())
}

// <ul class="cate_sel clearfix" opentype="page">
//  <li><label><input type="radio" name="hisItem" rel="/dalianshangpin/resource/cms/article/6301842/6302967/2022012410265645980.xlsx">生猪</label></li>
#[test]
fn test_dce_html() -> Result<()> {
    let html = include_str!("dce.html");
    let data = parse_download_links(html)?;
    let mut years: Vec<_> = data.iter().map(|(k, _)| k.year).collect();
    years.dedup();
    shot!(years.len(), @"17"); // 年数
    shot!(format!("{:?}", years), @"[2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]");
    shot!(data.len(), @"260"); // 链接数量
    shot!("dce-downloadlink", Table::new(data.iter()));

    // 序列化测试
    let config = bincode::config::standard();
    let buf = bincode::encode_to_vec(&data, config)?;
    ensure!(
        buf == DOWNLOAD_LINKS,
        "测试目录下的 dce.bincode 字节与库中的不一致"
    );
    const FILE: &str = "dce.bincode";
    let file = std::path::Path::new("tests").join(FILE);
    let buf_size = buf.len();
    if let Ok(mut dce) = File::open(&file) {
        let mut buf_file = Vec::with_capacity(buf_size);
        ensure!(
            dce.read_to_end(&mut buf_file)? == buf_size,
            "最新数据与 dce.bincode 数据长度不一致"
        );
        ensure!(
            buf_file == buf,
            "测试解析的序列化结果与 dce.bincode 文件不一致"
        );
    } else {
        // 如果无 dec.bincode 文件，则生成
        File::create(&file)?.write_all(&buf)?;
        println!(
            "已写入 {} ({})",
            file.display(),
            bytesize::ByteSize(buf_size as u64)
        );
    }

    // 反序列化测试
    let (lib_data, size) = bincode::decode_from_slice::<DownloadLinks, _>(DOWNLOAD_LINKS, config)?;
    ensure!(
        lib_data == data,
        "测试解析的 dce.bincode 与库中反序列化的 Vec<DownloadLink> 不一致"
    );
    ensure!(
        buf_size == size,
        "测试目录下的 dce.bincode 大小与库中的不一致"
    );
    Ok(())
}

#[test]
fn dce_xlsx() -> Result<()> {
    let file = "cache/v.xlsx";
    let mut wb: calamine::Xlsx<_> = calamine::open_workbook(file)?;
    let end_row = wb.worksheet_range_at(0).unwrap()?.end().unwrap().0 as usize;
    let mut table = Vec::with_capacity(end_row);
    read_dce_xlsx(wb, |data| {
        table.push(data);
        Ok(())
    })?;
    let len = table.len();
    ensure!(
        end_row == len,
        "{file} 总共有 {end_row} 条数据，但只解析了 {len} 条"
    );
    shot!("dce-v-2022", Table::new(table));
    Ok(())
}
