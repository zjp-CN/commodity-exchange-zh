use crate::{util, Result, Str};
use bincode::{Decode, Encode};
use calamine::{DataType, Reader};
use color_eyre::eyre::{Context, ContextCompat};
use indexmap::{Equivalent, IndexMap};
use serde::{Deserialize, Serialize};
use std::io;
use time::Date;

mod parse;
pub use parse::parse_download_links;
mod select;
pub use select::select;

pub static DOWNLOAD_LINKS: &[u8] = include_bytes!("../../tests/dce.bincode");
pub const URL_PREFIX: &str = "http://www.dce.com.cn";

#[derive(Debug, Decode, Encode, PartialEq, Eq)]
pub struct DownloadLinks(#[bincode(with_serde)] IndexMap<Key, String>);

impl DownloadLinks {
    pub fn new_static() -> Result<DownloadLinks> {
        let d = bincode::decode_from_slice::<DownloadLinks, _>(
            DOWNLOAD_LINKS,
            bincode::config::standard(),
        )?;
        Ok(d.0)
    }
    pub fn iter(&self) -> impl Iterator<Item = (&Key, &String)> {
        self.0.iter()
    }
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "tabled", derive(tabled::Tabled))]
pub struct Key {
    pub year: u16,
    pub name: Str,
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Key { year, name } = self;
        write!(f, "({year}, {name})")
    }
}

impl Equivalent<Key> for (u16, &str) {
    fn equivalent(&self, key: &Key) -> bool {
        self.0 == key.year && self.1 == key.name
    }
}

pub fn get_url(year: u16, name: &str) -> Result<String> {
    let index_map = &util::init_data().links_dce.0;
    let postfix = index_map
        .get(&(year, name))
        .with_context(|| format!("无法找到 {year} 年 {name} 品种的下载链接"))?;
    Ok(format!("{URL_PREFIX}{postfix}"))
}

/// 读取 xlsx 文件，并处理解析过的每行数据
pub fn read_xlsx<R: io::Read + io::Seek>(
    mut wb: calamine::Xlsx<R>,
    mut handle: impl FnMut(Data) -> Result<()>,
) -> Result<()> {
    let sheet = match wb.worksheet_range_at(0) {
        Some(Ok(sheet)) => sheet,
        Some(Err(err)) => bail!("无法读取第 0 个表，因为 {err:?}"),
        None => bail!("无法读取第 0 个表"),
    };
    let mut rows = sheet.rows();
    let header = rows.next().context("无法读取第一行")?;
    let pos = parse::parse_xslx_header(header)?;
    for row in rows {
        handle(Data::new(row, &pos)?)?;
    }
    Ok(())
}

pub fn run(year: u16, name: &str) -> Result<()> {
    let link = get_url(year, name)?;
    let xlsx = if link.ends_with(".xlsx") || link.ends_with(".csv") {
        // xxx.csv 其实也是 xlsx 文件 :(
        util::fetch(&link)?
    } else if link.ends_with(".zip") {
        // TODO: zip 压缩的是 csv 文件（文件名乱码），所以需要解析 （GBK 编码）
        // NOTE: zip 文件只在 2017 年及其之前提供，并且无法通过直接的 get 下载到
        //       （貌似最重要的是请求时带上 cookies，但它有时效性，很快失效，因此暂时不要 .zip 数据）
        // tests/snapshots/data__dce-downloadlink.snap
        // let mut v = Vec::with_capacity(1);
        util::fetch_zip(&link, |raw, fname| {
            let (csv, _) = util::read_txt(&raw, &fname)?;
            for line in csv.lines().take(3) {
                info!("{line}");
            }
            // v.push((raw, fname));
            Ok(())
        })?;
        return Ok(());
        // ensure!(
        //     v.len() == 1,
        //     "无法处理 {link} 内的多文件：{:?}",
        //     v.iter().map(|v| &v.1).collect::<Vec<_>>()
        // );
        // Cursor::new(v.remove(0).0)
    } else {
        bail!("暂时无法处理 {link}，因为只支持 xlsx 或者 zip 文件");
    };
    let len = xlsx.get_ref().len();
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .buffer_capacity(len)
        .from_writer(Vec::with_capacity(len));
    read_xlsx(calamine::Xlsx::new(xlsx)?, |data| {
        writer.serialize(&data)?;
        Ok(())
    })?;
    writer.flush()?;
    let fname = format!("dce-{year}-{name}.csv");
    let bytes = writer.get_ref();
    util::save_to_csv_and_clickhouse(
        || util::save_csv(bytes, &fname),
        || {
            util::clickhouse_execute(include_str!("../sql/dce.sql"))?;
            const TABLE: &str = "qihuo.dce";
            util::clichouse_insert_with_count_reported(TABLE, writer.get_ref())
        },
    )?;
    Ok(())
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "tabled", derive(tabled::Tabled))]
pub struct Data {
    /// 合约代码
    pub code: Str,
    /// 交易日期
    pub date: Date,
    /// 昨结算
    pub prev: f32,
    /// 今开盘
    pub open: f32,
    /// 最高价
    pub high: f32,
    /// 最低价
    pub low: f32,
    /// 今收盘
    pub close: f32,
    /// 今结算
    pub settle: f32,
    /// 涨跌1：涨幅百分数??
    pub zd1: f32,
    /// 涨跌2：涨跌数??
    pub zd2: f32,
    /// 成交量
    pub vol: u32,
    /// 交易额
    pub amount: u32,
    /// 持仓量
    pub position: u32,
}

impl Data {
    pub fn new(row: &[DataType], pos: &[usize]) -> Result<Data> {
        use parse::{as_date, as_f32, as_str, as_u32, LEN};

        ensure!(pos.len() == LEN, "xlsx 的表头有效列不足 {LEN}：{pos:?}");
        let err = |n: usize| format!("{row:?} 无法获取到第 {n} 个单元格数据");
        Ok(Data {
            code: as_str(row.get(pos[0]).with_context(|| err(0))?)?,
            date: as_date(row.get(pos[1]).with_context(|| err(1))?)?,
            prev: as_f32(row.get(pos[2]).with_context(|| err(2))?)?,
            open: as_f32(row.get(pos[3]).with_context(|| err(3))?)?,
            high: as_f32(row.get(pos[4]).with_context(|| err(4))?)?,
            low: as_f32(row.get(pos[5]).with_context(|| err(5))?)?,
            close: as_f32(row.get(pos[6]).with_context(|| err(6))?)?,
            settle: as_f32(row.get(pos[7]).with_context(|| err(7))?)?,
            zd1: as_f32(row.get(pos[8]).with_context(|| err(8))?)?,
            zd2: as_f32(row.get(pos[9]).with_context(|| err(9))?)?,
            vol: as_u32(row.get(pos[10]).with_context(|| err(10))?)?,
            amount: as_u32(row.get(pos[11]).with_context(|| err(11))?)?,
            position: as_u32(row.get(pos[12]).with_context(|| err(12))?)?,
        })
    }
}
