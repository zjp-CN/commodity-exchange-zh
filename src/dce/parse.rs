use super::{Context, ContextCompat, DataType, Date, DownloadLinks, IndexMap, Key, Result, Str};

pub fn parse_download_links(html: &str) -> Result<DownloadLinks> {
    fn query_err(s: &str) -> String {
        format!("无法根据 `{s}` 搜索到内容")
    }
    fn as_tag_err(s: &str) -> String {
        format!("`{s}` 搜索到的内容无法识别为标签")
    }
    fn get_err(s: &str) -> String {
        format!("无法解析 `{s}`")
    }
    fn attribute_err(s: &str) -> String {
        format!("`{s}` attribute 没有值")
    }
    const UL: &str = r#"ul.cate_sel.clearfix[opentype="page"]"#;
    const LABEL: &str = "label";
    const INPUT: &str = r#"input[type="radio"][name="hisItem"]"#;
    const REL: &str = "rel";
    const OPTION: &str = "option";
    const OPT_VALUE: &str = "value";
    let dom = tl::parse(html, Default::default())?;
    let parser = dom.parser();
    let uls: Vec<_> = dom
        .query_selector(UL)
        .with_context(|| query_err(UL))?
        .collect();
    let options: Vec<_> = dom
        .query_selector(OPTION)
        .with_context(|| query_err(OPTION))?
        .collect();
    let (uls_len, options_len) = (uls.len(), options.len());
    ensure!(
        uls_len == options_len,
        "年份数量 {options_len} 与列表数量 {uls_len} 不相等，需检查 HTML"
    );
    let mut data = IndexMap::with_capacity(options_len * 16);
    for (option, ul) in options.into_iter().zip(uls) {
        let year_str = option
            .get(parser)
            .with_context(|| get_err(OPTION))?
            .as_tag()
            .with_context(|| as_tag_err(OPTION))?
            .attributes()
            .get(OPT_VALUE)
            .with_context(|| get_err(OPT_VALUE))?
            .with_context(|| attribute_err(OPT_VALUE))?
            .as_utf8_str();
        let year = year_str
            .parse::<u16>()
            .with_context(|| format!("年份 `{year_str}` 无法解析为 u16"))?;
        let labels = ul
            .get(parser)
            .with_context(|| get_err(UL))?
            .as_tag()
            .with_context(|| as_tag_err(UL))?
            .query_selector(parser, LABEL)
            .with_context(|| query_err(LABEL))?;
        for label in labels {
            let label = label.get(parser).with_context(|| get_err(LABEL))?;
            let input = label
                .as_tag()
                .with_context(|| as_tag_err(LABEL))?
                .query_selector(parser, INPUT)
                .with_context(|| query_err(INPUT))?
                .next()
                .with_context(|| query_err(INPUT))?
                .get(parser)
                .with_context(|| get_err(INPUT))?
                .as_tag()
                .with_context(|| as_tag_err(INPUT))?;
            data.insert(
                Key {
                    year,
                    name: label.inner_text(parser).into(),
                },
                input
                    .attributes()
                    .get(REL)
                    .with_context(|| get_err(REL))?
                    .with_context(|| attribute_err(REL))?
                    .as_utf8_str()
                    .into_owned(),
            );
        }
    }
    data.sort_keys();
    Ok(DownloadLinks(data))
}

/// Xlsx 中的数据的正确位置（不同年份具有不同的表头），因此需要先识别表头。
pub fn parse_xslx_header(header: &[DataType]) -> Result<Vec<usize>> {
    use Field::*;
    let mut pos = IndexMap::with_capacity(LEN);
    let mut cols = Vec::with_capacity(LEN + 8);
    for (idx, h) in header.iter().enumerate() {
        let col = h
            .get_string()
            .with_context(|| format!("无法按照字符串读取第一行：{header:?}"))?;
        cols.push(col);
        match col {
            "合约" => {
                pos.insert(合约, idx);
            }
            "日期" => {
                pos.insert(日期, idx);
            }
            // "前收盘价" => {
            //     pos.insert(前收盘价, idx);
            // }
            "前结算价" => {
                pos.insert(前结算价, idx);
            }
            "开盘价" => {
                pos.insert(开盘价, idx);
            }
            "最高价" => {
                pos.insert(最高价, idx);
            }
            "最低价" => {
                pos.insert(最低价, idx);
            }
            "收盘价" => {
                pos.insert(收盘价, idx);
            }
            "结算价" => {
                pos.insert(结算价, idx);
            }
            "涨跌1" => {
                pos.insert(涨跌1, idx);
            }
            "涨跌2" => {
                pos.insert(涨跌2, idx);
            }
            "成交量" => {
                pos.insert(成交量, idx);
            }
            "成交额" | "成交金额" => {
                // 2017 年 csv （实际 xlsx）这列出现“成交金额”
                pos.insert(成交额, idx);
            }
            "持仓量" => {
                pos.insert(持仓量, idx);
            }
            _ => (),
        }
    }
    const FIELDS: [Field; LEN] = [
        合约,
        日期,
        // 前收盘价,
        前结算价,
        开盘价,
        最高价,
        最低价,
        收盘价,
        结算价,
        涨跌1,
        涨跌2,
        成交量,
        成交额,
        持仓量,
    ];
    let len = pos.len();
    if len != LEN {
        let missing: Vec<_> = FIELDS
            .into_iter()
            .filter(|f| pos.get(f).is_none())
            .collect();
        bail!(
            "xlsx 的表头有效列只有 {len} 个（不足 {LEN}），\
            缺少 {missing:?}\n有效列应为 {FIELDS:?}\n但实际列为 {cols:?}"
        );
    }
    // 通过 HashMap 确定所有字段在第几列，并按字段顺序解析
    // 所以 Field 的变体顺序与 Data 的字段顺序必须一致
    pos.sort_keys();
    Ok(pos.into_iter().map(|v| v.1).collect())
}

pub fn as_str(cell: &DataType) -> Result<Str> {
    cell.get_string()
        .map(Str::from)
        .with_context(|| format!("{cell:?} 无法读取为 &str"))
}

pub fn as_date(cell: &DataType) -> Result<Date> {
    use time::{format_description::FormatItem, macros::format_description};
    const FMT: &[FormatItem<'static>] = format_description!("[year][month][day]");
    if let Ok(s) = as_str(cell) {
        Date::parse(&s, &FMT).map_err(|err| eyre!("{s} 无法解析为日期：{err:?}"))
    } else if let Ok(u) = as_u32(cell) {
        let year = u / 10000;
        let minus_year = u - year * 10000;
        let month = (minus_year) / 100;
        let day = minus_year - month * 100;
        Ok(Date::from_calendar_date(
            year.try_into()
                .with_context(|| format!("{year} 无法转成 i32"))?,
            u8::try_from(month)
                .with_context(|| format!("{month} 无法转成 u8"))?
                .try_into()
                .with_context(|| format!("{month} 无法转成月份"))?,
            u8::try_from(day).with_context(|| format!("{day} 无法转成 u8"))?,
        )?)
    } else {
        bail!("{cell} 无法通过 as_str 或 as_u32 解析")
    }
}

pub fn as_f32(cell: &DataType) -> Result<f32> {
    cell.get_float()
        .map(|f| f as f32)
        .with_context(|| format!("{cell:?} 无法读取为 f32"))
}

pub fn as_u32(cell: &DataType) -> Result<u32> {
    if let Some(f) = cell.get_float() {
        Ok(f as u32)
    } else if let Some(int) = cell.get_int() {
        int.try_into()
            .map_err(|err| eyre!("{int}i64 无法转化为 u32：{err:?}"))
    } else {
        bail!("{cell:?} 无法读取为 u32")
    }
}

/// 注意：源数据中多了一列“前收盘价”，但尚未研究它；由于 czce 数据不具备它，所以舍弃。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Field {
    合约,
    日期,
    // 前收盘价,
    前结算价,
    开盘价,
    最高价,
    最低价,
    收盘价,
    结算价,
    涨跌1,
    涨跌2,
    成交量,
    成交额,
    持仓量,
}
pub const LEN: usize = 13;
