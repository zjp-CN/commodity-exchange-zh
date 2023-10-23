use crate::{Result, Str};
use bincode::{Decode, Encode};
use color_eyre::eyre::{Context, ContextCompat};
use time::Date;

pub static DOWNLOAD_LINKS: &[u8] = include_bytes!("../tests/dce.bincode");

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Decode, Encode)]
#[cfg_attr(feature = "tabled", derive(tabled::Tabled))]
pub struct DownloadLink {
    pub year: u16,
    #[bincode(with_serde)]
    pub name: Str,
    pub link: String,
}

pub fn parse_download_links(html: &str) -> Result<Vec<DownloadLink>> {
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
    if uls_len != options_len {
        bail!("年份数量 {options_len} 与列表数量 {uls_len} 不相等，需检查 HTML");
    }
    let mut data = Vec::with_capacity(options_len);
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
        let v: Result<Vec<_>> = labels
            .map(|label| -> Result<_> {
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
                Ok((
                    input
                        .attributes()
                        .get(REL)
                        .with_context(|| get_err(REL))?
                        .with_context(|| attribute_err(REL))?
                        .as_utf8_str(),
                    label.inner_text(parser),
                ))
            })
            .collect();
        data.push((year, v?));
    }

    Ok(data
        .into_iter()
        .flat_map(|(year, v)| {
            v.into_iter().map(move |(link, name)| DownloadLink {
                year,
                name: name.into(),
                link: link.into(),
            })
        })
        .collect())
}

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
    pub amount: u64,
    /// 持仓量
    pub position: u32,
}
