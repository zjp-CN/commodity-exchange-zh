use crate::Str;
use time::Date;

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
