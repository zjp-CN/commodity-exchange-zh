use crate::Str;
use time::Date;

pub struct Data {
    /// 合约代码
    code: Str,
    /// 交易日期
    date: Date,
    /// 昨结算
    prev: f32,
    /// 今开盘
    open: f32,
    /// 最高价
    high: f32,
    /// 最低价
    low: f32,
    /// 今收盘
    close: f32,
    /// 今结算
    settle: f32,
    /// 涨跌1：涨幅百分数??
    zd1: f32,
    /// 涨跌2：涨跌数??
    zd2: f32,
    /// 成交量
    vol: u32,
    /// 交易额
    amount: u64,
    /// 持仓量
    position: u32,
}
