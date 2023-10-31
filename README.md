# 商品交易所数据获取工具 (ce)

[![Release](https://github.com/zjp-CN/commodity-exchange-zh/actions/workflows/release.yml/badge.svg)](https://github.com/zjp-CN/commodity-exchange-zh/actions/workflows/release.yml)
[![Crates.io](https://img.shields.io/crates/v/commodity-exchange-zh)](https://crates.io/crates/commodity-exchange-zh)
![Crates.io](https://img.shields.io/crates/d/commodity-exchange-zh)

ce = **c**ommodity **e**xchange

## 安装方式

* 方式一：`cargo install commodity-exchange-zh`，然后使用 `ce` 获取数据
* 方式二：[release] 页面下载编译好的最新二进制文件

[release]: https://github.com/zjp-CN/commodity-exchange-zh/releases

```bash
$ ce help
Usage: ce <command> [<args>]

下载、解析和保存商品期货交易所数据。子命令示例：

* `czce -y 2010..2023`：下载郑州交易所 2010 至 2022 年所有合约数据
* `dce -y 2020..=2023 C M`：下载大连交易所 2019 至 2022 年玉米和豆粕两个品种的数据
* `dce`：交互式选择大连交易所年份和品种

Options:
  --help            display usage information

Commands:
  czce              郑州交易所
  dce               大连交易所
```

## 准备

* [clickhouse] 数据库

[clickhouse]: https://clickhouse.com/

## 解析说明

交易所给的数据是公开的、免费下载的，但需要很多校验和清洗。

### 郑州交易所 (czce)

对于郑州交易所的数据，情况还算好，因为
* 提供一整年每个交易日完整的所有品种合约数据：获取容易，只需要请求哪一年的
* 最新交易日会直接更新，且下载链接不变：每次获取当前年都是最新数据
* 提供 xlsx 和 csv 两种格式（但 xlsx 貌似为机写，无法正确读取，所以只解析 csv）
* 2020-01-01 之后的 成交量、持仓量、成交额 字段为单边计算：拼接历史数据需要统一把
  2020 年前的那些字段做单边处理
* 成交额单位为万元

### 大连交易所 (dce)

对于大连交易所的数据，情况很糟糕，因为
* 只提供单品种年数据：与 czce 相比，除了需要哪一年，还需要哪个品种，搜集起来很麻烦
* 年份久远（早于 2017 年）的数据暂时无法轻松获取，因为它们以 zip 格式提供（并且解压为真正的 csv 格式）；
  2017 年之后为 xlsx 格式
* 有时年与年的数据都不太一样：比如直接提供的 .csv 文件其实为 .xlsx 文件、列数据类型有时为 float，有时为
  string、原本相同列的名称与往年些许不一致
* 不提供当年年数据
* 成交量、持仓量为双边，但成交额疑似为单边
* 成交额单位为元

### 共同点

* 按年提供
* 每个交易所每年的字段都可能与其自身历史数据不一致（核心字段相同）
* 涉及单双边的字段都需要统一处理成单边
* 成交额需要统一单位：万元

```SQL
-- 适用于 czce/dce
CREATE TABLE IF NOT EXISTS qihuo.ce (
  date     Date    COMMENT '日期',
  code     String  COMMENT '合约代码',
  open     Float32 COMMENT '开盘价',
  high     Float32 COMMENT '最高价',
  low      Float32 COMMENT '最低价',
  close    Float32 COMMENT '收盘价',
  settle   Float32 COMMENT '结算价',
  vol      UInt32  COMMENT '成交量（单边）',
  amount   Float32 COMMENT '交易额（万元）',
  position UInt32  COMMENT '持仓量（单边）',
  ce       Enum('czce' = 1, 'dce' = 2) COMMENT '交易所'
) ENGINE = ReplacingMergeTree
PRIMARY KEY (ce, date, code)
ORDER BY    (ce, date, code);

```

<details>

  <summary>示例：所有甲醇合约代码以及开始、结束日期</summary>

```SQL
SELECT *
FROM
(
    SELECT
        ce,
        code,
        first_value(date) AS start,
        last_value(date) AS end
    FROM qihuo.ce
    GROUP BY (ce, code)
    ORDER BY start ASC
)
WHERE code LIKE 'MA%'

┌─ce───┬─code──┬──────start─┬────────end─┐
│ czce │ MA506 │ 2014-06-17 │ 2015-06-12 │
│ czce │ MA507 │ 2014-07-15 │ 2015-07-14 │
│ czce │ MA508 │ 2014-08-15 │ 2015-08-14 │
│ czce │ MA509 │ 2014-09-16 │ 2015-09-16 │
│ czce │ MA510 │ 2014-10-22 │ 2015-10-21 │
│ czce │ MA511 │ 2014-11-17 │ 2015-11-13 │
│ czce │ MA512 │ 2014-12-15 │ 2015-12-14 │
│ czce │ MA601 │ 2015-01-19 │ 2016-01-15 │
│ czce │ MA602 │ 2015-02-16 │ 2016-02-19 │
│ czce │ MA603 │ 2015-03-16 │ 2016-03-14 │
│ czce │ MA604 │ 2015-04-16 │ 2016-04-15 │
│ czce │ MA605 │ 2015-05-18 │ 2016-05-16 │
│ czce │ MA606 │ 2015-06-15 │ 2016-06-16 │
│ czce │ MA607 │ 2015-07-15 │ 2016-07-14 │
│ czce │ MA608 │ 2015-08-17 │ 2016-08-12 │
│ czce │ MA609 │ 2015-09-17 │ 2016-09-14 │
│ czce │ MA610 │ 2015-10-22 │ 2016-10-21 │
│ czce │ MA611 │ 2015-11-16 │ 2016-11-14 │
│ czce │ MA612 │ 2015-12-15 │ 2016-12-14 │
│ czce │ MA701 │ 2016-01-18 │ 2017-01-16 │
│ czce │ MA702 │ 2016-02-22 │ 2017-02-16 │
│ czce │ MA703 │ 2016-03-15 │ 2017-03-14 │
│ czce │ MA704 │ 2016-04-18 │ 2017-04-18 │
│ czce │ MA705 │ 2016-05-17 │ 2017-05-15 │
│ czce │ MA706 │ 2016-06-17 │ 2017-06-14 │
│ czce │ MA707 │ 2016-07-15 │ 2017-07-14 │
│ czce │ MA708 │ 2016-08-15 │ 2017-08-14 │
│ czce │ MA709 │ 2016-09-19 │ 2017-09-14 │
│ czce │ MA710 │ 2016-10-24 │ 2017-10-20 │
│ czce │ MA711 │ 2016-11-15 │ 2017-11-14 │
│ czce │ MA712 │ 2016-12-15 │ 2017-12-14 │
│ czce │ MA801 │ 2017-01-17 │ 2018-01-15 │
│ czce │ MA802 │ 2017-02-17 │ 2018-02-14 │
│ czce │ MA803 │ 2017-03-15 │ 2018-03-14 │
│ czce │ MA804 │ 2017-04-19 │ 2018-04-17 │
│ czce │ MA805 │ 2017-05-16 │ 2018-05-15 │
│ czce │ MA806 │ 2017-06-15 │ 2018-06-14 │
│ czce │ MA807 │ 2017-07-17 │ 2018-07-13 │
│ czce │ MA808 │ 2017-08-15 │ 2018-08-14 │
│ czce │ MA809 │ 2017-09-15 │ 2018-09-14 │
│ czce │ MA810 │ 2017-10-23 │ 2018-10-19 │
│ czce │ MA811 │ 2017-11-15 │ 2018-11-14 │
│ czce │ MA812 │ 2017-12-15 │ 2018-12-14 │
│ czce │ MA901 │ 2018-01-16 │ 2019-01-15 │
│ czce │ MA902 │ 2018-02-22 │ 2019-02-21 │
│ czce │ MA903 │ 2018-03-15 │ 2019-03-14 │
│ czce │ MA904 │ 2018-04-18 │ 2019-04-15 │
│ czce │ MA905 │ 2018-05-16 │ 2019-05-17 │
│ czce │ MA906 │ 2018-06-15 │ 2019-06-17 │
│ czce │ MA907 │ 2018-07-16 │ 2019-07-12 │
│ czce │ MA908 │ 2018-08-15 │ 2019-08-14 │
│ czce │ MA909 │ 2018-09-17 │ 2019-09-16 │
│ czce │ MA910 │ 2018-10-22 │ 2019-10-21 │
│ czce │ MA911 │ 2018-11-15 │ 2019-11-14 │
│ czce │ MA912 │ 2018-12-17 │ 2019-12-13 │
│ czce │ MA001 │ 2019-01-16 │ 2020-01-15 │
│ czce │ MA002 │ 2019-02-22 │ 2020-02-14 │
│ czce │ MA003 │ 2019-03-15 │ 2020-03-13 │
│ czce │ MA004 │ 2019-04-16 │ 2020-04-15 │
│ czce │ MA005 │ 2019-05-20 │ 2020-05-19 │
│ czce │ MA006 │ 2019-06-18 │ 2020-06-12 │
│ czce │ MA007 │ 2019-07-15 │ 2020-07-14 │
│ czce │ MA008 │ 2019-08-15 │ 2020-08-14 │
│ czce │ MA009 │ 2019-09-17 │ 2020-09-14 │
│ czce │ MA010 │ 2019-10-22 │ 2020-10-22 │
│ czce │ MA011 │ 2019-11-15 │ 2020-11-13 │
│ czce │ MA012 │ 2019-12-16 │ 2020-12-14 │
│ czce │ MA101 │ 2020-01-16 │ 2021-01-15 │
│ czce │ MA102 │ 2020-02-17 │ 2021-02-19 │
│ czce │ MA103 │ 2020-03-16 │ 2021-03-12 │
│ czce │ MA104 │ 2020-04-16 │ 2021-04-15 │
│ czce │ MA105 │ 2020-05-20 │ 2021-05-19 │
│ czce │ MA106 │ 2020-06-15 │ 2021-06-15 │
│ czce │ MA107 │ 2020-07-15 │ 2021-07-14 │
│ czce │ MA108 │ 2020-08-17 │ 2021-08-13 │
│ czce │ MA109 │ 2020-09-15 │ 2021-09-14 │
│ czce │ MA110 │ 2020-10-23 │ 2021-10-21 │
│ czce │ MA111 │ 2020-11-16 │ 2021-11-12 │
│ czce │ MA112 │ 2020-12-15 │ 2021-12-14 │
│ czce │ MA201 │ 2021-01-18 │ 2022-01-17 │
│ czce │ MA202 │ 2021-02-22 │ 2022-02-18 │
│ czce │ MA203 │ 2021-03-15 │ 2022-03-14 │
│ czce │ MA204 │ 2021-04-16 │ 2022-04-18 │
│ czce │ MA205 │ 2021-05-20 │ 2022-05-18 │
│ czce │ MA206 │ 2021-06-16 │ 2022-06-15 │
│ czce │ MA207 │ 2021-07-15 │ 2022-07-14 │
│ czce │ MA208 │ 2021-08-16 │ 2022-08-12 │
│ czce │ MA209 │ 2021-09-15 │ 2022-09-15 │
│ czce │ MA210 │ 2021-10-22 │ 2022-10-21 │
│ czce │ MA211 │ 2021-11-15 │ 2022-11-14 │
│ czce │ MA212 │ 2021-12-15 │ 2022-12-14 │
│ czce │ MA301 │ 2022-01-18 │ 2023-01-16 │
│ czce │ MA302 │ 2022-02-21 │ 2023-02-14 │
│ czce │ MA303 │ 2022-03-15 │ 2023-03-14 │
│ czce │ MA304 │ 2022-04-19 │ 2023-04-17 │
│ czce │ MA305 │ 2022-05-19 │ 2023-05-17 │
│ czce │ MA306 │ 2022-06-16 │ 2023-06-14 │
│ czce │ MA307 │ 2022-07-15 │ 2023-07-14 │
│ czce │ MA308 │ 2022-08-15 │ 2023-08-14 │
│ czce │ MA309 │ 2022-09-16 │ 2023-09-14 │
│ czce │ MA310 │ 2022-10-24 │ 2023-10-19 │
│ czce │ MA311 │ 2022-11-15 │ 2023-10-19 │
│ czce │ MA312 │ 2022-12-15 │ 2023-10-19 │
│ czce │ MA401 │ 2023-01-17 │ 2023-10-19 │
│ czce │ MA402 │ 2023-02-15 │ 2023-10-19 │
│ czce │ MA403 │ 2023-03-15 │ 2023-10-19 │
│ czce │ MA404 │ 2023-04-18 │ 2023-10-19 │
│ czce │ MA405 │ 2023-05-18 │ 2023-10-19 │
│ czce │ MA406 │ 2023-06-15 │ 2023-10-19 │
│ czce │ MA407 │ 2023-07-17 │ 2023-10-19 │
│ czce │ MA408 │ 2023-08-15 │ 2023-10-19 │
│ czce │ MA409 │ 2023-09-15 │ 2023-10-19 │
└──────┴───────┴────────────┴────────────┘
```
</details>
