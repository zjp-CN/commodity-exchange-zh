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

  <summary>示例 1：甲醇所有合约代码以及开始、结束日期</summary>

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
...
│ czce │ MA601 │ 2015-01-19 │ 2016-01-15 │
│ czce │ MA602 │ 2015-02-16 │ 2016-02-19 │
...
│ czce │ MA408 │ 2023-08-15 │ 2023-10-19 │
│ czce │ MA409 │ 2023-09-15 │ 2023-10-19 │
└──────┴───────┴────────────┴────────────┘
```
</details>

<details>

  <summary>示例 2：根据成交量加权平均得到每日甲醇价格 K 线</summary>

```SQL
DROP FUNCTION IF EXISTS weighted_avg;
CREATE FUNCTION weighted_avg AS (price, weight)->round(
  arraySum(arrayMap((o, w)->(o*w), price, weight)),
  0
);

WITH
  df AS (
    SELECT
      date,
      groupArray(open) AS arr_open,
      groupArray(high) AS arr_high,
      groupArray(low) AS arr_low,
      groupArray(close) AS arr_close,
      groupArray(settle) AS arr_settle,
      sum(vol) AS vol_sum,
      arrayMap((v)->(v/vol_sum), groupArray(vol)) AS weight
    FROM
      qihuo.ce
    WHERE
      code LIKE 'MA%'
    GROUP BY
      date
    ORDER BY
      date DESC
  )
SELECT
  date,
  vol_sum,
  weighted_avg(arr_open, weight) AS open,
  weighted_avg(arr_high, weight) AS high,
  weighted_avg(arr_low, weight) AS low,
  weighted_avg(arr_close, weight) AS close,
  weighted_avg(arr_settle, weight) AS settle
FROM df FORMAT PrettyCompactNoEscapes;

┌───────date─┬─vol_sum─┬─open─┬─high─┬──low─┬─close─┬─settle─┐
│ 2023-10-19 │ 1515499 │ 2391 │ 2427 │ 2365 │  2414 │   2399 │
│ 2023-10-18 │ 1749159 │ 2398 │ 2410 │ 2371 │  2379 │   2391 │
...
│ 2014-06-18 │    1059 │ 2858 │ 2902 │ 2833 │  2896 │   2885 │
│ 2014-06-17 │    1053 │ 2868 │ 2897 │ 2826 │  2854 │   2862 │
└────────────┴─────────┴──────┴──────┴──────┴───────┴────────┘
```
</details>
