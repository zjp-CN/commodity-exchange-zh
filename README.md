# 商品交易所数据获取工具 (ce)

ce = **c**ommodity **e**xchange

## 安装方式

* 方式一：`cargo install commodity-exchange-zh`，然后使用 `ce` 获取数据

```console
$ ce help
Usage: ce <command> [<args>]

下载、解析和保存商品期货交易所数据。子命令示例：

* czce -y 2010..2023：下载郑州交易所 2010 至 2022 年所有合约数据
* dce -y 2020..=2023 C M：下载大连交易所 2019 至 2022 年玉米和豆粕两个品种的数据
* dce -s：交互式选择大连交易所年份和品种

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
  code      String  COMMENT '合约代码',
  date      Date    COMMENT '日期',
  open      Float32 COMMENT '开盘价',
  high      Float32 COMMENT '最高价',
  low       Float32 COMMENT '最低价',
  close     Float32 COMMENT '收盘价',
  settle    Float32 COMMENT '结算价',
  vol       UInt32  COMMENT '成交量',
  amount    Float32 COMMENT '交易额',
  position  UInt32  COMMENT '持仓量'
) ENGINE = ReplacingMergeTree
PRIMARY KEY (date, code)
ORDER BY    (date, code);
```
