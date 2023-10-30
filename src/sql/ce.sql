/* 适用于 czce/dce */
DROP TABLE IF EXISTS qihuo.ce;
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

/* czce: 2020 及其之后的数据数据 */
INSERT INTO qihuo.ce
SELECT date, upper(code), open, high, low, close, settle, vol/2, amount/2, position/2, 'czce'
FROM qihuo.czce
WHERE date < '2020-01-01';

/* czce: 2020 及其之后的数据数据 */
INSERT INTO qihuo.ce
SELECT date, upper(code), open, high, low, close, settle, vol, amount, position, 'czce'
FROM qihuo.czce
WHERE date >= '2020-01-01';

/* dce */
INSERT INTO qihuo.ce
SELECT date, upper(code), open, high, low, close, settle, vol/2, amount/10000, position/2, 'dce'
FROM qihuo.dce;

SELECT COUNT() FROM qihuo.ce;
