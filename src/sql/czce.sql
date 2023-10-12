/* for clickhouse
DROP TABLE IF EXISTS qihuo.czce;
*/
CREATE TABLE IF NOT EXISTS qihuo.czce (
  date      Date              COMMENT '日期',
  code      String            COMMENT '合约代码',
  prev      Float32           COMMENT '昨结算',
  open      Float32           COMMENT '开盘价',
  high      Float32           COMMENT '最高价',
  low       Float32           COMMENT '最低价',
  close     Float32           COMMENT '收盘价',
  settle    Float32           COMMENT '结算价',
  zd1       Float32           COMMENT '涨跌1：涨幅百分数??',
  zd2       Float32           COMMENT '涨跌2：涨跌数??',
  vol       UInt32            COMMENT '成交量',
  position  UInt32            COMMENT '持仓量',
  pos_delta Int32             COMMENT '增减量',
  amount    Float32           COMMENT '交易额（万）',
  dsp       Nullable(Float32) COMMENT '交割结算价'
) ENGINE = ReplacingMergeTree
PRIMARY KEY (date, code)
ORDER BY    (date, code);
/*
SET format_csv_delimiter = '|';
INSERT INTO qihuo.czce FROM INFILE 'cache/郑州-ALLFUTURES2022.csv';
SELECT count(*) FROM qihuo.czce;
*/
