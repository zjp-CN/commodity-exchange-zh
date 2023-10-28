/*
DROP TABLE IF EXISTS qihuo.dce;
*/
CREATE TABLE IF NOT EXISTS qihuo.dce (
  code      String            COMMENT '合约代码',
  date      Date              COMMENT '日期',
  prev      Float32           COMMENT '昨结算',
  open      Float32           COMMENT '开盘价',
  high      Float32           COMMENT '最高价',
  low       Float32           COMMENT '最低价',
  close     Float32           COMMENT '收盘价',
  settle    Float32           COMMENT '结算价',
  zd1       Float32           COMMENT '涨跌1',
  zd2       Float32           COMMENT '涨跌2',
  vol       UInt32            COMMENT '成交量',
  amount    Float32           COMMENT '交易额',
  position  UInt32            COMMENT '持仓量'
) ENGINE = ReplacingMergeTree
PRIMARY KEY (date, code)
ORDER BY    (date, code);
/*
INSERT INTO qihuo.dce FROM INFILE 'cache/dce-2017-聚氯乙烯.csv';
SELECT count(*) FROM qihuo.dce;
*/
