CREATE TABLE `sink_hbase`(
    rowkey string
    ,cf ROW<login_time bigint>
    ,PRIMARY KEY (rowkey) NOT ENFORCED
)WITH (
   'connector' = 'hbase-2.2'
  ,'combine.column' = 'cf:login_time' -- 新增的参数，指定cf中的login_time作为写入列的timestamp
);


-- 最近一次登录
INSERT INTO `sink_hbase`
SELECT CAST(user_id AS STRING) AS rowkey
      ,ROW(login_time)
FROM `user_login_action`;



-- 最早一次登录
CREATE VIEW view_source
SELECT CAST(user_id AS STRING) AS rowkey
      ,(9223372036854775807 - login_time) AS login_time -- INTEGER.MAX_VALUE
FROM `user_login_action`;

INSERT INTO `sink_hbase`
SELECT *
FROM view_source;
