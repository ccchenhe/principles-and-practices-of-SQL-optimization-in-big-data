-- 用于展示在Flink SQL中，不存在对自定义函数/昂贵函数的列合并操作
CREATE FUNCTION map_to_json_string AS 'com.xxx.udf.MapToJsonString';
CREATE TABLE IF NOT EXISTS kafka_source (
     `create_time` string,
     `extinfo` string,
     `id` STRING ,
     `order_id` string,
     `payer_id` string,
     `payer_platform` string,
     `state` string,
     `type` string)
WITH(
     'connector' = 'kafka'
);

CREATE TABLE IF NOT EXISTS print_sink(
     `a` string
    ,`b` string 
    ,`c` string
)
WITH(
    'connector' = 'print'
);

CREATE VIEW tmp AS 
-- 构造一个包含所有列的MAP，并通过UDF转换为JSON字符串
SELECT map_to_json_string(MAP['create_time', `create_time`, 'extinfo', extinfo, 'id', `id`, 'order_id', order_id
          ,'payer_id', payer_id, 'payer_platform', payer_platform, 'state', `state`,'type', `type`]) AS ext
FROM kafka_source;

-- 为了说明示意，仅使用内置函数对JSON字符串进行切分
INSERT INTO print_sink
SELECT SPLIT_INDEX(ext, ',', 0)
      ,SPLIT_INDEX(ext, ',', 1)
      ,SPLIT_INDEX(ext, ',', 2)
FROM tmp;




-- 用于展示在Spark SQL中，过滤条件顺序的不同，查询耗时也不相同
SELECT *
FROM `order`
WHERE get_json_object(extinfo, '$.pay_device') = 'xx' -- 特定设备
  AND order_status = 1 -- 支付成功
  AND order_type = 15; -- 电商支付


SELECT *
FROM `order`
WHERE order_status = 1 -- 支付成功
  AND order_type = 15 -- 电商支付
  AND get_json_object(extinfo, '$.pay_device') = 'xx';

SELECT *
FROM `order`
WHERE order_type = 15 -- 电商支付
  AND order_status = 1 -- 支付成功
  AND get_json_object(extinfo, '$.pay_device') = 'xx';
