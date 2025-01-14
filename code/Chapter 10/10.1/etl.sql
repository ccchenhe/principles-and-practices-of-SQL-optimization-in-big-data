CREATE TABLE `order_source`
(
    flink_row_kind     String,
    origin_database    STRING METADATA FROM 'value.database' VIRTUAL,
    origin_table       STRING METADATA FROM 'value.table' VIRTUAL,
    order_id           Bigint,
    user_id            Bigint, 
    order_type         Integer, -- 订单类型
    amount             Bigint, -- 订单金额
    order_status       Integer, -- 订单状态
    update_time        Integer, -- 订单状态发生变更时的更新时间
    extend_update_time Integer, -- 新值的update_time
    proctime AS PROCTIME()
) WITH (
      'connector' = 'kafka',
      'format' = 'custom-json',
      'custom-json.extend.fields' = 'extend_update_time:update_time' -- 将新值的update_time赋给extend_update_time
);
  
-- Flink计算每个用户、每种订单类型的金额、笔数  
SELECT user_id
      ,order_type
      ,order_status
      ,UNIX_TIMESTAMP(FROM_UNIXTIME(extend_update_time, 'yyyy-MM-dd'), 'yyyy-MM-dd') as extend_update_date
      ,COUNT(order_id)
      ,SUM(amount)
FROM `order_source` 
GROUP BY order_status
       , order_type
       , UNIX_TIMESTAMP(FROM_UNIXTIME(extend_update_time, 'yyyy-MM-dd'), 'yyyy-MM-dd')
       , user_id;
