-- 订单新表的Binlog
CREATE TABLE new_order(
     amount	STRING
    ,completed_time	STRING
    ,create_time	STRING
    ,extinfo	STRING
    ,fee_amount	STRING
    ,id	STRING
    ,order_id	 STRING
    ,order_status	STRING
    ,order_type	STRING
    ,update_time	STRING
    ,user_id	STRING
    ,`_event` MAP<STRING, STRING>
) WITH (
    'connector' = 'kafka'
    ,'scan.startup.mode' = 'timestamp'
    ,'topic' = 'new_order'
    ,'properties.allow.auto.create.topics' = 'false'
    ,'properties.group.id' = 'groupid'
    ,'value.format' = 'json'
    ,'value.json.ignore-parse-errors' = 'true'
    ,'value.json.fail-on-missing-field' = 'false'
);

-- 合并后放入订单数据Topic
CREATE TABLE order_merge_sink(
    `database`           STRING
    ,`table`             STRING
    ,`type`              STRING
    ,`maxwell_ts`        BIGINT
    ,`is_new_system`        INT
    ,`data` Map<STRING, STRING>
    ,PRIMARY KEY (`database`, `table`, `type`) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'order_merge_sink',
      'properties.allow.auto.create.topics' = 'false',
      'key.format' = 'json',
      'value.format' = 'json',
);
CREATE VIEW IF NOT EXISTS view_new_order AS
-- 仿照Maxwell的数据结构拼接Binlog
SELECT  `_event`['database'] AS `database`
        ,`_event`['table'] AS `table`
        ,`_event`['type'] AS `type`
        ,CAST(`_event`['ts_ns'] AS BIGINT) AS `maxwell_ts`
        ,1 AS `is_new_system` -- 通过人为标识区分数据来源
        ,MAP['amount', amount
             ,'completed_time', completed_time
             ,'create_time', create_time
             ,'fee_amount', fee_amount
             ,'id', id
             ,'order_id', order_id
             ,'order_status', order_status
             ,'order_type', order_type
             ,'update_time', update_time
             ,'user_id', user_id
             ,'extinfo', DECODE_PROTOBUF(extinfo,'class') -- 将敏感字段解密脱敏后转换为JSON字符串
             ]
FROM new_order
WHERE `_event`['type'] IN ('insert', 'update')
;


INSERT INTO uws_merge_sink
SELECT *
FROM new_order
UNION ALL
SELECT *
FROM view_old_order
;
