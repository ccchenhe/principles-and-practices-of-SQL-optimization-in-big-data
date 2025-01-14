-- Flink SQL，用于展示其执行计划
CREATE VIEW IF NOT EXISTS view_amount AS
SELECT user_id
      ,ctime AS etl_time
      ,(amount) AS etl_amount
      ,`state`
FROM (SELECT *
             ,ROW_NUMBER() OVER (PARTITION BY concat(CAST(t1.promotion_id AS STRING), CAST (t2.order_id AS STRING)) ORDER BY t1.mtime DESC, CAST(t1._event['ts_ns'] AS BIGINT) DESC) AS rn
FROM promotion t1
LEFT JOIN (SELECT *
           FROM `order`
           WHERE LOWER(reference_id) LIKE 'promo%'
            AND LOWER(_event['type']) IN ('insert', 'update')) t2
  ON t1.user_id = t2.user_id
WHERE 1 = 1
  AND LOWER(t1._event['type']) IN ('insert', 'update')
  AND t2.amount IS NOT NULL
  AND t2.amount > 0) t
WHERE rn = 1;

CREATE VIEW IF NOT EXISTS view_aggr AS
SELECT DATE_FORMAT(TO_TIMESTAMP(FROM_UNIXTIME(etl_time, 'yyyy-MM-dd HH:mm:ss')), 'yyyy-MM-dd HH:00:00') as etl_hour
      ,`state`
      ,SUM(etl_amount) AS total_amount
FROM view_amount
GROUP BY DATE_FORMAT(TO_TIMESTAMP(FROM_UNIXTIME(etl_time, 'yyyy-MM-dd HH:mm:ss')), 'yyyy-MM-dd HH:00:00'), `state`;

INSERT INTO result
SELECT UNIX_TIMESTAMP(etl_hour, 'yyyy-MM-dd HH:mm:ss')
       ,total_amount
       ,UNIX_TIMESTAMP()
FROM view_aggr
WHERE `state` = 17; -- 成功








-- Spark SQL
-- 优化前
SELECT get_json_object(a1.resp_payload_json,'$.data.device_id') AS device_id
      ,COUNT(DISTINCT t1.transaction_id)
FROM transaction AS t1
JOIN action_tab AS a1
  ON t1.transaction_id = a1.transaction_id
JOIN order AS o1
  ON t1.order_id = o1.order_id
WHERE a1.type = 44 -- 反欺诈，获取设备信息
  AND get_json_object(o1.order_info, '$.merchant_info.merchant_type')!='4'
  AND t1.type = 1 -- 支付payment
  AND t1.state = 4 -- 支付成功
GROUP BY get_json_object(a1.resp_payload_json,'$.data.device_id');


-- 优化后
SELECT get_json_object(a1.resp_payload_json,'$.data.device_id') AS device_id
      ,COUNT(DISTINCT t1.transaction_id)
FROM transaction AS t1
JOIN action_tab AS a1
  ON t1.transaction_id = a1.transaction_id
JOIN order AS o1
  ON t1.order_id = o1.order_id
WHERE a1.type = 44 -- 反欺诈，获取设备信息
  AND (get_json_object(o1.order_info, '$.merchant_info.merchant_type') != '4' OR get_json_object(o1.order_info, '$.merchant_info.merchant_type') IS NULL OR get_json_object(o1.order_info, '$.merchant_info.merchant_type') = '')
  AND t1.type = 1 -- 支付payment
GROUP BY get_json_object(a1.resp_payload_json,'$.data.device_id');

