INSERT OVERWRITE TABLE tracking_impression PARTITION (country, partition_date, page_type_partition)
SELECT /*+ BROADCAST(t2) */
-- ...
-- ETL处理
,NVL(user_id, 0) AS user_id
,NVL(device_id, '') AS device_id
-- 复杂字段提取和结构拍扁
,CASE WHEN regexp_extract(regexp_extract(get_json_object(`data`,'$.rcmd'),'(.*ABTEST:)(.*?)(,.*)',2),'(.*)(@)(.*)',3) IS NULL THEN ''
      ELSE regexp_extract(regexp_extract(get_json_object(`data`,'$.rcmd'),'(.*ABTEST:)(.*?)(,.*)',2),'(.*)(@)(.*)',3) END AS experiment_group_list
-- 对事件/日志时间的额外处理
,CAST(from_unixtime(CAST(event_timestamp / 1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss') AS STRING) AS event_time
,CAST(from_unixtime(CAST(log_timestamp / 1000 AS BIGINT),'yyyy-MM-dd HH:mm:ss') AS STRING) AS log_time
-- UDF对加密字段解密
,decodeId(CAST(get_json_object(`data`, '$.content_id') AS STRING)) AS content_id
,'US' AS country
,'2021-01-01' AS partition_date
-- 分区键的额外处理
,CASE WHEN page_type IN ('comment','product','us','shop') THEN page_type ELSE 'others' END AS page_type_partition
-- 维度退化
,t2.author_id
,t2.content_type
,t2.content_status
-- 埋点小时表
FROM ods_tracking_impression_hi t1
LEFT JOIN (SELECT content_id
                 ,author_id
                 ,content_type
                 ,content_status
                 ,create_date
                 -- ...
           FROM dim) t2
  ON t1.content_id = t2.content_id
WHERE country = 'US'
  AND partition_date = '2021-01-01';
