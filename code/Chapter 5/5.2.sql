-- 在Flink中，多半是参数透传
-- 创建Kafka逻辑表
CREATE TABLE `traffic` (
    event_id STRING
   ,event_timestamp BIGINT
   ,log_timestamp BIGINT
   ,user_id BIGINT
   ,device_id STRING
   ,process_time AS PROCTIME()
) WITH (
    'connector' = 'kafka'
);
-- 通过Hint控制消费和写入Topic的配置
INSERT INTO kafka_sink /*+ OPTIONS('sink.semantic' = 'at-least-once','topic' = '...','properties.bootstrap.servers' = '...','format' = 'json','properties.ack' = '1','sink.parallelism' = '4') */
SELECT ...
FROM traffic /*+ OPTIONS('topic'='topic','properties.group.id' = '...','properties.bootstrap.servers'='...') */
WHERE ...;


-- 在Spark中，可以通过重分区来缩减文件数
-- 重分区为1个文件后写入表author
INSERT OVERWRITE TABLE author PARTITION (partition_date="...",country="...")
SELECT /*+REPARTITION(1)*/ uid AS user_id
                           FROM_UNIXTIME(CAST(MIN(ctime) AS bigint)/1000, 'yyyy-MM-dd') AS first_publish_date
                           FROM_UNIXTIME(CAST(MAX(ctime) AS bigint)/1000, 'yyyy-MM-dd') AS last_publish_date
                           CASE WHEN FROM_UNIXTIME(CAST(MIN(ctime) AS bigint)/1000, 'yyyy-MM-dd') = "..." THEN 1
                                ELSE 0 END AS is_new_author
FROM ...
WHERE ...
GROUP BY uid;

-- 在Spark中，可以指定表的JOIN形式
-- 例如在关联表payment_channel时强制指定JOIN方式为broadcastjoin
SELECT /*+ broadcastjoin(payment_channel) */ item_tab.channel_item_id AS channel_item_id
                                                     ,item_tab.user_id AS user_id
                                                     -- ...    
FROM item_tab
LEFT JOIN linked_tab 
  ON item_tab.item_ref = linked_tab.id
 AND item_tab.user_id = linked_tab.user_id
LEFT JOIN payment_channel 
  ON item_tab.channel_id = payment_channel.id;