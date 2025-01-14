CREATE VIEW view_increment_data AS
SELECT   TUMBLE_START(proctime, INTERVAL '30' SECOND) AS window_start
       , TUMBLE_END(proctime, INTERVAL '30' SECOND) AS window_end
       , CONCAT('real_time_key', '_' ,CAST(CURRENT_DATE AS string))  AS join_key
       , list_to_bitmap(collect_list(t1.uid)) AS data_list
       , MOD(CAST(t1.uid AS bigint), 1024) AS uid_pre
FROM order_source t1
GROUP BY TUMBLE(proctime, INTERVAL '30' SECOND)
       , MOD(CAST(t1.uid AS bigint), 1024);

-- 两阶段聚合
CREATE VIEW view_two_pause_increment_data AS
SELECT merge_bitmap_list(collect_list(data_list)) AS data_list
      ,join_key
      ,window_start
      ,window_end
      ,PROCTIME() AS proct
FROM view_increment_data
GROUP BY join_key
        ,window_end
        ,window_start;

-- 和历史数据合并
CREATE VIEW view_tmp_merge_bitmap AS
SELECT t4.join_key AS jk
      ,merge_bitmap(t4.data_list, t5.cf.detail) AS detail
      ,CAST(UNIX_TIMESTAMP(CAST(t4.window_start AS STRING )) AS string) AS wi
      ,CAST(UNIX_TIMESTAMP(CAST(t4.window_end AS STRING)) AS string) AS we
FROM view_two_pause_increment_data t4
LEFT JOIN hbase_source FOR SYSTEM_TIME AS OF t4.proct AS t5
  ON t4.join_key = t5.rowkey;

-- 写入HBase
INSERT INTO `sink`
SELECT jk
       ,Row(detail, wi, we)
FROM view_tmp_merge_bitmap;
