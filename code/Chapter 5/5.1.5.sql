-- 在Hive中数据重用
SET hive.optimize.cte.materialize.threshold=1;
WITH AS...

-- 在Spark中数据重用
CACHE TABLE txn_audit AS(
SELECT *,
       LEAD(new_status, 1, NULL) OVER(PARTITION BY transaction_id ORDER BY min_ms_ctime, min_id) AS next_new_status,
       LEAD(min_ctime, 1, NULL) OVER(PARTITION BY transaction_id ORDER BY min_ms_ctime, min_id) AS next_ctime
FROM(SELECT transaction_id,
            new_status,
            MIN(ctime) AS min_ctime,
            MIN(id) AS min_id,
            MIN(COALESCE(CAST(get_json_object(replace(CAST(get_json_object(changes, '$.extra_data') AS string), '\\', ''), '$.action_time_ms') AS BIGINT), CAST(ctime AS BIGINT)*1000)) AS min_ms_ctime
     FROM audit_log
     WHERE from_unixtime(ctime)>=DATE('2022-01-01')
     GROUP BY transaction_id,
               new_status));    


-- 抽样
SELECT *
FROM txn_audit limit 1;
-- 计数
SELECT COUNT(1)
FROM txn_audit;
