-- 优化前
SELECT *
FROM performance_report
JOIN core_report
  ON performance_report.groupid = core_report.groupid
JOIN user_retained_report
  ON user_retained_report.groupid = core_report.groupid
WHERE user_retained_report.groupid IN  ('7', '16', '17', '18', '19', '20', '21', '22', '23', '30', '25') -- 特定的实验组id
LIMIT 3000;


-- 优化后
SELECT a.groupid,
       a.partition_date
FROM (SELECT *
      FROM performance_report
      WHERE partition_date >= '2022-05-20'
        AND partition_date <= '2022-06-09'
        AND groupid IN  ('7', '16', '17', '18', '19', '20', '21', '22', '23', '30', '25') -- 手下推过滤条件，先筛选数据，再进行关联
        AND rn_version IS NOT NULL) AS a
JOIN(SELECT *
     FROM core_report
     WHERE partition_date >= '2022-05-20'
       AND partition_date <= '2022-06-09'
       AND groupid IN  ('7', '16', '17', '18', '19', '20', '21', '22', '23', '30', '25')
       AND rn_version IS NOT NULL
       AND 1 = 1 ) AS b
  ON a.groupid = b.groupid
  AND a.partition_date = b.partition_date
JOIN(SELECT *
     FROM user_retained_report
     WHERE partition_date >= '2022-05-20'
       AND partition_date <= '2022-06-09'
       AND 1 = 1
       AND groupid IN ('7', '16', '17', '18', '19', '20', '21', '22', '23', '30', '25')
       AND rn_version IS NOT NULL) AS c 
  ON a.groupid = c.groupid
  AND a.partition_date = c.partition_date
GROUP BY a.groupid,
         a.partition_date
 LIMIT 3000;
