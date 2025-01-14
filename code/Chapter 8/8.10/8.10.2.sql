-- 优化前
SELECT 'PUSH' AS `type`
       ,SUM(startup_cnt_push) AS startup_cnt
FROM core_kpi_stat
WHERE partition_date = '2023-09-01'
UNION ALL
SELECT '浏览器' AS `type`
       ,SUM(startup_cnt_browser) AS startup_cnt
FROM core_kpi_stat
WHERE partition_date = '2023-09-01'
UNION ALL
SELECT '百度APP' AS `type`
       ,SUM(startup_cnt_baidu) AS startup_cnt
FROM core_kpi_stat
WHERE partition_date = '2023-09-01'
UNION ALL
SELECT 'UC' AS `type`
       ,SUM(startup_cnt_uc) AS startup_cnt
FROM core_kpi_stat
WHERE partition_date = '2023-09-01';



-- 优化后
SELECT  SPLIT(data_str, '#')[0] AS `type`
       ,SPLIT(data_str, '#')[1] AS startup_cnt
FROM (SELECT CONCAT('PUSH#', startup_cnt_push
                   ,'&浏览器#', startup_cnt_browser
                   ,'&百度#', startup_cnt_baidu
                   ,'&UC#', startup_cnt_uc) AS concat_str
      FROM (SELECT SUM(startup_cnt_push) AS startup_cnt_push
                  ,SUM(startup_cnt_browser) AS startup_cnt_browser
                  ,SUM(startup_cnt_baidu) AS startup_cnt_baidu
                  ,SUM(startup_cnt_uc) AS startup_cnt_uc
           FROM core_kpi_stat
           WHERE partition_date = '2023-09-01') t1 ) t2
LATERAL VIEW EXPLODE(SPLIT(concat_str, '&')) t2 AS data_str;