-- Hive中开启并行执行

-- 开启并行执行
SET hive.exec.parallel=TRUE;
-- 同一个SQL允许的最大并行度
SET hive.exec.parallel.thread.number=8; 


SELECT t1.partition_date
      ,t1.startup_cnt
      ,t2.view_item_cnt
FROM(SELECT partition_date -- SQL片段 1，计算APP启动次数
           ,SUM(startup_cnt) AS startup_cnt
     FROM table1
     WHERE partition_date >= '2022-08-01'
       AND partition_date <= '2022-08-31'
     GROUP BY partition_date) t1
LEFT OUTER JOIN (SELECT partition_date -- SQL片段 2，计算浏览商品详情页的次数
                       ,SUM(view_item_cnt) AS view_item_cnt
                 FROM table2
                 WHERE partition_date >= '2022-08-01'
                   AND partition_date <= '2022-08-31'
                 GROUP BY partition_date) t2
  ON t1.partition_date = t2.partition_date;
