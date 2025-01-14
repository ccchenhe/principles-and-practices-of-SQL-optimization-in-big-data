-- 优化前
SELECT avg_duration -- 人均阅读时长
      ,toutiao_avg_duration -- 今日头条人均阅读时长
      ,uc_avg_duration -- UC人均阅读时长
      ,bd_avg_duration -- 百度APP人均阅读时长
FROM (SELECT t1.partition_date
            ,SUM(total_minutes) / SUM(news_dau) AS avg_duration
      FROM tracking_click t1
      INNER JOIN dws_news_dau t2
        ON t1.partition_date = t2.partition_date
        AND t1.user_group = t2.user_group
      WHERE t1.partition_date = '2023-09-01'
      GROUP BY t1.partition_date) t3
INNER JOIN (SELECT t4.partition_date
                  ,SUM(total_minutes) / SUM(news_dau) AS toutiao_avg_duration
            FROM tracking_click t4
            INNER JOIN dws_news_dau t5
              ON t4.partition_date = t5.partition_date
              AND t4.user_group = t4.user_group
            WHERE t4.partition_date = '2023-09-01'
              AND t4.user_group = '今日头条'
            GROUP BY t4.partition_date) t6
  ON t3.partition_date = t6.partition_date
INNER JOIN (SELECT t7.partition_date
                  ,SUM(total_minutes) / SUM(news_dau) AS uc_avg_duration
            FROM tracking_click t7
            INNER JOIN dws_news_dau t8
              ON t7.partition_date = t8.partition_date
              AND t7.user_group = t8.user_group
            WHERE t7.partition_date = '2023-09-01'
              AND t7.user_group = 'UC'
            GROUP BY t7.partition_date) t9
  ON t3.partition_date = t9.partition_date
INNER JOIN (SELECT t10.partition_date
                  ,SUM(total_minutes) / SUM(news_dau) AS bd_avg_duration
            FROM tracking_click t10
            INNER JOIN dws_news_dau t11
              ON t10.partition_date = t11.partition_date
              AND t10.user_group = t11.user_group
            WHERE t10.partition_date = '2023-09-01'
              AND t10.user_group = '百度'
            GROUP BY t10.partition_date) t12
  ON t3.partition_date = t12.partition_date;


-- 优化后
SELECT total_minutes / total_dau AS avg_duration -- 人均阅读数
      ,toutiao_minutes / toutiao_dau AS toutiao_avg_duration
      ,uc_minutes / uc_dau AS uc_avg_duration
      ,bd_minutes / bd_dau AS bd_avg_duration
FROM(SELECT t1.partition_date
           ,SUM(total_minutes) AS total_minutes
           ,SUM(IF(t1.user_group = '今日头条', total_minutes, 0)) AS toutiao_minutes
           ,SUM(IF(t1.user_group = 'UC', total_minutes, 0)) AS uc_minutes
           ,SUM(IF(t1.user_group = '百度', total_minutes, 0)) AS bd_minutes
           ,SUM(news_dau) AS total_dau
           ,SUM(IF(t1.user_group = '今日头条', news_dau, 0)) AS toutiao_dau
           ,SUM(IF(t1.user_group = 'UC', news_dau, 0)) AS uc_dau
           ,SUM(IF(t1.user_group = '百度', news_dau, 0)) AS bd_dau
     FROM tracking_click t1
     INNER JOIN duration t2
       ON t1.partition_date = t2.partition_date
       AND t1.user_group = t2.user_group
     GROUP BY t1.partition_date
             ,t1.user_group) t;
