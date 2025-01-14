-- 优化前
SELECT t1.page_type -- 访问页面
      ,COUNT(DISTINCT t1.user_id) AS imp_uv -- 页面曝光人数
      ,COUNT(t1.user_id) AS imp_pv -- 页面曝光次数
      ,COUNT(DISTINCT t2.user_id) AS click_uv -- 页面点击人数
      ,COUNT(t2.user_id) AS click_pv -- 页面点击次数
FROM impression_table t1
LEFT JOIN (SELECT page_type
                 ,user_id
           FROM click_table
           WHERE country = 'ID'
             AND partition_date = '2023-09-09') t2
  ON t1.page_type = t2.page_type
  AND t1.user_id = t2.user_id
WHERE t1.country = 'ID'
  AND t1.partition_date = '2023-09-09'
GROUP BY t1.page_type;


-- 优化后
SELECT page_type
      ,COUNT(DISTINCT CASE WHEN operation = 'impression' THEN user_id ELSE NULL END) AS imp_uv -- 页面曝光人数
      ,COUNT(CASE WHEN operation = 'impression' THEN 1 ELSE NULL END ) AS imp_pv -- 页面曝光次数
      ,COUNT(DISTINCT CASE WHEN operation = 'click' THEN user_id ELSE NULL END) AS click_uv -- 页面点击人数
      ,COUNT(CASE WHEN operation = 'click' THEN 1 ELSE NULL END) AS click_pv -- 页面点击次数
FROM (SELECT page_type
            ,'impression' AS operation -- 不同埋点事件人为赋值，用于CASE WHEN的判断
            ,user_id
      FROM impression_table
      WHERE country = 'ID'
        AND partition_date = '2023-09-09'
      UNION ALL
      SELECT page_type
            ,'click' AS operation
            ,user_id
      FROM click_table
      WHERE country = 'ID'
        AND partition_date = '2023-09-09') t
GROUP BY page_type;
