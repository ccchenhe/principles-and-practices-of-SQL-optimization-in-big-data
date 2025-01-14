-- 优化前
SELECT t1.user_id
FROM (SELECT user_id
      FROM tracking_impression
      WHERE partition_date = '2023-09-01'
        AND content_type = '短视频'
      GROUP BY user_id) t1
LEFT JOIN (SELECT user_id
           FROM tracking_impression
           WHERE partition_date = '2023-09-01'
             AND content_type <> '短视频'
           GROUP BY user_id) t2
  ON t1.user_id = t2.user_id
WHERE t2.user_id IS NULL;


-- 优化后
SELECT user_id
FROM (SELECT user_id
            ,SUM(content_type) AS res
      FROM (SELECT user_id
                  ,CASE WHEN content_type = '长视频' THEN 1
                        WHEN content_type = '短视频' THEN 2
                        WHEN content_type = '图文' THEN 4
                        ELSE NULL END AS content_type
            FROM tracking_impression
            WHERE partition_date = '2023-09-01') t1
      GROUP BY user_id) t2
WHERE res = 2; -- 二进制010，十进制为2

