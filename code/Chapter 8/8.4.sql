-- 优化前
SELECT COUNT(DISTINCT user_id) AS uv
FROM tracking
WHERE partition_date >= '2023-08-10'
  AND partition_date <= '2023-08-16';


-- 优化后
SELECT COUNT(DISTINCT user_id) AS uv
FROM tracking
WHERE partition_date >= '2023-08-10'
  AND partition_date <= '2023-08-16'
  AND user_id IS NOT NULL
  AND user_id > 0;
