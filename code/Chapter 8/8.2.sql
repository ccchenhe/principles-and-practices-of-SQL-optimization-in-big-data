-- 优化前
SELECT order_type
      ,COUNT(1)
      ,SUM(amount)
FROM `order`
GROUP BY order_type;

-- 优化后
SELECT SPLIT(first_phase_type, '-')[1] AS second_phase_type
      ,SUM(cnt)
      ,SUM(amt)
FROM (SELECT CONCAT(CAST(order_status AS STRING), '-' , CAST(order_type AS STRING)) AS 1st_type
            ,COUNT(1) AS cnt
            ,SUM(amount) AS amt
      FROM `order`
      GROUP BY CONCAT(CAST(order_status AS STRING), '-' , CAST(order_type AS STRING))) t
GROUP BY SPLIT(first_phase_type, '-')[1];
