-- 优化前
SELECT order_id
FROM `order`
-- 根据place_date最大的日期来计算order表数据
WHERE partition_date IN (SELECT MAX(partition_date)
                         FROM `place_date`);



-- 优化后
SELECT COUNT(order_id)
FROM `order` t1
-- 先计算place_date表最大的日期，再INNER JOIN
INNER JOIN (SELECT MAX(partition_date) AS max_partition_date
            FROM place_date) t2
  ON t1.partition_date = t2.max_partition_date;
