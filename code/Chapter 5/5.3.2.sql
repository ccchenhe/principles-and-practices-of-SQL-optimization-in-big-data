-- 优化前
SELECT COUNT(1)
FROM `order`
WHERE create_time BETWEEN UNIX_TIMESTAMP('2023-08-01 00:00:00', 'yyyy-MM-dd HH:mm:ss')
  AND UNIX_TIMESTAMP('2023-08-01 23:59:59', 'yyyy-MM-dd HH:mm:ss'); -- 从全量数据中筛选订单创建时间为2023-08-01的所有记录


-- 优化后
-- 以订单创建时间create_time格式化后的分区字段partition_date
SELECT COUNT(1)
FROM order
WHERE partition_date = '2023-08-01'; -- 以订单创建时间create_time格式化后的分区字段
