-- 优化前
SELECT order_type -- 订单类型
      ,order_status -- 订单状态
      ,COUNT(1) AS cnt -- 订单量
      ,COUNT(DISTINCT user_id) as pay_num -- 付款人数
      ,SUM(amount) AS pay_sum -- 订单金额
FROM `order`
GROUP BY order_type, order_status
GROUPING SETS((order_type, order_status), (order_type));


-- 优化后
-- GROUP BY (order_type, order_status) 
SELECT 
    order_type,
    order_status,
    COUNT(1) AS cnt,
    COUNT(DISTINCT user_id) as pay_num,
    SUM(amount) AS pay_sum
FROM `order`
GROUP BY order_type, order_status

UNION ALL

-- GROUP BY order_type
SELECT 
    order_type,
    NULL as order_status,
    COUNT(1) AS cnt,
    COUNT(DISTINCT user_id) as pay_num,
    SUM(amount) AS pay_sum
FROM `order`
GROUP BY order_type;
