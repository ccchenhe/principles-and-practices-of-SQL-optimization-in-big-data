-- 优化前
SELECT t1.merchant_id -- 卖家id
      ,t2.merchant_name  -- 卖家名
      ,COUNT(1) AS cnt -- 订单量
      ,SUM(amount) as amount -- 订单金额
FROM `order` t1
LEFT OUTER JOIN merchant_info t2
  ON t1.merchant_id = t2.merchant_id
GROUP BY t1.merchant_id
        ,t2.merchant_name;


-- 优化后
-- 1.统计订单量最多的前100名卖家
CREATE TABLE tmp AS
SELECT merchant_id
FROM `order`
GROUP BY merchant_id
ORDER BY COUNT(1) DESC
LIMIT 100;


-- 2.分批处理
SELECT t1.merchant_id
      ,t3.merchant_name
      ,COUNT(1) AS cnt
      ,SUM(amount) as amount
FROM order t1
INNER JOIN tmp t2
  ON t1.merchant_id = t2.merchant_id
LEFT OUTER JOIN merchant_info t3
  ON t1.merchant_id = t3.merchant_id -- 关联前100名卖家
GROUP BY t1.merchant_id
        ,t3.merchant_name
UNION ALL
SELECT t1.merchant_id
      ,t3.merchant_name
      ,COUNT(1) AS cnt
      ,SUM(amount) as amount
FROM order t1
LEFT OUTER JOIN tmp t2
  ON t1.merchant_id = t2.merchant_id
  AND t2.merchant_id IS NULL -- 关联其余的卖家
LEFT OUTER JOIN merchant_info t3
  ON t1.merchant_id = t3.merchant_id
GROUP BY t1.merchant_id
        ,t3.merchant_name;
