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
SELECT t1.merchant_id
      ,t2.merchant_name
FROM (SELECT merchant_id
            ,COUNT(1) AS cnt
            ,SUM(amount) as amount
      FROM order 
      GROUP BY merchant_id) t1 -- 先聚合每个卖家的订单量和订单金额
LEFT OUTER JOIN merchant_info t2 -- 再关联维表获取卖家信息
  ON t1.merchant_id = t2.merchant_id;
