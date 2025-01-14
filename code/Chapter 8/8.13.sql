-- 优化前
SELECT *
FROM (SELECT merchant_id
            ,order_id
            ,order_type
            ,amount
            ,row_number() over(PARTITION BY merchant_id ORDER BY create_time DESC) rn
       FROM `order`
       WHERE order_type = 1) t -- 电商支付
WHERE rn = 1;


-- 优化后
SELECT *
FROM(SELECT /*+ broadcastjoin(t2) */ t1.* 
           ,row_number() over(PARTITION BY t1.merchant_id ORDER BY t1.create_time DESC) rn
     FROM `order` t1
     LEFT OUTER JOIN (SELECT merchant_id
                            ,order_type
                            ,MAX(create_time) AS create_time
                     FROM `order`
                     WHERE order_type = 1 -- 电商支付
                     GROUP BY merchant_id
                             ,order_type) t2 -- 获取每个卖家每种订单类型最近的订单创建时间
       ON t1.merchant_id = t2.merchant_id
      AND t1.order_type = t2.order_type
      AND t1.create_time = t2.create_time) t -- 根据订单时间进行关联
WHERE rn = 1; -- 分组排序处理同一时间多笔订单的情况
