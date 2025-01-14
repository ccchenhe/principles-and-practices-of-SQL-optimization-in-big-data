-- 优化前
SELECT t1.payer_id -- 支付用户id
      ,t1.order_id
      ,t1.create_time
      ,t1.merchant_id -- 卖家id
      ,t2.merchant_name -- 店铺名
FROM `order` t1
INNER JOIN `merchant_info` t2
 ON t1.merchant_id = t2.merchant_id
WHERE t1.order_id IN (SELECT t3.order_id -- 获取支付用户最近一笔订单id
                      FROM `order` t3
                      INNER JOIN (SELECT payer_id
                                           ,MAX(create_time) AS last_pay_time
                                  FROM `order`
                                  GROUP BY payer_id) t4
                        ON t3.payer_id = t4.payer_id
                        AND t3.create_time = t4.last_pay_time);

-- 优化后
SELECT t1.payer_id
      ,t1.order_id
      ,t1.merchant_id
      ,t1.create_time
      ,t2.merchant_name
FROM (SELECT payer_id
            ,order_id
            ,create_time
            ,merchant_id
             -- 分组排序，每个用户按订单创建时间降序，取最大的一条
            ,ROW_NUMBER() OVER (PARTITION BY payer_id ORDER BY create_time DESC) AS rn
      FROM order) t1
INNER JOIN merchant_info t2
  ON t1.merchant_id = t2.merchant_id
WHERE t1.rn = 1;
