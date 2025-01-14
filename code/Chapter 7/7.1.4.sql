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
SELECT SPLIT(t4.new_merchant_id, '_')[1] AS merchant_id
      ,t4.merchant_name
      ,SUM(cnt)
      ,SUM(amount)
FROM (SELECT t1.new_merchant_id
            ,t3.merchant_name
            ,COUNT(1) AS cnt
            ,SUM(amount) AS amount
      FROM (SELECT CONCAT(FLOOR(RAND() * 10), '_', merchant_id) AS new_merchant_id
                  ,*
            FROM `order`) t1
      LEFT JOIN (SELECT CONCAT(pre, '_', merchant_id) AS new_merchant_id
                       ,merchant_name
                 FROM (SELECT merchant_id
                             ,merchant_name
                             ,SEQUENCE(0, 9) AS number_list
                       FROM merchant_info) t2
                 LATERAL VIEW EXPLODE(number_list) t AS pre) t3
        ON t1.new_merchant_id = t3.new_merchant_id
      GROUP BY t1.new_merchant_id
             , t3.merchant_name) t4
GROUP BY SPLIT(t4.new_merchant_id, '_')[1] -- 去除前缀进行二次聚合
        ,t4.merchant_name;
