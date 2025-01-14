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

-- 优化后，再对pre，也就是炸裂后的order id进行处理
CREATE TABLE result AS
SELECT t3.merchant_id
      ,t3.merchant_name
      ,pre
FROM (SELECT t1.merchant_id
            ,t2.merchant_name
            ,t1.orders
      FROM (SELECT merchant_id
                  ,collect_list(order_id) AS orders -- 收集每个卖家的订单列表
            FROM order
            GROUP BY merchant_id) t1
      LEFT OUTER JOIN merchant_info t2 -- 关联卖家信息
        ON t1.merchant_id = t2.merchant_id) t3
LATERAL VIEW EXPLODE(orders) t3 as pre; -- 再根据订单列表进行行转列操作。最终获取明细数据

