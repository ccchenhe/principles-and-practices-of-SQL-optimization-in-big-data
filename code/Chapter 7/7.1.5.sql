-- 优化前
SELECT t1.user_id AS client_user_id,
       t1.create_time,
       t1.order_id AS client_order_id,
       amount AS client_transaction_amount,
       nvl(t2.bank_name, if(order_type=6, 'DEFAULT', NULL)) AS destination_bank, -- 支付银行
       t1.order_status, -- 订单状态
       t1.bank_account_id -- 银行卡id
FROM(SELECT order_type,
             user_id,
             create_time,
             update_time,
             completed_time,
             order_id,
             amount,
             fee_amount,
             order_status,
             reference_id,
             get_json_object(extinfo, '$.new.account_id') AS bank_account_id,
      FROM `order`
      WHERE partition_date >= '2023-05-27'
        AND order_type in (1, 12, 13)
        AND order_status in (1,2,7,8,12)) t1
LEFT JOIN card_info t2 
  ON t1.bank_account_id = t2.bank_account_id;


-- 优化后
SELECT t1.user_id AS client_user_id,
       t1.create_time,
       t1.order_id AS client_order_id,
       amount AS client_transaction_amount,
       nvl(t2.bank_name, if(order_type=6, 'DEFAULT', NULL)) AS destination_bank, -- 支付银行
       t1.order_status, -- 订单状态
       t1.bank_account_id -- 银行卡id
FROM(SELECT order_type,
             user_id,
             create_time,
             update_time,
             completed_time,
             order_id,
             amount,
             fee_amount,
             order_status,
             reference_id,
             get_json_object(extinfo, '$.new.account_id') AS bank_account_id,
      FROM `order`
      WHERE partition_date >= '2023-05-27'
        AND order_type in (1, 12, 13)
        AND order_status in (1,2,7,8,12)) t1
LEFT JOIN card_info t2 
    ON t1.bank_account_id = CAST(t2.bank_account_id AS STRING); -- 关联键显式转换成String类型

