-- 创建物化视图
CREATE MATERIALIZED VIEW view_receive
STORED AS ORC
AS 
SELECT related_order_id, -- 红包的order id
      COUNT(1) AS receive_order_count, -- 红包领取人数
      SUM(amount) AS total_received -- 红包领取金额
FROM `order`
WHERE order_type = 18 -- 接收红包
 AND order_status = 1 -- 成功
GROUP BY related_order_id;


-- 优化前
SELECT create_date,
        order_id,
        order_status,
        user_id,
        amount,
        fee_amount,
        virtual_card_category_id,
        virtual_card_id,
        user_quantity_input,
        angbao_type,
        receive_order_count,
        total_received
FROM (SELECT from_unixtime(create_time, 'yyyy-MM-dd') AS create_date,
             order_id,
             CASE WHEN order_status = 1 THEN 'SUCCESS'
                  WHEN order_status = 2 THEN 'PROCESSING'
                  WHEN order_status = 3 THEN 'CONFIRMING'
                  WHEN order_status = 4 THEN 'REFUNDING'
                  WHEN order_status = 5 THEN 'FAILED'
                  WHEN order_status = 6 THEN 'REFUNDED'
                  WHEN order_status = 7 THEN 'CANCELLED' END AS order_status, -- 订单状态，此处指领取红包的状态
             user_id,
             amount,
             get_json_object(extinfo, '$.angbao_send_parent_extinfo.virtual_card_category_id') AS virtual_card_category_id, -- 发放红包的卡类型
             nvl(get_json_object(extinfo, '$.angbao_send_parent_extinfo.virtual_card_id'), 'Unknown') AS virtual_card_id, -- 发放红包的卡id
             nvl(get_json_object(extinfo, '$.angbao_send_parent_extinfo.total_count'), 'Unknown') AS user_quantity_input, -- 发放红包金额
             CASE WHEN get_json_object(extinfo, '$.angbao_send_parent_extinfo.angbao_type') = 1 THEN 'Random'
                  WHEN get_json_object(extinfo, '$.angbao_send_parent_extinfo.angbao_type') = 2 THEN 'Fixed'
                  ELSE 'Unknown' END AS angbao_type -- 红包算法类型
     FROM order
     WHERE order_type = 17) AS send -- 发红包
INNER JOIN (SELECT related_order_id
                   ,COUNT(1) AS receive_order_count
                   ,SUM(amount) AS total_received
            FROM `order`
            WHERE order_type = 18 -- 接收红包
             AND order_status = 1 -- 成功
            GROUP BY related_order_id) AS receive          
;

-- 优化后
SELECT create_date,
        order_id,
        order_status,
        user_id,
        amount,
        fee_amount,
        virtual_card_category_id,
        virtual_card_id,
        user_quantity_input,
        angbao_type,
        receive_order_count,
        total_received
FROM (SELECT from_unixtime(create_time, 'yyyy-MM-dd') AS create_date,
             order_id,
             CASE WHEN order_status = 1 THEN 'SUCCESS'
                  WHEN order_status = 2 THEN 'PROCESSING'
                  WHEN order_status = 3 THEN 'CONFIRMING'
                  WHEN order_status = 4 THEN 'REFUNDING'
                  WHEN order_status = 5 THEN 'FAILED'
                  WHEN order_status = 6 THEN 'REFUNDED'
                  WHEN order_status = 7 THEN 'CANCELLED' END AS order_status, -- 订单状态，此处指领取红包的状态
             user_id,
             amount,
             get_json_object(extinfo, '$.angbao_send_parent_extinfo.virtual_card_category_id') AS virtual_card_category_id, -- 发放红包的卡类型
             nvl(get_json_object(extinfo, '$.angbao_send_parent_extinfo.virtual_card_id'), 'Unknown') AS virtual_card_id, -- 发放红包的卡id
             nvl(get_json_object(extinfo, '$.angbao_send_parent_extinfo.total_count'), 'Unknown') AS user_quantity_input, -- 发放红包金额
             CASE WHEN get_json_object(extinfo, '$.angbao_send_parent_extinfo.angbao_type') = 1 THEN 'Random'
                  WHEN get_json_object(extinfo, '$.angbao_send_parent_extinfo.angbao_type') = 2 THEN 'Fixed'
                  ELSE 'Unknown' END AS angbao_type -- 红包算法类型
     FROM order
     WHERE order_type = 17) AS send -- 发红包
INNER JOIN view_receive 
  ON send.order_id = receive.related_order_id
GROUP BY create_date,
         order_id,
         order_status,
         user_id,
         amount,
         fee_amount,
         virtual_card_category_id,
         virtual_card_id,
         user_quantity_input,
         angbao_type,
         receive_order_count,
         total_received;
