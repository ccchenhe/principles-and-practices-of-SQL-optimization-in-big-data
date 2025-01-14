-- 优化前
SELECT COUNT(order_id)
FROM order
WHERE partition_date = '2023-01-01'
  -- merchant_partner_type为空
  AND get_json_object(order_info, '$.merchant_partner_type') IS NULL
  -- merchant_partner_type 不等于2
  OR get_json_object(order_info, '$.merchant_partner_type') != '2';


-- 优化后
SELECT COUNT(order_id)
FROM order
WHERE partition_date = '2023-01-01'
  AND (get_json_object(order_info, '$.merchant_partner_type') IS NULL
         OR get_json_object(order_info, '$.merchant_partner_type') != '2');
