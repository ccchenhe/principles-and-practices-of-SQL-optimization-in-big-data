-- 优化前
SELECT COUNT(1)
FROM `db`.`order`
WHERE partition_date = '2023-09-04'
  -- 2023-09-04下单的用户id不在2023-09-03下过单的用户id列表中
  AND user_id NOT IN (SELECT user_id
                      FROM `db`.`order`
                      WHERE partition_date = '2023-09-03');




-- 优化后
SELECT COUNT(1)
FROM `db`.`order` t1 
LEFT OUTER JOIN (SELECT user_id
                 FROM `db`.`order`
                 WHERE partition_date = '2023-09-03') t2
  ON t1.user_id = t2.user_id
WHERE t1.partition_date = '2023-09-04'
  -- 2023-09-04 LEFT OUTER JOIN 2023-09-03
  -- 且2023-09-03的用户id 为NULL
  AND t2.user_id IS NULL; 
