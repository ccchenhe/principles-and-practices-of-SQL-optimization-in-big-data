-- 两阶段聚合示例

-- 语句 1
SELECT order_type
      ,COUNT(1)
      ,SUM(amount)
FROM `order`
GROUP BY order_type;

-- 语句 2
SELECT SPLIT(first_phase_type, '-')[1] AS second_phase_type
      ,SUM(cnt)
      ,SUM(amt)
FROM (SELECT first_phase_type
            ,COUNT(1) AS cnt
            ,SUM(amount) AS amt
      FROM (SELECT concat(CAST(RAND() * 90 + 10 AS INT), '-' , order_type) AS first_phase_type
                  ,amount
            FROM `order`) t1
      GROUP BY first_phase_type) t2
GROUP BY SPLIT(first_phase_type, '-')[1];
