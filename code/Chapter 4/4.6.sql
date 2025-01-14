-- 优化前
SELECT COUNT(1)
FROM transaction t1
LEFT OUTER JOIN action t2
  ON t1.transaction_id = t2.transaction_id
WHERE t2.extinfo IS NULL;


-- 优化后
SELECT COUNT(1)
FROM transaction t1
LEFT OUTER JOIN (SELECT *
                 FROM action
                 WHERE extinfo IS NULL) t2
  ON t1.transaction_id = t2.transaction_id;
