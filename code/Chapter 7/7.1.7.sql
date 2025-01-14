-- 全外连接
SELECT  t1.user_id
       ,t1.name
       ,t2.age
FROM tmp_user_info t1
FULL OUTER JOIN tmp_user_info_ext t2
  ON t1.user_id = t2.user_id
WHERE t1.user_id > 2;

-- 在语义上等于左外连接
SELECT  t1.user_id
       ,t1.name
       ,t2.age
FROM tmp_user_info t1
LEFT OUTER JOIN tmp_user_info_ext t2
  ON t1.user_id = t2.user_id
WHERE t1.user_id > 2;
