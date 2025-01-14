-- 优化前
SELECT COUNT(DISTINCT ba.uid) AS distinct_uid_count
FROM bank_account ba -- 银行账户表
LEFT JOIN user_register b -- 用户注册表
 ON ba.uid = b.uid 
LEFT JOIN user_info a -- 用户个人信息表
  ON ba.uid = a.uid 
WHERE ba.channel_id != 10004 -- 数字钱包服务
  AND ba.flag IN (1, 257); -- 用户没有被封禁


-- 优化后
SELECT COUNT(DISTINCT ba.uid) AS distinct_uid_count
FROM bank_account_tab ba
WHERE ba.channel_id != 10004 -- 数字钱包服务
  AND ba.flag IN (1, 257); -- 用户没有被封禁
