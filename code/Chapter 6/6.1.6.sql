-- 优化前
SELECT *
FROM(SELECT '7' AS metric_index,
            'Number of churn login users, who had login in the counting period but no login in the following period' AS metric_definition,
            COUNT(DISTINCT(uid)) AS `value`
     FROM user_login_log AS login_2
     WHERE from_unixtime(`time`, 'yyyy-MM-dd') >= '2022-06-23'
       AND from_unixtime(`time`, 'yyyy-MM-dd') <= '2022-06-23'
       AND `action` = 0 -- 0=登录 1=退出
       AND EXISTS(SELECT uid
                  FROM user_login_log AS login_1
                  WHERE from_unixtime(`time`, 'yyyy-MM-dd') >= '2022-06-24'
                    AND from_unixtime(`time`, 'yyyy-MM-dd') <= '2022-06-24'
                    AND `action` = 0
                    AND login_1.uid = login_2.uid ));


-- 优化后
-- 采用SEMI JOIN
SELECT '7' AS metric_index,
       'Number of churn login users, who had login in the counting period but no login in the following period' AS metric_definition,
       COUNT(DISTINCT login_2.uid) AS `value`
FROM user_login_log AS login_2
LEFT SEMI JOIN user_login_log AS login_1
  ON login_2.uid = login_1.uid
  AND from_unixtime(login_1.`time`, 'yyyy-MM-dd') >= '2022-06-24'
  AND from_unixtime(login_1.`time`, 'yyyy-MM-dd') <= '2022-06-24'
  AND login_1.`action` = 0
WHERE from_unixtime(login_2.`time`, 'yyyy-MM-dd') >= '2022-06-23'
  AND from_unixtime(login_2.`time`, 'yyyy-MM-dd') <= '2022-06-23'
  AND login_2.`action` = 0;

-- 优化后
-- 采用LEFT OUTER JOIN
SELECT '7' AS metric_index,
       'Number of churn login users, who had login in the counting period but no login in the following period' AS metric_definition,
       COUNT(DISTINCT login_2.uid) AS `value`
FROM user_login_log AS login_2
LEFT OUTER JOIN user_login_log AS login_1
  ON login_2.uid = login_1.uid
  AND from_unixtime(login_1.`time`, 'yyyy-MM-dd') >= '2022-06-24'
  AND from_unixtime(login_1.`time`, 'yyyy-MM-dd') <= '2022-06-24'
  AND login_1.`action` = 0
WHERE from_unixtime(login_2.`time`, 'yyyy-MM-dd') >= '2022-06-23'
  AND from_unixtime(login_2.`time`, 'yyyy-MM-dd') <= '2022-06-23'
  AND login_2.`action` = 0
  AND login_1.uid IS NOT NULL;
