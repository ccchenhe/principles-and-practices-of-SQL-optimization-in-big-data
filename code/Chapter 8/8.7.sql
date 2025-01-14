-- 优化前
SELECT NVL(t1.user_id, t2.user_id)
      ,IF(t1.user_id IS NOT NULL AND t2.user_id IS NULL, 1, 0) AS only_push
      ,IF(t1.user_id IS NULL AND t2.user_id IS NOT NULL, 1, 0) AS only_app
FROM (SELECT user_id
      FROM tracking_click
      WHERE operation = 'into_push' -- 通过消息推送进入APP
      GROUP BY user_id) t1
FULL OUTER JOIN (SELECT user_id
                 FROM tracking_click
                 WHERE operation = 'into_app' -- 直接点击桌面图标
                 GROUP BY user_id) t2 
  ON t1.user_id = t2.user_id;


-- 优化后
SELECT user_id
      ,IF(push = 1 AND app = 0, 1, 0) AS only_push
      ,IF(push = 0 AND app = 1, 1, 0) AS only_app
FROM (SELECT user_id
            ,MAX(IF(operation = 'into_push', 1, 0)) AS push
            ,MAX(IF(operation = 'into_app', 1, 0)) AS app
      FROM tracking_click
      WHERE operation in ('into_app', 'into_push')
      GROUP BY user_id)t; -- 以user id聚合，对明细数据进行标记，判断用户通过何种方式进入APP
