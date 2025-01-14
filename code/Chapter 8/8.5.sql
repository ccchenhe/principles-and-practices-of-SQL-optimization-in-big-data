-- 优化前
SELECT COUNT(DISTINCT IF(is_act_user = 1, user_id, NULL)) AS act_user_num -- 活跃用户数
      ,COUNT(DISTINCT IF(is_rcmd_view = 1, user_id, NULL)) AS rcmd_view_user_num -- 通过算法推荐进入直播间的活跃用户数
      ,COUNT(DISTINCT IF(is_hot_view = 1, user_id, NULL)) AS hot_view_user_num -- 通过运营配置（热门）进入直播间的活跃用户数
      ,COUNT(DISTINCT IF(is_follow_view = 1, user_id, NULL)) AS follow_view_user_num -- 通过关注列表进入直播间的活跃用户数
FROM tracking_impression
WHERE partition_date = '2023-09-01';

-- 优化后
SELECT SUM(is_act_user_num)
      ,SUM(is_rcmd_view_num)
      ,SUM(is_hot_view_num)
      ,SUM(is_follow_view_num)
FROM (SELECT user_id
            ,MAX(is_act_user) AS is_act_user_num
            ,MAX(IF(is_rcmd_view > 0, 1, 0)) AS is_rcmd_view_num
            ,MAX(IF(is_hot_view > 0, 1, 0)) AS is_hot_view_num
            ,MAX(IF(is_follow_view > 0, 1, 0)) AS is_follow_view_num
      FROM tracking_impression
      WHERE partition_date = '2023-09-01'
      GROUP BY user_id) t; -- 根据用户id进行聚合，实际上只需要确认这一用户是否有通过该渠道进入直播间，因此只判断明细数据中渠道标识的最大值。