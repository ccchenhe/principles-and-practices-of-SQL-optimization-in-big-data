-- 优化前
SELECT experiment_group_id ,
       count(CASE
                 WHEN target_type = 'follow_button' THEN 1
                 ELSE NULL
             END) AS ls_follow_cnt_1d , -- 关注按钮次数
       count(CASE
                 WHEN target_type = 'streamer_icon' THEN 1
                 ELSE NULL
             END) AS ls_streamer_shop_click_cnt_1d , -- 主播主页次数
       count(CASE
                 WHEN target_type = 'item_basket' THEN 1
                 ELSE NULL
             END) AS ls_basket_click_cnt_1d , -- 直播间购物栏点击次数
       count(CASE
                 WHEN target_type = 'like' THEN 1
                 ELSE NULL
             END) AS ls_like_cnt_1d , -- 点赞次数
       count(CASE
                 WHEN target_type = 'send_comment' THEN 1
                 ELSE NULL
             END) AS ls_comment_cnt_1d , -- 发送评论次数
       count(CASE
                 WHEN target_type = 'sharing_option'
                      AND page_section[0] = 'sharing_panel' THEN 1
                 ELSE NULL
             END) AS ls_share_cnt_1d , -- 分享次数
       count(CASE
                 WHEN target_type = 'item'
                      AND page_section[0] = 'display_window' THEN 1
                 ELSE NULL -- 悬浮窗点击次数
             END) AS ls_product_click_cnt_1d
FROM
  (SELECT CASE
              WHEN regexp_extract(regexp_extract(get_json_object(`data`, '$.recommendation_info'), '(.*ABTEST:)(.*?)(,.*)', 2), '(.*)(@)(.*)', 3) IS NULL THEN ''
              ELSE regexp_extract(regexp_extract(get_json_object(`data`, '$.recommendation_info'), '(.*ABTEST:)(.*?)(,.*)', 2), '(.*)(@)(.*)', 3)
          END AS experiment_groups ,
          target_type ,
          page_section
   FROM db.tracking
   WHERE page_type = 'streaming_room'
     AND OPERATION = 'click'
     AND user_id > 0 and(target_type = 'follow_button'
                         OR target_type = 'streamer_icon'
                         OR target_type = 'item_basket'
                         OR target_type = 'like'
                         OR target_type = 'send_comment'
                         OR target_type = 'follow_button'
                         OR (target_type = 'sharing_option'
                             AND page_section[0] = 'sharing_panel')
                         OR (target_type = 'item'
                             AND page_section[0] = 'display_window')) and(get_json_object(`data`, '$.ctx_from_source') = 'lp_topscroll'
                                                                          OR get_json_object(`data`, '$.ctx_from_source') = 'home_live'
                                                                          OR (get_json_object(`data`, '$.ctx_from_source') = 'lp_tab'
                                                                              AND get_json_object(`data`, '$.recommendation_info') like '%REQID%')) )LATERAL VIEW EXPLODE(split(experiment_groups, '_')) t AS experiment_group_id
GROUP BY experiment_group_id;


-- 优化后
SELECT experiment_group_id ,
       COUNT(CASE WHEN target_type = 'follow_button' THEN 1 ELSE NULL END) AS ls_follow_cnt_1d ,
       COUNT(CASE WHEN target_type = 'streamer_icon' THEN 1 ELSE NULL END) AS ls_streamer_shop_click_cnt_1d ,
       COUNT(CASE WHEN target_type = 'item_basket' THEN 1 ELSE NULL END) AS ls_basket_click_cnt_1d ,
       COUNT(CASE WHEN target_type = 'like' THEN 1 ELSE NULL END) AS ls_like_cnt_1d ,
       COUNT(CASE WHEN target_type = 'send_comment' THEN 1 ELSE NULL END) AS ls_comment_cnt_1d ,
       COUNT(CASE WHEN target_type = 'sharing_option' AND page_section[0] = 'sharing_panel' THEN 1 ELSE NULL END) AS ls_share_cnt_1d ,
       COUNT(CASE WHEN target_type = 'item' AND page_section[0] = 'display_window' THEN 1 ELSE NULL END) AS ls_product_click_cnt_1d
FROM (SELECT CASE WHEN regexp_extract(regexp_extract(get_json_object(`data`, '$.recommendation_info'), '(.*ABTEST:)(.*?)(,.*)', 2), '(.*)(@)(.*)', 3) IS NULL THEN ''
                  ELSE regexp_extract(regexp_extract(get_json_object(`data`, '$.recommendation_info'), '(.*ABTEST:)(.*?)(,.*)', 2), '(.*)(@)(.*)', 3)
                  END AS experiment_groups ,
      target_type ,
      page_section ,
      COUNT(1) OVER(PARTITION BY user_id) AS cnt --给最内层的子查询增加一个没意义分组聚合的shuffle操作，使子查询和explode分为2个stage

      FROM db.tracking
      WHERE partition_date = cast('2022-05-25' AS date)
        AND page_type = 'streaming_room'
        AND OPERATION = 'click'
        AND user_id > 0 AND (target_type = 'follow_button' OR target_type = 'streamer_icon' OR target_type = 'item_basket' OR target_type = 'like' OR target_type = 'send_comment' OR target_type = 'follow_button'
                            OR (target_type = 'sharing_option' AND page_section[0] = 'sharing_panel')
                            OR (target_type = 'item' AND page_section[0] = 'display_window')) 
        AND (get_json_object(`data`, '$.ctx_from_source') = 'lp_topscroll' OR get_json_object(`data`, '$.ctx_from_source') = 'home_live' 
             OR (get_json_object(`data`, '$.ctx_from_source') = 'lp_tab' AND get_json_object(`data`, '$.recommendation_info') LIKE '%REQID%')))LATERAL VIEW EXPLODE(SPLIT(experiment_groups, '_')) t AS experiment_group_id
WHERE cnt > 0 --这里需要使用cnt,如果检测到cnt未被使用则上面的shuffle操作不会执行
GROUP BY experiment_group_id;
