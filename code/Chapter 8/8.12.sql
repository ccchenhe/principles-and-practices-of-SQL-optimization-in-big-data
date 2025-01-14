-- 优化前
SELECT  experiment_group_id
       ,partition_date
       ,nvl(scene,'all') AS scene -- 推荐场景
       ,nvl(uaf,'all') AS uaf -- 用户活跃标识，例如新老用户、低活跃用户
       ,COUNT(DISTINCT CASE WHEN experiment_pv_1d > 0 THEN user_id ELSE NULL END) AS experiment_uv_1d
       ,COUNT(DISTINCT CASE WHEN streaming_pv_1d > 0 THEN user_id ELSE NULL END) AS streaming_uv_1d
       ,COUNT(DISTINCT CASE WHEN stream_cover_impression_1d > 0 THEN user_id ELSE NULL END ) AS stream_cover_impression_uv_1d       ,COUNT(DISTINCT CASE WHEN stream_cover_quality_click_1d > 0 THEN user_id ELSE NULL END ) AS stream_cover_quality_click_uv_1d
       ,COUNT(DISTINCT CASE WHEN stream_cover_click_1d > 0 THEN user_id ELSE NULL END ) AS stream_cover_click_uv_1d       ,COUNT(DISTINCT CASE WHEN order_cnt_1d > 0 THEN user_id ELSE NULL END) AS order_buyer_cnt_1d
       ,COUNT(DISTINCT CASE WHEN f24h_order_cnt_1d > 0 THEN user_id ELSE NULL END) AS f24h_order_buyer_cnt_1d
       ,COUNT(DISTINCT CASE WHEN contain_slide_quality_view_pv_1d > 0 THEN user_id ELSE NULL END) AS contain_slide_quality_view_uv_1d
       ,COUNT(DISTINCT CASE WHEN contain_slide_view_pv_1d > 0 THEN user_id ELSE NULL END) AS contain_slide_view_uv_1d
       ,COUNT(DISTINCT CASE WHEN last_order_cnt_1d > 0 THEN user_id ELSE NULL END) AS last_order_uv_1d
       ,COUNT(DISTINCT CASE WHEN last_direct_order_cnt_1d > 0 THEN user_id ELSE NULL END) AS last_direct_order_uv_1d
       ,COUNT(DISTINCT CASE WHEN last_indirect_order_cnt_1d > 0 THEN user_id ELSE NULL END) AS last_indirect_order_uv_1d
FROM overview_user
WHERE partition_date BETWEEN '2023-09-01' 
  AND '2023-09-02'
GROUP BY experiment_group_id
        ,partition_date
        ,scene
        ,uaf
GROUPING SETS ((experiment_group_id,partition_date,scene,uaf)
              ,(experiment_group_id,partition_date,uaf)
              ,(experiment_group_id,partition_date));

-- 优化后
SELECT experiment_group_id
       ,partition_date
       ,nvl(scene,'rcmd_all') AS scene
       ,nvl(uaf,'all') AS uaf
       ,COUNT(CASE WHEN experiment_pv_1d > 0 THEN user_id ELSE NULL END) AS experiment_uv_1d
       ,COUNT(CASE WHEN streaming_pv_1d > 0 THEN user_id ELSE NULL END) AS streaming_uv_1d
       ,COUNT(CASE WHEN stream_cover_impression_1d > 0 THEN user_id ELSE NULL END ) AS stream_cover_impression_uv_1d
       ,COUNT(CASE WHEN stream_cover_quality_click_1d > 0 THEN user_id ELSE NULL END ) AS stream_cover_quality_click_uv_1d
       ,COUNT(CASE WHEN stream_cover_click_1d > 0 THEN user_id ELSE NULL END ) AS stream_cover_click_uv_1d
       ,COUNT(CASE WHEN order_cnt_1d > 0 THEN user_id ELSE NULL END) AS order_buyer_cnt_1d
       ,COUNT(CASE WHEN f24h_order_cnt_1d > 0 THEN user_id ELSE NULL END) AS f24h_order_buyer_cnt_1d
       ,COUNT(CASE WHEN contain_slide_quality_view_pv_1d > 0 THEN user_id ELSE NULL END) AS contain_slide_quality_view_uv_1d
       ,COUNT(CASE WHEN contain_slide_view_pv_1d > 0 THEN user_id ELSE NULL END) AS contain_slide_view_uv_1d
       ,COUNT(CASE WHEN last_order_cnt_1d > 0 THEN user_id ELSE NULL END) AS last_order_uv_1d
       ,COUNT(CASE WHEN last_direct_order_cnt_1d > 0 THEN user_id ELSE NULL END) AS last_direct_order_uv_1d
       ,COUNT(CASE WHEN last_indirect_order_cnt_1d > 0 THEN user_id ELSE NULL END) AS last_indirect_order_uv_1d
FROM (SELECT experiment_group_id
            ,partition_date
            ,scene
            ,uaf
            ,user_id
            ,SUM(experiment_pv_1d) AS experiment_pv_1d
            ,SUM(streaming_pv_1d) AS streaming_pv_1d
            ,SUM(stream_cover_impression_1d) AS stream_cover_impression_1d
            ,SUM(stream_cover_quality_click_1d) AS stream_cover_quality_click_1d
            ,SUM(stream_cover_click_1d) AS stream_cover_click_1d
            ,SUM(order_cnt_1d) AS order_cnt_1d
            ,SUM(f24h_order_cnt_1d) AS f24h_order_cnt_1d
            ,SUM(contain_slide_quality_view_pv_1d) AS contain_slide_quality_view_pv_1d
            ,SUM(contain_slide_view_pv_1d) AS contain_slide_view_pv_1d
            ,SUM(last_order_cnt_1d) AS last_order_cnt_1d
            ,SUM(last_direct_order_cnt_1d) AS last_direct_order_cnt_1d
            ,SUM(last_indirect_order_cnt_1d) AS last_indirect_order_cnt_1d
      FROM overview_user
      WHERE partition_date BETWEEN '2023-09-01'
        AND '2023-09-02'
      GROUP BY experiment_group_id
              ,partition_date
              ,scene
              ,uaf
              ,user_id) t
GROUP BY experiment_group_id
        ,partition_date
        ,scene
        ,uaf
GROUPING SETS ((experiment_group_id,partition_date,scene,uaf)
              ,(experiment_group_id,partition_date,uaf)
              ,(experiment_group_id,partition_date));

