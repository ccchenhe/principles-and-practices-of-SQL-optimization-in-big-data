-- ...
,content_id BIGINT COMMENT '内容ID'
,content_type STRING COMMENT '内容类型，例如短视频、图文'
,author_id BIGINT COMMENT '创作者ID'
,content_status STRING COMMENT '内容状态，发布、删除、封禁'
,is_view INT COMMENT '该曝光事件是否算做有效浏览文章或帖子，1=是，0=否'
,content_source INT COMMENT '这篇文章或帖子的来源，是帖子全文还是只有帖子的封面，1=是，0=否'
,content_impression_type INT COMMENT '这篇文章或帖子的曝光类型，0=不属于帖子的曝光，1=明细曝光, 2=封面曝光'
,is_item_impression INT comment '帖子中是否有商品曝光，0=否，1=是'
,is_voucher_impression INT comment '帖子中是否有优惠券的曝光，0=否，1=是'
,content_create_date DATE comment '内容发布的时间yyyy-MM-dd'
,page_tab_id INT comment '发现页和内容首页多个tab栏ID'
,page_tab_name STRING comment '发现页和内容首页多个tab栏，例如美妆、笔记、热门等等'
,experiment_group_list ARRAY<STRING> comment '算法AB实验组字段，只提取实验组字段，不做扩展，例如实验组1,实验组23,实验组345'
-- ...


-- 下沉前
SELECT partition_date,
       COUNT(case WHEN page_type = 'list' AND target_type = 'article' AND json_extract_scalar(data, '$.tab_id') <> '2' THEN user_id
                  WHEN page_type = 'hashtag_detail' AND target_type = 'article' THEN user_id
                  WHEN page_type = 'article' THEN user_id
                  WHEN page_type = 'video' AND target_type = 'article' THEN user_id
                  WHEN page_type = 'explore' AND target_type = 'article' THEN user_id
                  WHEN page_type in ('us', 'shop', 'my_like') AND target_type = 'article' THEN user_id
                  ELSE NULL END ) AS impression_pv
FROM tracking_impression
WHERE partition_date = '2021-11-01'
GROUP BY partition_date;

-- 下沉后
SELECT partition_date,
       COUNT(1) AS impression_pv
FROM tracking_impression
WHERE partition_date = '2021-11-01'
  AND content_impression_type = 1 -- 上述复杂SQL的抽象
GROUP BY partition_date;
