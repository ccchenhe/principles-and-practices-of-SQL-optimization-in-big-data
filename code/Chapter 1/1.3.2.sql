-- 用于计算直播间的观看时长
WITH streaming_detail_data AS(
SELECT  MAX(viewer_id) AS viewer_id -- 用户uid
        ,ls_session_id -- 直播间id
        ,device_id -- 设备id
        -- ...
        ,MAX(next_view_timestamp) AS next_view_timestamp -- 下一次浏览直播间的时间戳
        ,COUNT(1) AS heartbeat_cnt -- 上报埋点次数
        ,MIN(heart_timestamp) AS first_heartbeat_timestamp -- 首次心跳上报的时间戳
        ,MAX(heart_timestamp) AS last_heartbeat_timestamp  -- 末次心跳上报的时间戳
        ,SUM(CASE WHEN time_diff_a < 10 THEN time_diff_a ELSE 0 END) AS duration_a -- 面向运营的直播观看时长
        ,SUM(CASE WHEN time_diff_b < 10 THEN time_diff_b ELSE 0 END) AS duration_b -- 面向算法的直播观看时长
FROM(SELECT  viewer_id
            ,ls_session_id
            ,device_id
            -- ...
            ,CASE WHEN next_heart_timestamp = 0 THEN 5 ELSE (next_heart_timestamp - heart_timestamp)/1000 END AS time_diff_a
            ,CASE WHEN next_heart_timestamp = 0 THEN 2.5 ELSE (next_heart_timestamp - heart_timestamp)/1000 END AS time_diff_b
     FROM(SELECT t1.user_id AS viewer_id
                ,t1.ls_session_id
                ,t1.device_id
                ,t1.event_id AS view_event_id
                ,t1.event_time AS view_event_time
                ,t1.event_timestamp AS view_event_timestamp
                ,t2.event_timestamp AS heart_timestamp
                ,LEAD(t2.event_timestamp,1,0) OVER(PARTITION BY t1.event_id ORDER BY t2.event_timestamp ASC) AS next_heart_timestamp --同一个view事件只会有一个最末心跳
          FROM(SELECT user_id
                     ,device_id
                     ,event_timestamp
                     ,event_time
                     ,event_id
                     ,pre_source['event_id'] AS view_pre_event_id
                     ,pre_source AS view_pre_event_source
                     ,get_json_object(`data`,'$.from_source') AS view_from_source
                     ,ls_session_id
                     ,CAST(get_json_object(get_json_object(`data`,'$.ls_pass_through_params'),'$.ls_info.tab_type') AS INT) AS from_tab_type
                     ,LEAD(event_timestamp,1, 9999999999999) OVER(PARTITION BY user_id,device_id,ls_session_id ORDER BY event_timestamp ASC) AS next_event_timestamp
               FROM db.tracking
               WHERE page_type = 'streaming_room'
                 AND operation = 'view'
                 AND device_id IS NOT NULL
                 AND ls_session_id > 0) t1 -- 取每次进入直播间后上报的浏览埋点事件
          INNER JOIN(SELECT event_timestamp
                           ,ls_session_id
                           ,userid
                           ,deviceid
                     FROM db.tracking
                     WHERE operation_type = 'other'
                       AND operation = 'action_active_in_streaming'
                       AND deviceid IS NOT NULL
                       AND ls_session_id IS NOT NULL) t2  -- 取进入直播间后每隔5秒上报的心跳事件
            ON t1.user_id = t2.userid
            AND t1.device_id = t2.deviceid
            AND t1.ls_session_id = t2.ls_session_id
            AND (t2.event_timestamp >= t1.event_timestamp AND t2.event_timestamp < t1.next_event_timestamp)) t_event) t --将心跳匹配到view中
GROUP BY ls_session_id
        ,device_id)
;