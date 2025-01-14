-- 优化前
-- 定义UDF
CREATE OR REPLACE TEMPORARY FUNCTION pb_to_json AS 'com.xx.udf' USING JAR 'hdfs path';

INSERT OVERWRITE TABLE result
-- 将extinfo转换为JSON字符串，取支付渠道id、银行卡id、卡指纹等
SELECT get_json_object(pb_to_json(get_json_object(`data`, '$.extinfo'), 'class'), '$.info.channel_id')
      ,get_json_object(pb_to_json(get_json_object(`data`, '$.extinfo'), 'class'), '$.bank.id_no')
      ,get_json_object(pb_to_json(get_json_object(`data`, '$.extinfo'), 'class'), '$.bank.fingerprint')
FROM order;


-- 尝试优化*1
-- 定义UDF
CREATE OR REPLACE TEMPORARY FUNCTION pb_to_json AS 'com.xx.udf' USING JAR 'hdfs path';

INSERT OVERWRITE TABLE result
SELECT get_json_object(info, '$.info.channel_id')
      ,get_json_object(info, '$.bank.id_no')
      ,get_json_object(info, '$.bank.fingerprint')
-- extinfo转为JSON字符串的子查询
FROM (SELECT pb_to_json(get_json_object(`data`, '$.extinfo'), 'class') AS info
      FROM order) t;

-- 优化后
-- 定义UDF
CREATE OR REPLACE TEMPORARY FUNCTION pb_to_json AS 'com.xx.udf' USING JAR 'hdfs path';

INSERT OVERWRITE TABLE result
SELECT get_json_object(info, '$.info.channel_id')
      ,get_json_object(info, '$.bank.id_no')
      ,get_json_object(info, '$.bank.fingerprint')
FROM (SELECT pb_to_json(get_json_object(`data`, '$.extinfo'), 'class') AS info -- 将extinfo转为JSON字符串
            ,RAND() AS random_key -- 定义返回0-1之间随机数的字段
      FROM order) t
WHERE random_key < 2; -- 外层查询调用，且条件恒为TRUE
