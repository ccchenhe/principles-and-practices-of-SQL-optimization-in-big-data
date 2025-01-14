-- 优化前
WITH base AS (SELECT id
                     ,partner_id -- 渠道id
                     ,from_unixtime(txn.create_time) AS txn_date
                     ,gateway -- 支付网关
                     ,error_code -- 错误码
                     ,get_json_object(extra_data, '$.bank_error_code') AS bank_error_code -- 错误码对应的银行
                     ,get_json_object(extra_data, '$.bank_error_desc') AS bank_error_desc -- 具体描述信息
                     ,report_status
                     ,currency
                     ,bank_id -- 卡id
                     ,amount
                     ,update_time
                     ,create_time
                     ,country
              FROM remittance txn),
-- 时间维表
date_tab AS(SELECT `date`
                   ,year_month
                   ,week_begin
                   ,week_range
            FROM date_mapping),
-- 支付明细表
dwd_txn AS(SELECT 
                  country
                 ,txn_date
                 ,date_tab.week_range AS txn_week
                 ,date_tab.year_month AS txn_month
                 ,CONCAT(CAST(txn.partner_id AS string), '_', IF(partner.partner_name IS NULL, 'NULL', partner.partner_name)) AS partner_name
                 ,CONCAT(IF(bg.id IS NULL, 'NULL', bg.id), '_', CAST(txn.gateway AS string)) AS gateway_name
                 ,CONCAT(IF(txn.error_code IS NULL, 'NULL', txn.error_code), '_', IF(sme.name IS NULL, 'NULL', sme.name)) AS error_code
                 ,CONCAT(IF(txn.bank_error_code IS NULL, 'NULL', txn.bank_error_code), '_', IF(txn.bank_error_desc IS NULL, 'NULL', txn.bank_error_desc)) AS bank_error_code
                 ,CONCAT(CAST(txn.report_status AS string), '_', IF(sms.name IS NULL, 'NULL', sms.name)) AS txn_status
                 ,report_status
                 ,currency
                 ,concat(CAST(txn.bank_id AS string), '_', IF(bank.bank_name IS NULL, 'NULL', bank.bank_name)) AS bank_name
                 ,txn.create_time
                 ,txn.update_time
                 ,txn.id AS id
                 ,txn.amount AS amount
   FROM base txn
   LEFT JOIN  partner
     ON txn.partner_id = partner.partner_id
   LEFT JOIN  bank
     ON txn.bank_id = bank.id
   LEFT JOIN  bg
     ON txn.gateway = bg.gateway_name
   LEFT JOIN sme
     ON txn.error_code = sme.id
     AND sme.field_name = 'error_code'
   LEFT JOIN sms
     ON txn.report_status = sms.id
     AND sms.field_name = 'report_status'
   LEFT JOIN date_tab
     ON DATE(txn.txn_date) = date_tab.`date`)

SELECT country
       ,txn_date
       ,txn_week
       ,txn_month
       ,partner_name
       ,gateway_name
       ,error_code
       ,bank_error_code
       ,txn_status
       ,currency
       ,bank_name
       ,COUNT(DISTINCT id) AS txn_count
       ,COUNT(DISTINCT IF(report_status=4, id, NULL)) AS success_txn_count
       ,COUNT(DISTINCT IF(report_status=5, id, NULL)) AS fail_txn_count
       ,SUM(amount) AS txn_amount
       ,COUNT(DISTINCT IF(report_status=4, amount, NULL)) AS success_txn_amount
       ,COUNT(DISTINCT IF(report_status=5, amount, NULL)) AS fail_txn_amount
       ,SUM(IF(report_status=4, update_time - create_time, NULL)) AS success_process_time_s
       ,AVG(CAST(IF(report_status=4, update_time - create_time, NULL) AS DOUBLE)) AS success_process_time_avg_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=4, update_time - create_time, NULL) AS DOUBLE), 0.25) AS success_process_time_p25_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=4, update_time - create_time, NULL) AS DOUBLE), 0.50) AS success_process_time_p50_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=4, update_time - create_time, NULL) AS DOUBLE), 0.75) AS success_process_time_p75_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=4, update_time - create_time, NULL) AS DOUBLE), 0.95) AS success_process_time_p95_s
       ,SUM(IF(report_status=5, update_time - create_time, NULL)) AS fail_process_time_s
       ,AVG(CAST(IF(report_status=5, update_time - create_time, NULL) AS DOUBLE)) AS fail_process_time_avg_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=5, update_time - create_time, NULL) AS DOUBLE), 0.25) AS fail_process_time_p25_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=5, update_time - create_time, NULL) AS DOUBLE), 0.50) AS fail_process_time_p50_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=5, update_time - create_time, NULL) AS DOUBLE), 0.75) AS fail_process_time_p75_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=5, update_time - create_time, NULL) AS DOUBLE), 0.95) AS fail_process_time_p95_s
   FROM dwd_txn
   GROUP BY 1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11
;


-- 优化后
WITH base AS (SELECT id
                     ,partner_id -- 渠道id
                     ,from_unixtime(txn.create_time) AS txn_date
                     ,gateway -- 支付网关
                     ,error_code -- 错误码
                     ,get_json_object(extra_data, '$.bank_error_code') AS bank_error_code -- 错误码对应的银行
                     ,get_json_object(extra_data, '$.bank_error_desc') AS bank_error_desc -- 具体描述信息
                     ,report_status
                     ,currency
                     ,bank_id -- 卡id
                     ,amount
                     ,update_time
                     ,create_time
                     ,country
              FROM remittance txn),
-- 时间维表
date_tab AS(SELECT `date`
                   ,year_month
                   ,week_begin
                   ,week_range
            FROM date_mapping),
-- 支付明细表
dwd_txn AS(SELECT /*+ broadcastjoin(partner,bank,bg,sme,sms,date_tab) */
                  country
                 ,txn_date
                 ,date_tab.week_range AS txn_week
                 ,date_tab.year_month AS txn_month
                 ,CONCAT(CAST(txn.partner_id AS string), '_', IF(partner.partner_name IS NULL, 'NULL', partner.partner_name)) AS partner_name
                 ,CONCAT(IF(bg.id IS NULL, 'NULL', bg.id), '_', CAST(txn.gateway AS string)) AS gateway_name
                 ,CONCAT(IF(txn.error_code IS NULL, 'NULL', txn.error_code), '_', IF(sme.name IS NULL, 'NULL', sme.name)) AS error_code
                 ,CONCAT(IF(txn.bank_error_code IS NULL, 'NULL', txn.bank_error_code), '_', IF(txn.bank_error_desc IS NULL, 'NULL', txn.bank_error_desc)) AS bank_error_code
                 ,CONCAT(CAST(txn.report_status AS string), '_', IF(sms.name IS NULL, 'NULL', sms.name)) AS txn_status
                 ,report_status
                 ,currency
                 ,concat(CAST(txn.bank_id AS string), '_', IF(bank.bank_name IS NULL, 'NULL', bank.bank_name)) AS bank_name
                 ,txn.create_time
                 ,txn.update_time
                 ,txn.id AS id
                 ,txn.amount AS amount
   FROM base txn
   LEFT JOIN  partner
     ON txn.partner_id = partner.partner_id
   LEFT JOIN  bank
     ON txn.bank_id = bank.id
   LEFT JOIN  bg
     ON txn.gateway = bg.gateway_name
   LEFT JOIN sme
     ON txn.error_code = sme.id
     AND sme.field_name = 'error_code'
   LEFT JOIN sms
     ON txn.report_status = sms.id
     AND sms.field_name = 'report_status'
   LEFT JOIN date_tab
     ON DATE(txn.txn_date) = date_tab.`date`)

SELECT country
       ,txn_date
       ,txn_week
       ,txn_month
       ,partner_name
       ,gateway_name
       ,error_code
       ,bank_error_code
       ,txn_status
       ,currency
       ,bank_name
       ,COUNT(DISTINCT id) AS txn_count
       ,COUNT(DISTINCT IF(report_status=4, id, NULL)) AS success_txn_count
       ,COUNT(DISTINCT IF(report_status=5, id, NULL)) AS fail_txn_count
       ,SUM(amount) AS txn_amount
       ,COUNT(DISTINCT IF(report_status=4, amount, NULL)) AS success_txn_amount
       ,COUNT(DISTINCT IF(report_status=5, amount, NULL)) AS fail_txn_amount
       ,SUM(IF(report_status=4, update_time - create_time, NULL)) AS success_process_time_s
       ,AVG(CAST(IF(report_status=4, update_time - create_time, NULL) AS DOUBLE)) AS success_process_time_avg_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=4, update_time - create_time, NULL) AS DOUBLE), 0.25) AS success_process_time_p25_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=4, update_time - create_time, NULL) AS DOUBLE), 0.50) AS success_process_time_p50_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=4, update_time - create_time, NULL) AS DOUBLE), 0.75) AS success_process_time_p75_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=4, update_time - create_time, NULL) AS DOUBLE), 0.95) AS success_process_time_p95_s
       ,SUM(IF(report_status=5, update_time - create_time, NULL)) AS fail_process_time_s
       ,AVG(CAST(IF(report_status=5, update_time - create_time, NULL) AS DOUBLE)) AS fail_process_time_avg_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=5, update_time - create_time, NULL) AS DOUBLE), 0.25) AS fail_process_time_p25_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=5, update_time - create_time, NULL) AS DOUBLE), 0.50) AS fail_process_time_p50_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=5, update_time - create_time, NULL) AS DOUBLE), 0.75) AS fail_process_time_p75_s
       ,APPROX_PERCENTILE(CAST(IF(report_status=5, update_time - create_time, NULL) AS DOUBLE), 0.95) AS fail_process_time_p95_s
   FROM dwd_txn
   GROUP BY 1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11
;
