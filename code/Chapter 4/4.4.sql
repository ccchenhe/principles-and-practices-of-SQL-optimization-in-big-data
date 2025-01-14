SELECT   user_id
        ,order_id
        ,item_id
        ,create_time
        ,gmv_usd
        ,nmv_usd
        ,is_official_shop -- 官方渠道
        ,IF(b.payment_l1_mapping = 'pay', 1, 0) AS is_pay -- 三方支付
        ,IF(b.payment_l1_mapping = 'wallet', 1, 0) AS is_wallet -- 钱包支付
        -- ...
        ,(item_rebate_usd + voucher_rebate_usd + coin_rebate_usd + shipping_rebate_usd) AS net_total_cost_usd -- 不包含增值税的所有费用
        ,(IF(c.promotion_id IS NOT NULL, pv_voucher_rebate_usd+pv_coin_rebate_usd, 0)+ if(d.promotion_id IS NOT NULL, sv_voucher_rebate_usd+sv_coin_rebate_usd, 0)+ if(e.promotion_id IS NOT NULL, shipping_rebate_usd, 0)) AS net_total_campaign_cost_usd
        ,IF(c.promotion_id IS NOT NULL AND c.voucher_type = 'voucher', pv_voucher_rebate_usd, 0) AS b_voucher_cost_usd -- 优惠券
        ,IF(c.promotion_id IS NOT NULL AND c.voucher_type = 'coin', pv_coin_rebate_usd, 0) AS a_voucher_cost_usd -- 金币
        ,IF(e.promotion_id IS NOT NULL, shipping_rebate_usd, 0) AS net_fsv_voucher_cost_usd
        ,IF(c.promotion_id IS NOT NULL OR d.promotion_id IS NOT NULL OR e.promotion_id IS NOT NULL, 1, 0) AS has_abc_voucher
        ,IF(a.pv_promotion_id IS NOT NULL OR a.sv_promotion_id IS NOT NULL OR a.fsv_promotion_id IS NOT NULL, 1, 0) AS has_any_voucher
FROM `order` a -- 支付流水表
LEFT OUTER JOIN payment b -- 支付渠道表
  ON a.payment_channel_id = b.payment_channel_id
LEFT OUTER JOIN voucher c -- 优惠券表
  ON a.abc_journey_id = c.journey_id
  AND a.abc_version_id = c.version_id
  AND CAST(a.pv_promotion_id AS STRING) = c.promotion_id -- 平台的优惠券id
LEFT OUTER JOIN voucher d -- 优惠券表
  ON a.abc_journey_id = d.journey_id
  AND a.abc_version_id = d.version_id
  AND CAST(a.sv_promotion_id AS STRING) = d.promotion_id -- 卖家的优惠券id
LEFT OUTER JOIN voucher e -- 优惠券表
  ON a.abc_journey_id = e.journey_id
  AND a.abc_version_id = e.version_id
  AND CAST(a.fsv_promotion_id AS STRING) = e.promotion_id; -- 免费配送的优惠券id
