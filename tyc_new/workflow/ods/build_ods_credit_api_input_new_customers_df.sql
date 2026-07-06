-- ============================================
-- 新增客户入参表 (819+1001定向跑批专用)
-- 每日对比 ods_view_account_base_df 今天vs昨天分区(同过滤条件),LEFT ANTI JOIN 找新增客户
-- 位置: workflow/ods/ (与build_ods_credit_api_input_branch_company_df.sql同目录)
-- 编排: ods_init 4.2 之前,定向 819‖1001 之前执行
-- ============================================
--
-- 逻辑:
--   1. 从 ods_view_account_base_df 读今天分区(同ods_init 4.2过滤条件),得今天客户集
--   2. 从 ods_view_account_base_df 读昨天分区(同过滤条件),得昨天客户集
--   3. LEFT ANTI JOIN: 今天有但昨天没有的客户 = 新增客户
--   4. 写入 ods_credit_api_input_new_customers_df, dt=今天
--
-- 参数(在Databricks Jobs的SQL task里配):
--   ${dt} = 今天日期(yyyyMMdd), 写入的分区 & 源分区
--           昨天通过 date_format(date_sub(to_date('${dt}','yyyyMMdd'),1),'yyyyMMdd') 计算
--
-- 过滤条件(与ods_init 4.2 tmp_ods_credit_company_parent_check_df_temp01完全一致):
--   - new_accountgroup_id JOIN ods_new_accountgroup_df, new_name IN ('Y001-Sold-to party','Y003-Payer')
--   - new_isclosed <> true (排除已关闭)
--   - new_sapcode 非空且纯数字
--   - new_dispensercustomer <> true (排除分配客户)
--
-- 注意:
--   - 新增客户含预付款和非预付款,定向跑批都跑(819/1001不区分prepaid_filter)
--   - HK/TW过滤不在本SQL做,由819/1001-step1的exclude_hk_tw在调用时过滤
--   - 819+1001定向跑批完成后,ods_init 4.2的parent_from_1001 CTE读dt>=昨天分区,
--     能拿到当天定向1001解析的新增分公司母公司,无1天延迟
-- ============================================

INSERT OVERWRITE TABLE powerlink.pw_ods.ods_credit_api_input_new_customers_df PARTITION (dt)
WITH today_customers AS (
    SELECT DISTINCT
        trim(t1.accountName) as company_name,
        if(t1.new_ord_payment_new_code='Y419','是','否') as is_prepaid
    FROM powerlink.pw_ods.ods_view_account_base_df t1
    JOIN powerlink.pw_ods.ods_new_accountgroup_df t2
      ON t1.new_accountgroup_id = t2.new_accountgroupid
    WHERE t2.new_name in ('Y001-Sold-to party','Y003-Payer')
      AND t1.dt = '${dt}'
      AND t2.dt = '${dt}'
      AND coalesce(t1.new_isclosed,false) <> true
      AND coalesce(t1.new_sapcode,'') <> ''
      AND trim(t1.new_sapcode) rlike '^\\d+$'
      AND coalesce(t1.new_dispensercustomer,false) <> true
),
yesterday_customers AS (
    SELECT DISTINCT
        trim(y1.accountName) as company_name
    FROM powerlink.pw_ods.ods_view_account_base_df y1
    JOIN powerlink.pw_ods.ods_new_accountgroup_df y2
      ON y1.new_accountgroup_id = y2.new_accountgroupid
    WHERE y2.new_name in ('Y001-Sold-to party','Y003-Payer')
      AND y1.dt = date_format(date_sub(to_date('${dt}', 'yyyyMMdd'), 1), 'yyyyMMdd')
      AND y2.dt = date_format(date_sub(to_date('${dt}', 'yyyyMMdd'), 1), 'yyyyMMdd')
      AND coalesce(y1.new_isclosed,false) <> true
      AND coalesce(y1.new_sapcode,'') <> ''
      AND trim(y1.new_sapcode) rlike '^\\d+$'
      AND coalesce(y1.new_dispensercustomer,false) <> true
),
new_customers AS (
    -- 今天有但昨天没有的客户 = 新增客户
    -- 同一company_name可能有多条(不同accountgroup),用row_number去重取一条
    SELECT
        t.company_name,
        t.is_prepaid,
        ROW_NUMBER() OVER (PARTITION BY t.company_name ORDER BY t.is_prepaid) as rn
    FROM today_customers t
    LEFT ANTI JOIN yesterday_customers y
      ON t.company_name = y.company_name
)
SELECT
    ROW_NUMBER() OVER (ORDER BY company_name) as id,
    company_name,
    is_prepaid,
    current_timestamp() as create_time,
    '${dt}' as dt
FROM new_customers
WHERE rn = 1
  AND company_name IS NOT NULL AND company_name != '';
