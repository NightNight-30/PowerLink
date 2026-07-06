-- ============================================
-- 分公司入参公司表 (1001接口专用)
-- 每日全量重建dt分区,作为1001-step1的入参来源
-- 位置: workflow/ods/ (与build_ods_init_white_company_list_nd.sql同目录)
-- 编排: 819-step2 完成后执行(依赖当天819解析结果),不在ods_init中跑
-- ============================================
--
-- 逻辑:
--   1. 合并819的 当天分区(账期客户) + 上月度跑批日分区(预付款客户) 两个分区数据
--   2. 按company_name分组,取update_time最新一条(最新工商信息)
--   3. JOIN客户表 ods_credit_api_input_company_df (拿is_prepaid,只保留客户表里的公司)
--   4. 过滤 company_org_type LIKE '%分%' (只保留分公司)
--   5. 写入 ods_credit_api_input_branch_company_df, dt=今天
--
-- 参数(在notebook里用Python算后传参,或Databricks SQL变量):
--   ${dt}                     = 今天日期(yyyyMMdd), 写入的分区 & 819当天源分区
--   ${last_monthly_batch_date}= 最近月度跑批日(yyyyMMdd), 819预付款客户分区
--                               (月度跑批日=monthly_day, 当天819跑预付款)
--
-- 注意:
--   - 819-step2跑完后当天819分区就绪,直接读当天分区,当天新增客户当天进名单,无滞后
--   - 预付款客户819只在月度跑批日跑,故合并读上月度跑批日分区拿最新预付款819数据
--   - 客户表取MAX(dt)分区(最新),保证新增客户当天能进名单(只要819跑过了)
--   - HK/TW过滤不在init SQL做,由1001-step1的exclude_hk_tw在调用时过滤
-- ============================================

INSERT OVERWRITE TABLE powerlink.pw_ods.ods_credit_api_input_branch_company_df PARTITION (dt)
SELECT
  row_number() OVER (ORDER BY c.name) as id,
  c.name as company_name,
  c.is_prepaid,
  b.company_org_type,
  DATE_FORMAT(b.update_time, 'yyyy-MM-dd HH:mm:ss') as update_time,
  b.source_dt,
  current_timestamp() as create_time,
  '${dt}' as dt
FROM (
  SELECT
    company_name,
    company_org_type,
    update_time,
    dt as source_dt,
    ROW_NUMBER() OVER (PARTITION BY company_name ORDER BY update_time DESC) as rn
  FROM powerlink.pw_ods.ods_tyc_819_df
  WHERE dt IN ('${dt}', '${last_monthly_batch_date}')
    AND company_name IS NOT NULL AND company_name != ''
) b
JOIN (
  SELECT name, is_prepaid
  FROM powerlink.pw_ods.ods_credit_api_input_company_df
  WHERE dt = (SELECT MAX(dt) FROM powerlink.pw_ods.ods_credit_api_input_company_df)
    AND name IS NOT NULL AND name != ''
) c
  ON b.company_name = c.name
WHERE b.rn = 1
  AND b.company_org_type LIKE '%分%';
