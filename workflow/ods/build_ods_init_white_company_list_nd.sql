-- ============================================
-- HK/TW白名单表全量重建 - ods_init_white_company_list_nd
-- ============================================
-- 用途: 从819解析表(ods_tyc_819_df)提取province_short为'hk'/'tw'的公司,
--       作为"免跑接口"白名单, 各接口step1调用前读取此表排除HK/TW客户
--
-- 触发位置: Databricks Jobs的init前置Task (所有step1之前)
-- 数据源: 819表昨天及之前的分区 (不依赖今天819, 保持step1并行)
-- 维护策略: 每日全量重建(INSERT OVERWRITE), 天然幂等, 自动处理province_short变化
--
-- 表特性: 全量快照表(无dt分区), 只保留最新状态
-- 依赖: ods_tyc_819_df表必须已建且819_step2已至少跑过一次
-- ============================================

INSERT OVERWRITE TABLE powerlink.pw_ods.ods_init_white_company_list_nd
SELECT
  row_number() OVER (ORDER BY company_name) AS id,
  company_name,
  province_short,
  source_dt,
  data_create_time,
  current_timestamp() AS create_time
FROM (
  SELECT
    company_name,
    province_short,
    dt AS source_dt,
    data_create_time,
    ROW_NUMBER() OVER (
      PARTITION BY company_name
      ORDER BY data_create_time DESC
    ) AS rn
  FROM powerlink.pw_ods.ods_tyc_819_df
  WHERE dt < date_format(current_date(), 'yyyyMMdd')
    AND province_short IN ('hk', 'tw')
    AND company_name IS NOT NULL
    AND company_name != ''
)
WHERE rn = 1;
