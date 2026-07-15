-- ============================================
-- 下游 MySQL 表: ads_pw_credit_metric_df
-- 上游 Databricks Delta: powerlink_prod.pw_ads.ads_pw_credit_metric_df
-- 推送方式: pymysql 单连接批量插入(executemany 自动 multi-value 重写), TRUNCATE+INSERT 全量覆盖
-- 字段顺序与上游 DDL 保持一致, 按列名对齐插入
-- 字符集: utf8mb4 (中文公司名/字段值)
-- 主键: 自增 id (truncate+insert 模式下每次重置, 不影响业务)
-- 索引: sap_code + company_name (下游查询常用过滤字段)
-- ============================================

-- 已有表升级用以下 ALTER (新表直接跑下面的 CREATE TABLE, 或 DROP TABLE 后由推送脚本自动建):
-- ALTER TABLE ads_pw_credit_metric_df
--   ADD COLUMN branch_flag                         VARCHAR(10)   DEFAULT NULL COMMENT '分公司标识'        AFTER approval_status,
--   ADD COLUMN parent_company_name                 VARCHAR(255)  DEFAULT NULL COMMENT '母公司名称'        AFTER branch_flag,
--   ADD COLUMN company_org_type                    VARCHAR(100)  DEFAULT NULL COMMENT '公司类型'          AFTER company_scale,
--   ADD COLUMN is_hk_tw_customer                   VARCHAR(10)   DEFAULT NULL COMMENT '是否香港台湾客户'   AFTER employee_num,
--   MODIFY COLUMN is_listed_company                VARCHAR(10)   DEFAULT NULL COMMENT '是否上市公司',
--   MODIFY COLUMN is_provide_tyc_finanical_report  VARCHAR(10)   DEFAULT NULL COMMENT '是否提供天眼查财务报告';

CREATE TABLE IF NOT EXISTS ads_pw_credit_metric_df (
    -- 自增主键
    id                                  BIGINT         NOT NULL AUTO_INCREMENT COMMENT '自增主键',

    -- 客户标识
    sap_code                            VARCHAR(50)    DEFAULT NULL COMMENT 'SAP编码',
    ccp_code                            VARCHAR(50)    DEFAULT NULL COMMENT 'CCP编码',
    crm_code                            VARCHAR(50)    DEFAULT NULL COMMENT 'CRM编码',
    company_name                        VARCHAR(255)   DEFAULT NULL COMMENT '公司名称',

    -- 内部指标
    group_name                          VARCHAR(255)   DEFAULT NULL COMMENT '客户组名称',
    business_unit                       VARCHAR(255)   DEFAULT NULL COMMENT '业务单元',
    parent_name                         VARCHAR(255)   DEFAULT NULL COMMENT '母公司名称',
    is_payer                            VARCHAR(10)    DEFAULT NULL COMMENT '是否payer客户(是/否)',
    payer_sold_to_id                    VARCHAR(50)    DEFAULT NULL COMMENT 'PAYER对应SoldToID',
    payer_sold_to_name                  VARCHAR(255)   DEFAULT NULL COMMENT 'PAYER对应SoldToName',
    is_prepaid                          VARCHAR(10)    DEFAULT NULL COMMENT '是否预付款客户(是/否)',
    within_1y_overduetime_cnt           INT            DEFAULT NULL COMMENT '近一年逾期次数',
    within_1y_blocktime_cnt             INT            DEFAULT NULL COMMENT '近一年冻结次数',
    crm_created_on                      DATE           DEFAULT NULL COMMENT 'CRM创建日期',
    ccp_first_approval_date             DATE           DEFAULT NULL COMMENT 'CCP第一次审核时间',
    cooperate_duration                  INT            DEFAULT NULL COMMENT '合作时长(月)',
    last_year_potential_sales           DECIMAL(20,2)  DEFAULT NULL COMMENT '去年潜在销售额(元)',
    current_year_potential_sales        DECIMAL(20,2)  DEFAULT NULL COMMENT '当年潜在销售额(元)',
    last_year_pns                       DECIMAL(20,2)  DEFAULT NULL COMMENT '去年PNS(净销售额)',
    last_year_pns_currency              VARCHAR(20)    DEFAULT NULL COMMENT '去年PNS货币单位',
    approval_status                     VARCHAR(50)    DEFAULT NULL COMMENT '审核状态',

    -- 外部指标(工商、风险、财务等)
    branch_flag                         VARCHAR(10)    DEFAULT NULL COMMENT '分公司标识',
    parent_company_name                 VARCHAR(255)   DEFAULT NULL COMMENT '母公司名称',
    est_date                            DATE           DEFAULT NULL COMMENT '成立日期',
    est_years                           DECIMAL(10,1)  DEFAULT NULL COMMENT '成立年限(年)',
    reg_capital                         DECIMAL(18,2)  DEFAULT NULL COMMENT '注册资本',
    reg_capital_currency                VARCHAR(20)    DEFAULT NULL COMMENT '注册资本币种',
    within_1y_max_reg_capital           DECIMAL(18,2)  DEFAULT NULL COMMENT '注册资本(一年内最高)',
    within_1y_max_reg_capital_currency  VARCHAR(20)    DEFAULT NULL COMMENT '注册资本币种(一年内最高)',
    within_1y_min_reg_capital           DECIMAL(18,2)  DEFAULT NULL COMMENT '注册资本(一年内最低)',
    within_1y_min_reg_capital_currency  VARCHAR(20)    DEFAULT NULL COMMENT '注册资本币种(一年内最低)',
    paid_capital                        DECIMAL(18,2)  DEFAULT NULL COMMENT '实缴资本',
    paid_capital_currency               VARCHAR(20)    DEFAULT NULL COMMENT '实缴资本币种',
    company_scale                       VARCHAR(50)    DEFAULT NULL COMMENT '公司规模',
    company_org_type                    VARCHAR(100)   DEFAULT NULL COMMENT '公司类型',
    economy_type_level1                 VARCHAR(100)   DEFAULT NULL COMMENT '经济类型一级分类',
    economy_type_level2                 VARCHAR(100)   DEFAULT NULL COMMENT '经济类型二级分类',
    org_type_level1                     VARCHAR(100)   DEFAULT NULL COMMENT '机构类型一级分类',
    org_type_level2                     VARCHAR(100)   DEFAULT NULL COMMENT '机构类型二级分类',
    employee_num                        INT            DEFAULT NULL COMMENT '员工人数',
    is_hk_tw_customer                   VARCHAR(10)    DEFAULT NULL COMMENT '是否香港台湾客户',
    is_listed_company                   VARCHAR(10)    DEFAULT NULL COMMENT '是否上市公司',
    financial_report_show_year          VARCHAR(20)    DEFAULT NULL COMMENT '财报显示年份',
    is_provide_tyc_finanical_report     VARCHAR(10)    DEFAULT NULL COMMENT '是否提供天眼查财务报告',
    net_profit_atsopc                   DECIMAL(24,4)  DEFAULT NULL COMMENT '归属净利润(元)',
    asset_liab_ratio                    DECIMAL(24,4)  DEFAULT NULL COMMENT '资产负债率(%)',
    receivable_turnover_days            DECIMAL(24,4)  DEFAULT NULL COMMENT '应收账款周转天数(天)',
    current_ratio                       DECIMAL(24,4)  DEFAULT NULL COMMENT '流动⽐率',
    cash_flow_show_year                 VARCHAR(20)    DEFAULT NULL COMMENT '现金流量显示年份',
    ncf_from_oa                         VARCHAR(100)   DEFAULT NULL COMMENT '经营活动产生的现金流量净额',
    company_paydex                      INT            DEFAULT NULL COMMENT '企业支付指数(paydex)',

    -- 风险类指标
    judgmen_defaulter_risk_cnt          INT            DEFAULT NULL COMMENT '失信被执行人风险数量',
    judgment_debtor_risk_cnt            INT            DEFAULT NULL COMMENT '被执行人风险数量',
    administrative_penalty_risk_cnt     INT            DEFAULT NULL COMMENT '行政处罚风险数量',
    tax_arrears_risk_cnt                INT            DEFAULT NULL COMMENT '欠税风险数量',
    equity_freeze_risk_cnt              INT            DEFAULT NULL COMMENT '股权冻结风险数量',
    consumption_restrictions_risk_cnt   INT            DEFAULT NULL COMMENT '限制消费风险数量',
    closure_enforcement_risk_cnt       INT            DEFAULT NULL COMMENT '终本案件风险数量',
    within_2y_case_cnt                  INT            DEFAULT NULL COMMENT '两年内涉诉数量',
    within_2y_case_money                DECIMAL(18,2)  DEFAULT NULL COMMENT '两年内涉诉金额',
    within_1y_adress_change_cnt         INT            DEFAULT NULL COMMENT '一年内地址变更数量',
    within_2y_adress_change_cnt         INT            DEFAULT NULL COMMENT '两年内地址变更数量',

    -- 分区字段(上游 dt 是 yyyyMMdd 字符串)
    dt                                  VARCHAR(8)     DEFAULT NULL COMMENT '分区日期(yyyyMMdd)',

    PRIMARY KEY (id),
    KEY idx_sap_code (sap_code),
    KEY idx_company_name (company_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='powerlink_ADS层客户信用指标汇总表(融合内外部数据)';
