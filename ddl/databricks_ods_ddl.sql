-- ============================================
-- PowerLink ODS层建表DDL (Databricks)
-- 表名前缀: ods_pl_ (PowerLink项目缩写)
-- 存储格式: PARquet
-- 字段名与MySQL一致,便于数据追溯
-- 类型映射: VARCHAR/TEXT/LONGTEXT/JSON→STRING, DATETIME→TIMESTAMP, DECIMAL/BIGINT/INT保持不变
-- ODS层无约束(无PK/UK/索引),数据完整性由ETL保证
-- ============================================

-- 1. 三方接口调用记录表
CREATE TABLE IF NOT EXISTS ods_pl_api_call_record (
  id                          BIGINT        COMMENT '主键ID',
  interface_name              STRING        COMMENT '接口名，如819/967/P51060等',
  call_datetime               TIMESTAMP     COMMENT '调用日期时间',
  input_param                 STRING        COMMENT '入参，公司名',
  status_code                 INT           COMMENT '状态码：0=成功，负数=异常(-1=HTTP异常,-2=其他)，正数=API业务错误码',
  output_result               STRING        COMMENT '出参结果，成功时为API完整响应，失败时为错误信息JSON',
  create_time                 TIMESTAMP     COMMENT '创建时间'
) USING PARquet
COMMENT '三方接口调用记录表';

-- 2. 企业基本信息
CREATE TABLE IF NOT EXISTS ods_pl_company_819_info (
  id                          BIGINT        COMMENT '主键ID',
  api_record_id               BIGINT        COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time            TIMESTAMP     COMMENT '数据创建时间',
  company_name                STRING        COMMENT '公司名(搜索关键字)',
  company_id                  BIGINT        COMMENT '企业ID',
  legal_person_type           INT           COMMENT '法人类型(1=人,2=公司)',
  reg_status                  STRING        COMMENT '经营状态',
  legal_person_name           STRING        COMMENT '法定代表人',
  reg_capital                 STRING        COMMENT '注册资本(含单位)',
  reg_capital_currency        STRING        COMMENT '注册资本币种',
  paid_capital                STRING        COMMENT '实缴资本(含单位)',
  paid_capital_currency       STRING        COMMENT '实缴资本币种',
  est_date                    TIMESTAMP     COMMENT '成立日期',
  from_date                   TIMESTAMP     COMMENT '经营开始时间',
  to_date                     TIMESTAMP     COMMENT '经营结束时间',
  approval_date               TIMESTAMP     COMMENT '核准时间',
  cancel_date                 TIMESTAMP     COMMENT '注销日期',
  revoke_date                 TIMESTAMP     COMMENT '吊销日期',
  cancel_reason               STRING        COMMENT '注销原因',
  revoke_reason               STRING        COMMENT '吊销原因',
  social_credit_code          STRING        COMMENT '统一社会信用代码',
  org_code                    STRING        COMMENT '组织机构代码',
  tax_number                  STRING        COMMENT '纳税人识别号',
  reg_number                  STRING        COMMENT '注册号',
  brn_number                  STRING        COMMENT '商业登记号',
  reg_institute               STRING        COMMENT '登记机关',
  reg_location                STRING        COMMENT '注册地址',
  reg_location_half_width     STRING        COMMENT '注册地址(半角/英文)',
  business_scope              STRING        COMMENT '经营范围',
  company_org_type            STRING        COMMENT '企业类型(如:其他股份有限公司(上市))',
  industry                    STRING        COMMENT '行业',
  province_short              STRING        COMMENT '省份简称',
  city                        STRING        COMMENT '市',
  district                    STRING        COMMENT '区',
  district_code               STRING        COMMENT '行政区划代码',
  economic_function_zone1     STRING        COMMENT '经济功能区1',
  economic_function_zone2     STRING        COMMENT '经济功能区2',
  above_scale                 STRING        COMMENT '是否规模以上',
  company_alias               STRING        COMMENT '简称',
  email                       STRING        COMMENT '邮箱(单个)',
  email_list                  STRING        COMMENT '全部邮箱(逗号分隔)',
  history_names               STRING        COMMENT '曾用名(逗号分隔)',
  used_bond_name              STRING        COMMENT '股票曾用名',
  bond_name                   STRING        COMMENT '股票名',
  bond_num                    STRING        COMMENT '股票号',
  bond_type                   STRING        COMMENT '股票类型',
  tags                        STRING        COMMENT '企业标签',
  phone_number                STRING        COMMENT '企业联系方式',
  website_list                STRING        COMMENT '网址',
  staff_num_range             STRING        COMMENT '人员规模',
  social_staff_num            INT           COMMENT '参保人数',
  percentile_score            INT           COMMENT '企业评分(万分制)',
  is_micro_ent                INT           COMMENT '是否小微企业(0=否,1=是)',
  property3                   STRING        COMMENT '英文名',
  staff_list_total            INT           COMMENT '主要人员总数',
  staff_list_json             STRING        COMMENT '主要人员列表(JSON)',
  update_time                 TIMESTAMP     COMMENT '更新时间',
  industry_all_category       STRING        COMMENT '国民经济行业分类-门类',
  industry_all_category_big   STRING        COMMENT '国民经济行业分类-大类',
  industry_all_category_middleSTRING        COMMENT '国民经济行业分类-中类',
  industry_all_category_small STRING        COMMENT '国民经济行业分类-小类',
  industry_all_category_code_firstSTRING        COMMENT '国民经济行业分类-门类代码',
  industry_all_category_code_secondSTRING        COMMENT '国民经济行业分类-大类代码',
  industry_all_category_code_thirdSTRING        COMMENT '国民经济行业分类-中类代码',
  industry_all_category_code_fourthSTRING        COMMENT '国民经济行业分类-小类代码'
) USING PARquet
COMMENT '企业基本信息(819接口),1:1关系';

-- 3. 企业天眼风险
CREATE TABLE IF NOT EXISTS ods_pl_company_1058_risk_info (
  id                          BIGINT        COMMENT '主键ID',
  api_record_id               BIGINT        COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time            TIMESTAMP     COMMENT '数据创建时间',
  main_company_name           STRING        COMMENT '主公司名(搜索关键字/入参)',
  risk_level                  STRING        COMMENT '风险等级(result.riskLevel)',
  risk_category_count         INT           COMMENT '该风险类别下的风险条数(riskList[].count)',
  risk_category_name          STRING        COMMENT '风险类别名(riskList[].name: 自身风险/周边风险/历史风险/预警提醒)',
  risk_type_total             INT           COMMENT '该风险类型下的条数(riskList[].list[].total)',
  risk_type_tag               STRING        COMMENT '风险标签(riskList[].list[].tag: 警示/高风险/提示信息)',
  company_id                  BIGINT        COMMENT '涉及公司ID(riskList[].list[].list[].companyId, 可空)',
  company_name                STRING        COMMENT '涉及公司名(riskList[].list[].list[].companyName, 可空)',
  risk_id                     BIGINT        COMMENT '风险条目ID(riskList[].list[].list[].id)',
  risk_count                  INT           COMMENT '风险数量(riskList[].list[].list[].riskCount)',
  risk_title                  STRING        COMMENT '风险描述(riskList[].list[].list[].title)',
  risk_type                   INT           COMMENT '风险类型码(riskList[].list[].list[].type)',
  risk_desc                   STRING        COMMENT '风险简述(riskList[].list[].list[].desc)'
) USING PARquet
COMMENT '企业天眼风险(1058接口)';

-- 4. 变更记录
CREATE TABLE IF NOT EXISTS ods_pl_company_822_change_info (
  id                          BIGINT        COMMENT '主键ID',
  api_record_id               BIGINT        COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time            TIMESTAMP     COMMENT '数据创建时间',
  company_name                STRING        COMMENT '主公司名(搜索关键字/入参)',
  total                       INT           COMMENT '变更记录总数(result.total)',
  change_item                 STRING        COMMENT '变更项名称(result.items[].changeItem)',
  content_before              STRING        COMMENT '变更前内容(result.items[].contentBefore)',
  content_after               STRING        COMMENT '变更后内容(result.items[].contentAfter)',
  change_time                 STRING        COMMENT '变更时间(result.items[].changeTime)',
  create_time                 STRING        COMMENT '记录创建时间(result.items[].createTime)'
) USING PARquet
COMMENT '变更记录(822接口)';

-- 5. 上市公司企业简介
CREATE TABLE IF NOT EXISTS ods_pl_company_854_stock_info (
  id                          BIGINT        COMMENT '主键ID',
  api_record_id               BIGINT        COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time            TIMESTAMP     COMMENT '数据创建时间',
  company_name                STRING        COMMENT '主公司名(搜索关键字/入参)',
  area                        STRING        COMMENT '区域(result.area)',
  website                     STRING        COMMENT '网址(result.website)',
  stock_code                  STRING        COMMENT '股票代码(result.code)',
  address                     STRING        COMMENT '地址(result.address)',
  gm_type                     INT           COMMENT '总经理类型(result.generalManager.cType: 1=公司,2=人)',
  gm_name                     STRING        COMMENT '总经理姓名(result.generalManager.name)',
  gm_id                       BIGINT        COMMENT '总经理ID(result.generalManager.id)',
  stock_company_name          STRING        COMMENT 'API返回公司名(result.companyName)',
  employees_num               STRING        COMMENT '员工人数(result.employeesNum)',
  main_business               STRING        COMMENT '主营业务(result.mainBusiness)',
  mobile                      STRING        COMMENT '电话(result.mobile)',
  chairman_type               INT           COMMENT '董事长类型(result.chairman.cType: 1=公司,2=人)',
  chairman_name               STRING        COMMENT '董事长姓名(result.chairman.name)',
  chairman_id                 BIGINT        COMMENT '董事长ID(result.chairman.id)',
  industry                    STRING        COMMENT '行业(result.industry)',
  product_name                STRING        COMMENT '产品名称(result.productName)',
  secretary_type              INT           COMMENT '董秘类型(result.secretaries.cType: 1=公司,2=人)',
  secretary_name              STRING        COMMENT '董秘姓名(result.secretaries.name)',
  secretary_id                BIGINT        COMMENT '董秘ID(result.secretaries.id)',
  actual_controller           STRING        COMMENT '实际控制人(result.actualController)',
  controlling_shareholder     STRING        COMMENT '控股股东(result.controllingShareholder)',
  eng_name                    STRING        COMMENT '英文名(result.engName)',
  registered_capital          STRING        COMMENT '注册资本(result.registeredCapital)',
  postalcode                  STRING        COMMENT '邮编(result.postalcode)',
  legal_person_type           INT           COMMENT '法人类型(result.legal.cType: 1=公司,2=人)',
  legal_person_name           STRING        COMMENT '法人姓名(result.legal.name)',
  legal_person_id             BIGINT        COMMENT '法人ID(result.legal.id)',
  listed_name                 STRING        COMMENT '上市公司简称(result.name)',
  fax                         STRING        COMMENT '传真(result.fax)',
  used_name                   STRING        COMMENT '曾用名(result.usedName)',
  final_controller            STRING        COMMENT '最终控制人(result.finalController)',
  introduction                STRING        COMMENT '简介(result.introduction)'
) USING PARquet
COMMENT '上市公司企业简介(854接口),1:1关系';

-- 6. 组织机构类型
CREATE TABLE IF NOT EXISTS ods_pl_company_1168_org_type_info (
  id                          BIGINT        COMMENT '主键ID',
  api_record_id               BIGINT        COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time            TIMESTAMP     COMMENT '数据创建时间',
  company_name                STRING        COMMENT '主公司名(搜索关键字/入参)',
  org_type_level1             STRING        COMMENT '一级机构类型(result.orgTypes[].level1,逗号分隔)',
  org_type_level2             STRING        COMMENT '二级机构类型(result.orgTypes[].level2,逗号分隔)',
  economy_type_level1         STRING        COMMENT '一级经济类型(result.economyTypes[].level1,逗号分隔)',
  economy_type_level2         STRING        COMMENT '二级经济类型(result.economyTypes[].level2,逗号分隔)'
) USING PARquet
COMMENT '组织机构类型(1168接口),1:1关系';

-- 7. 企业规模
CREATE TABLE IF NOT EXISTS ods_pl_company_1149_scale_info (
  id                          BIGINT        COMMENT '主键ID',
  api_record_id               BIGINT        COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time            TIMESTAMP     COMMENT '数据创建时间',
  company_name                STRING        COMMENT '主公司名(搜索关键字/入参)',
  company_scale               STRING        COMMENT '企业规模(result字符串,如"大型")'
) USING PARquet
COMMENT '企业规模(1149接口),1:1关系';

-- 8. 主要指标-年度
CREATE TABLE IF NOT EXISTS ods_pl_company_967_main_index_info (
  id                          BIGINT        COMMENT '主键ID',
  api_record_id               BIGINT        COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time            TIMESTAMP     COMMENT '数据创建时间',
  company_name                STRING        COMMENT '主公司名(搜索关键字/入参)',
  show_year                   STRING        COMMENT '年份(result[].showYear)',
  crfgsasr_to_revenue         DECIMAL(24,4) COMMENT '销售现金流/营业收入',
  np_atsopc_nrgal_yoy         DECIMAL(24,4) COMMENT '扣非净利润同比增长(%)',
  asset_liab_ratio            DECIMAL(24,4) COMMENT '资产负债率(%)',
  op_to_revenue               DECIMAL(24,4) COMMENT '营业利润/营业收入(%)',
  revenue_yoy                 DECIMAL(24,4) COMMENT '营业总收入同比增长(%)',
  net_profit_atsopc_yoy       DECIMAL(24,4) COMMENT '归属净利润同比增长(%)',
  receivable_turnover_days    DECIMAL(24,4) COMMENT '应收账款周转天数(天)',
  current_ratio               DECIMAL(24,4) COMMENT '流动比率',
  operate_cash_flow_ps        DECIMAL(24,4) COMMENT '每股经营现金流(元)',
  gross_selling_rate          DECIMAL(24,4) COMMENT '毛利率(%)',
  current_liab_to_total_liab  DECIMAL(24,4) COMMENT '流动负债/总负债(%)',
  quick_ratio                 DECIMAL(24,4) COMMENT '速动比率',
  fully_dlt_roe               DECIMAL(24,4) COMMENT '摊薄净资产收益率(%)',
  tax_rate                    DECIMAL(24,4) COMMENT '实际税率(%)',
  net_interest_of_total_assetsDECIMAL(24,4) COMMENT '摊薄总资产收益率(%)',
  operating_total_revenue_lrr_sqDECIMAL(24,4) COMMENT '营业总收入滚动环比增长(%)',
  profit_deduct_nrgal_lrr_sq  DECIMAL(24,4) COMMENT '扣非净利润滚动环比增长(%)',
  wgt_avg_roe                 DECIMAL(24,4) COMMENT '加权净资产收益率(%)',
  net_profit_per_share        DECIMAL(24,4) COMMENT '每股净资产(元)',
  ncf_from_oa_to_revenue      DECIMAL(24,4) COMMENT '经营现金流/营业收入',
  profit_nrgal_sq             DECIMAL(24,4) COMMENT '扣非净利润(元)',
  basic_eps                   DECIMAL(24,4) COMMENT '基本每股收益(元)',
  net_selling_rate            DECIMAL(24,4) COMMENT '净利率(%)',
  total_capital_turnover      DECIMAL(24,4) COMMENT '总资产周转率(次)',
  net_profit_atsopc_lrr_sq    DECIMAL(24,4) COMMENT '归属净利润滚动环比增长(%)',
  inventory_turnover_days     DECIMAL(24,4) COMMENT '存货周转天数(天)',
  pre_receivable              DECIMAL(24,4) COMMENT '预收款/营业收入',
  total_revenue               DECIMAL(24,4) COMMENT '营业总收入(元)',
  undistri_profit_ps          DECIMAL(24,4) COMMENT '每股未分配利润(元)',
  dlt_earnings_per_share      DECIMAL(24,4) COMMENT '稀释每股收益(元)',
  net_profit_atsopc           DECIMAL(24,4) COMMENT '归属净利润(元)',
  basic_e_ps_net_of_nrgal     DECIMAL(24,4) COMMENT '扣非每股收益(元)',
  capital_reserve             DECIMAL(24,4) COMMENT '每股公积金(元)'
) USING PARquet
COMMENT '主要指标-年度(967接口)';

-- 9. 法律诉讼
CREATE TABLE IF NOT EXISTS ods_pl_company_1114_lawsuit_info (
  id                          BIGINT        COMMENT '主键ID',
  api_record_id               BIGINT        COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time            TIMESTAMP     COMMENT '数据创建时间',
  company_name                STRING        COMMENT '主公司名(搜索关键字/入参)',
  total                       INT           COMMENT '诉讼记录总数(result.total)',
  lawsuit_id                  BIGINT        COMMENT '诉讼条目ID(items[].id,避免与表主键冲突)',
  doc_type                    STRING        COMMENT '文书类型(items[].docType)',
  lawsuit_url                 STRING        COMMENT '天眼查URL-Web(items[].lawsuitUrl)',
  lawsuit_h5_url              STRING        COMMENT '天眼查URL-H5(items[].lawsuitH5Url)',
  title                       STRING        COMMENT '案件名称(items[].title)',
  court                       STRING        COMMENT '审理法院(items[].court)',
  judge_time                  STRING        COMMENT '裁判日期(items[].judgeTime)',
  uuid                        STRING        COMMENT 'UUID(items[].uuid)',
  case_no                     STRING        COMMENT '案号(items[].caseNo)',
  case_type                   STRING        COMMENT '案件类型(items[].caseType)',
  case_reason                 STRING        COMMENT '案由(items[].caseReason)',
  case_money                  STRING        COMMENT '案件金额(items[].caseMoney)',
  submit_time                 TIMESTAMP     COMMENT '发布日期(items[].submitTime,毫秒时间戳→datetime)',
  case_result                 STRING        COMMENT '案件结果标签(casePersons[0].result)',
  role1                       STRING        COMMENT '案件身份1(casePersons[0].role)',
  gid1                        STRING        COMMENT 'ID1(casePersons[0].gid)',
  emotion1                    INT           COMMENT '情感倾向1(casePersons[0].emotion: 1=正面,0=中性,-1=负面)',
  sptname1                    STRING        COMMENT '疑似名称1(casePersons[0].sptname)',
  name1                       STRING        COMMENT '名称1(casePersons[0].name)',
  type1                       STRING        COMMENT '类型1(casePersons[0].type: 1=人员,2=公司)',
  role2                       STRING        COMMENT '案件身份2(casePersons[1].role)',
  gid2                        STRING        COMMENT 'ID2(casePersons[1].gid)',
  emotion2                    INT           COMMENT '情感倾向2(casePersons[1].emotion: 1=正面,0=中性,-1=负面)',
  sptname2                    STRING        COMMENT '疑似名称2(casePersons[1].sptname)',
  name2                       STRING        COMMENT '名称2(casePersons[1].name)',
  type2                       STRING        COMMENT '类型2(casePersons[1].type: 1=人员,2=公司)'
) USING PARquet
COMMENT '法律诉讼(1114接口)';

-- 10. 现金流量表
CREATE TABLE IF NOT EXISTS ods_pl_company_973_cash_flow_info (
  id                          BIGINT        COMMENT '主键ID',
  api_record_id               BIGINT        COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time            TIMESTAMP     COMMENT '数据创建时间',
  company_name                STRING        COMMENT '主公司名(搜索关键字/入参)',
  show_year                   STRING        COMMENT '报告期(result.corpCashFlow[].showYear)',
  ncf_from_oa                 STRING        COMMENT '经营活动产生的现金流量净额',
  sub_total_of_ci_from_oa     STRING        COMMENT '经营活动现金流入小计',
  sub_total_of_cos_from_oa    STRING        COMMENT '经营活动现金流出小计',
  cash_received_of_sales_serviceSTRING        COMMENT '销售商品、提供劳务收到的现金',
  payments_of_all_taxes       STRING        COMMENT '支付的各项税费',
  cash_paid_to_staff_etc      STRING        COMMENT '支付给职工以及为职工支付的现金',
  goods_buy_and_service_cash_paySTRING        COMMENT '购买商品、接受劳务支付的现金',
  other_cash_paid_related_to_oaSTRING        COMMENT '支付其他与经营活动有关的现金',
  cash_received_of_other_fa   STRING        COMMENT '收到其他与经营活动有关的现金',
  ncf_from_ia                 STRING        COMMENT '投资活动产生的现金流量净额',
  sub_total_of_ci_from_ia     STRING        COMMENT '投资活动现金流入小计',
  sub_total_of_cos_from_ia    STRING        COMMENT '投资活动现金流出小计',
  cash_received_of_dspsl_investSTRING        COMMENT '收回投资收到的现金',
  invest_income_cash_received STRING        COMMENT '取得投资收益收到的现金',
  net_cash_of_disposal_assets STRING        COMMENT '处置固定资产、无形资产和其他长期资产收回的现金净额',
  net_cash_of_disposal_branch STRING        COMMENT '处置子公司及其他营业单位收到的现金净额',
  cash_received_of_other_ia   STRING        COMMENT '收到其他与投资活动有关的现金',
  invest_paid_cash            STRING        COMMENT '投资支付的现金',
  cash_paid_for_assets        STRING        COMMENT '购建固定资产、无形资产和其他长期资产支付的现金',
  ncf_from_fa                 STRING        COMMENT '筹资活动产生的现金流量净额',
  sub_total_of_ci_from_fa     STRING        COMMENT '筹资活动现金流入小计',
  sub_total_of_cos_from_fa    STRING        COMMENT '筹资活动现金流出小计',
  cash_received_of_absorb_investSTRING        COMMENT '吸收投资收到的现金',
  cash_received_from_investor STRING        COMMENT '子公司吸收少数股东投资收到的现金',
  cash_received_of_borrowing  STRING        COMMENT '取得借款收到的现金',
  cash_received_from_bond_issueSTRING        COMMENT '发行债券收到的现金',
  cash_received_of_othr_fa    STRING        COMMENT '收到其他与筹资活动有关的现金',
  cash_pay_for_debt           STRING        COMMENT '偿还债务支付的现金',
  cash_paid_of_distribution   STRING        COMMENT '分配股利、利润或偿付利息支付的现金',
  other_cash_paid_relating_to_faSTRING        COMMENT '支付其他与筹资活动有关的现金',
  branch_paid_to_minority_holderSTRING        COMMENT '子公司支付给少数股东的股利、利润',
  net_increase_in_cce         STRING        COMMENT '现金及现金等价物净增加额',
  initial_balance_of_cce      STRING        COMMENT '期初现金及现金等价物余额',
  final_balance_of_cce        STRING        COMMENT '期末现金及现金等价物余额',
  net_cash_amt_from_branch    STRING        COMMENT '取得子公司及其他营业单位支付的现金净额',
  effect_of_exchange_chg_on_cceSTRING        COMMENT '汇率变动对现金及现金等价物的影响'
) USING PARquet
COMMENT '现金流量表(973接口)';

-- 11. 付款指数
CREATE TABLE IF NOT EXISTS ods_pl_company_P51060_paydex_info (
  id                          BIGINT        COMMENT '主键ID',
  api_record_id               BIGINT        COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time            TIMESTAMP     COMMENT '数据创建时间',
  company_name                STRING        COMMENT '主公司名(搜索关键字/入参,entityName)',
  uscc                        STRING        COMMENT '统一社会信用代码(res.uscc)',
  company_paydex              STRING        COMMENT 'PayDex评分数值(最新)(res.companyPayDex)',
  company_paydex_date         STRING        COMMENT 'PayDex评分日期(最新)(res.companyPayDexDate)',
  company_history_paydexes    STRING        COMMENT 'PayDex历史信息(res.companyHistoryPayDexes,JSON字符串)',
  sic2                        STRING        COMMENT 'SIC前2位(res.sic2)',
  sic3                        STRING        COMMENT 'SIC前3位(res.sic3)',
  sic4                        STRING        COMMENT 'SIC前4位(res.sic4)',
  industry_paydex_date        STRING        COMMENT '行业PayDex评分日期(最新)(res.industryPayDexDate)',
  industry_lower_quartile_paydexSTRING        COMMENT '行业25分位PayDex评分数值(res.industryLowerQuartilePayDex)',
  industry_median_paydex      STRING        COMMENT '行业50分位PayDex评分数值(res.industryMedianPayDex)',
  industry_upper_quartile_paydexSTRING        COMMENT '行业75分位PayDex评分数值(res.industryUpperQuartilePayDex)',
  industry_count_num          STRING        COMMENT '行业统计数据-样本数量(res.industryCountNum)',
  industry_company_position   STRING        COMMENT '行业位置(res.industryCompanyPosition)',
  company_average             STRING        COMMENT '平均付款天数(中文)(res.companyAverage)',
  en_company_average          STRING        COMMENT '平均付款天数(英文)(res.encompanyAverage)',
  industry_average            STRING        COMMENT '行业平均付款天数(中文)(res.industryAverage)',
  en_industry_average         STRING        COMMENT '行业平均付款天数(英文)(res.enindustryAverage)'
) USING PARquet
COMMENT '付款指数(邓白氏P51060接口),1:1关系';
