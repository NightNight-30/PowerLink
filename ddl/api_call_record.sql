-- ============================================
-- 天眼查/邓白氏数据接入 - 建表DDL (powerlink库)
-- ============================================

-- 1. 接口调用记录表
CREATE TABLE IF NOT EXISTS api_call_record (
  id             BIGINT AUTO_INCREMENT PRIMARY KEY,
  interface_name VARCHAR(32)  NOT NULL COMMENT '接口名，如819/967/P51060等',
  call_datetime  DATETIME     NOT NULL COMMENT '调用日期时间',
  input_param    VARCHAR(200) NOT NULL COMMENT '入参，公司名',
  status_code    INT          NOT NULL COMMENT '状态码：0=成功，负数=异常(-1=HTTP异常,-2=其他)，正数=API业务错误码',
  output_result  JSON              COMMENT '出参结果，成功时为API完整响应，失败时为错误信息JSON',
  create_time    DATETIME     DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  INDEX idx_query (interface_name, input_param),
  INDEX idx_date (call_datetime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='三方接口调用记录表';

-- 2. 企业基本信息表 (819接口解析目标)
-- 解析规则：
--   Array + child String → 逗号分隔字符串（email_list, history_names）
--   Object + 多KV       → 展开每个KV为独立列（industryAll）
--   Object + 可能多条   → JSON字符串（staff_list_json）
--   Number时间戳        → DATETIME（自动判断毫秒/秒）
CREATE TABLE IF NOT EXISTS company_819_info (
  id                        BIGINT AUTO_INCREMENT PRIMARY KEY,
  api_record_id             BIGINT              COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time          DATETIME     DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
  company_name              VARCHAR(200) NOT NULL COMMENT '公司名(搜索关键字)',
  company_id                BIGINT              COMMENT '企业ID',
  legal_person_type         INT                 COMMENT '法人类型(1=人,2=公司)',
  reg_status                VARCHAR(31)         COMMENT '经营状态',
  legal_person_name         VARCHAR(120)        COMMENT '法定代表人',
  reg_capital               VARCHAR(50)         COMMENT '注册资本(含单位)',
  reg_capital_currency      VARCHAR(10)         COMMENT '注册资本币种',
  paid_capital              VARCHAR(50)         COMMENT '实缴资本(含单位)',
  paid_capital_currency     VARCHAR(10)         COMMENT '实缴资本币种',
  est_date                  DATETIME            COMMENT '成立日期',
  from_date                 DATETIME            COMMENT '经营开始时间',
  to_date                   DATETIME            COMMENT '经营结束时间',
  approval_date             DATETIME            COMMENT '核准时间',
  cancel_date               DATETIME            COMMENT '注销日期',
  revoke_date               DATETIME            COMMENT '吊销日期',
  cancel_reason             VARCHAR(500)        COMMENT '注销原因',
  revoke_reason             VARCHAR(500)        COMMENT '吊销原因',
  social_credit_code        VARCHAR(255)        COMMENT '统一社会信用代码',
  org_code                  VARCHAR(31)         COMMENT '组织机构代码',
  tax_number                VARCHAR(255)        COMMENT '纳税人识别号',
  reg_number                VARCHAR(31)         COMMENT '注册号',
  brn_number                VARCHAR(50)         COMMENT '商业登记号',
  reg_institute             VARCHAR(255)        COMMENT '登记机关',
  reg_location              VARCHAR(255)        COMMENT '注册地址',
  reg_location_half_width   VARCHAR(255)        COMMENT '注册地址(半角/英文)',
  business_scope            TEXT                COMMENT '经营范围',
  company_org_type          VARCHAR(127)        COMMENT '企业类型(如:其他股份有限公司(上市))',
  industry                  VARCHAR(255)        COMMENT '行业',
  province_short            VARCHAR(31)         COMMENT '省份简称',
  city                      VARCHAR(20)         COMMENT '市',
  district                  VARCHAR(20)         COMMENT '区',
  district_code             VARCHAR(20)         COMMENT '行政区划代码',
  economic_function_zone1   VARCHAR(20)         COMMENT '经济功能区1',
  economic_function_zone2   VARCHAR(20)         COMMENT '经济功能区2',
  above_scale               VARCHAR(10)         COMMENT '是否规模以上',
  company_alias             VARCHAR(255)        COMMENT '简称',
  email                     VARCHAR(1024)       COMMENT '邮箱(单个)',
  email_list                TEXT                COMMENT '全部邮箱(逗号分隔)',
  history_names             TEXT                COMMENT '曾用名(逗号分隔)',
  used_bond_name            VARCHAR(50)         COMMENT '股票曾用名',
  bond_name                 VARCHAR(20)         COMMENT '股票名',
  bond_num                  VARCHAR(20)         COMMENT '股票号',
  bond_type                 VARCHAR(31)         COMMENT '股票类型',
  tags                      VARCHAR(255)        COMMENT '企业标签',
  phone_number              VARCHAR(1024)       COMMENT '企业联系方式',
  website_list              TEXT                COMMENT '网址',
  staff_num_range           VARCHAR(200)        COMMENT '人员规模',
  social_staff_num          INT                 COMMENT '参保人数',
  percentile_score          INT                 COMMENT '企业评分(万分制)',
  is_micro_ent              INT                 COMMENT '是否小微企业(0=否,1=是)',
  property3                 VARCHAR(255)        COMMENT '英文名',
  staff_list_total          INT                 COMMENT '主要人员总数',
  staff_list_json           TEXT                COMMENT '主要人员列表(JSON)',
  update_time               DATETIME            COMMENT '更新时间',
  -- industryAll 展开的KV（国民经济行业分类）
  industry_all_category          VARCHAR(255)   COMMENT '国民经济行业分类-门类',
  industry_all_category_big      VARCHAR(255)   COMMENT '国民经济行业分类-大类',
  industry_all_category_middle   VARCHAR(255)   COMMENT '国民经济行业分类-中类',
  industry_all_category_small    VARCHAR(255)   COMMENT '国民经济行业分类-小类',
  industry_all_category_code_first  VARCHAR(255) COMMENT '国民经济行业分类-门类代码',
  industry_all_category_code_second VARCHAR(255) COMMENT '国民经济行业分类-大类代码',
  industry_all_category_code_third  VARCHAR(255) COMMENT '国民经济行业分类-中类代码',
  industry_all_category_code_fourth VARCHAR(255) COMMENT '国民经济行业分类-小类代码',
  UNIQUE KEY uk_company_name (company_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='企业基本信息(819接口)';

-- 3. 客户公司列表表 (数据源)
CREATE TABLE IF NOT EXISTS customer_info (
  id             BIGINT AUTO_INCREMENT PRIMARY KEY,
  customer_name  VARCHAR(200) NOT NULL COMMENT '公司名',
  create_time    DATETIME     DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  INDEX idx_customer_name (customer_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户公司列表';

-- 插入测试公司
INSERT IGNORE INTO customer_info (customer_name) VALUES ('广东领益智造股份有限公司');

-- 4. 企业天眼风险表 (1058接口解析目标)
-- 解析规则：3层嵌套展平（riskList→list→list），1:N关系
-- 每家公司每条风险记录为一行
CREATE TABLE IF NOT EXISTS company_1058_risk_info (
  id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
  api_record_id       BIGINT              COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time    DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
  main_company_name   VARCHAR(200) NOT NULL COMMENT '主公司名(搜索关键字/入参)',
  risk_level          VARCHAR(50)          COMMENT '风险等级(result.riskLevel)',
  risk_category_count INT                  COMMENT '该风险类别下的风险条数(riskList[].count)',
  risk_category_name  VARCHAR(50)          COMMENT '风险类别名(riskList[].name: 自身风险/周边风险/历史风险/预警提醒)',
  risk_type_total     INT                  COMMENT '该风险类型下的条数(riskList[].list[].total)',
  risk_type_tag       VARCHAR(50)          COMMENT '风险标签(riskList[].list[].tag: 警示/高风险/提示信息)',
  company_id          BIGINT               COMMENT '涉及公司ID(riskList[].list[].list[].companyId, 可空)',
  company_name        VARCHAR(200)         COMMENT '涉及公司名(riskList[].list[].list[].companyName, 可空)',
  risk_id             BIGINT               COMMENT '风险条目ID(riskList[].list[].list[].id)',
  risk_count          INT                  COMMENT '风险数量(riskList[].list[].list[].riskCount)',
  risk_title          VARCHAR(500)         COMMENT '风险描述(riskList[].list[].list[].title)',
  risk_type           INT                  COMMENT '风险类型码(riskList[].list[].list[].type)',
  risk_desc           VARCHAR(200)         COMMENT '风险简述(riskList[].list[].list[].desc)',
  INDEX idx_main_company (main_company_name),
  INDEX idx_api_record (api_record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='企业天眼风险(1058接口)';

-- 5. 变更记录表 (822接口解析目标)
-- 解析规则：2层展平（result.items数组），1:N关系
-- 每家公司每条变更记录为一行
CREATE TABLE IF NOT EXISTS company_822_change_info (
  id               BIGINT AUTO_INCREMENT PRIMARY KEY,
  api_record_id    BIGINT              COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
  company_name     VARCHAR(200) NOT NULL COMMENT '主公司名(搜索关键字/入参)',
  total            INT                  COMMENT '变更记录总数(result.total)',
  change_item      VARCHAR(200)         COMMENT '变更项名称(result.items[].changeItem)',
  content_before   TEXT                 COMMENT '变更前内容(result.items[].contentBefore)',
  content_after    TEXT                 COMMENT '变更后内容(result.items[].contentAfter)',
  change_time      VARCHAR(20)          COMMENT '变更时间(result.items[].changeTime)',
  create_time      VARCHAR(20)          COMMENT '记录创建时间(result.items[].createTime)',
  INDEX idx_company_name (company_name),
  INDEX idx_api_record (api_record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='变更记录(822接口)';

-- 6. 上市公司企业简介表 (854接口解析目标)
-- 解析规则：1:1关系，ON DUPLICATE KEY UPDATE
-- 4个Object字段展开：generalManager/chairman/secretaries/legal 各→type+name+id
-- 非上市公司查询成功但result为空 → step2跳过不解析
CREATE TABLE IF NOT EXISTS company_854_stock_info (
  id                      BIGINT AUTO_INCREMENT PRIMARY KEY,
  api_record_id           BIGINT              COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time        DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
  company_name            VARCHAR(200) NOT NULL COMMENT '主公司名(搜索关键字/入参)',
  area                    LONGTEXT            COMMENT '区域(result.area)',
  website                 LONGTEXT            COMMENT '网址(result.website)',
  stock_code              VARCHAR(255)        COMMENT '股票代码(result.code)',
  address                 VARCHAR(255)        COMMENT '地址(result.address)',
  gm_type                 INT                 COMMENT '总经理类型(result.generalManager.cType: 1=公司,2=人)',
  gm_name                 VARCHAR(120)        COMMENT '总经理姓名(result.generalManager.name)',
  gm_id                   BIGINT              COMMENT '总经理ID(result.generalManager.id)',
  stock_company_name      VARCHAR(255)        COMMENT 'API返回公司名(result.companyName)',
  employees_num           VARCHAR(255)        COMMENT '员工人数(result.employeesNum)',
  main_business           LONGTEXT            COMMENT '主营业务(result.mainBusiness)',
  mobile                  VARCHAR(255)        COMMENT '电话(result.mobile)',
  chairman_type           INT                 COMMENT '董事长类型(result.chairman.cType: 1=公司,2=人)',
  chairman_name           VARCHAR(120)        COMMENT '董事长姓名(result.chairman.name)',
  chairman_id             BIGINT              COMMENT '董事长ID(result.chairman.id)',
  industry                VARCHAR(255)        COMMENT '行业(result.industry)',
  product_name            LONGTEXT            COMMENT '产品名称(result.productName)',
  secretary_type          INT                 COMMENT '董秘类型(result.secretaries.cType: 1=公司,2=人)',
  secretary_name          VARCHAR(120)        COMMENT '董秘姓名(result.secretaries.name)',
  secretary_id            BIGINT              COMMENT '董秘ID(result.secretaries.id)',
  actual_controller       LONGTEXT            COMMENT '实际控制人(result.actualController)',
  controlling_shareholder LONGTEXT            COMMENT '控股股东(result.controllingShareholder)',
  eng_name                VARCHAR(255)        COMMENT '英文名(result.engName)',
  registered_capital      VARCHAR(255)        COMMENT '注册资本(result.registeredCapital)',
  postalcode              VARCHAR(255)        COMMENT '邮编(result.postalcode)',
  legal_person_type       INT                 COMMENT '法人类型(result.legal.cType: 1=公司,2=人)',
  legal_person_name       VARCHAR(120)        COMMENT '法人姓名(result.legal.name)',
  legal_person_id         BIGINT              COMMENT '法人ID(result.legal.id)',
  listed_name             VARCHAR(255)        COMMENT '上市公司简称(result.name)',
  fax                     VARCHAR(255)        COMMENT '传真(result.fax)',
  used_name               VARCHAR(255)        COMMENT '曾用名(result.usedName)',
  final_controller        LONGTEXT            COMMENT '最终控制人(result.finalController)',
  introduction            TEXT                COMMENT '简介(result.introduction)',
  UNIQUE KEY uk_company_name (company_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='上市公司企业简介(854接口)';

-- 7. 组织机构类型表 (1168接口解析目标)
-- 解析规则：1:1关系，ON DUPLICATE KEY UPDATE
-- orgTypes/economyTypes数组 → 逗号分隔字符串，分别拆为level1/level2列
CREATE TABLE IF NOT EXISTS company_1168_org_type_info (
  id                BIGINT AUTO_INCREMENT PRIMARY KEY,
  api_record_id     BIGINT              COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time  DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
  company_name      VARCHAR(200) NOT NULL COMMENT '主公司名(搜索关键字/入参)',
  org_type_level1   TEXT                COMMENT '一级机构类型(result.orgTypes[].level1,逗号分隔)',
  org_type_level2   TEXT                COMMENT '二级机构类型(result.orgTypes[].level2,逗号分隔)',
  economy_type_level1 TEXT              COMMENT '一级经济类型(result.economyTypes[].level1,逗号分隔)',
  economy_type_level2 TEXT              COMMENT '二级经济类型(result.economyTypes[].level2,逗号分隔)',
  UNIQUE KEY uk_company_name (company_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='组织机构类型(1168接口)';

-- 8. 企业规模表 (1149接口解析目标)
-- 解析规则：1:1关系，ON DUPLICATE KEY UPDATE
-- result为简单字符串(如"大型")
CREATE TABLE IF NOT EXISTS company_1149_scale_info (
  id                BIGINT AUTO_INCREMENT PRIMARY KEY,
  api_record_id     BIGINT              COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time  DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
  company_name      VARCHAR(200) NOT NULL COMMENT '主公司名(搜索关键字/入参)',
  company_scale     VARCHAR(50)          COMMENT '企业规模(result字符串,如"大型")',
  UNIQUE KEY uk_company_name (company_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='企业规模(1149接口)';

-- 9. 主要指标-年度表 (967接口解析目标)
-- 解析规则：1:N关系，每年度一行，DELETE+INSERT
-- result为数组，每个年度对象含~28个decimal字段+showYear
-- 非上市公司返回error_code=300000，step1记录失败，step2天然跳过
CREATE TABLE IF NOT EXISTS company_967_main_index_info (
  id                          BIGINT AUTO_INCREMENT PRIMARY KEY,
  api_record_id               BIGINT              COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time            DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
  company_name                VARCHAR(200) NOT NULL COMMENT '主公司名(搜索关键字/入参)',
  show_year                   VARCHAR(32)          COMMENT '年份(result[].showYear)',
  crfgsasr_to_revenue         DECIMAL(24,4)        COMMENT '销售现金流/营业收入',
  np_atsopc_nrgal_yoy         DECIMAL(24,4)        COMMENT '扣非净利润同比增长(%)',
  asset_liab_ratio            DECIMAL(24,4)        COMMENT '资产负债率(%)',
  op_to_revenue               DECIMAL(24,4)        COMMENT '营业利润/营业收入(%)',
  revenue_yoy                 DECIMAL(24,4)        COMMENT '营业总收入同比增长(%)',
  net_profit_atsopc_yoy       DECIMAL(24,4)        COMMENT '归属净利润同比增长(%)',
  receivable_turnover_days    DECIMAL(24,4)        COMMENT '应收账款周转天数(天)',
  current_ratio               DECIMAL(24,4)        COMMENT '流动比率',
  operate_cash_flow_ps        DECIMAL(24,4)        COMMENT '每股经营现金流(元)',
  gross_selling_rate          DECIMAL(24,4)        COMMENT '毛利率(%)',
  current_liab_to_total_liab  DECIMAL(24,4)        COMMENT '流动负债/总负债(%)',
  quick_ratio                 DECIMAL(24,4)        COMMENT '速动比率',
  fully_dlt_roe               DECIMAL(24,4)        COMMENT '摊薄净资产收益率(%)',
  tax_rate                    DECIMAL(24,4)        COMMENT '实际税率(%)',
  net_interest_of_total_assets DECIMAL(24,4)       COMMENT '摊薄总资产收益率(%)',
  operating_total_revenue_lrr_sq DECIMAL(24,4)     COMMENT '营业总收入滚动环比增长(%)',
  profit_deduct_nrgal_lrr_sq  DECIMAL(24,4)        COMMENT '扣非净利润滚动环比增长(%)',
  wgt_avg_roe                 DECIMAL(24,4)        COMMENT '加权净资产收益率(%)',
  net_profit_per_share        DECIMAL(24,4)        COMMENT '每股净资产(元)',
  ncf_from_oa_to_revenue      DECIMAL(24,4)        COMMENT '经营现金流/营业收入',
  profit_nrgal_sq             DECIMAL(24,4)        COMMENT '扣非净利润(元)',
  basic_eps                   DECIMAL(24,4)        COMMENT '基本每股收益(元)',
  net_selling_rate            DECIMAL(24,4)        COMMENT '净利率(%)',
  total_capital_turnover      DECIMAL(24,4)        COMMENT '总资产周转率(次)',
  net_profit_atsopc_lrr_sq    DECIMAL(24,4)        COMMENT '归属净利润滚动环比增长(%)',
  inventory_turnover_days     DECIMAL(24,4)        COMMENT '存货周转天数(天)',
  pre_receivable              DECIMAL(24,4)        COMMENT '预收款/营业收入',
  total_revenue               DECIMAL(24,4)        COMMENT '营业总收入(元)',
  undistri_profit_ps          DECIMAL(24,4)        COMMENT '每股未分配利润(元)',
  dlt_earnings_per_share      DECIMAL(24,4)        COMMENT '稀释每股收益(元)',
  net_profit_atsopc           DECIMAL(24,4)        COMMENT '归属净利润(元)',
  basic_e_ps_net_of_nrgal     DECIMAL(24,4)        COMMENT '扣非每股收益(元)',
  capital_reserve             DECIMAL(24,4)        COMMENT '每股公积金(元)',
  INDEX idx_company_name (company_name),
  INDEX idx_api_record (api_record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='主要指标-年度(967接口)';

-- 10. 法律诉讼表 (1114接口解析目标)
-- 解析规则：1:N关系，每条诉讼记录一行，DELETE+INSERT
-- casePersons取前2人：role1/gid1/emotion1/sptname1/name1/type1 + role2/gid2/emotion2/sptname2/name2/type2
-- submitTime为毫秒时间戳 → datetime
-- id映射为lawsuit_id避免与表主键冲突
-- ⚠️ 特别备注：1114接口支持翻页(pageNum/pageSize)，天眼查最多返回500条记录
--   step1采用循环翻页策略，合并所有页数据存入一条api_call_record(JSON类型可存储大对象)
--   保守方案：若未来数据量超出JSON存储上限，需考虑分页存储或LONGTEXT替代
CREATE TABLE IF NOT EXISTS company_1114_lawsuit_info (
  id                BIGINT AUTO_INCREMENT PRIMARY KEY,
  api_record_id     BIGINT              COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time  DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
  company_name      VARCHAR(200) NOT NULL COMMENT '主公司名(搜索关键字/入参)',
  total             INT                 COMMENT '诉讼记录总数(result.total)',
  lawsuit_id        BIGINT              COMMENT '诉讼条目ID(items[].id,避免与表主键冲突)',
  doc_type          VARCHAR(200)        COMMENT '文书类型(items[].docType)',
  lawsuit_url       VARCHAR(500)        COMMENT '天眼查URL-Web(items[].lawsuitUrl)',
  lawsuit_h5_url    VARCHAR(500)        COMMENT '天眼查URL-H5(items[].lawsuitH5Url)',
  title             VARCHAR(1000)       COMMENT '案件名称(items[].title)',
  court             VARCHAR(200)        COMMENT '审理法院(items[].court)',
  judge_time        VARCHAR(50)         COMMENT '裁判日期(items[].judgeTime)',
  uuid              VARCHAR(100)        COMMENT 'UUID(items[].uuid)',
  case_no           VARCHAR(200)        COMMENT '案号(items[].caseNo)',
  case_type         VARCHAR(100)        COMMENT '案件类型(items[].caseType)',
  case_reason       VARCHAR(500)        COMMENT '案由(items[].caseReason)',
  case_money        VARCHAR(200)        COMMENT '案件金额(items[].caseMoney)',
  submit_time       DATETIME            COMMENT '发布日期(items[].submitTime,毫秒时间戳→datetime)',
  case_result       VARCHAR(200)        COMMENT '案件结果标签(casePersons[0].result)',
  role1             VARCHAR(100)        COMMENT '案件身份1(casePersons[0].role)',
  gid1              VARCHAR(200)        COMMENT 'ID1(casePersons[0].gid)',
  emotion1          INT                 COMMENT '情感倾向1(casePersons[0].emotion: 1=正面,0=中性,-1=负面)',
  sptname1          VARCHAR(500)        COMMENT '疑似名称1(casePersons[0].sptname)',
  name1             VARCHAR(500)        COMMENT '名称1(casePersons[0].name)',
  type1             VARCHAR(50)         COMMENT '类型1(casePersons[0].type: 1=人员,2=公司)',
  role2             VARCHAR(100)        COMMENT '案件身份2(casePersons[1].role)',
  gid2              VARCHAR(200)        COMMENT 'ID2(casePersons[1].gid)',
  emotion2          INT                 COMMENT '情感倾向2(casePersons[1].emotion: 1=正面,0=中性,-1=负面)',
  sptname2          VARCHAR(500)        COMMENT '疑似名称2(casePersons[1].sptname)',
  name2             VARCHAR(500)        COMMENT '名称2(casePersons[1].name)',
  type2             VARCHAR(50)         COMMENT '类型2(casePersons[1].type: 1=人员,2=公司)',
  INDEX idx_company_name (company_name),
  INDEX idx_api_record (api_record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='法律诉讼(1114接口)';

-- 11. 现金流量表 (973接口解析目标)
-- 解析规则：1:N关系，每报告期一行，DELETE+INSERT
-- 只提取result.corpCashFlow下的所有字段，不提取corpFinancialYears
-- 所有字段为VARCHAR(带单位的字符串如"1.78 亿")
-- showYear表示报告期(如"2018 年报"、"2018 一季度")
-- 非上市公司返回error_code=300000，step1记录失败，step2天然跳过
CREATE TABLE IF NOT EXISTS company_973_cash_flow_info (
  id                          BIGINT AUTO_INCREMENT PRIMARY KEY,
  api_record_id               BIGINT              COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time            DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
  company_name                VARCHAR(200) NOT NULL COMMENT '主公司名(搜索关键字/入参)',
  show_year                   VARCHAR(32)          COMMENT '报告期(result.corpCashFlow[].showYear)',
  ncf_from_oa                 VARCHAR(200)         COMMENT '经营活动产生的现金流量净额',
  sub_total_of_ci_from_oa     VARCHAR(200)         COMMENT '经营活动现金流入小计',
  sub_total_of_cos_from_oa    VARCHAR(200)         COMMENT '经营活动现金流出小计',
  cash_received_of_sales_service VARCHAR(200)      COMMENT '销售商品、提供劳务收到的现金',
  payments_of_all_taxes       VARCHAR(200)         COMMENT '支付的各项税费',
  cash_paid_to_staff_etc      VARCHAR(200)         COMMENT '支付给职工以及为职工支付的现金',
  goods_buy_and_service_cash_pay VARCHAR(200)      COMMENT '购买商品、接受劳务支付的现金',
  other_cash_paid_related_to_oa VARCHAR(200)       COMMENT '支付其他与经营活动有关的现金',
  cash_received_of_other_fa   VARCHAR(200)         COMMENT '收到其他与经营活动有关的现金',
  ncf_from_ia                 VARCHAR(200)         COMMENT '投资活动产生的现金流量净额',
  sub_total_of_ci_from_ia     VARCHAR(200)         COMMENT '投资活动现金流入小计',
  sub_total_of_cos_from_ia    VARCHAR(200)         COMMENT '投资活动现金流出小计',
  cash_received_of_dspsl_invest VARCHAR(200)       COMMENT '收回投资收到的现金',
  invest_income_cash_received VARCHAR(200)         COMMENT '取得投资收益收到的现金',
  net_cash_of_disposal_assets VARCHAR(200)         COMMENT '处置固定资产、无形资产和其他长期资产收回的现金净额',
  net_cash_of_disposal_branch VARCHAR(200)         COMMENT '处置子公司及其他营业单位收到的现金净额',
  cash_received_of_other_ia   VARCHAR(200)         COMMENT '收到其他与投资活动有关的现金',
  invest_paid_cash            VARCHAR(200)         COMMENT '投资支付的现金',
  cash_paid_for_assets        VARCHAR(200)         COMMENT '购建固定资产、无形资产和其他长期资产支付的现金',
  ncf_from_fa                 VARCHAR(200)         COMMENT '筹资活动产生的现金流量净额',
  sub_total_of_ci_from_fa     VARCHAR(200)         COMMENT '筹资活动现金流入小计',
  sub_total_of_cos_from_fa    VARCHAR(200)         COMMENT '筹资活动现金流出小计',
  cash_received_of_absorb_invest VARCHAR(200)      COMMENT '吸收投资收到的现金',
  cash_received_from_investor VARCHAR(200)         COMMENT '子公司吸收少数股东投资收到的现金',
  cash_received_of_borrowing  VARCHAR(200)         COMMENT '取得借款收到的现金',
  cash_received_from_bond_issue VARCHAR(200)       COMMENT '发行债券收到的现金',
  cash_received_of_othr_fa    VARCHAR(200)         COMMENT '收到其他与筹资活动有关的现金',
  cash_pay_for_debt           VARCHAR(200)         COMMENT '偿还债务支付的现金',
  cash_paid_of_distribution   VARCHAR(200)         COMMENT '分配股利、利润或偿付利息支付的现金',
  other_cash_paid_relating_to_fa VARCHAR(200)      COMMENT '支付其他与筹资活动有关的现金',
  branch_paid_to_minority_holder VARCHAR(200)      COMMENT '子公司支付给少数股东的股利、利润',
  net_increase_in_cce         VARCHAR(200)         COMMENT '现金及现金等价物净增加额',
  initial_balance_of_cce     VARCHAR(200)         COMMENT '期初现金及现金等价物余额',
  final_balance_of_cce        VARCHAR(200)         COMMENT '期末现金及现金等价物余额',
  net_cash_amt_from_branch    VARCHAR(200)         COMMENT '取得子公司及其他营业单位支付的现金净额',
  effect_of_exchange_chg_on_cce VARCHAR(200)      COMMENT '汇率变动对现金及现金等价物的影响',
  INDEX idx_company_name (company_name),
  INDEX idx_api_record (api_record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='现金流量表(973接口)';

-- 12. 付款指数表 (邓白氏P51060接口解析目标)
-- 解析规则：1:1关系，ON DUPLICATE KEY UPDATE
-- res为JSON字符串，解析后各字段入库
-- companyHistoryPayDexes为List → JSON字符串存储
CREATE TABLE IF NOT EXISTS company_P51060_paydex_info (
  id                            BIGINT AUTO_INCREMENT PRIMARY KEY,
  api_record_id                 BIGINT              COMMENT 'API调用记录ID(关联api_call_record.id)',
  data_create_time              DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间',
  company_name                  VARCHAR(200) NOT NULL COMMENT '主公司名(搜索关键字/入参,entityName)',
  uscc                          VARCHAR(50)          COMMENT '统一社会信用代码(res.uscc)',
  company_paydex                VARCHAR(50)          COMMENT 'PayDex评分数值(最新)(res.companyPayDex)',
  company_paydex_date           VARCHAR(20)          COMMENT 'PayDex评分日期(最新)(res.companyPayDexDate)',
  company_history_paydexes      TEXT                 COMMENT 'PayDex历史信息(res.companyHistoryPayDexes,JSON字符串)',
  sic2                          VARCHAR(20)          COMMENT 'SIC前2位(res.sic2)',
  sic3                          VARCHAR(20)          COMMENT 'SIC前3位(res.sic3)',
  sic4                          VARCHAR(20)          COMMENT 'SIC前4位(res.sic4)',
  industry_paydex_date          VARCHAR(20)          COMMENT '行业PayDex评分日期(最新)(res.industryPayDexDate)',
  industry_lower_quartile_paydex VARCHAR(50)         COMMENT '行业25分位PayDex评分数值(res.industryLowerQuartilePayDex)',
  industry_median_paydex        VARCHAR(50)          COMMENT '行业50分位PayDex评分数值(res.industryMedianPayDex)',
  industry_upper_quartile_paydex VARCHAR(50)         COMMENT '行业75分位PayDex评分数值(res.industryUpperQuartilePayDex)',
  industry_count_num            VARCHAR(50)          COMMENT '行业统计数据-样本数量(res.industryCountNum)',
  industry_company_position     VARCHAR(50)          COMMENT '行业位置(res.industryCompanyPosition)',
  company_average               VARCHAR(100)         COMMENT '平均付款天数(中文)(res.companyAverage)',
  en_company_average            VARCHAR(100)         COMMENT '平均付款天数(英文)(res.encompanyAverage)',
  industry_average              VARCHAR(100)         COMMENT '行业平均付款天数(中文)(res.industryAverage)',
  en_industry_average           VARCHAR(100)         COMMENT '行业平均付款天数(英文)(res.enindustryAverage)',
  UNIQUE KEY uk_company_name (company_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='付款指数(邓白氏P51060接口)';