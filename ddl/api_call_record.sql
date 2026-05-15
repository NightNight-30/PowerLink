-- ============================================
-- 天眼查数据接入 - 建表DDL (powerlink库)
-- ============================================

-- 1. 接口调用记录表
CREATE TABLE IF NOT EXISTS api_call_record (
  id             BIGINT AUTO_INCREMENT PRIMARY KEY,
  interface_name VARCHAR(32)  NOT NULL COMMENT '接口名，如819/967/971/973',
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