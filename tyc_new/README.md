# PowerLink - Databricks ODS层三方数据接入(tyc_new)

> Databricks Notebook + PySpark + Delta + Unity Catalog 数据接入方案
> 
> 全部脚本为Notebook版，直接复制到Databricks Notebook cell中运行

## 与原版(tyc)的主要差异

| 维度 | 原版(tyc) | 新版(tyc_new) |
|------|-----------|---------------|
| 数据平台 | MySQL 8.0 | Databricks (Unity Catalog) |
| 数据格式 | InnoDB | Delta Lake |
| 表命名 | `company_xxx` / `api_call_record` | `ods_tyc_819_df` / `ods_dnb_P51060_df` / `ods_api_call_record_df` |
| 分区方式 | 无分区 | PARTITIONED BY (dt STRING), dt=yyyyMMdd格式 |
| 运行方式 | spark-submit | Notebook cell (复制粘贴即可) |
| 时间字段 | DATETIME字符串 | TIMESTAMP(datetime对象) |
| 写入方式 | INSERT / ON DUPLICATE KEY UPDATE | 动态分区覆盖(overwrite) |
| Schema | 硬编码Schema定义 | 从Delta表动态读取schema |
| 重试逻辑 | 事不过三(API+写入一起重试) | 两阶段分离: API重试 + 写入失败直接终止 |

## 核心设计: 两阶段分离 + 前置SQL过滤(Phase 2已删除)

**Step1 (API拉取)** 采用两阶段分离，节省API调用次数：
- **阶段1 - API调用**: 事不过三重试，仅对HTTP请求异常和API业务错误重试;`normal_error_codes`(如300000=非分公司/无数据)为正常业务错误,不重试直接退出
- **阶段2 - Delta写入**: API成功后才写入，写入失败直接报错终止，**不浪费API调用次数重试写入**

**Step2 (数据解析)** 直接读取成功记录并解析写入，无重试逻辑。

**⛔ Phase 2(补充跑批)已从所有step1删除**:
- 改为前置SQL(ods_init)标记 `is_new_customer`/`in_monthly_batch` 字段,`get_company_list()` 根据frequency+4字段直接过滤
- daily非月度跑批日跑"账期+新增预付款",monthly非月度跑批日跑"新增客户且非月度已跑"
- `should_run_today()` 改为每天都返回True,`get_run_dt()` 统一计算写入分区
- step2仍保留Phase 2代码(死代码,待清理)

## 表名格式

所有ODS表统一格式: `powerlink.pw_ods.ods_{接口类型}_{接口id}_df`

| 接口类型 | 接口ID | 表名 | 说明 |
|---------|--------|------|------|
| tyc | 819 | ods_tyc_819_df | 企业基本信息 |
| tyc | 1058 | ods_tyc_1058_df | 天眼风险 |
| tyc | 822 | ods_tyc_822_df | 变更记录 |
| tyc | 854 | ods_tyc_854_df | 上市公司简介 |
| tyc | 1168 | ods_tyc_1168_df | 组织机构 |
| tyc | 1149 | ods_tyc_1149_df | 企业规模 |
| tyc | 967 | ods_tyc_967_df | 主要指标-年度 |
| tyc | ~~1114~~ | ods_tyc_1114_df | ~~法律诉讼~~ (已弃用) |
| tyc | 973 | ods_tyc_973_df | 现金流量表 |
| tyc | 1001 | ods_tyc_1001_df | 工商信息(分公司查总公司) |
| dnb | P51060 | ods_dnb_P51060_df | 付款指数(邓白氏) |
| - | - | ods_api_call_record_{接口id}_df | 各接口独立调用记录(并发安全,13个Task各写不同表) |
| - | - | ods_credit_preprocessing_company_list_df | 客户预处理表(sap_code粒度,is_prepaid+account_group_name) |
| - | - | ods_credit_company_parent_check_df | 总公司检查表(manual_add_flag+parent_company_name+is_new_customer+in_monthly_batch) |
| - | - | ods_credit_api_input_company_df | 接口调用入参公司表(name+is_prepaid+is_new_customer+in_monthly_batch) |
| - | - | ods_credit_api_input_branch_company_df | 分公司入参公司表(1001专用,819-step2后SQL生成,已按company_org_type含'分'过滤) |
| - | - | ods_credit_api_input_new_customers_df | 新增客户入参表(定向1001专用,每日对比分区生成) |
| - | - | ods_credit_api_white_company_list_nd | HK/TW白名单(免跑接口,每日全量重建,读历史819 dt<=bizdate_1) |

## 目录结构

```
tyc_new/
├── config/
│   └── config.json.example          # 配置模板(不含真实token)
├── ddl/
│   └── databricks_ods_ddl.sql       # 所有ODS表DDL(Delta+分区)
├── etl_script/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── config_loader.py         # 配置加载(Unity Catalog Volume)
│   │   └── spark_utils.py           # Spark/Delta通用工具(动态schema)
│   ├── notebook_init.py             # Notebook初始化cell
│   ├── diagnostic_test.py           # 环境诊断测试
│   ├── 819-step1_api_fetch_notebook.py    # 企业基本信息 - API拉取
│   ├── 819-step2_data_parse_notebook.py   # 企业基本信息 - 数据解析
│   ├── 1058-step1_api_fetch_notebook.py   # 天眼风险 - API拉取
│   ├── 1058-step2_data_parse_notebook.py  # 天眼风险 - 数据解析
│   ├── 822-step1_api_fetch_notebook.py    # 变更记录 - API拉取
│   ├── 822-step2_data_parse_notebook.py   # 变更记录 - 数据解析
│   ├── 854-step1_api_fetch_notebook.py    # 上市公司简介 - API拉取
│   ├── 854-step2_data_parse_notebook.py   # 上市公司简介 - 数据解析
│   ├── 1168-step1_api_fetch_notebook.py   # 组织机构 - API拉取
│   ├── 1168-step2_data_parse_notebook.py  # 组织机构 - 数据解析
│   ├── 1149-step1_api_fetch_notebook.py   # 企业规模 - API拉取
│   ├── 1149-step2_data_parse_notebook.py  # 企业规模 - 数据解析
│   ├── 967-step1_api_fetch_notebook.py    # 主要指标 - API拉取
│   ├── 967-step2_data_parse_notebook.py   # 主要指标 - 数据解析
│   ├── 1114-step1_api_fetch_notebook.py   # 法律诉讼(含翻页) - API拉取 (已弃用)
│   ├── 1114-step2_data_parse_notebook.py  # 法律诉讼 - 数据解析 (已弃用)
│   ├── 973-step1_api_fetch_notebook.py    # 现金流量表 - API拉取
│   ├── 973-step2_data_parse_notebook.py   # 现金流量表 - 数据解析
│   ├── 1001-step1_api_fetch_notebook.py   # 工商信息(分公司查总公司) - API拉取
│   ├── 1001-step2_data_parse_notebook.py  # 工商信息(分公司查总公司) - 数据解析
│   ├── P51060-step1_api_fetch_notebook.py # 付款指数(邓白氏) - API拉取
│   └── P51060-step2_data_parse_notebook.py# 付款指数(邓白氏) - 数据解析
├── tools/
│   ├── verify_schema.py             # 表结构验证
│   ├── verify_data.py               # 数据质量验证
│   └── verify_idempotency.py        # 幂等性验证
├── workflow/
│   └── ods/
│       ├── build_ods_credit_api_input_branch_company_df.sql   # 分公司入参表(1001专用,819-step2后全量重建)
│       └── build_ods_credit_api_input_new_customers_df.sql    # 新增客户入参表(819+1001定向跑批前置,对比今天vs昨天分区)
└── README.md
```

## 部署步骤

### 1. 创建Schema和表

在Databricks SQL Editor或Notebook中执行DDL:

```sql
CREATE SCHEMA IF NOT EXISTS powerlink.pw_ods
MANAGED LOCATION 'abfss://powerlink@powerlink.dfs.core.chinacloudapi.cn/pw_ods';

-- 逐个执行ddl/databricks_ods_ddl.sql中的CREATE TABLE语句
```

### 2. 上传配置文件到Unity Catalog Volume

```sql
CREATE VOLUME IF NOT EXISTS powerlink.default.env;
```

上传config.json到Workspace，路径: `/Workspace/Shared/powerlink_warehouse/tyc_new/config/config.json`

### 3. 上传脚本到Workspace

将整个`etl_script/`目录上传到Databricks Workspace，比如:
- `/Workspace/Shared/tyc_new/etl_script/`

或通过Repos用git同步。

### 4. Notebook中运行

每个接口需要3个cell:

**Cell 1 - 初始化** (所有接口共用):
```python
# 复制 notebook_init.py 内容到cell
# 修改COMMON_PATH为你的实际部署路径
```

**Cell 2 - Step1 API拉取** (复制对应接口的step1 notebook脚本)

**Cell 3 - Step2 数据解析** (复制对应接口的step2 notebook脚本)

### 5. 验证

```python
# 复制 diagnostic_test.py 到cell运行
# 或复制 tools/verify_schema.py / verify_data.py / verify_idempotency.py
```

## 解析规则(与原版相同)

详细解析规则见Obsidian方法论文档 `claude变更记录/天眼查数据接入方法论.md`。

核心规则：
- 两步流水线: step1拉取 + step2解析
- 幂等检查: 当天dt分区已有status_code=0则跳过
- 事不过三: Step1 API调用最多3次重试
- 字段映射: camelCase→snake_case + FIELD_MAPPING显式映射
- 空字符串→NULL, DECIMAL字段0为有效值
- 1:1和1:N: 动态分区覆盖(dt分区全量刷新)

## 关键设计说明

1. **动态Schema**: `write_api_records`和`write_target_data`从Delta表读取schema创建DataFrame，避免硬编码类型与DDL不一致导致的schema冲突
2. **datetime对象**: call_datetime、data_create_time、所有TIMESTAMP字段使用Python datetime对象而非字符串，与DDL的TIMESTAMP类型匹配
3. **dt分区格式**: yyyyMMdd (如20260527)，与客户表ads_customer_wide_tab_tmp_df的dt格式保持一致
4. **两阶段分离**: API调用重试与Delta写入分离，写入失败不浪费API调用次数
5. **邓白氏uscc**: 从ods_tyc_819_df读取social_credit_code作为入参
6. **normal_error_codes**: TYC接口[300000](非分公司/无数据)不重试直接break;邓白氏[1,1021,2001];`.get('normal_error_codes', [])`向后兼容
7. **get_run_dt**: 统一计算写入分区(daily→t-1, monthly→月度跑批日-1, INIT_MODE+prepaid_run_months→半年跑批日-1)
8. **各接口独立调用记录表**: `ods_api_call_record_{接口id}_df`,13个Task各写不同表,并发安全
9. **HK/TW白名单**: `exclude_hk_tw=true`所有接口含819默认过滤,白名单读历史819可和init并行

## 1001分公司查总公司 - 闭环设计(3-notebook拆分,定向1001,当天覆盖)

1001接口(分公司查总公司)采用daily频次,定向1001跑新增客户,ods_init_2读昨天+今天1001分区兜底母公司,当天新增分公司当天拿到母公司,无1天延迟。不需要定向819(白名单读历史819,新增非HK/TW客户由全量819兜底)。

**3个ods_init notebook拆分**:
- **ods_init_1**: 上游同步 + 新增客户表 + 白名单(读历史819 `dt<=bizdate_1`,可和init并行)
- **ods_init_2**: parent_check + input_company(4.2读 `dt>=昨天` 1001分区兜底,需等定向1001完成)
- **ods_init_3**: 分公司名单(读当天819+月度跑批日819,需等全量819-step2完成)

**编排顺序**(Databricks Jobs):
1. **init ‖ ods_init_1** — 并行(环境初始化 + 上游同步+新增客户表+白名单)
2. **定向 1001-step1 → 1001-step2**(`--targeted_mode=true`) — 只跑1001,不需要定向819;1001对分公司返回母公司,对非分公司返回error/null(`charge_per_query=false`不扣费,`normal_error_codes=[300000]`不重试)
3. **ods_init_2** — parent_check+input_company,4.2读 `dt>=昨天` 1001分区兜底,生成当天入参清单
4. **全量 819-step1 → 819-step2** — 跑全量客户(幂等跳过已定向跑的新增客户)
5. **ods_init_3** — 从当天819+月度跑批日819过滤 `company_org_type LIKE '%分%'` 重建分公司入参表
6. **全量 1001-step1 → 1001-step2** — 跑全量分公司(幂等跳过已定向跑的新增分公司)
7. **其他11组step1 → step2** — 并行跑(851/1058/822/854/1168/1149/967/~~1114(已弃用)~~/1041/973/P51060)

**闭环说明**:
- 新增客户识别: ods_init_1对比 `ods_view_account_base_df` 今天vs昨天分区(同4.2过滤条件)LEFT ANTI JOIN
- 定向1001(只跑1001): 1001对非分公司返回error/null(`charge_per_query=false`不扣费),`normal_error_codes=[300000]`不重试;parent_check只取`parent_company_name`非空的自动过滤;新增HK/TW客户当天会被调用1次(可接受,次日入白名单后所有接口跳过)
- ods_init_2兜底: 规则(`公司.+公司`)匹配不到母公司时,从 `ods_tyc_1001_df` `dt>=昨天` 分区取分公司→总公司映射(`ROW_NUMBER`取每个分公司最新一条)
- 新分公司当天闭环: 当天识别新增分公司 → 定向1001解析 → ods_init_2当天兜底取到母公司,无1天延迟
- 全量跑幂等: 定向跑过的客户,全量819/1001通过 `has_success_today` 跳过,不重复调用
- 1001 `prepaid_filter=false`: 入参表已过滤分公司,无需再判断预付款;定向模式不区分预付款,所有新增客户都跑

## 频次与过滤逻辑(Phase 2删除后)

客户表 `ods_credit_api_input_company_df` 4个字段: `name`, `is_prepaid`(是/否), `is_new_customer`(是/否), `in_monthly_batch`(是/否)
- `is_new_customer`: 今天客户表有但昨天没有(新增客户)
- `in_monthly_batch`: 在1168最近月度跑批日分区出现过(月度全量已跑,剔除"曾经存在→消失→又回来")

`get_company_list()` 根据 `frequency` + 4字段过滤:

| frequency | 月度跑批日(5号) | 非月度跑批日 |
| --------- | ------------ | --------- |
| daily | 全部客户 | 账期 + 新增预付款 (`is_prepaid='否' OR (is_new_customer='是' AND is_prepaid='是')`) |
| monthly | 全部客户(prepaid_run_months非配置月份时仅账期) | 新增客户且非"曾经存在→消失→又回来" (`is_new_customer='是' AND in_monthly_batch='否'`) |

`get_run_dt()` 统一计算写入分区:
- daily: t-1 (每天写昨天分区)
- monthly 月度跑批日: 月度跑批日-1 (当月跑批分区)
- monthly 非月度跑批日: 最近月度跑批日-1 (新增客户追加到最近月度分区)
- monthly INIT_MODE + prepaid_run_months: 半年跑批日-1 (P51060预付款init与半年跑批同分区)

## HK/TW白名单(免跑接口)

香港/台湾客户(`province_short` 为 `hk`/`tw`)的天眼查/邓白氏接口无意义,识别后加入白名单,所有接口跳过。

- **白名单表** `ods_credit_api_white_company_list_nd` (全量快照,无dt分区,每日由ods_init_1 section 4重建)
- **读历史819** `dt <= bizdate_1` (不依赖今天819,可和init并行)
- **所有接口含819都设 `exclude_hk_tw=true`**: HK/TW属性基本不变,识别后无需重复调用
- **新客户自动识别**: 新客户不在白名单→首次被819调用识别HK/TW→次日入白名单→所有接口跳过