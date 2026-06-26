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
| 预付款过滤 | 无 | 三层调用规则 + Phase 2补充跑批 |
| 预警邮件 | 无 | Graph API + tesa品牌样式 |
| 接口数量 | 10个(9天眼查+1邓白氏) | 13个(12天眼查+1邓白氏) |

## 核心设计: 两阶段分离

**Step1 (API拉取)** 采用两阶段分离，节省API调用次数：
- **阶段1 - API调用**: 事不过三重试(HTTP异常+业务错误重试)；查询即计费接口(`charge_per_query=true`)失败不重试
- **阶段2 - Delta写入**: API成功后才写入，写入失败直接报错终止，**不浪费API调用次数重试写入**

**Step2 (数据解析)** 直接读取成功记录并解析写入，无重试逻辑。

## 三层调用规则

每个接口的调用由三层判断链控制：

| 层级 | 判断 | 说明 |
|:-----|:-----|:-----|
| 1. 频次 | `should_run_today()` | daily=每天跑, monthly=只在月度跑批日(10号)跑 |
| 2. 预付款过滤 | `is_prepaid_filter_enabled()` | 非月度跑批日只处理非预付款客户(is_prepaid='否') |
| 3. 幂等 | `has_success_today()` | 当天dt分区已有status_code=0则跳过 |

## Phase 2 补充跑批(新增预付款客户)

月度跑批日之后新增的预付款客户需要补充处理：

| 场景 | daily接口 | monthly接口 |
|:-----|:---------|:-----------|
| Phase1(月度跑批日) | 全部客户，写t-1分区 | 全部客户，写t-1分区(=月度跑批日-1) |
| Phase1(非月度跑批日) | 非预付款客户，写t-1分区 | 不执行 |
| Phase2(补充) | 新增预付款客户，写月度跑批日-1分区 | 新增预付款客户，写月度跑批日-1分区 |
| INIT_MODE(初始化) | 全部客户(含预付款)，写t-1分区 | 全部客户(含预付款)，写月度跑批日-1分区 |

- **检测**: call record表查最近月度跑批日至今无成功记录的预付款客户
- **写入**: 补充数据写入最近月度跑批日跑批时写入的分区(月度跑批日-1)，与正常月度跑批写入分区一致
- **防重复**: 月度分区有记录后，下次检测查到 → 跳过
- **分区统一**: `get_last_monthly_batch_date()` 返回 t-1 分区(monthly_day-1)，保证三种写入路径分区一致

## INIT_MODE 初始化模式

每个 step1 脚本顶部 `INIT_MODE = False` → 改为 `True` 即可全量初始化所有客户：

- **跳过频次检查**: `should_run_today(force_run=True)`，monthly接口非跑批日也能跑
- **跳过预付款过滤**: `get_company_list(force_all=True)`，处理全部客户(含预付款)
- **monthly接口dt覆盖**: `if INIT_MODE: dt = get_last_monthly_batch_date(CONFIG)`，写月度跑批日-1分区
- **保留幂等检查**: 当天已有成功记录的客户仍跳过(避免重复扣费)
- **跳过 Phase 2**: Phase 1 已全量处理，补充跑批无意义

适用场景: 测试跑部分客户后全量重跑 / 修正历史数据 / 首次初始化

## HK/TW白名单(免跑接口)

香港/台湾客户(`province_short` 为 `hk`/`tw`)的天眼查/邓白氏接口无意义，识别后加入白名单，所有接口跳过：

- **白名单表**: `powerlink.pw_ods.ods_init_white_company_list_nd` (全量快照，无dt分区)
- **每日全量重建**: `workflow/ods/build_ods_init_white_company_list_nd.sql`，读昨天819数据，按company_name取最新，过滤hk/tw
- **Jobs编排**: build_hk_tw_whitelist作为ods_init.ipynb的section 5，和init环境脚本并行跑，所有step1之前完成(白名单SQL读昨天819数据，不依赖今天819，可安全并行)
- **配置**: 每个接口 `exclude_hk_tw: true`(含819)，`is_hk_tw_filter_enabled()` 读取
- **过滤实现**: `get_company_list(exclude_hk_tw=True)` 读取白名单并排除

**自动识别流程**(白名单从空开始，无需手动切config)：
- 首次跑批: 白名单空→全部调用→819发现HK/TW→次日build SQL填充白名单
- 次日起: 已知HK/TW跳过，新客户不在白名单→819调用识别→次日入表
- HK/TW属性基本不变，所有接口含819都设true，新客户通过"不在白名单"自动被819识别

## 表名格式

所有ODS表统一格式: `powerlink.pw_ods.ods_{接口类型}_{接口id}_df`

| 接口类型 | 接口ID | 表名 | 说明 |
|---------|--------|------|------|
| tyc | 819 | ods_tyc_819_df | 企业基本信息 |
| tyc | 1058 | ods_tyc_1058_df | 天眼风险 |
| tyc | 822 | ods_tyc_822_df | 变更记录 |
| tyc | 851 | ods_tyc_851_df | 欠税公告 |
| tyc | 854 | ods_tyc_854_df | 上市公司简介 |
| tyc | 1168 | ods_tyc_1168_df | 组织机构 |
| tyc | 1149 | ods_tyc_1149_df | 企业规模 |
| tyc | 967 | ods_tyc_967_df | 主要指标-年度 |
| tyc | 1114 | ods_tyc_1114_df | 法律诉讼 |
| tyc | 1041 | ods_tyc_1041_df | 司法解析 |
| tyc | 973 | ods_tyc_973_df | 现金流量表 |
| dnb | P51060 | ods_dnb_P51060_df | 付款指数(邓白氏) |
| - | - | ods_api_call_record_df | API调用记录(共享) |

## 目录结构

```
tyc_new/
├── config/
│   ├── config.json.example          # 配置模板(频次/预付款/预警邮件)
│   └── tesa_logo.png               # 预警邮件品牌logo
├── ddl/
│   ├── databricks_ods_ddl.sql       # 所有ODS表DDL(Delta+分区)
│   └ migrate_api_call_record.sql    # 调用记录表DDL
├── etl_script/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── config_loader.py         # 配置加载(Workspace路径)
│   │   └── spark_utils.py           # Spark/Delta通用工具(动态schema+补充跑批)
│   ├── notebook_init.py             # Notebook初始化cell
│   ├── diagnostic_test.py           # 环境诊断测试
│   ├── daily_call_analysis_alert_notebook.py  # 调用分析预警邮件
│   ├── 819-step1_api_fetch_notebook.py    # 企业基本信息 - API拉取
│   ├── 819-step2_data_parse_notebook.py   # 企业基本信息 - 数据解析
│   ├── 1058-step1_api_fetch_notebook.py   # 天眼风险 - API拉取
│   ├── 1058-step2_data_parse_notebook.py  # 天眼风险 - 数据解析
│   ├── 822-step1_api_fetch_notebook.py    # 变更记录 - API拉取
│   ├── 822-step2_data_parse_notebook.py   # 变更记录 - 数据解析
│   ├── 851-step1_api_fetch_notebook.py    # 欠税公告(含翻页) - API拉取
│   ├── 851-step2_data_parse_notebook.py   # 欠税公告 - 数据解析
│   ├── 854-step1_api_fetch_notebook.py    # 上市公司简介 - API拉取
│   ├── 854-step2_data_parse_notebook.py   # 上市公司简介 - 数据解析
│   ├── 1168-step1_api_fetch_notebook.py   # 组织机构 - API拉取
│   ├── 1168-step2_data_parse_notebook.py  # 组织机构 - 数据解析
│   ├── 1149-step1_api_fetch_notebook.py   # 企业规模 - API拉取
│   ├── 1149-step2_data_parse_notebook.py  # 企业规模 - 数据解析
│   ├── 967-step1_api_fetch_notebook.py    # 主要指标 - API拉取
│   ├── 967-step2_data_parse_notebook.py   # 主要指标 - 数据解析
│   ├── 1114-step1_api_fetch_notebook.py   # 法律诉讼(含翻页) - API拉取
│   ├── 1114-step2_data_parse_notebook.py  # 法律诉讼 - 数据解析
│   ├── 1041-step1_api_fetch_notebook.py   # 司法解析(含翻页) - API拉取
│   ├── 1041-step2_data_parse_notebook.py  # 司法解析 - 数据解析
│   ├── 973-step1_api_fetch_notebook.py    # 现金流量表 - API拉取
│   ├── 973-step2_data_parse_notebook.py   # 现金流量表 - 数据解析
│   ├── P51060-step1_api_fetch_notebook.py # 付款指数(邓白氏) - API拉取
│   └── P51060-step2_data_parse_notebook.py# 付款指数(邓白氏) - 数据解析
├── tools/
│   ├── verify_schema.py             # 表结构验证
│   ├── verify_data.py               # 数据质量验证
│   └── verify_idempotency.py        # 幂等性验证
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

### 2. 上传配置文件到Workspace

上传config.json到Workspace，路径: `/Workspace/Shared/powerlink_warehouse/tyc_new/config/config.json`

### 3. 上传脚本到Workspace

将整个`etl_script/`目录上传到Databricks Workspace，比如:
- `/Workspace/Shared/tyc_new/etl_script/`

或通过Repos用git同步。

### 4. Notebook中运行

Jobs编排: 两个初始化Task并行执行 — `init`(环境) + `ods_init.ipynb`(上游数据同步+客户表+HK/TW白名单)，所有12组step1依赖两者都完成。白名单SQL读昨天819数据，不依赖今天819，可安全并行。

每个接口需要3个cell:

**Cell 1 - 初始化** (所有接口共用):
```python
# 复制 notebook_init.py 内容到cell
# 修改COMMON_PATH为你的实际部署路径
```

**Cell 2 - Step1 API拉取** (复制对应接口的step1 notebook脚本)

**Cell 3 - Step2 数据解析** (复制对应接口的step2 notebook脚本)

**自定义分区**: Step1脚本中可设置 `CUSTOMER_DT = '20260614'` 指定客户表分区日期，默认自动取MAX(dt)。

**初始化模式**: Step1脚本顶部 `INIT_MODE = False` → 改为 `True` 可全量跑所有客户(含预付款)，跳过频次检查和Phase 2，monthly接口写月度跑批日-1分区。详见上方"INIT_MODE 初始化模式"章节。

### 5. 验证

```python
# 复制 diagnostic_test.py 到cell运行
# 或复制 tools/verify_schema.py / verify_data.py / verify_idempotency.py
```

## 解析规则(与原版相同)

详细解析规则见Obsidian方法论文档。

核心规则：
- 两步流水线: step1拉取 + step2解析
- 幂等检查: 当天dt分区已有status_code=0则跳过
- 事不过三: Step1 API调用最多3次重试(查询计费接口失败不重试)
- 字段映射: camelCase→snake_case + FIELD_MAPPING显式映射
- 空字符串→NULL, DECIMAL字段0为有效值
- 1:1和1:N: 动态分区覆盖(dt分区全量刷新)
- **null安全**: `.get('key') or []` 防御JSON null(而非`.get('key', [])`)

## 关键设计说明

1. **动态Schema**: `write_api_records`和`write_target_data`从Delta表读取schema创建DataFrame，避免硬编码类型与DDL不一致导致的schema冲突
2. **datetime对象**: call_datetime、data_create_time、所有TIMESTAMP字段使用Python datetime对象而非字符串，与DDL的TIMESTAMP类型匹配
3. **dt分区格式**: yyyyMMdd (如20260527)，与客户表ads_customer_wide_tab_tmp_df的dt格式保持一致
4. **两阶段分离**: API调用重试与Delta写入分离，写入失败不浪费API调用次数
5. **邓白氏uscc**: 从ods_tyc_819_df读取social_credit_code作为入参
6. **频次/预付款过滤**: 配置表`frequency`和`prepaid_filter`双开关控制调用范围
7. **Phase 2补充跑批**: 新增预付款客户自动检测并写入月度分区，下游无需改动
8. **预警邮件**: Graph API + tesa品牌样式，每日调用分析+异常预警
9. **配置路径**: 从Volume迁移到Workspace `/Workspace/Shared/powerlink_warehouse/tyc_new/config/`
