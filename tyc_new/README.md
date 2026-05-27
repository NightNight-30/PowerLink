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
| 分区方式 | 无分区 | PARTITIONED BY (dt STRING), dt=昨天yyyyMMdd |
| 运行方式 | spark-submit | Notebook cell (复制粘贴即可) |
| 时间字段 | DATETIME字符串 | TIMESTAMP(datetime对象) |
| 写入方式 | INSERT / ON DUPLICATE KEY UPDATE | 动态分区覆盖(overwrite) |
| Schema | 硬编码Schema定义 | 从Delta表动态读取schema |
| 重试逻辑 | 事不过三(API+写入一起重试) | 两阶段分离: API重试 + 写入失败直接终止 |

## 核心设计: 两阶段分离

**Step1 (API拉取)** 采用两阶段分离，节省API调用次数：
- **阶段1 - API调用**: 事不过三重试，仅对HTTP请求异常和API业务错误重试
- **阶段2 - Delta写入**: API成功后才写入，写入失败直接报错终止，**不浪费API调用次数重试写入**

**Step2 (数据解析)** 直接读取成功记录并解析写入，无重试逻辑。

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
| tyc | 1114 | ods_tyc_1114_df | 法律诉讼 |
| tyc | 973 | ods_tyc_973_df | 现金流量表 |
| dnb | P51060 | ods_dnb_P51060_df | 付款指数(邓白氏) |
| - | - | ods_api_call_record_df | API调用记录(共享) |

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
│   ├── 1114-step1_api_fetch_notebook.py   # 法律诉讼(含翻页) - API拉取
│   ├── 1114-step2_data_parse_notebook.py  # 法律诉讼 - 数据解析
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

### 2. 上传配置文件到Unity Catalog Volume

```sql
CREATE VOLUME IF NOT EXISTS powerlink.default.env;
```

上传config.json到Volume，路径: `/Volumes/powerlink/default/env/config.json`

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
3. **dt分区格式**: 昨天yyyyMMdd (如20260526)，分区日期为数据所属日期而非执行日期
4. **两阶段分离**: API调用重试与Delta写入分离，写入失败不浪费API调用次数
5. **邓白氏uscc**: 从ods_tyc_819_df读取social_credit_code作为入参