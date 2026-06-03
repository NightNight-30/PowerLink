# PowerLink - Databricks ODS层三方数据接入(tyc_new)

> Databricks Jobs + PySpark + Delta + Unity Catalog 数据接入方案
>
> 11个接口并行执行，各接口独立调用记录表（并发安全）

## 与原版(tyc)的主要差异

| 维度 | 原版(tyc) | 新版(tyc_new) |
|------|-----------|---------------|
| 数据平台 | MySQL 8.0 | Databricks (Unity Catalog) |
| 数据格式 | InnoDB | Delta Lake |
| 调用记录表 | 共享1张 `api_call_record` | ⭐ 各接口独立 `ods_api_call_record_{id}_df` |
| 目标解析表 | `company_xxx` | `ods_tyc_819_df` / `ods_dnb_P51060_df` |
| 分区方式 | 无分区 | PARTITIONED BY (dt STRING), dt=昨天yyyyMMdd |
| 运行方式 | spark-submit | ⭐ Databricks Jobs编排(11个Task并行) |
| 时间字段 | DATETIME字符串 | TIMESTAMP(datetime对象) |
| 写入方式 | INSERT / ON DUPLICATE KEY UPDATE | 动态分区覆盖(overwrite) |
| Schema | 硬编码Schema定义 | 从Delta表动态读取schema |
| 并发安全 | 无 | ⭐ 各接口独立表，11个Task零冲突 |

## 核心设计

### 并发安全

⭐ **各接口独立调用记录表** — 11个Task并行执行时各写不同表，零id冲突、零Delta锁冲突

| | 旧方案 | 新方案 |
|---|---|---|
| 调用记录表 | 共享 `ods_api_call_record_df` | 各接口独立 `ods_api_call_record_{id}_df` |
| 并发写 | id冲突+Delta锁 | 零冲突 |
| id生成 | 全局MAX(id)+1 | 各表独立MAX(id)+1 |

### 两阶段分离

**Step1 (API拉取)**:
- **阶段1 - API调用**: 不计费接口事不过三重试；查询即计费接口(1168/1114/851)失败直接返回
- **阶段2 - Delta写入**: 写入失败直接报错终止，不浪费API调用次数重试写入

**Step2 (数据解析)**: 读取成功记录并解析写入，无重试逻辑

### Databricks Jobs编排

```
init (环境初始化)
  ├── 819_step1   → 819_step2      (并行)
  ├── 851_step1   → 851_step2      (并行)
  ├── 1058_step1  → 1058_step2     (并行)
  ├── 822_step1   → 822_step2      (并行)
  ├── 854_step1   → 854_step2      (并行)
  ├── 1168_step1  → 1168_step2     (并行)
  ├── 1149_step1  → 1149_step2     (并行)
  ├── 967_step1   → 967_step2      (并行)
  ├── 1114_step1  → 1114_step2     (并行)
  ├── 973_step1   → 973_step2      (并行)
  └── P51060_step1 → P51060_step2  (并行)
```

P51060不需要等819完成（entityName单独也能调用DNB接口）

### 重试策略

| 接口类型 | 重试策略 | 适用接口 |
|---------|---------|---------|
| 查询不计费 | 事不过三(最多3次) | 819, 1058, 822, 854, 1149, 967, 973 |
| ⭐ 查询即计费 | **不重试，失败直接返回** | 1168, 1114, 851 |
| 邓白氏 | 事不过三(最多3次) | P51060 |

## 表名格式

**调用记录表**: `powerlink.pw_ods.ods_api_call_record_{接口id}_df`
**目标解析表**: `powerlink.pw_ods.ods_{接口类型}_{接口id}_df`

| 接口ID | 调用记录表 | 目标解析表 | 说明 | 关系 |
|--------|-----------|-----------|------|------|
| 819 | ods_api_call_record_819_df | ods_tyc_819_df | 企业基本信息 | 1:1 |
| 851 | ods_api_call_record_851_df | ods_tyc_851_df | 欠税公告(含翻页) | 1:N |
| 1058 | ods_api_call_record_1058_df | ods_tyc_1058_df | 天眼风险 | 1:N |
| 822 | ods_api_call_record_822_df | ods_tyc_822_df | 变更记录 | 1:N |
| 854 | ods_api_call_record_854_df | ods_tyc_854_df | 上市公司简介 | 1:1 |
| 1168 | ods_api_call_record_1168_df | ods_tyc_1168_df | 组织机构 | 1:1 |
| 1149 | ods_api_call_record_1149_df | ods_tyc_1149_df | 企业规模 | 1:1 |
| 967 | ods_api_call_record_967_df | ods_tyc_967_df | 主要指标-年度 | 1:N |
| 1114 | ods_api_call_record_1114_df | ods_tyc_1114_df | 法律诉讼(含翻页) | 1:N |
| 973 | ods_api_call_record_973_df | ods_tyc_973_df | 现金流量表 | 1:N |
| P51060 | ods_api_call_record_P51060_df | ods_dnb_P51060_df | 付款指数(邓白氏) | 1:1 |

## 目录结构

```
tyc_new/
├── config/
│   └── config.json.example          # 配置模板
├── ddl/
│   ├── databricks_ods_ddl.sql       # 所有DDL(11个独立调用记录表+11个目标表)
│   └── migrate_api_call_record.sql  # 历史数据迁移DML(共享表→独立表)
├── etl_script/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── config_loader.py         # 配置加载(Unity Catalog Volume)
│   │   └── spark_utils.py           # ⭐核心(独立表映射+动态schema+去重写入+id自增)
│   ├── notebook_init.py             # Notebook初始化cell
│   ├── diagnostic_test.py           # 环境诊断测试
│   ├── 819-step1/2_*.py             # 企业基本信息
│   ├── 851-step1/2_*.py             # 欠税公告(含翻页,查询即计费)
│   ├── 1058-step1/2_*.py            # 天眼风险
│   ├── 822-step1/2_*.py             # 变更记录
│   ├── 854-step1/2_*.py             # 上市公司简介
│   ├── 1168-step1/2_*.py            # 组织机构(查询即计费)
│   ├── 1149-step1/2_*.py            # 企业规模
│   ├── 967-step1/2_*.py             # 主要指标-年度
│   ├── 1114-step1/2_*.py            # 法律诉讼(含翻页,查询即计费)
│   ├── 973-step1/2_*.py             # 现金流量表
│   ├── P51060-step1/2_*.py          # 付款指数(邓白氏)
│   └── ...
├── api/
│   ├── tyc/                         # 天眼查API文档PDF
│   └── db/                          # 邓白氏API文档PDF
├── tools/
│   ├── verify_schema.py             # 表结构验证
│   ├── verify_data.py               # 数据质量验证
│   └── verify_idempotency.py        # 幂等性验证
└── README.md
```

## 部署步骤

### 1. 创建Schema和表

在Databricks SQL Editor执行DDL:

```sql
CREATE SCHEMA IF NOT EXISTS powerlink.pw_ods
MANAGED LOCATION 'abfss://powerlink@powerlink.dfs.core.chinacloudapi.cn/pw_ods';

-- 执行ddl/databricks_ods_ddl.sql中的所有CREATE TABLE语句
```

### 2. 上传配置文件

```sql
CREATE VOLUME IF NOT EXISTS powerlink.default.env;
```

上传config.json到 `/Volumes/powerlink/default/env/config.json`

### 3. 上传脚本到Workspace

将 `etl_script/` 上传到 `/Workspace/Shared/tyc_new/etl_script/`

### 4. 创建Databricks Jobs

编排方式: init Task → 11个fetch Task并行 → 各parse Task依赖对应fetch

每个Task对应一个Notebook（2个cell: Cell1=notebook_init, Cell2=对应step脚本）

### 5. 历史数据迁移（如需）

执行 `ddl/migrate_api_call_record.sql` 将共享表数据分配到各接口独立表

### 6. 验证

```python
# 复制 diagnostic_test.py 到cell运行
```

## 关键设计说明

1. **并发安全**: 各接口独立调用记录表，11个Task各写不同表，零冲突
2. **动态Schema**: 从Delta表读取schema创建DataFrame，避免schema冲突
3. **datetime对象**: 所有TIMESTAMP字段使用Python datetime对象
4. **dt分区**: yyyyMMdd格式，取昨天日期 `(datetime.now() - timedelta(days=1))`
5. **两阶段分离**: API重试与Delta写入分离
6. **查询即计费**: 1168/1114/851失败不重试，直接返回
7. **翻页接口**: 1114/851循环翻页合并存储，翻页失败保留已获取数据
8. **邓白氏uscc**: 从ods_tyc_819_df读取social_credit_code，首次运行无uscc时仅用entityName

详细方法论见Obsidian文档 `claude变更记录/天眼查数据接入方法论.md`
