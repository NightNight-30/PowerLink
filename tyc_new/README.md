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
| 预警邮件 | 无 | ⭐ tesa品牌UI + Graph API发送 |
| 频次控制 | 无 | ⭐ daily/monthly双频次 + 预付款过滤 |
| 计费区分 | 无 | ⭐ 查询即计费/不计费分类 |

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

### 预警邮件系统(tesa品牌UI)

Databricks Jobs新增Task: 所有step1完成后运行 `daily_call_analysis_alert_notebook.py`

**功能**:
- 读取11个接口独立调用记录表，按status_code分组统计
- 区分成功(0)/正常失败(normal_error_codes)/异常失败(其他)
- 实际数据优先: 有调用数据的接口显示真实状态，无数据看配置预测
- 调用次数按计费规则: 查询即计费(1168/1114/851)=总调用, 查询不计费(其余)=成功数
- 生成tesa品牌UI的HTML邮件，通过Microsoft Graph API发送

**tesa品牌设计规范**:
- 配色: 红(#E3000F)/蓝(#009fdf)/煤灰(#373737)/深灰(#5E5E5E)层级
- 页首尾品牌栏: 红66% + 白2%间隙 + 蓝32%，6px高
- Header: Power Link徽章(红蓝白三色) + tesa logo
- 表头煤灰背景白字，标题蓝色+煤灰底线，异常红色高亮

**邮件发送**:
- 方式: Microsoft Graph API (OAuth2 client_credentials)
- 原因: smtp.tesa.com(DNS不可达) + smtp.office365.com(SMTP AUTH禁用535 5.7.139)
- 需配置: Azure AD app注册 + Mail.Send权限 + tenant_id/client_id/client_secret

### 接口频次+预付款过滤

config.json新增双开关控制各接口运行策略:

| 字段 | 说明 | 示例 |
|------|------|------|
| frequency | 调用频次(daily/monthly) | 819=daily, 851=monthly |
| prepaid_filter | 预付款客户过滤 | true=启用, false=不过滤 |
| charge_per_query | 查询即计费 | 1168/1114/851=true, 其余=false |
| normal_error_codes | 正常错误码(不计入预警) | tyc=[300000], dnb=[1,1021,2001] |

过滤逻辑: prepaid_filter=true的接口，月度跑批日处理全部客户(含预付款)，非月度跑批日仅处理非预付款客户

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
│   ├── config.json.example          # 配置模板(含频次/计费/预警)
│   └── tesa_logo.png               # tesa品牌logo资源
├── ddl/
│   ├── databricks_ods_ddl.sql       # 所有DDL(11个独立调用记录表+11个目标表)
│   └── migrate_api_call_record.sql  # 历史数据迁移DML(共享表→独立表)
├── etl_script/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── config_loader.py         # ⭐配置加载(频次/计费/预警/错误码描述)
│   │   └── spark_utils.py           # ⭐核心(独立表映射+动态schema+去重写入+id自增)
│   ├── notebook_init.py             # Notebook初始化cell
│   ├── daily_call_analysis_alert_notebook.py  # ⭐预警邮件(tesa品牌UI+Graph API)
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

上传以下文件到 `/Volumes/powerlink/default/env/`:
- `config.json` — 主配置(含频次/计费/预警/错误码描述)
- `tesa_logo.png` — tesa品牌logo(预警邮件嵌入)

config.json参考 `config/config.json.example`，需填入:
- providers: tyc token, dnb client_key/client_secret
- alert: Azure AD tenant_id/client_id/client_secret (Graph API发送)

### 3. 上传脚本到Workspace

将 `etl_script/` 上传到 `/Workspace/Shared/tyc_new/etl_script/`

### 4. 创建Databricks Jobs

编排方式: init Task → 11个step1 Task并行 → 各step2 Task依赖对应step1 → 预警Task依赖所有step1完成

每个Task对应一个Notebook（2个cell: Cell1=notebook_init, Cell2=对应step脚本）

预警Task: Cell1=notebook_init, Cell2=daily_call_analysis_alert_notebook.py，依赖所有step1

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
9. **频次+预付款过滤**: daily/monthly双频次 + 预付款客户月度跑批日才处理
10. **预警邮件**: tesa品牌UI(红蓝煤灰层级) + Graph API发送 + 正常/异常错误码区分
11. **调用次数**: 查询即计费=总调用, 查询不计费=成功数

详细方法论见Obsidian文档 `claude变更记录/天眼查数据接入方法论.md`
