# PowerLink

天眼查/邓白氏三方数据接入项目 — Databricks (PySpark + Delta + Unity Catalog) 全流程ETL流水线。

> 旧版MySQL脚本仍保留在 `etl_script/` 目录下作为参考，当前活跃版本为 `tyc_new/` (Databricks Notebook版)。

## 项目结构

```
PowerLink/
├── tyc_new/                          # ★ 当前活跃版本(Databricks)
│   ├── config/
│   │   ├── config.json.example       # 配置模板(频次/预付款/预警邮件)
│   │   └── tesa_logo.png             # 预警邮件品牌logo
│   ├── ddl/
│   │   ├── databricks_ods_ddl.sql    # Delta ODS表DDL
│   │   └ migrate_api_call_record.sql # 调用记录表DDL
│   ├── etl_script/
│   │   ├── common/
│   │   │   ├── config_loader.py      # 配置加载+频次/预付款/月度跑批日判断
│   │   │   └── spark_utils.py        # Spark/Delta通用工具+补充跑批检测
│   │   ├── notebook_init.py          # Notebook初始化cell
│   │   ├── diagnostic_test.py        # 环境诊断
│   │   ├── daily_call_analysis_alert_notebook.py  # 调用分析预警邮件
│   │   ├── {接口号}-step1_api_fetch_notebook.py   # 12个接口的API拉取
│   │   └── {接口号}-step2_data_parse_notebook.py  # 12个接口的数据解析
│   └── tools/
│       ├── verify_schema.py          # 表结构验证
│       ├── verify_data.py            # 数据质量验证
│       └── verify_idempotency.py     # 幂等性验证
│
├── etl_script/                       # 旧版MySQL脚本(参考)
├── ddl/                              # 旧版MySQL DDL(参考)
├── config/                           # 旧版配置模板(参考)
└── tools/                            # 旧版数据字典工具(参考)
```

## 接口总览

### 天眼查(12个接口)

| 接口 | 名称 | 频次 | 查询计费 | 翻页 | 数据关系 | prepaid_filter |
|:-----|:-----|:-----|:---------|:-----|:---------|:--------------|
| 819 | 企业基本信息（含主要人员） | daily | 否 | 无 | 1:1 | 是 |
| 1058 | 企业天眼风险 | daily | 否 | 无 | 1:N(3层嵌套) | 是 |
| 822 | 变更记录 | daily | 否 | 无 | 1:N(2层展平) | 是 |
| 1168 | 组织机构 | daily | 是 | 无 | 1:1(2个Array) | 是 |
| 1149 | 企业规模 | daily | 否 | 无 | 1:1(简单字符串) | 是 |
| 1114 | 法律诉讼 | daily | 是 | 有(250页/5000条) | 1:N+翻页 | 是 |
| 1041 | 司法解析 | daily | 是 | 有(250页/5000条) | 1:N+翻页 | 是 |
| 851 | 欠税公告 | monthly | 是 | 有(250页/5000条) | 1:N+翻页 | 是 |
| 854 | 上市公司企业简介 | monthly | 否 | 无 | 1:1+4个Object | 是 |
| 967 | 主要指标-年度 | monthly | 否 | 无 | 1:N(数组) | 是 |
| 973 | 现金流量表 | monthly | 否 | 无 | 1:N(数组) | 是 |

### 邓白氏(1个接口)

| 接口 | 名称 | 频次 | 数据关系 | prepaid_filter | 认证方式 |
|:-----|:-----|:-----|:---------|:--------------|:---------|
| P51060 | 付款指数(PAYDEX®) | monthly | 1:1 | 是 | SHA256签名 |

## 核心架构

### 两步流水线

每个接口遵循统一的 **Step1(API拉取) → Step2(数据解析)** 流程：

- **Step1**: 调用三方API，原始响应存入 `ods_api_call_record_df` (dt分区)
- **Step2**: 读取成功记录，解析后写入对应的ODS目标表 (dt分区)

### 两阶段分离(Step1)

Step1内部采用两阶段分离，节省API调用次数：
- **Phase1 - API调用**: 事不过三重试(HTTP异常+业务错误重试)；查询即计费接口失败不重试
- **Phase2 - Delta写入**: 写入失败直接报错终止，不浪费API配额重试写入

### 三层调用规则

每个接口的调用由三层判断链控制：

| 层级 | 判断 | 说明 |
|:-----|:-----|:-----|
| 1. 频次 | `should_run_today()` | daily=每天跑, monthly=只在月度跑批日(5号)跑 |
| 2. 预付款过滤 | `is_prepaid_filter_enabled()` | 非月度跑批日只处理非预付款客户(is_prepaid='否') |
| 3. 幂等 | `has_success_today()` | 当天dt分区已有status_code=0则跳过 |

### Phase 2 补充跑批(新增预付款客户)

月度跑批日之后新增的预付款客户需要补充处理：

| 场景 | daily接口 | monthly接口 |
|:-----|:---------|:-----------|
| Phase1(月度跑批日) | 全部客户，写t-1分区 | 全部客户，写月度分区 |
| Phase1(非月度跑批日) | 非预付款客户，写t-1分区 | 不执行 |
| Phase2(补充) | 新增预付款客户，写月度分区 | 新增预付款客户，写月度分区 |

- **检测**: call record表查最近月度跑批日至今无成功记录的预付款客户
- **写入**: 补充数据写入最近月度跑批日分区，下游无需改动
- **防重复**: 月度分区有记录后，下次检测查到 → 跳过

## 表名格式

所有ODS表: `powerlink.pw_ods.ods_{接口类型}_{接口id}_df`，PARTITIONED BY (dt STRING)

| 接口 | 表名 | 数据关系 |
|:-----|:-----|:---------|
| 819 | ods_tyc_819_df | 1:1 |
| 1058 | ods_tyc_1058_df | 1:N |
| 822 | ods_tyc_822_df | 1:N |
| 851 | ods_tyc_851_df | 1:N |
| 854 | ods_tyc_854_df | 1:1 |
| 1168 | ods_tyc_1168_df | 1:1 |
| 1149 | ods_tyc_1149_df | 1:1 |
| 967 | ods_tyc_967_df | 1:N |
| 1114 | ods_tyc_1114_df | 1:N |
| 1041 | ods_tyc_1041_df | 1:N |
| 973 | ods_tyc_973_df | 1:N |
| P51060 | ods_dnb_P51060_df | 1:1 |
| call record | ods_api_call_record_df | 调用记录 |

## 预警邮件系统

`daily_call_analysis_alert_notebook.py` — 每日调用分析+异常预警邮件：
- 统计各接口调用频次和计费消耗
- 检测异常调用模式(高频失败、余额不足等)
- 通过 Microsoft Graph API 发送 tesa 品牌样式预警邮件
- 收件人为业务相关人员

## 配置说明

`config.json` 包含以下核心配置(详见 `tyc_new/config/config.json.example`)：

| 配置块 | 说明 |
|:-------|:-----|
| `providers` | 天眼查token / 邓白氏client_key+client_secret |
| `schedule.monthly_day` | 月度跑批日(默认5号) |
| `apis.*` | 各接口URL/频次/计费/预付款过滤/正常错误码 |
| `alert` | 预警邮件(Graph API认证+收件人+logo路径) |
| `error_code_desc` | 天眼查/邓白氏错误码对照表 |

**配置路径**: `/Workspace/Shared/powerlink_warehouse/tyc_new/config/config.json`

**注意**: config.json包含敏感信息，已在.gitignore中排除。

## 运行方式

所有脚本为Databricks Notebook版，复制到Notebook cell中运行。

每个接口需要3个cell：

```python
# Cell 1 - 初始化(所有接口共用)
# 复制 notebook_init.py 内容，修改COMMON_PATH为实际部署路径

# Cell 2 - Step1 API拉取
# 复制 {接口号}-step1_api_fetch_notebook.py

# Cell 3 - Step2 数据解析
# 复制 {接口号}-step2_data_parse_notebook.py
```

**自定义分区**: Step1脚本中可设置 `CUSTOMER_DT = '20260614'` 指定客户表分区日期，默认自动取MAX(dt)。

## 部署步骤

1. 在Databricks SQL Editor中执行DDL建表
2. 将config.json上传到 `/Workspace/Shared/powerlink_warehouse/tyc_new/config/`
3. 将etl_script/目录上传到Databricks Workspace或通过Repos同步
4. 每个接口创建Notebook，粘贴3个cell内容运行
5. 运行 `diagnostic_test.py` 验证环境

## 与旧版(MySQL)的主要差异

| 维度 | 旧版(tyc) | 新版(tyc_new) |
|:-----|:----------|:-------------|
| 数据平台 | MySQL 8.0 | Databricks (Unity Catalog) |
| 数据格式 | InnoDB | Delta Lake |
| 表命名 | company_xxx | ods_tyc_xxx_df / ods_dnb_xxx_df |
| 分区 | 无 | PARTITIONED BY (dt STRING) |
| 运行方式 | spark-submit | Notebook cell |
| 写入方式 | INSERT / ON DUPLICATE KEY UPDATE | 动态分区覆盖(overwrite) |
| Schema | 硬编码 | 从Delta表动态读取 |
| 重试逻辑 | 事不过三(API+写入一起) | 两阶段分离 + 查询计费接口失败不重试 |
| 预付款过滤 | 无 | 三层调用规则 + Phase 2补充跑批 |
| 预警邮件 | 无 | Graph API + tesa品牌样式 |

## 已知踩坑

| # | Bug | Fix |
|:--|:----|:----|
| 1 | `Optional`类型导入缺失 | 加 `from typing import Optional` |
| 2 | `historyNames`分号字符串非合法JSON | 用 `historyNameList` 替代 |
| 3 | 时间戳阈值`>1e12`对12位毫秒值误判 | 改为 `≥1e10` |
| 4 | 遗漏aboveScale等4个字段 | ALTER TABLE + DDL + 脚本同步补齐 |
| 5 | `BRNNumber`→`b_r_n_number` | FIELD_MAPPING显式映射→`brn_number` |
| 6 | `id`字段与表主键冲突 | 显式映射id→company_id/risk_id等 |
| 7 | 失败3次产生3条重复记录 | delete旧记录+insert最新1条 |
| 8 | 854-step2假设result为空需跳过 | 非上市公司error_code=300000，移除死代码 |
| 9 | config.json每个API重复携带相同token | 改为顶层`providers`统一管理 |
| 10 | `dict.get('orgTypes', [])`对JSON null返回None | 改为 `.get('orgTypes') or []` |
| 11 | step1改了dt但客户表仍从MAX(dt)读取 | 新增`CUSTOMER_DT`参数支持指定客户表分区 |
| 12 | config.json.example中7个URL与PDF文档不符 | 逐个校验PDF文档，修正所有URL |
