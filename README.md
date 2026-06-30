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
│   │   ├── daily_call_analysis_alert_notebook_v2.py  # 调用分析预警邮件(V2正式版)
│   │   ├── daily_data_export_notebook.py            # 每日解析数据导出+邮件附件发送
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
| 1168 | 组织机构 | monthly | 是 | 无 | 1:1(2个Array) | 是 |
| 1149 | 企业规模 | daily | 否 | 无 | 1:1(简单字符串) | 是 |
| 1114 | 法律诉讼 | daily | 是 | 有(250页/5000条) | 1:N+翻页 | 是 |
| 1041 | 司法解析 | daily | 是 | 有(250页/5000条) | 1:N+翻页 | 是 |
| 851 | 欠税公告 | daily | 是 | 有(250页/5000条) | 1:N+翻页 | 是 |
| 854 | 上市公司企业简介 | daily | 否 | 无 | 1:1+4个Object | 是 |
| 967 | 主要指标-年度 | monthly | 否 | 无 | 1:N(数组) | 是 |
| 973 | 现金流量表 | monthly | 否 | 无 | 1:N(数组) | 是 |

### 邓白氏(1个接口)

| 接口 | 名称 | 频次 | 数据关系 | prepaid_filter | 认证方式 |
|:-----|:-----|:-----|:---------|:--------------|:---------|
| P51060 | 付款指数(PAYDEX®) | monthly | 1:1 | 是 | SHA256签名 |

## 核心架构

### 初始化Task并行

Databricks Jobs中两个初始化Task并行执行，所有12组step1依赖两者都完成：

```
init (环境初始化)          ods_init.ipynb (数据初始化)
  sys.path+导入公共模块      ├─ 上游内部数据离线同步(t-1)
                            ├─ 接口调用客户初始化(客户表)
                            └─ build_hk_tw_whitelist(HK/TW白名单重建)
         ↓                              ↓
         └────── 两个init并行完成 ──────→ 12组step1(并行)
```

- **init**: 设置sys.path、导入公共模块(环境准备)
- **ods_init.ipynb**: 同步上游数据 + 构建客户表 + 重建HK/TW白名单(数据准备)
- **白名单SQL读昨天819数据**，不依赖今天819_step1，可安全并行

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
| Phase1(月度跑批日) | 全部客户，写t-1分区 | 全部客户，写t-1分区(=月度跑批日-1) |
| Phase1(非月度跑批日) | 非预付款客户，写t-1分区 | 不执行 |
| Phase2(补充) | 新增预付款客户，写月度跑批日-1分区 | 新增预付款客户，写月度跑批日-1分区 |
| INIT_MODE(初始化) | 全部客户(含预付款)，写t-1分区 | 全部客户(含预付款)，写月度跑批日-1分区 |

- **检测**: call record表查最近月度跑批日至今无成功记录的预付款客户
- **写入**: 补充数据写入最近月度跑批日跑批时写入的分区(月度跑批日-1)，与正常月度跑批写入分区一致
- **防重复**: 月度分区有记录后，下次检测查到 → 跳过
- **分区统一**: `get_last_monthly_batch_date()` 返回 t-1 分区(monthly_day-1)，保证三种写入路径分区一致

### INIT_MODE 初始化模式

每个 step1 脚本顶部 `INIT_MODE = False` → 改为 `True` 即可全量初始化所有客户：

- **跳过频次检查**: `should_run_today(force_run=True)`，monthly接口非跑批日也能跑
- **跳过预付款过滤**: `get_company_list(force_all=True)`，处理全部客户(含预付款)
- **monthly接口dt覆盖**: `if INIT_MODE: dt = get_last_monthly_batch_date(CONFIG)`，写月度跑批日-1分区
- **保留幂等检查**: 当天已有成功记录的客户仍跳过(避免重复扣费)
- **跳过 Phase 2**: Phase 1 已全量处理，补充跑批无意义

适用场景: 测试跑部分客户后全量重跑 / 修正历史数据 / 首次初始化

### HK/TW白名单(免跑接口)

香港/台湾客户(`province_short` 为 `hk`/`tw`)的天眼查/邓白氏接口无意义，识别后加入白名单，所有接口跳过：

- **白名单表**: `powerlink.pw_ods.ods_init_white_company_list_nd` (全量快照，无dt分区)
- **每日全量重建**: `workflow/ods/build_ods_init_white_company_list_nd.sql`，读昨天819数据，按company_name取最新，过滤hk/tw
- **Jobs编排**: build_hk_tw_whitelist作为ods_init.ipynb的section 5，和init环境脚本并行跑，所有step1之前完成
- **配置**: 每个接口 `exclude_hk_tw: true`(含819)，`is_hk_tw_filter_enabled()` 读取
- **过滤实现**: `get_company_list(exclude_hk_tw=True)` 读取白名单并排除

**自动识别流程**(白名单从空开始，无需手动切config)：
- 首次跑批: 白名单空→全部调用→819发现HK/TW→次日build SQL填充白名单
- 次日起: 已知HK/TW跳过，新客户不在白名单→819调用识别→次日入表
- HK/TW属性基本不变，所有接口含819都设true，新客户通过"不在白名单"自动被819识别

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

`daily_call_analysis_alert_notebook_v2.py` — 每日调用分析+异常预警邮件(V2正式版，V1已弃用)：
- 统计各接口调用频次和计费消耗，**含账期/预付款客户拆分**(LEFT JOIN客户表按is_prepaid分组)
- 检测异常调用模式(高频失败、余额不足等)
- 通过 Microsoft Graph API 发送 tesa 品牌样式预警邮件
- 收件人为业务相关人员
- 邮件标题格式: `✅【接口调用数据】业务日期 YYYYMMDD` / 异常时 `⚠️【接口调用数据】业务日期 YYYYMMDD`

**数据取值范围**(2026-06-22 调整)：查 `dt IN (T-1, 最近月度跑批日)` 两分区 + `create_time ∈ [T, T+1)` 当天创建时间过滤。覆盖账期客户(每天跑)+新增预付款客户(非月度跑批日补充跑,调用记录写 T-1 分区)两种场景,排除月度跑批日跑的历史数据。客户表 JOIN 统一用 `dt = T-1`(最新分区)。

## 数据附件邮件系统(新增)

`daily_data_export_notebook.py` — 每日解析数据导出+邮件附件发送：

| 阶段 | 动作 |
|:-----|:-----|
| Phase 1 | 读取每接口 step2 解析 Delta 表 → CSV(UTF-8 BOM) → 打包 ZIP → 滚动保留 N 天 |
| Phase 2 | Graph API 复用 alert 段凭据发送邮件，ZIP 普通附件 + tesa logo 内联 |

- 邮件统计表 9 列: 接口ID/接口名称/频次/调用成功/解析行数/数据关系(1:1/1:N)/账期客户/预付款客户/未分类
- Databricks Workspace 写入限制绕过: `toPandas()` + Python 文件 API(driver 可写 Workspace)
- TIMESTAMP 列超范围坑: cast 成 string 再 toPandas
- 1058 特殊处理: 优先 `main_company_name`(搜索入参=客户公司)，`company_name` 是风险相关公司不可关联

**数据取值范围**(2026-06-22 调整)：与预警脚本同策略,但过滤字段是解析表的 `data_create_time`(不是调用记录表的 `create_time`)。查 `dt IN (T-1, 最近月度跑批日)` + `data_create_time ∈ [T, T+1)`。新增预付款客户补充跑时 step2 把 `dt` 改成 `last_batch_date`,但 `data_create_time=T` 仍能被捕获。ZIP 只包含有数据的表(`cnt==0` 跳过导出)。

详细设计文档存放在 Obsidian `claude变更记录/project/powerlink/其他/数据附件邮件发送设计.md`

## 配置说明

`config.json` 包含以下核心配置(详见 `tyc_new/config/config.json.example`)：

| 配置块 | 说明 |
|:-------|:-----|
| `providers` | 天眼查token / 邓白氏client_key+client_secret |
| `schedule.monthly_day` | 月度跑批日(默认5号) |
| `apis.*` | 各接口URL/频次/计费/预付款过滤/正常错误码 |
| `alert` | 预警邮件+数据附件邮件(Graph API认证+`cloud`字段+收件人+logo路径) |
| `data_export` | 每日数据导出配置(输出目录`base_dir`+保留天数`retention_days`) |
| `error_code_desc` | 天眼查/邓白氏错误码对照表 |

**Graph API 云环境**(`alert.cloud` 字段)：
- `global`(默认): `login.microsoftonline.com` + `graph.microsoft.com`
- `china`(世纪互联): `login.partner.microsoftonline.cn` + `microsoftgraph.chinacloudapi.cn`

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
| 13 | Spark executor 不能写 `/Workspace/Shared`(Mkdirs failed) + DBFS root 禁用 | `toPandas()` 收集到driver + Python文件API直接写Workspace |
| 14 | 819表TIMESTAMP列存在超范围值，`toPandas()` 报 out of bounds | 所有TIMESTAMP列 cast 成 string 再 toPandas |
| 15 | 预警邮件彩带"很粗" — 裸CSS `td{padding:8px}` 污染了布局表，2px彩带被撑成~18px | CSS选择器加 `.data` 前缀，数据表加 `class="data"` |
| 16 | 1058表 `company_name` 是API返回的风险相关公司，不是客户公司 | LEFT JOIN 客户表用 `main_company_name`(搜索入参=客户公司) |
| 17 | 预警/附件脚本只查 T-1 单分区,漏掉新增预付款客户的补充跑批数据(step2 写入月度跑批日分区) | 改为双分区查询 `dt IN (T-1, 月度跑批日)` + 当天创建时间过滤(`create_time`/`data_create_time ∈ [T, T+1)`),排除月度跑批日跑的历史数据 |
| 18 | `get_last_monthly_batch_date` 返回月度跑批日当天(20260605),与跑批实际写入的t-1分区(20260604)不一致 | 修正为返回 `monthly_day - 1`(t-1分区),保证正常跑批/Phase2补充/INIT_MODE初始化三者写入分区一致 |
| 19 | 测试跑部分预付款后需全量重跑,但预付款客户在非月度跑批日会被过滤掉 | 新增 `INIT_MODE=True` 开关:跳过频次检查+预付款过滤,monthly接口写月度跑批日-1分区,跳过Phase2,保留幂等 |
| 20 | HK/TW客户(province_short为hk/tw)调用tyc/dnb接口无意义浪费配额 | 新增 `ods_init_white_company_list_nd`白名单表 + `exclude_hk_tw`配置(全接口含819默认true) + ods_init.ipynb每日全量重建(和init并行) + 新客户通过不在白名单自动被819识别 |
