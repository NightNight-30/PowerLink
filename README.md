# PowerLink

天眼查/邓白氏三方数据接入项目 — 从API拉取到本地MySQL存储的完整ETL流水线。

## 项目结构

```
PowerLink/
├── ddl/                    # 数据库建表DDL
│   └── api_call_record.sql
├── etl_script/             # ETL脚本（拉取+解析）
│   ├── 819-step1_api_fetch.py
│   ├── 819-step2_data_parse.py
│   ├── 1058-step1_api_fetch.py
│   ├── 1058-step2_data_parse.py
│   ├── 822-step1_api_fetch.py
│   ├── 822-step2_data_parse.py
│   ├── 854-step1_api_fetch.py
│   ├── 854-step2_data_parse.py
│   ├── 1168-step1_api_fetch.py
│   ├── 1168-step2_data_parse.py
│   ├── 1149-step1_api_fetch.py
│   ├── 1149-step2_data_parse.py
│   ├── 967-step1_api_fetch.py
│   ├── 967-step2_data_parse.py
│   ├── 1114-step1_api_fetch.py
│   └── 1114-step2_data_parse.py
├── config/                 # 配置文件
│   └── config.json.example
├── tools/                  # 辅助工具
│   └── gen_data_dict.py
└── README.md
```

## 接口总览

| 接口 | 名称 | 数据关系 | 嵌套层级 | 入库方式 | 主公司名来源 |
|:-----|:-----|:---------|:---------|:---------|:------------|
| 819 | 企业基本信息（含主要人员） | 1:1 | 1层扁平 | ON DUPLICATE KEY UPDATE | API返回 |
| 1058 | 企业天眼风险 | 1:N | 3层嵌套 | DELETE+INSERT | 搜索入参 |
| 822 | 变更记录 | 1:N | 2层展平 | DELETE+INSERT | 搜索入参 |
| 854 | 上市公司企业简介 | 1:1 | 1层+4个Object | ON DUPLICATE KEY UPDATE | 搜索入参 |
| 1168 | 组织机构 | 1:1 | 2个Array(2级) | ON DUPLICATE KEY UPDATE | 搜索入参 |
| 1149 | 企业规模 | 1:1 | 简单字符串 | ON DUPLICATE KEY UPDATE | 搜索入参 |
| 967 | 主要指标-年度 | 1:N | 数组(每年度一行) | DELETE+INSERT | 搜索入参 |
| 1114 | 法律诉讼 | 1:N | 数组+翻页+casePersons | DELETE+INSERT | 搜索入参 |

## 共享规则

所有接口共用以下核心规则：

### Step1：API数据拉取

| 机制 | 说明 |
|:-----|:-----|
| 幂等检查 | 查当天 `api_call_record`，已有 `status_code=0` 的成功记录则跳过，不重复调用 |
| 重试机制 | 事不过三：每次运行都尝试调用（即使当天已有失败记录也继续），最多3次。重试过程不插入DB，最终失败时先删除旧失败记录再插入1条最新错误信息 |
| interface_name | 使用 `config.json` 中 `apis.{接口号}.name` 的值（如'企业基本信息（含主要人员）'、'变更记录'），而非硬编码接口号 |
| token来源 | 通过 `apis.{接口号}.provider` 关联到 `providers` 统一管理，token续费只改一处 |
| 原始保存 | API完整响应存入 `output_result` JSON列，失败时存错误详情JSON（error_type/error_code/error_msg/traceback） |
| 状态码约定 | `status_code=0` 成功；负数=异常（-1 HTTP异常，-2 其他异常）；正数=API业务错误码 |

执行方式：
```bash
python3 {接口号}-step1_api_fetch.py          # 拉取所有公司
python3 {接口号}-step1_api_fetch.py "公司名"  # 拉取指定公司
```

### Step2：数据解析

| 机制 | 说明 |
|:-----|:-----|
| 去重取最新 | 按 `input_param` 分组，取 `create_time` 最大的一条成功记录 |
| 关联追溯 | 带出 `api_call_record.id` 写入 `api_record_id`；`data_create_time` 自动记录 |
| 主公司名来源 | 来自搜索入参 `input_param`，非API返回的name字段 |
| 空值规范 | 空字符串 `""` 和 `0` → NULL |
| 空结果/非目标公司 | 部分接口（如854）对非上市公司返回error_code=300000，step1记录为失败，step2天然跳过 |

---

## 819接口 — 企业基本信息

### [819-step1_api_fetch.py](etl_script/819-step1_api_fetch.py)

从天眼查819接口拉取企业基本信息，原始响应存入 `api_call_record` 表。

### [819-step2_data_parse.py](etl_script/819-step2_data_parse.py)

1:1关系，解析后写入 `company_819_info` 表（65个字段）。

**解析规则（最细粒度拆分）：**

| 数据类型 | 处理方式 | 示例 |
|:---------|:---------|:-----|
| `Array + child String` | 逗号分隔字符串 | `emailList` → `"a@b.com,c@d.com"` |
| `Object + 多KV` | 每个KV展开为独立列 | `industryAll` → 8列（4级分类 + 4级代码） |
| `Object + 可能多条` | JSON字符串 + 提取total | `staffList` → `staff_list_json` + `staff_list_total` |
| `Number时间戳` | ≥1e10为毫秒÷1000 → datetime | `173376000000` → `1975-07-01` |
| `简单字段` | 驼峰→下划线 + 必要映射 | `creditCode`→`social_credit_code`，`BRNNumber`→`brn_number` |

**显式字段映射：**

| API原始key | DB列名 | 原因 |
|:-----------|:-------|:-----|
| `id` | `company_id` | 避免与表主键冲突 |
| `type` | `legal_person_type` | 避免SQL关键字，1=人 2=公司 |
| `orgNumber` | `org_code` | 标准命名 |
| `creditCode` | `social_credit_code` | 标准命名 |
| `BRNNumber` | `brn_number` | 连续大写缩写保留 |
| `actualCapital` | `paid_capital` | 标准命名 |
| `base` | `province_short` | 避免歧义 |
| `alias` | `company_alias` | 避免歧义 |
| `estiblishTime` | `est_date` | 时间戳→datetime |
| `fromTime` | `from_date` | 时间戳→datetime |
| `toTime` | `to_date` | 时间戳→datetime |
| `approvedTime` | `approval_date` | 时间戳→datetime |
| `updateTimes` | `update_time` | 时间戳→datetime |

---

## 1058接口 — 企业天眼风险

### [1058-step1_api_fetch.py](etl_script/1058-step1_api_fetch.py)

从天眼查1058接口拉取企业天眼风险数据。与819-step1同构。

### [1058-step2_data_parse.py](etl_script/1058-step2_data_parse.py)

1:N关系，3层嵌套展平后写入 `company_1058_risk_info` 表（16个字段）。

**3层展平路径：** `riskList[]` → `list[]` → `list[]`

| API路径 | DB列名 | 说明 |
|:---------|:-------|:-----|
| `input_param` | `main_company_name` | 来自搜索入参 |
| `result.riskLevel` | `risk_level` | 顶层字段 |
| `riskList[].count` | `risk_category_count` | 风险类别条数 |
| `riskList[].name` | `risk_category_name` | 自身/周边/历史/预警 |
| `riskList[].list[].total` | `risk_type_total` | 风险类型条数 |
| `riskList[].list[].tag` | `risk_type_tag` | 警示/高风险/提示 |
| `riskList[].list[].list[].id` | `risk_id` | 避免与表主键冲突 |
| `riskList[].list[].list[].companyId` | `company_id` | 可空 |
| `riskList[].list[].list[].companyName` | `company_name` | 可空 |

---

## 822接口 — 变更记录

### [822-step1_api_fetch.py](etl_script/822-step1_api_fetch.py)

从天眼查822接口拉取企业变更记录数据。与819/1058-step1同构。

### [822-step2_data_parse.py](etl_script/822-step2_data_parse.py)

1:N关系，2层展平后写入 `company_822_change_info` 表（10个字段）。

**2层展平路径：** `result.total` + `result.items[]`

| API路径 | DB列名 | 说明 |
|:---------|:-------|:-----|
| `input_param` | `company_name` | 来自搜索入参，非API返回 |
| `result.total` | `total` | 变更记录总数（meta字段） |
| `result.items[].changeItem` | `change_item` | 变更项名称 |
| `result.items[].contentBefore` | `content_before` | 变更前内容(TEXT) |
| `result.items[].contentAfter` | `content_after` | 变更后内容(TEXT) |
| `result.items[].changeTime` | `change_time` | 变更时间(VARCHAR) |
| `result.items[].createTime` | `create_time` | 记录创建时间(VARCHAR) |

---

## 854接口 — 上市公司企业简介

### [854-step1_api_fetch.py](etl_script/854-step1_api_fetch.py)

从天眼查854接口拉取上市公司企业简介数据。与819/822/1058-step1同构。

### [854-step2_data_parse.py](etl_script/854-step2_data_parse.py)

1:1关系，4个Object字段展开后写入 `company_854_stock_info` 表（36个字段）。

**特殊逻辑：** 非上市公司查询成功但result为空 → step2跳过（SKIP_EMPTY），不插入空数据。

**4个Object字段展开规则（每个→type/name/id 3列）：**

| API Object | DB前缀 | 展开列 | 说明 |
|:-----------|:-------|:-------|:-----|
| `result.generalManager` | `gm` | `gm_type/gm_name/gm_id` | 总经理 |
| `result.chairman` | `chairman` | `chairman_type/chairman_name/chairman_id` | 董事长 |
| `result.secretaries` | `secretary` | `secretary_type/secretary_name/secretary_id` | 董秘 |
| `result.legal` | `legal_person` | `legal_person_type/legal_person_name/legal_person_id` | 法人 |

- `cType`: 1=公司, 2=人（INT）
- `id`: 人物ID（BIGINT）；`id="0"` → NULL
- `name`: 人物姓名（VARCHAR(120)）

**显式字段映射：**

| API原始key | DB列名 | 原因 |
|:-----------|:-------|:-----|
| `code` | `stock_code` | 避免与其他code混淆 |
| `companyName` | `stock_company_name` | 区别于入参company_name |
| `name` | `listed_name` | 上市公司简称，区别于搜索入参 |

---

## 1168接口 — 组织机构

### [1168-step1_api_fetch.py](etl_script/1168-step1_api_fetch.py)

从天眼查1168接口拉取企业组织机构类型数据。与819/1058/822/854-step1同构。

### [1168-step2_data_parse.py](etl_script/1168-step2_data_parse.py)

1:1关系，解析后写入 `company_1168_org_type_info` 表（7个字段）。

**解析规则（数组→逗号分隔拆列）：**

| API路径 | DB列名 | 说明 |
|:---------|:-------|:-----|
| `input_param` | `company_name` | 来自搜索入参 |
| `result.orgTypes[].level1` | `org_type_level1` | 一级机构类型（逗号分隔） |
| `result.orgTypes[].level2` | `org_type_level2` | 二级机构类型（逗号分隔） |
| `result.economyTypes[].level1` | `economy_type_level1` | 一级经济类型（逗号分隔） |
| `result.economyTypes[].level2` | `economy_type_level2` | 二级经济类型（逗号分隔） |

---

## 1149接口 — 企业规模

### [1149-step1_api_fetch.py](etl_script/1149-step1_api_fetch.py)

从天眼查1149接口拉取企业规模数据。与其他step1同构。

### [1149-step2_data_parse.py](etl_script/1149-step2_data_parse.py)

1:1关系，解析后写入 `company_1149_scale_info` 表（5个字段）。

**解析规则：** `result` 直接为字符串（如"大型"），映射为 `company_scale` 列。

---

## 967接口 — 主要指标-年度

### [967-step1_api_fetch.py](etl_script/967-step1_api_fetch.py)

从天眼查967接口拉取上市公司主要指标数据。与其他step1同构。

### [967-step2_data_parse.py](etl_script/967-step2_data_parse.py)

1:N关系，解析后写入 `company_967_main_index_info` 表（38个字段）。

**解析规则：**

- `result` 为数组，每个年度对象 → 一行记录
- ~28个DECIMAL(24,4)字段 + `showYear`(VARCHAR)
- 非上市公司返回error_code=300000，step1记录失败，step2天然跳过
- DECIMAL字段中0是有效值（如营收为0），不转NULL

---

## 1114接口 — 法律诉讼

### [1114-step1_api_fetch.py](etl_script/1114-step1_api_fetch.py)

从天眼查1114接口拉取企业法律诉讼数据。**支持翻页**（pageNum/pageSize），step1循环拉取所有页并合并存入一条api_call_record。

⚠️ **特别备注**：天眼查最多返回500条记录，合并后存入JSON类型列（可存储约1GB）。保守方案，若未来数据量超限需调整存储策略。

### [1114-step2_data_parse.py](etl_script/1114-step2_data_parse.py)

1:N关系，解析后写入 `company_1114_lawsuit_info` 表（31个字段）。

**解析规则：**

| 数据类型 | 处理方式 | 示例 |
|:---------|:---------|:-----|
| 诉讼基本信息 | 14个字段展开 | `docType`→`doc_type`, `caseNo`→`case_no`等 |
| 涉案方 | 取前2人，各展开6列 | casePersons[0]→`role1/gid1/emotion1/sptname1/name1/type1` |
| 毫秒时间戳 | ≥1e10÷1000→datetime | `submitTime`: 1628784000000 → 2021-08-12 |

**显式字段映射：**

| API原始key | DB列名 | 原因 |
|:-----------|:-------|:-----|
| `id` | `lawsuit_id` | 避免与表主键冲突 |
| `casePersons[0].result` | `case_result` | 避免与API顶层result混淆 |

---

## [api_call_record.sql](ddl/api_call_record.sql) — 数据库DDL

在 `powerlink` 库下建表：
- `api_call_record` — 三方接口调用记录表（7个字段，所有接口共用）
- `company_819_info` — 企业基本信息表（65个字段）
- `customer_info` — 客户公司列表（3个字段）
- `company_1058_risk_info` — 企业天眼风险表（16个字段）
- `company_822_change_info` — 变更记录表（10个字段）
- `company_854_stock_info` — 上市公司企业简介表（36个字段）
- `company_1168_org_type_info` — 组织机构类型表（7个字段）
- `company_1149_scale_info` — 企业规模表（5个字段）
- `company_967_main_index_info` — 主要指标-年度表（38个字段）
- `company_1114_lawsuit_info` — 法律诉讼表（31个字段）

---

## [config.json.example](config/config.json.example) — 配置模板

使用前复制为 `config.json` 并填入真实值：
- `apis.*.token` — 已移除，改用 `providers` 统一管理
- `providers.tyc.token` — 天眼查API授权token
- `providers.dnb.token` — 邓白氏API授权token
- `mysql.user / mysql.password` — MySQL账号密码

**注意：** `config.json` 包含敏感信息，已在 `.gitignore` 中排除，不会提交到仓库。

---

## [gen_data_dict.py](tools/gen_data_dict.py) — 数据字典生成工具

读取DDL和解析规则，生成 `数据字典_powerlink.xlsx`：
- 每表一个sheet（表概览 + 字段明细）
- 字段明细12列：序号 / 字段名 / 中文名 / 类型 / 长度 / 主键 / 空值 / 默认值 / 来源 / 原始路径 / 转换规则 / 备注

---

## 运行环境

| 依赖 | 说明 |
|:-----|:-----|
| Python 3.10+ | DS容器或本机 |
| pymysql | MySQL连接 |
| requests | HTTP请求 |
| openpyxl | 数据字典Excel生成 |
| MySQL 8.x | powerlink库 |
| DolphinScheduler 3.2.0 | 定时调度（可选） |

## 使用流程

```bash
# 1. 填写配置
cp config/config.json.example config/config.json
# 编辑config.json填入真实token和密码

# 2. 执行DDL建表
mysql -u root -p powerlink < ddl/api_call_record.sql

# 3. 拉取+解析（819接口）
python3 etl_script/819-step1_api_fetch.py
python3 etl_script/819-step2_data_parse.py

# 4. 拉取+解析（1058接口）
python3 etl_script/1058-step1_api_fetch.py
python3 etl_script/1058-step2_data_parse.py

# 5. 拉取+解析（822接口）
python3 etl_script/822-step1_api_fetch.py
python3 etl_script/822-step2_data_parse.py

# 6. 拉取+解析（854接口）
python3 etl_script/854-step1_api_fetch.py
python3 etl_script/854-step2_data_parse.py

# 7. 拉取+解析（1168接口）
python3 etl_script/1168-step1_api_fetch.py
python3 etl_script/1168-step2_data_parse.py

# 8. 拉取+解析（1149接口）
python3 etl_script/1149-step1_api_fetch.py
python3 etl_script/1149-step2_data_parse.py

# 9. 拉取+解析（967接口）
python3 etl_script/967-step1_api_fetch.py
python3 etl_script/967-step2_data_parse.py

# 10. 拉取+解析（1114接口）
python3 etl_script/1114-step1_api_fetch.py
python3 etl_script/1114-step2_data_parse.py

# 11. 生成数据字典
python3 tools/gen_data_dict.py
```

## 已知踩坑

| # | Bug | Fix |
|:--|:----|:----|
| 1 | `Optional` 类型导入缺失 | 加 `from typing import Optional` |
| 2 | `historyNames` 分号字符串非合法JSON | 用 `historyNameList` 替代 |
| 3 | 时间戳阈值 `>1e12` 对12位毫秒值误判 | 改为 `≥1e10` |
| 4 | 遗漏 aboveScale 等4个字段 | ALTER TABLE + DDL + 脚本同步补齐 |
| 5 | `BRNNumber` → `b_r_n_number` | FIELD_MAPPING 显式映射 → `brn_number` |
| 6 | `id` 字段与表主键冲突 | 显式映射 id→company_id/risk_id 等 |
| 7 | 失败3次产生3条重复记录，且失败后SKIP阻止重试 | 每次都尝试（不跳过失败），最终失败时delete旧记录+insert最新1条 |
| 8 | 854-step2假设result为空需跳过 | 非上市公司返回error_code=300000，移除`if not result`死代码 |
| 9 | config.json每个API重复携带相同token | 改为顶层`providers`统一管理，各API用`provider`字段关联 |