# PowerLink

天眼查/邓白氏三方数据接入项目 — 从API拉取到本地MySQL存储的完整ETL流水线。

## 项目结构

```
PowerLink/
├── ddl/                    # 数据库建表DDL
│   └── api_call_record.sql
├── etl_script/             # ETL脚本（拉取+解析）
│   ├── 819-step1_api_fetch.py
│   └── 819-step2_data_parse.py
├── config/                 # 配置文件
│   └── config.json.example
├── tools/                  # 辅助工具
│   └── gen_data_dict.py
└── README.md
```

## 脚本说明

### [819-step1_api_fetch.py](etl_script/819-step1_api_fetch.py) — API数据拉取

从天眼查819接口拉取企业基本信息，原始响应完整存入 `api_call_record` 表。

**方法论：**

| 机制 | 说明 |
|:-----|:-----|
| 幂等检查 | 查当天 `api_call_record`，已有 `status_code=0` 的成功记录则跳过，不重复调用 |
| 重试机制 | 事不过三：当天失败记录 <3 次则重试，第3次失败记录最后一次错误信息后放弃 |
| 原始保存 | API完整响应存入 `output_result` JSON列，失败时存错误详情JSON（error_type/error_code/traceback） |
| 调用记录 | 每次调用写入 `api_call_record`（interface_name / call_datetime / input_param / status_code / output_result / create_time） |

**执行方式：**

```bash
# 拉取所有公司（从customer_info表读取）
python3 819-step1_api_fetch.py

# 指定单个公司
python3 819-step1_api_fetch.py "广东领益智造股份有限公司"
```

---

### [819-step2_data_parse.py](etl_script/819-step2_data_parse.py) — 数据解析

从 `api_call_record` 读取当天成功记录，按解析规则拆分后写入 `company_819_info` 表。

**方法论：**

**去重逻辑：** 按公司名+日期分组，取 `create_time` 最近的一条成功记录（SQL子查询 `MAX(create_time)`）

**关联追溯：** 带出 `api_call_record.id` 写入 `api_record_id`，可反向查找原始API调用；`data_create_time` 自动记录解析入库时间

**解析规则（最细粒度拆分，客户使用方便优先）：**

| 数据类型 | 处理方式 | 示例 |
|:---------|:---------|:-----|
| `Array + child String` | 逗号分隔字符串 | `emailList` → `"a@b.com,c@d.com"` |
| `Object + 多KV` | 每个KV展开为独立列 | `industryAll` → 8列（4级分类 + 4级代码） |
| `Object + 可能多条` | JSON字符串 + 提取total | `staffList` → `staff_list_json` + `staff_list_total` |
| `Number时间戳` | ≥1e10为毫秒÷1000 → datetime | `173376000000` → `1975-07-01` |
| `简单字段` | 驼峰→下划线 + 必要映射 | `creditCode`→`social_credit_code`，`BRNNumber`→`brn_number` |

**字段映射（显式重命名，避免歧义）：**

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

**执行方式：**

```bash
# 解析当天所有成功记录
python3 819-step2_data_parse.py

# 指定单个公司
python3 819-step2_data_parse.py "广东领益智造股份有限公司"
```

---

### [api_call_record.sql](ddl/api_call_record.sql) — 数据库DDL

在 `powerlink` 库下建表：
- `api_call_record` — 三方接口调用记录表（7个字段）
- `company_819_info` — 企业基本信息表（63个字段，含industryAll展开8列、staffList拆2列、api_record_id关联）
- `customer_info` — 客户公司列表（3个字段）

---

### [config.json.example](config/config.json.example) — 配置模板

使用前复制为 `config.json` 并填入真实值：
- `apis.*.token` — 天眼查API授权token
- `mysql.user / mysql.password` — MySQL账号密码

**注意：** `config.json` 包含敏感信息，已在 `.gitignore` 中排除，不会提交到仓库。

---

### [gen_data_dict.py](tools/gen_data_dict.py) — 数据字典生成工具

读取DDL和解析规则，生成 `数据字典_powerlink.xlsx`：
- 每表一个sheet（表概览 + 字段明细）
- 字段明细12列：序号 / 字段名 / 中文名 / 类型 / 长度 / 主键 / 空值 / 默认值 / 来源 / 原始路径 / 转换规则 / 备注
- 原始字段路径使用完整嵌套名（如 `result.industryAll.categoryCodeFourth`）

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

# 3. 拉取数据
python3 etl_script/819-step1_api_fetch.py

# 4. 解析数据
python3 etl_script/819-step2_data_parse.py

# 5. 生成数据字典
python3 tools/gen_data_dict.py
```

## 已知踩坑

| # | Bug | Fix |
|:--|:----|:----|
| 1 | `Optional` 类型导入缺失 | 加 `from typing import Optional` |
| 2 | `historyNames` 分号字符串非合法JSON | 用 `historyNameList` 替代 |
| 3 | 时间戳阈值 `>1e12` 对12位毫秒值误判 | 改为 `≥1e10` |
| 4 | 遗漏 aboveScale 等4个字段 | ALTER TABLE + DDL + 脚本同步补齐 |
| 5 | `BRNNumber` → `b_r_n_number` ❌ | FIELD_MAPPING 显式映射 → `brn_number` ✅ |