# PowerLink - Databricks ODS层三方数据接入(tyc_new)

> 从MySQL+pymysql迁移到Databricks+PySpark+Delta+Unity Catalog的数据接入方案

## 与原版(tyc)的主要差异

| 维度 | 原版(tyc) | 新版(tyc_new) |
|------|-----------|---------------|
| 数据平台 | MySQL 8.0 | Databricks (Unity Catalog) |
| 数据格式 | InnoDB | Delta Lake |
| 表命名 | `company_xxx` / `api_call_record` | `powerlink.pw_ods.ods_xxxx_df` |
| 分区方式 | 无分区 | PARTITIONED BY (dt STRING) |
| 入参客户表 | `customer_info` (MySQL) | `powerlink.pw_ads.ads_customer_wide_tab_tmp_df` |
| 连接方式 | pymysql | PySpark SparkSession |
| 写入方式 | INSERT / ON DUPLICATE KEY UPDATE | 动态分区覆盖(overwrite) |
| 幂等查询 | SQL WHERE DATE() | WHERE dt = '{today}' |
| 数据类型 | VARCHAR/TEXT/JSON/DATETIME | STRING/TIMESTAMP/DECIMAL |

## 目录结构

```
tyc_new/
├── config/
│   └── config.json.example       # 配置模板(不含真实token)
├── ddl/
│   └── databricks_ods_ddl.sql    # 所有ODS表DDL(Delta+分区)
├── etl_script/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── config_loader.py      # 配置加载(支持DBFS)
│   │   └── spark_utils.py        # Spark/Delta通用工具+Schema定义
│   ├── 819-step1_api_fetch.py    # 企业基本信息 - API拉取
│   ├── 819-step2_data_parse.py   # 企业基本信息 - 数据解析
│   ├── 1058-step1_api_fetch.py   # 企业天眼风险 - API拉取
│   ├── 1058-step2_data_parse.py  # 企业天眼风险 - 数据解析
│   ├── 822-step1_api_fetch.py    # 变更记录 - API拉取
│   ├── 822-step2_data_parse.py   # 变更记录 - 数据解析
│   ├── 854-step1_api_fetch.py    # 上市公司简介 - API拉取
│   ├── 854-step2_data_parse.py   # 上市公司简介 - 数据解析
│   ├── 1168-step1_api_fetch.py   # 组织机构 - API拉取
│   ├── 1168-step2_data_parse.py  # 组织机构 - 数据解析
│   ├── 1149-step1_api_fetch.py   # 企业规模 - API拉取
│   ├── 1149-step2_data_parse.py  # 企业规模 - 数据解析
│   ├── 967-step1_api_fetch.py    # 主要指标-年度 - API拉取
│   ├── 967-step2_data_parse.py   # 主要指标-年度 - 数据解析
│   ├── 1114-step1_api_fetch.py   # 法律诉讼(含翻页) - API拉取
│   ├── 1114-step2_data_parse.py  # 法律诉讼 - 数据解析
│   ├── 973-step1_api_fetch.py    # 现金流量表 - API拉取
│   ├── 973-step2_data_parse.py   # 现金流量表 - 数据解析
│   ├── P51060-step1_api_fetch.py # 付款指数(邓白氏) - API拉取
│   └── P51060-step2_data_parse.py# 付款指数(邓白氏) - 数据解析
├── tools/
│   ├── verify_schema.py          # 表结构验证
│   ├── verify_data.py            # 数据质量验证
│   └── verify_idempotency.py     # 幂等性验证
└── README.md
```

## 部署步骤

### 1. 创建Schema和表

在Databricks SQL Editor或Notebook中执行DDL:

```sql
-- 创建schema(如已有则跳过)
CREATE SCHEMA IF NOT EXISTS powerlink.pw_ods
MANAGED LOCATION 'abfss://powerlink@powerlink.dfs.core.chinacloudapi.cn/pw_ods';

-- 执行DDL建表
-- 逐个执行ddl/databricks_ods_ddl.sql中的CREATE TABLE语句
```

### 2. 配置config.json

```bash
# 复制模板并填入真实token
cp config/config.json.example config/config.json
# 编辑config.json，填入TYC_TOKEN和DNB的client_key/client_secret
```

上传config.json到Databricks:
```bash
# 方式1: DBFS
dbfs cp config/config.json dbfs:/opt/tyc_new/config.json

# 方式2: Workspace文件
# 在Databricks Workspace中创建文件并粘贴内容
```

### 3. 上传脚本

```bash
# 上传etl_script目录到DBFS
dbfs cp -r etl_script/ dbfs:/opt/tyc_new/etl_script/
dbfs cp -r tools/ dbfs:/opt/tyc_new/tools/
```

### 4. 运行脚本

```bash
# Step1: API数据拉取(示例: 819接口)
spark-submit dbfs:/opt/tyc_new/etl_script/819-step1_api_fetch.py

# 拉取指定公司
spark-submit dbfs:/opt/tyc_new/etl_script/819-step1_api_fetch.py "广东领益智造股份有限公司"

# Step2: 数据解析
spark-submit dbfs:/opt/tyc_new/etl_script/819-step2_data_parse.py

# 验证
spark-submit dbfs:/opt/tyc_new/tools/verify_schema.py
spark-submit dbfs:/opt/tyc_new/tools/verify_data.py 819
spark-submit dbfs:/opt/tyc_new/tools/verify_idempotency.py 819
```

或在Databricks Notebook中运行:
```python
import sys
sys.path.insert(0, '/opt/tyc_new/etl_script')
# 然后import并调用各脚本的main函数
```

## 解析规则(与原版相同)

详细解析规则见Obsidian方法论文档 `claude变更记录/天眼查数据接入方法论.md`。

核心规则不变：
- 两步流水线: step1拉取 + step2解析
- 幂等检查: 当天dt分区已有status_code=0则跳过
- 事不过三: 最多3次重试，失败只保留一条记录
- 字段映射: camelCase→snake_case + FIELD_MAPPING显式映射
- 空字符串→NULL, DECIMAL字段0为有效值
- 1:1关系: 动态分区覆盖(dt分区全量刷新)
- 1:N关系: 动态分区覆盖(dt分区全量刷新)

## 新版特有注意事项

1. **config路径**: 环境变量`TYC_CONFIG_PATH`可指定配置路径，默认DBFS `/dbfs/opt/tyc_new/config.json`
2. **客户表读取**: 从`ads_customer_wide_tab_tmp_df`最新dt分区获取公司列表
3. **分区写入**: 所有表按dt分区，每天全量刷新(overwrite)整个dt分区
4. **单公司模式**: 指定公司名时，先过滤旧数据再合入新数据，不会丢失其他公司数据
5. **动态分区覆盖**: `spark.sql.sources.partitionOverwriteMode=dynamic`，只替换dt分区
6. **id生成**: `monotonically_increasing_id()`替代MySQL AUTO_INCREMENT
7. **邓白氏uscc查取**: 从`ods_company_819_info_df`读取social_credit_code