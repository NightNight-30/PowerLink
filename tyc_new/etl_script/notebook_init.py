#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Databricks Notebook初始化脚本

在Notebook第一个cell中运行，完成：
  1. 将common模块路径加入sys.path
  2. 确认SparkSession可用，设置动态分区覆盖
  3. 导入公共模块

使用方式（在Notebook第一个cell中）：
  %run /path/to/notebook_init
  或直接复制以下代码到cell中执行
"""

import sys
import os
from datetime import datetime, timedelta

# ========== 1. 设置common模块路径 ==========

# 根据实际部署位置修改此路径
# 方式1: 脚本上传到Workspace
COMMON_PATH = "/Workspace/Shared/powerlink_warehouse/tyc_new/etl_script"

# 方式2: 脚本通过Repos(git)同步
# COMMON_PATH = "/Repos/<user>/PowerLink/tyc_new/etl_script"

# 方式3: 脚本上传到Volume
# COMMON_PATH = "/Volumes/powerlink/default/tyc_new/etl_script"

if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)

print(f"[INIT] common模块路径: {COMMON_PATH}")

# ========== 2. 确认SparkSession ==========

try:
    spark
    print("[INIT] SparkSession已就绪(Databricks Notebook自带)")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    print("[INIT] 动态分区覆盖模式已启用")
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    print("[INIT] SparkSession已创建")

# ========== 3. 导入公共模块 ==========

from common.config_loader import load_config, get_interface_name, get_api_config, get_provider_config, should_run_today, is_prepaid_filter_enabled, get_monthly_day, is_charge_per_query, get_normal_error_codes, get_error_code_desc, get_alert_config
from common.spark_utils import (
    get_spark, get_company_list, has_success_today,
    write_api_records, get_today_success_records, write_target_data,
    get_target_table_name, get_api_record_table, get_uscc,
    camel_to_snake, timestamp_to_datetime, array_to_string, null_if_empty,
    CATALOG, SCHEMA, MAX_RETRY
)

print("[INIT] 公共模块导入完成(含频次/预付款过滤/预警配置)")
print(f"[INIT] dt={(datetime.now() - timedelta(days=1)).strftime('%Y%m%d')}")

# ========== 4. 环境信息 ==========

print(f"运行环境:")
print(f"  Spark版本: {spark.version}")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  API记录表: 各接口独立表(并发安全), 通过get_api_record_table(interface_key)获取")
