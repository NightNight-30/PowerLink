#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Databricks环境诊断测试 - 复制到Notebook单个cell中运行

测试内容:
  1. common模块能否导入
  2. 配置文件能否从Volume读取
  3. SparkSession能否获取
  4. 客户表能否读取
  5. api_call_record表schema检查
  6. 目标表是否存在(新表名格式)
"""

# ========== 根据你的实际部署位置修改此路径 ==========
COMMON_PATH = "/Workspace/Shared/tyc_new/etl_script"

import sys
if COMMON_PATH not in sys.path:
    sys.path.insert(0, COMMON_PATH)

print("=" * 60)
print("【诊断测试】Databricks环境检查")
print("=" * 60)

# ========== 测试1: 导入common模块 ==========
print("\n[测试1] 导入common模块...")
try:
    from common.config_loader import load_config, get_api_config, get_interface_name
    from common.spark_utils import (
        get_spark, get_company_list, has_success_today,
        write_api_records, get_today_success_records, write_target_data,
        get_target_table_name, get_uscc,
        camel_to_snake, timestamp_to_datetime, array_to_string, null_if_empty,
        CATALOG, SCHEMA, API_RECORD_TABLE, MAX_RETRY
    )
    print("[PASS] common模块导入成功")
except Exception as e:
    print(f"[FAIL] common模块导入失败: {e}")
    print("  -> 检查COMMON_PATH是否正确，确认文件已上传到该路径")

# ========== 测试2: 加载配置文件 ==========
print("\n[测试2] 加载配置文件...")
try:
    config = load_config()
    print(f"[PASS] 配置文件加载成功")
    print(f"  providers: {list(config.get('providers', {}).keys())}")
    print(f"  apis: {list(config.get('apis', {}).keys())}")
except Exception as e:
    print(f"[FAIL] 配置文件加载失败: {e}")
    print("  -> 确认config.json已上传到 /Volumes/powerlink/default/env/config.json")

# ========== 测试3: SparkSession ==========
print("\n[测试3] SparkSession...")
try:
    spark = get_spark()
    print(f"[PASS] SparkSession获取成功, 版本: {spark.version}")
    print(f"  Catalog: {CATALOG}")
    print(f"  Schema: {SCHEMA}")
    print(f"  API_RECORD_TABLE: {API_RECORD_TABLE}")
except Exception as e:
    print(f"[FAIL] SparkSession获取失败: {e}")

# ========== 测试4: 客户表读取 ==========
print("\n[测试4] 客户表读取...")
try:
    companies = get_company_list(spark)
    print(f"[PASS] 客户表读取成功, 共 {len(companies)} 家公司")
    if companies:
        print(f"  前3家: {companies[:3]}")
except Exception as e:
    print(f"[FAIL] 客户表读取失败: {e}")
    print("  -> 确认 ads_customer_wide_tab_tmp_df 表有数据")

# ========== 测试5: api_call_record表schema检查 ==========
print("\n[测试5] ods_api_call_record_df schema检查...")
try:
    table_df = spark.table(API_RECORD_TABLE)
    table_schema = table_df.schema
    print(f"[PASS] 表读取成功, 列数: {len(table_schema.fields)}")

    for field in table_schema.fields:
        print(f"  {field.name}: {field.dataType}")
        if field.name == 'call_datetime':
            if 'TimestampType' in str(field.dataType):
                print(f"    [OK] call_datetime类型为TIMESTAMP")
            else:
                print(f"    [WARN] call_datetime类型非TIMESTAMP: {field.dataType}")

except Exception as e:
    print(f"[FAIL] 表读取失败: {e}")
    print("  -> 确认 ods_api_call_record_df 表已创建")

# ========== 测试6: 目标表检查(新表名格式) ==========
print("\n[测试6] 目标表检查(新表名格式 ods_tyc/ods_dnb_XXX_df)...")
interface_keys = ['819', '1058', '822', '854', '1168', '1149', '967', '1114', '973', 'P51060']
for key in interface_keys:
    table_name = get_target_table_name(key)
    try:
        df = spark.table(table_name)
        print(f"  [OK] {table_name} - {len(df.schema.fields)}列")
    except Exception as e:
        print(f"  [MISS] {table_name} - 表不存在,需先执行DDL建表")

print("\n" + "=" * 60)
print("诊断完成！")
print("=" * 60)