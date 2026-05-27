#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark/Delta通用工具模块 - Databricks版本

提供：
  1. SparkSession获取（兼容Databricks Notebook和standalone脚本）
  2. 客户公司列表读取（从ads_customer_wide_tab_tmp_df）
  3. 幂等检查（ods_api_call_record_df）
  4. Delta表读写操作
  5. PySpark Schema定义（api_call_record + 各目标表）
  6. 通用数据转换工具（驼峰转下划线、时间戳转换、空值处理等）
"""

import re
import json
from datetime import datetime
from typing import Dict, List, Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DecimalType, TimestampType
)
from pyspark.sql.functions import monotonically_increasing_id, current_timestamp, lit


# ========== 常量 ==========

CATALOG = 'powerlink'
SCHEMA = 'pw_ods'
CUSTOMER_TABLE = f'{CATALOG}.pw_ads.ads_customer_wide_tab_tmp_df'
API_RECORD_TABLE = f'{CATALOG}.{SCHEMA}.ods_api_call_record_df'

MAX_RETRY = 3


# ========== Schema定义 ==========

# write_api_records从Delta表动态读取schema，不再需要硬编码schema
# get_api_call_record_schema()已移除（硬编码类型与DDL不一致会导致schema冲突）


def get_target_table_name(interface_key: str) -> str:
    """接口号 → 目标Delta表全名 (格式: ods_{接口类型}_{接口id}_df)"""
    name_map = {
        '819':   'ods_tyc_819_df',
        '1058':  'ods_tyc_1058_df',
        '822':   'ods_tyc_822_df',
        '854':   'ods_tyc_854_df',
        '1168':  'ods_tyc_1168_df',
        '1149':  'ods_tyc_1149_df',
        '967':   'ods_tyc_967_df',
        '1114':  'ods_tyc_1114_df',
        '973':   'ods_tyc_973_df',
        'P51060': 'ods_dnb_P51060_df',
    }
    table_name = name_map.get(interface_key)
    if not table_name:
        raise ValueError(f"未知接口号: {interface_key}")
    return f'{CATALOG}.{SCHEMA}.{table_name}'


# ========== SparkSession ==========

def get_spark() -> SparkSession:
    """获取SparkSession（Databricks Notebook中已有spark，standalone脚本需创建）"""
    try:
        spark = SparkSession.builder.getOrCreate()
        # 启用动态分区覆盖（仅影响当前Session的写操作配置）
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        return spark
    except Exception as e:
        print(f"[FATAL] 获取SparkSession失败: {e}")
        raise


# ========== 客户公司列表 ==========

def get_company_list(spark, specific_company: str = None) -> List[str]:
    """
    从ads_customer_wide_tab_tmp_df读取公司列表
    优先读取最新dt分区，取distinct name
    """
    if specific_company:
        return [specific_company]

    # 获取最新dt分区
    latest_dt = spark.sql(
        f"SELECT MAX(dt) FROM {CUSTOMER_TABLE}"
    ).collect()[0][0]

    if not latest_dt:
        print("[WARNING] 客户表无数据，任务结束")
        return []

    df = spark.sql(
        f"SELECT DISTINCT name FROM {CUSTOMER_TABLE} WHERE dt = '{latest_dt}' AND name IS NOT NULL AND name != ''"
    )
    companies = sorted([row.name for row in df.collect()])
    print(f"[INFO] 从客户表获取到 {len(companies)} 家公司 (dt={latest_dt})")
    return companies


# ========== 幂等检查 ==========

def has_success_today(spark, interface_name: str, keyword: str, dt: str) -> bool:
    """检查当天是否已有成功调用记录（status_code=0）"""
    result = spark.sql(
        f"SELECT COUNT(*) FROM {API_RECORD_TABLE} "
        f"WHERE dt = '{dt}' AND interface_name = '{interface_name}' "
        f"AND input_param = '{keyword}' AND status_code = 0"
    ).collect()[0][0]
    return result > 0


# ========== Delta写操作 ==========

def write_api_records(spark, records: List[Dict], dt: str):
    """
    将API调用记录写入ods_api_call_record_df (append模式)
    从Delta表读取schema，避免硬编码类型不匹配
    自动添加id(monotonically_increasing_id)和create_time(current_timestamp)
    """
    if not records:
        print("[INFO] 无新记录需要写入")
        return

    for rec in records:
        rec['dt'] = dt

    table_schema = spark.table(API_RECORD_TABLE).schema
    schema_field_names = {f.name for f in table_schema.fields}

    filtered_records = []
    for rec in records:
        filtered_rec = {k: v for k, v in rec.items() if k in schema_field_names}
        filtered_records.append(filtered_rec)

    record_keys = set(filtered_records[0].keys())
    create_schema = StructType([f for f in table_schema.fields if f.name in record_keys])
    df = spark.createDataFrame(filtered_records, schema=create_schema)

    df = df.withColumn("id", monotonically_increasing_id())
    df = df.withColumn("create_time", current_timestamp())
    df = df.select(*[f.name for f in table_schema.fields])

    df.write.mode("append").format("delta").saveAsTable(API_RECORD_TABLE)
    print(f"[INFO] 写入API调用记录: {len(records)}条 (dt={dt})")


def write_target_data(spark, parsed_rows: List[Dict], table_name: str, dt: str,
                      is_one_to_one: bool = True, company_name: str = None):
    """
    将解析后的数据写入目标Delta表

    对于全量运行(无company_name):
      - 动态分区覆盖: 只替换dt分区，其他分区保留
      - 1:1和1:N都用覆盖写入(每天全量刷新)

    对于单公司运行(有company_name):
      - 读取现有dt分区数据
      - 过滤掉指定公司的旧数据
      - 合入新数据后覆盖写回
    """
    if not parsed_rows:
        # 即使无新数据，也需删除指定公司的旧数据（1:N场景）
        if company_name:
            existing_df = spark.sql(
                f"SELECT * FROM {table_name} WHERE dt = '{dt}' "
                f"AND company_name != '{company_name}'"
            )
            # 需要检查是否有主公司名字段的变体
            # 对于1058, 字段是main_company_name
            try:
                existing_df2 = spark.sql(
                    f"SELECT * FROM {table_name} WHERE dt = '{dt}' "
                    f"AND main_company_name != '{company_name}'"
                )
                if existing_df2.count() > 0:
                    existing_df = existing_df2
            except:
                pass
            existing_df.write.mode("overwrite").format("delta").saveAsTable(table_name)
            print(f"[INFO] 清除旧数据: {company_name} (dt={dt})")
        else:
            print("[INFO] 无解析数据需要写入")
        return

    for row in parsed_rows:
        row['dt'] = dt
        row['data_create_time'] = datetime.now()

    # 从现有Delta表获取schema用于DataFrame创建
    target_schema = spark.table(table_name).schema

    # 创建DataFrame - 只保留schema中定义的列
    filtered_rows = []
    schema_fields = [f.name for f in target_schema.fields]
    for row in parsed_rows:
        filtered_row = {}
        for k, v in row.items():
            if k in schema_fields:
                filtered_row[k] = v
        filtered_rows.append(filtered_row)

    new_df = spark.createDataFrame(filtered_rows, schema=target_schema)

    if company_name:
        # 单公司: 读取现有数据, 过滤旧数据, 合入新数据
        company_name_col = 'company_name'
        # 检查1058用的是main_company_name
        if 'main_company_name' in schema_fields and 'company_name' not in [f for f in schema_fields if f == 'company_name_col']:
            # 这不正确，让我重新考虑
            pass

        # 尝试用main_company_name过滤(1058接口)
        try:
            existing_df = spark.sql(
                f"SELECT * FROM {table_name} WHERE dt = '{dt}' "
                f"AND main_company_name != '{company_name}'"
            )
        except:
            existing_df = spark.sql(
                f"SELECT * FROM {table_name} WHERE dt = '{dt}' "
                f"AND company_name != '{company_name}'"
            )

        combined_df = existing_df.unionByName(new_df)
        combined_df.write.mode("overwrite").format("delta").saveAsTable(table_name)
        print(f"[INFO] 写入解析数据(单公司): {len(parsed_rows)}条 (dt={dt}, company={company_name})")
    else:
        # 全量: 动态分区覆盖(只替换dt分区)
        new_df.write.mode("overwrite").format("delta").saveAsTable(table_name)
        print(f"[INFO] 写入解析数据(全量): {len(parsed_rows)}条 (dt={dt})")


# ========== Step2: 读取成功记录 ==========

def get_today_success_records(spark, interface_name: str, dt: str,
                              company_name: str = None,
                              use_main_company_name: bool = False) -> List[Dict]:
    """
    从ods_api_call_record_df读取当天成功记录并去重
    每个公司取create_time最近的一条，同时带出id和output_result

    use_main_company_name: 1058接口用main_company_name字段名
    """
    if company_name:
        sql = f"""
        SELECT id, input_param, output_result, create_time
        FROM {API_RECORD_TABLE}
        WHERE dt = '{dt}'
          AND interface_name = '{interface_name}'
          AND input_param = '{company_name}'
          AND status_code = 0
          AND create_time = (
            SELECT MAX(create_time)
            FROM {API_RECORD_TABLE} r2
            WHERE r2.dt = '{dt}'
              AND r2.interface_name = '{interface_name}'
              AND r2.input_param = '{company_name}'
              AND r2.status_code = 0
          )
        """
    else:
        sql = f"""
        SELECT r.id, r.input_param, r.output_result, r.create_time
        FROM {API_RECORD_TABLE} r
        INNER JOIN (
            SELECT input_param, MAX(create_time) as max_ct
            FROM {API_RECORD_TABLE}
            WHERE dt = '{dt}'
              AND interface_name = '{interface_name}'
              AND status_code = 0
            GROUP BY input_param
        ) t ON r.input_param = t.input_param AND r.create_time = t.max_ct
        WHERE r.dt = '{dt}'
          AND r.interface_name = '{interface_name}'
          AND r.status_code = 0
        ORDER BY r.input_param
        """

    rows = spark.sql(sql).collect()
    records = []
    for row in rows:
        records.append({
            'id': row.id,
            'input_param': row.input_param,
            'output_result_str': row.output_result,
            'create_time': row.create_time,
        })
    print(f"[INFO] 从api_call_record读取到 {len(records)} 条去重后的成功记录")
    return records


def get_uscc(spark, company_name: str) -> Optional[str]:
    """从819信息表查询公司的统一社会信用代码(邓白氏接口用)"""
    table_819 = get_target_table_name('819')
    result = spark.sql(
        f"SELECT social_credit_code FROM {table_819} "
        f"WHERE company_name = '{company_name}' AND social_credit_code IS NOT NULL "
        f"AND social_credit_code != '' LIMIT 1"
    ).collect()
    if result:
        return result[0].social_credit_code
    return None


# ========== 通用数据转换工具 ==========

def camel_to_snake(name: str) -> str:
    """驼峰 → 下划线命名"""
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    return pattern.sub('_', name).lower()


def timestamp_to_datetime(ts: Any) -> Optional[datetime]:
    """
    BIGINT时间戳 → datetime对象
    >=1e10 → 毫秒级(÷1000)；<1e10 → 秒级(直接转)
    返回datetime对象而非字符串，适配Delta TIMESTAMP列
    """
    if ts is None or ts == '' or ts == 0:
        return None
    try:
        ts_num = float(ts)
        if ts_num >= 1e10:
            ts_seconds = int(ts_num // 1000)
        else:
            ts_seconds = int(ts_num)
        return datetime.fromtimestamp(ts_seconds)
    except (ValueError, TypeError, OSError, OverflowError):
        return None


def array_to_string(arr: List) -> Optional[str]:
    """Array[child String] → 逗号分隔字符串"""
    if not arr:
        return None
    return ','.join(str(item) for item in arr if item)


def null_if_empty(val: Any) -> Any:
    """空字符串/0 → None（DECIMAL字段除外）"""
    if val is None:
        return None
    if isinstance(val, str) and val == '':
        return None
    if val == 0:
        return None
    return val