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
CUSTOMER_TABLE = f'{CATALOG}.pw_ods.ods_credit_api_input_company_df'
HK_TW_WHITELIST_TABLE = f'{CATALOG}.{SCHEMA}.ods_init_white_company_list_nd'

MAX_RETRY = 3


# ========== 表名映射 ==========

def get_api_record_table(interface_key: str) -> str:
    """接口号 → 各接口独立的API调用记录表 (并发安全，各Task写不同表)"""
    record_map = {
        '819':   'ods_api_call_record_819_df',
        '851':   'ods_api_call_record_851_df',
        '1058':  'ods_api_call_record_1058_df',
        '822':   'ods_api_call_record_822_df',
        '854':   'ods_api_call_record_854_df',
        '1168':  'ods_api_call_record_1168_df',
        '1149':  'ods_api_call_record_1149_df',
        '967':   'ods_api_call_record_967_df',
        '1114':  'ods_api_call_record_1114_df',
        '1041':  'ods_api_call_record_1041_df',
        '973':   'ods_api_call_record_973_df',
        'P51060': 'ods_api_call_record_P51060_df',
    }
    table_name = record_map.get(interface_key)
    if not table_name:
        raise ValueError(f"未知接口号: {interface_key}")
    return f'{CATALOG}.{SCHEMA}.{table_name}'


def get_target_table_name(interface_key: str) -> str:
    """接口号 → 目标Delta表全名 (格式: ods_{接口类型}_{接口id}_df)"""
    name_map = {
        '819':   'ods_tyc_819_df',
        '851':   'ods_tyc_851_df',
        '1058':  'ods_tyc_1058_df',
        '822':   'ods_tyc_822_df',
        '854':   'ods_tyc_854_df',
        '1168':  'ods_tyc_1168_df',
        '1149':  'ods_tyc_1149_df',
        '967':   'ods_tyc_967_df',
        '1114':  'ods_tyc_1114_df',
        '1041':  'ods_tyc_1041_df',
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

def get_hk_tw_whitelist(spark) -> set:
    """
    读取HK/TW白名单(免跑接口的公司集合)
    白名单由 workflow/ods/build_ods_init_white_company_list_nd.sql 每日全量重建
    读取失败时返回空set(跳过过滤,保证可用性)
    """
    try:
        df = spark.sql(f"SELECT company_name FROM {HK_TW_WHITELIST_TABLE}")
        whitelist = set([row.company_name for row in df.collect() if row.company_name])
        print(f"[INFO] 读取HK/TW白名单: {len(whitelist)} 家公司免跑接口")
        return whitelist
    except Exception as e:
        print(f"[WARNING] 读取HK/TW白名单失败({e}), 跳过HK/TW过滤")
        return set()


def get_company_list(spark, specific_company: str = None, prepaid_filter: bool = False, monthly_day: int = 10, customer_dt: str = None, force_all: bool = False, exclude_hk_tw: bool = False, prepaid_run_months: Optional[List[int]] = None) -> List[str]:
    """
    从ads_customer_wide_tab_tmp_df读取公司列表
    customer_dt: 指定客户表分区日期，不指定则自动取MAX(dt)
    prepaid_filter=True时:
      月度跑批日期(monthly_day): 处理全部客户(含预付款)
      非月度跑批日期: 仅处理非预付款客户(is_prepaid='否')
    prepaid_filter=False时: 不过滤，处理全部客户
    force_all=True时: 跳过预付款过滤，强制处理全部客户(初始化模式用)
    exclude_hk_tw=True时: 读取HK/TW白名单,排除其中的公司(免跑接口)
    prepaid_run_months: 预付款半年跑批月份(如[6,12])。配了则:
      - 跑批日在配置月份(6/12月): 处理全部客户(含预付款) — 半年跑一次预付款
      - 跑批日不在配置月份(其他月): 仅处理非预付款 — 预付款跳过,省配额
      未配(None): 预付款每个跑批日都跑(原行为)
    """
    if specific_company:
        # 指定单公司时仍需检查HK/TW白名单(调试时也可能要跳过HK/TW)
        if exclude_hk_tw and specific_company in get_hk_tw_whitelist(spark):
            print(f"[SKIP] 指定公司 {specific_company} 在HK/TW白名单中,跳过")
            return []
        return [specific_company]

    if customer_dt:
        query_dt = customer_dt
    else:
        query_dt = spark.sql(
            f"SELECT MAX(dt) FROM {CUSTOMER_TABLE}"
        ).collect()[0][0]

    if not query_dt:
        print("[WARNING] 客户表无数据，任务结束")
        return []

    base_sql = (
        f"SELECT DISTINCT name FROM {CUSTOMER_TABLE} "
        f"WHERE dt = '{query_dt}' AND name IS NOT NULL AND name != ''"
    )

    if force_all:
        print(f"[INFO] 初始化模式: 跳过预付款过滤, 处理全部客户 (dt={query_dt})")
    elif prepaid_filter:
        today = datetime.now()
        is_batch_day = (today.day == monthly_day)
        # 半年跑批: 预付款仅在配置月份的跑批日跑,其他月份跑批日也只跑账期
        if is_batch_day and prepaid_run_months is not None and today.month not in prepaid_run_months:
            is_batch_day = False
            print(f"[INFO] 半年跑批配置{prepaid_run_months}: 当前{today.month}月非预付款跑批月, 预付款跳过, 仅处理账期客户")

        if not is_batch_day:
            base_sql += " AND is_prepaid = '否'"
            print(f"[INFO] 预付款过滤: 仅处理非预付款客户(is_prepaid='否')")
        else:
            print(f"[INFO] 预付款过滤: 今天是月度跑批日({monthly_day}号), 处理全部客户(含预付款)")

    df = spark.sql(base_sql)
    companies = sorted([row.name for row in df.collect()])
    print(f"[INFO] 从客户表获取到 {len(companies)} 家公司 (dt={query_dt})")

    # HK/TW白名单过滤(免跑接口的公司)
    if exclude_hk_tw:
        whitelist = get_hk_tw_whitelist(spark)
        if whitelist:
            before = len(companies)
            companies = [c for c in companies if c not in whitelist]
            excluded = before - len(companies)
            print(f"[INFO] HK/TW过滤: 排除 {excluded} 家HK/TW公司, 剩余 {len(companies)} 家")

    return companies


# ========== 补充跑批(新增预付款客户) ==========

def get_supplementary_prepaid_companies(spark, interface_key: str, monthly_day: int, customer_dt: str = None, exclude_hk_tw: bool = False, prepaid_run_months: Optional[List[int]] = None) -> List[str]:
    """
    获取需要补充处理的预付款客户列表
    条件: is_prepaid='是' 且 最近预付款跑批日至今无成功调用记录(status_code=0)
    补充处理写入月度跑批日分区，下游无需改动
    exclude_hk_tw=True时: 排除HK/TW白名单中的公司(免跑接口)
    prepaid_run_months: 预付款半年跑批月份(如[6,12])。配了则:
      - 非跑批日返回空(Phase2仅跑批日跑catch-up新增预付款,不每天跑)
      - processed_since截止日=最近半年跑批日分区(非当月跑批日),因为预付款上次调用在半年边界
      未配(None): 原行为(每天可跑Phase2,processed_since=当月跑批日)
    """
    # 半年跑批: 非跑批日不跑Phase 2 (仅跑批日catch-up新增预付款,省配额)
    if prepaid_run_months is not None:
        today = datetime.now()
        if today.day != monthly_day:
            print(f"[补充跑批] 半年跑批配置{prepaid_run_months}: 今天非月度跑批日({monthly_day}号), 跳过Phase 2")
            return []

    # 1. 获取客户表分区日期
    if customer_dt:
        query_dt = customer_dt
    else:
        query_dt = spark.sql(f"SELECT MAX(dt) FROM {CUSTOMER_TABLE}").collect()[0][0]

    if not query_dt:
        print("[INFO] 客户表无数据，无补充预付款客户")
        return []

    # 2. 获取预付款客户列表
    prepaid_df = spark.sql(
        f"SELECT DISTINCT name FROM {CUSTOMER_TABLE} "
        f"WHERE dt = '{query_dt}' AND is_prepaid = '是' AND name IS NOT NULL AND name != ''"
    )
    prepaid_list = sorted([row.name for row in prepaid_df.collect()])

    if not prepaid_list:
        print("[INFO] 无预付款客户，无需补充处理")
        return []

    # 3. 计算processed_since截止日(上次预付款跑批日分区)
    from common.config_loader import get_last_monthly_batch_date, get_last_prepaid_batch_date
    if prepaid_run_months is not None:
        # 半年跑批: 预付款上次调用在半年边界(如6月/12月跑批日),processed_since=最近半年跑批日分区
        # 否则会把所有预付款都当成"未处理"(当月跑批日Phase1非半年月没跑预付款)
        last_batch_date = get_last_prepaid_batch_date(monthly_day, prepaid_run_months)
        print(f"[补充跑批] 半年跑批: processed_since截止日={last_batch_date} (最近预付款跑批分区)")
    else:
        last_batch_date = get_last_monthly_batch_date({'schedule': {'monthly_day': monthly_day}})

    # 4. 查询processed_since至今已成功处理的预付款客户
    call_record_table = get_api_record_table(interface_key)
    processed_since = spark.sql(
        f"SELECT DISTINCT input_param FROM {call_record_table} "
        f"WHERE dt >= '{last_batch_date}' AND status_code = 0"
    )
    processed_set = set([row.input_param for row in processed_since.collect()])

    # 5. 补充 = 预付款 - 已处理
    supplementary = [c for c in prepaid_list if c not in processed_set]

    # 6. HK/TW白名单过滤(免跑接口的公司)
    if exclude_hk_tw and supplementary:
        whitelist = get_hk_tw_whitelist(spark)
        if whitelist:
            before = len(supplementary)
            supplementary = [c for c in supplementary if c not in whitelist]
            excluded = before - len(supplementary)
            if excluded > 0:
                print(f"[补充跑批] HK/TW过滤: 排除 {excluded} 家HK/TW公司, 剩余 {len(supplementary)} 家")

    if supplementary:
        print(f"[补充跑批] 检测到 {len(supplementary)} 个新增预付款客户需要补充处理 (dt={last_batch_date})")
        if len(supplementary) <= 10:
            print(f"  补充客户: {supplementary}")
        else:
            print(f"  补充客户(前10): {supplementary[:10]}...")
    else:
        print(f"[补充跑批] 所有预付款客户已在最近跑批日({last_batch_date})处理，无需补充")

    return supplementary


# ========== 幂等检查 ==========

def has_success_today(spark, keyword: str, dt: str, interface_key: str) -> bool:
    """检查当天是否已有成功调用记录（status_code=0），使用各接口独立的调用记录表"""
    table = get_api_record_table(interface_key)
    result = spark.sql(
        f"SELECT COUNT(*) FROM {table} "
        f"WHERE dt = '{dt}' AND input_param = '{keyword}' AND status_code = 0"
    ).collect()[0][0]
    return result > 0


# ========== Delta写操作 ==========

def write_api_records(spark, records: List[Dict], dt: str, interface_key: str):
    """
    将API调用记录写入各接口独立的调用记录表 (并发安全)
    写入前删除同公司同dt的旧记录，确保每天每公司只保留1条最终记录
    从Delta表读取schema，避免硬编码类型不匹配
    自动添加id(MAX(id)+1自增)和create_time
    """
    if not records:
        print("[INFO] 无新记录需要写入")
        return

    table = get_api_record_table(interface_key)

    for rec in records:
        rec['dt'] = dt

    # 删除旧记录: 同公司+同dt（各接口独立表，无需interface_name条件）
    for rec in records:
        input_param = rec.get('input_param', '')
        if input_param:
            spark.sql(
                f"DELETE FROM {table} "
                f"WHERE dt = '{dt}' AND input_param = '{input_param}'"
            )

    table_schema = spark.table(table).schema
    schema_field_names = {f.name for f in table_schema.fields}

    filtered_records = []
    for rec in records:
        filtered_rec = {k: v for k, v in rec.items() if k in schema_field_names}
        filtered_records.append(filtered_rec)

    # 生成唯一id: 当前表最大id + 递增偏移
    max_id_result = spark.sql(f"SELECT COALESCE(MAX(id), -1) FROM {table}").collect()[0][0]
    start_id = max_id_result + 1
    for i, rec in enumerate(filtered_records):
        rec['id'] = start_id + i
        rec['create_time'] = datetime.now()

    # 创建DataFrame时包含id和create_time
    record_keys_with_meta = set(filtered_records[0].keys())
    create_schema = StructType([f for f in table_schema.fields if f.name in record_keys_with_meta])
    df = spark.createDataFrame(filtered_records, schema=create_schema)
    df = df.select(*[f.name for f in table_schema.fields])

    df.write.mode("append").format("delta").saveAsTable(table)
    print(f"[INFO] 写入API调用记录: {len(records)}条 (dt={dt}, id从{start_id}起, 表={table})")


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
    # 需要类型转换的字段: DECIMAL类型需要将Python值转为Decimal对象
    decimal_fields = {f.name for f in target_schema.fields if 'DecimalType' in str(f.dataType)}
    for row in parsed_rows:
        filtered_row = {}
        for k, v in row.items():
            if k in schema_fields:
                if k in decimal_fields and v is not None:
                    from decimal import Decimal
                    try:
                        filtered_row[k] = Decimal(str(v))
                    except:
                        filtered_row[k] = None
                else:
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

def get_today_success_records(spark, dt: str, interface_key: str,
                              company_name: str = None) -> List[Dict]:
    """
    从各接口独立的调用记录表读取当天成功记录并去重
    每个公司取create_time最近的一条，同时带出id和output_result
    """
    table = get_api_record_table(interface_key)

    if company_name:
        sql = f"""
        SELECT id, input_param, output_result, create_time
        FROM {table}
        WHERE dt = '{dt}'
          AND input_param = '{company_name}'
          AND status_code = 0
          AND create_time = (
            SELECT MAX(create_time)
            FROM {table} r2
            WHERE r2.dt = '{dt}'
              AND r2.input_param = '{company_name}'
              AND r2.status_code = 0
          )
        """
    else:
        sql = f"""
        SELECT r.id, r.input_param, r.output_result, r.create_time
        FROM {table} r
        INNER JOIN (
            SELECT input_param, MAX(create_time) as max_ct
            FROM {table}
            WHERE dt = '{dt}'
              AND status_code = 0
            GROUP BY input_param
        ) t ON r.input_param = t.input_param AND r.create_time = t.max_ct
        WHERE r.dt = '{dt}'
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
    print(f"[INFO] 从api_call_record读取到 {len(records)} 条去重后的成功记录 (表={table})")
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