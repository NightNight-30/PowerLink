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

CATALOG = 'powerlink_prod'
SCHEMA = 'pw_ods'
CUSTOMER_TABLE = f'{CATALOG}.pw_ods.ods_credit_api_input_company_df'
HK_TW_WHITELIST_TABLE = f'{CATALOG}.{SCHEMA}.ods_credit_api_white_company_list_nd'
BRANCH_COMPANY_TABLE = f'{CATALOG}.{SCHEMA}.ods_credit_api_input_branch_company_df'
NEW_CUSTOMERS_TABLE = f'{CATALOG}.{SCHEMA}.ods_credit_api_input_new_customers_df'

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
        '1001':  'ods_api_call_record_1001_df',
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
        '1001':  'ods_tyc_1001_df',
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
        # 统一 Spark SQL 时区为上海（current_date / current_timestamp 受此控制）
        # Databricks 集群默认 UTC，3:30 北京时间跑批时 UTC 是前一天 19:30
        spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")
        return spark
    except Exception as e:
        print(f"[FATAL] 获取SparkSession失败: {e}")
        raise


# ========== 客户公司列表 ==========

def get_hk_tw_whitelist(spark) -> set:
    """
    读取HK/TW白名单(免跑接口的公司集合)
    白名单由 ods_init_1.ipynb section 4 每日全量重建(源SQL: workflow/ods/build_ods_credit_api_white_company_list_nd.sql)
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


def get_company_list(spark, specific_company: str = None, frequency: str = 'daily', monthly_day: int = 5, customer_dt: str = None, force_all: bool = False, exclude_hk_tw: bool = False, prepaid_run_months: Optional[List[int]] = None, prepaid_filter: bool = True) -> List[str]:
    """
    从 ods_credit_api_input_company_df 读取公司列表
    表字段: name, is_prepaid(是/否), is_new_customer(是/否), in_monthly_batch(是/否)

    过滤逻辑(Phase 2已删除,所有客户由前置SQL+本函数过滤决定):
      daily接口 (frequency='daily'):
        - 月度跑批日: 全部客户
        - 非月度跑批日: 账期 + 新增预付款 (is_prepaid='否' OR (is_new_customer='是' AND is_prepaid='是'))
      monthly接口 (frequency='monthly'):
        - 月度跑批日: 全部客户 (prepaid_run_months非配置月份时账期+新增预付款, 老预付款半年才跑)
        - 非月度跑批日: 新增客户且非"曾经存在→消失→又回来" (is_new_customer='是' AND in_monthly_batch='否')

    force_all=True (INIT_MODE): 全部客户
    prepaid_filter=False: 全部客户(向后兼容,1001不调用本函数)
    """
    if specific_company:
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
        print(f"[INFO] 初始化模式: 处理全部客户 (dt={query_dt})")
    elif not prepaid_filter:
        print(f"[INFO] prepaid_filter=False: 处理全部客户 (dt={query_dt})")
    else:
        today = datetime.now()
        is_batch_day = (today.day == monthly_day)

        if frequency == 'daily':
            if is_batch_day:
                print(f"[INFO] daily月度跑批日({monthly_day}号): 处理全部客户 (dt={query_dt})")
            else:
                base_sql += " AND (is_prepaid = '否' OR (is_new_customer = '是' AND is_prepaid = '是'))"
                print(f"[INFO] daily非月度跑批日: 账期 + 新增预付款客户 (dt={query_dt})")
        else:
            if is_batch_day:
                if prepaid_run_months is not None and today.month not in prepaid_run_months:
                    base_sql += " AND (is_prepaid = '否' OR (is_new_customer = '是' AND is_prepaid = '是'))"
                    print(f"[INFO] monthly月度跑批日 + 半年跑批配置{prepaid_run_months}: 当前{today.month}月非预付款跑批月, 处理账期+新增预付款客户 (dt={query_dt})")
                else:
                    print(f"[INFO] monthly月度跑批日({monthly_day}号): 处理全部客户 (dt={query_dt})")
            else:
                base_sql += " AND is_new_customer = '是' AND in_monthly_batch = '否'"
                print(f"[INFO] monthly非月度跑批日: 新增客户(剔除月度跑批日已出现) (dt={query_dt})")

    df = spark.sql(base_sql)
    companies = sorted([row.name for row in df.collect()])
    print(f"[INFO] 从客户表获取到 {len(companies)} 家公司 (dt={query_dt})")

    if exclude_hk_tw:
        whitelist = get_hk_tw_whitelist(spark)
        if whitelist:
            before = len(companies)
            companies = [c for c in companies if c not in whitelist]
            excluded = before - len(companies)
            print(f"[INFO] HK/TW过滤: 排除 {excluded} 家HK/TW公司, 剩余 {len(companies)} 家")

    return companies


def get_branch_company_list(spark, customer_dt: str = None, force_all: bool = False, exclude_hk_tw: bool = False) -> List[str]:
    """
    从分公司入参公司表读取分公司列表(1001接口专用)
    入参表每日由819-step2后SQL全量重建,已按company_org_type含'分'过滤+预付款客户JOIN,只含客户表里的分公司
    customer_dt: 指定入参表分区日期,不指定则自动取MAX(dt)
    force_all: INIT_MODE用,处理全部分公司(本就处理全部,参数为接口一致)
    exclude_hk_tw: 排除HK/TW白名单中的公司(免跑接口)
    1001 prepaid_filter=False,入参表已过滤好,无需再判断预付款
    """
    if customer_dt:
        query_dt = customer_dt
    else:
        query_dt = spark.sql(
            f"SELECT MAX(dt) FROM {BRANCH_COMPANY_TABLE}"
        ).collect()[0][0]

    if not query_dt:
        print("[WARNING] 分公司入参表无数据,任务结束")
        return []

    df = spark.sql(
        f"SELECT DISTINCT company_name FROM {BRANCH_COMPANY_TABLE} "
        f"WHERE dt = '{query_dt}' AND company_name IS NOT NULL AND company_name != ''"
    )
    companies = sorted([row.company_name for row in df.collect()])
    print(f"[INFO] 从分公司入参表获取到 {len(companies)} 家分公司 (dt={query_dt})")

    # HK/TW白名单过滤(免跑接口的公司)
    if exclude_hk_tw:
        whitelist = get_hk_tw_whitelist(spark)
        if whitelist:
            before = len(companies)
            companies = [c for c in companies if c not in whitelist]
            excluded = before - len(companies)
            print(f"[INFO] HK/TW过滤: 排除 {excluded} 家HK/TW公司, 剩余 {len(companies)} 家")

    return companies


def get_new_customers_list(spark, customer_dt: str = None, exclude_hk_tw: bool = False) -> List[str]:
    """
    从新增客户入参表读取新增客户列表(819+1001定向跑批专用,TARGETED_MODE=True时调用)
    表每日由 build_ods_credit_api_input_new_customers_df.sql 对比今天vs昨天分区生成
    customer_dt: 指定入参表分区日期,不指定则自动取MAX(dt)
    exclude_hk_tw: 排除HK/TW白名单中的公司(免跑接口)
    定向模式不区分预付款,所有新增客户都跑(含预付款)
    """
    if customer_dt:
        query_dt = customer_dt
    else:
        query_dt = spark.sql(
            f"SELECT MAX(dt) FROM {NEW_CUSTOMERS_TABLE}"
        ).collect()[0][0]

    if not query_dt:
        print("[WARNING] 新增客户入参表无数据,任务结束")
        return []

    df = spark.sql(
        f"SELECT DISTINCT company_name FROM {NEW_CUSTOMERS_TABLE} "
        f"WHERE dt = '{query_dt}' AND company_name IS NOT NULL AND company_name != ''"
    )
    companies = sorted([row.company_name for row in df.collect()])
    print(f"[INFO] 从新增客户入参表获取到 {len(companies)} 家新增客户 (dt={query_dt})")

    # HK/TW白名单过滤(免跑接口的公司)
    if exclude_hk_tw:
        whitelist = get_hk_tw_whitelist(spark)
        if whitelist:
            before = len(companies)
            companies = [c for c in companies if c not in whitelist]
            excluded = before - len(companies)
            print(f"[INFO] HK/TW过滤: 排除 {excluded} 家HK/TW公司, 剩余 {len(companies)} 家")

    return companies

def get_supplementary_prepaid_companies(spark, interface_key: str, monthly_day: int, customer_dt: str = None, exclude_hk_tw: bool = False, prepaid_run_months: Optional[List[int]] = None) -> List[str]:
    """
    获取需要补充处理的预付款客户列表
    条件: is_prepaid='是' 且 数据不在跑批日分区(跑批日分区无成功调用记录)
    补充处理写入跑批日分区，下游无需改动
    exclude_hk_tw=True时: 排除HK/TW白名单中的公司(免跑接口)
    prepaid_run_months: 预付款半年跑批月份(如[1,7])。配了则:
      - processed_since截止日=最近半年跑批日分区(非当月跑批日),因为预付款上次调用在半年边界
    未配(None): processed_since=当月月度跑批日分区
    processed_since查dt=last_batch_date(而非dt>=),找"数据不在跑批日分区的预付款客户":
      - daily: 新增预付款在t-1,不在last_batch_date→supp包含→Phase2从t-1补到last_batch_date
      - monthly: 新增预付款在last_batch_date(=step1写入分区)→supp不含→Phase2跳过(Phase1已覆盖)
      - P51060半年跑批: 新增预付款在last_monthly_batch_date,不在last_prepaid_batch_date→supp包含→Phase2从月度分区补到半年分区
    """
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
        # 半年跑批: 预付款上次调用在半年边界(如1月/7月跑批日),processed_since=最近半年跑批日分区
        # 否则会把所有预付款都当成"未处理"(当月跑批日Phase1非半年月没跑预付款)
        last_batch_date = get_last_prepaid_batch_date(monthly_day, prepaid_run_months)
        print(f"[补充跑批] 半年跑批: processed_since截止日={last_batch_date} (最近预付款跑批分区)")
    else:
        last_batch_date = get_last_monthly_batch_date({'schedule': {'monthly_day': monthly_day}})

    # 4. 查询跑批日分区已成功处理的预付款客户(查目标解析表, 而非调用记录表)
    # 原因: Phase 2 合并写入是写入目标解析表, 判据也必须查同一张表, 否则已合并过的公司
    # 每天都会被列入"新增"重新处理, 配合 write_supplementary_data 的覆盖写入会丢数据
    target_table = get_target_table_name(interface_key)
    schema_fields = [f.name for f in spark.table(target_table).schema]
    company_col = 'main_company_name' if 'main_company_name' in schema_fields else 'company_name'
    processed_since = spark.sql(
        f"SELECT DISTINCT {company_col} FROM {target_table} "
        f"WHERE dt = '{last_batch_date}' AND {company_col} IS NOT NULL"
    )
    processed_set = set([row[company_col] for row in processed_since.collect()])

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


def write_supplementary_data(spark, table_name: str, dt: str, last_batch_date: str,
                             supp_companies: List[str]):
    """
    Phase 2优化: 从dt分区目标表读取supp_companies已解析数据,
    INSERT 到last_batch_date分区(纯追加, 不覆盖现有数据, 不会丢数据)

    替代旧逻辑: 读last_batch_date全量现有数据→排除supp_companies→合并→overwrite整分区
    旧逻辑问题: existing_df 的 NOT IN 会把"上次合并过但本次step1没调用"的公司排除掉,
    overwrite 后这些公司永久丢失. 改用 INSERT 纯追加, 即使判据误判也只产生重复行(可清理), 不丢数据

    前置条件: get_supplementary_prepaid_companies 已改用解析表判据(而非调用记录表),
    保证 supp_companies 只含真正未在 last_batch_date 解析表出现过的公司, 不会重复 INSERT

    参数:
      - dt: 取数分区(P1已解析的数据在这里)
      - last_batch_date: 写入分区(跑批日分区)
      - supp_companies: 需要补充的预付款客户列表
    """
    if not supp_companies:
        print("[补充跑批] 无新增预付款客户需要补充解析")
        return

    # 适配1058的main_company_name字段(1058同时有main_company_name和company_name,前者是主公司)
    schema_fields = [f.name for f in spark.table(table_name).schema]
    if 'main_company_name' in schema_fields:
        company_col = 'main_company_name'
    else:
        company_col = 'company_name'

    company_list_sql = ",".join([f"'{c}'" for c in supp_companies])

    # 1. 从dt分区读取supp_companies已解析数据(无需重新解析JSON)
    new_data_df = spark.sql(
        f"SELECT * FROM {table_name} "
        f"WHERE dt = '{dt}' AND {company_col} IN ({company_list_sql}) "
        f"AND {company_col} IS NOT NULL"
    )

    new_count = new_data_df.count()
    if new_count == 0:
        print(f"[补充跑批] dt={dt}分区无supp客户已解析数据,跳过")
        return

    # 2. 改 dt 为 last_batch_date (保留原 data_create_time, 避免污染今天时间窗口统计)
    new_data_df = new_data_df.withColumn('dt', lit(last_batch_date))

    # 3. INSERT 追加到 last_batch_date 分区(纯追加, 不覆盖现有数据)
    new_data_df.write.mode("append").format("delta").saveAsTable(table_name)
    print(f"[补充跑批] INSERT 完成: {len(supp_companies)}个预付款客户({new_count}条) 从dt={dt}追加到dt={last_batch_date}")


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

    注意: 远期哨兵时间戳(常表示"未发生"/"无限期"/"长期有效", 如 UTC 9999-12-31
    或更晚)统一返回 None。原因:
      1. Python datetime.max = 9999-12-31 23:59:59, 上海+8h 后 UTC 9999-12-31
         16:00:00 之后的值在本地时区会溢出 year 10000;
      2. 即便不溢出, year 9999 的值传到 Spark → Arrow convert_timestamp
         (`value.astimezone(timezone.utc)`) 时, 若 Spark 按 session timeZone
         (Asia/Shanghai) 解析 naive datetime 再转 UTC, 仍可能在边界值触发
         "year 10000 is out of range";
      3. 业务语义上 "9999-12-31" = NULL, 返回 None 更准确。
    """
    if ts is None or ts == '' or ts == 0:
        return None
    try:
        ts_num = float(ts)
        if ts_num >= 1e10:
            ts_seconds = int(ts_num // 1000)
        else:
            ts_seconds = int(ts_num)
        from datetime import timezone
        dt_utc = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
        # 远期哨兵值: UTC 年份 >= 9999 直接返回 None, 避免 Spark Arrow 转换溢出
        if dt_utc.year >= 9999:
            return None
        # 上海时区转换可能溢出(理论上 dt_utc.year < 9999 不会, 但兜底)
        try:
            dt_local = dt_utc.astimezone()
        except (OverflowError, ValueError):
            return None
        if dt_local.year > 9999:
            return None
        return dt_local.replace(tzinfo=None)
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