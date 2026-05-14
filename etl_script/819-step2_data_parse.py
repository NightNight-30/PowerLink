#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
【Step2】天眼查819接口 - 数据解析（含去重）

功能：
  1. 从api_call_record表读取当天成功的调用记录
  2. 按公司名去重：每组取create_time最近的一条
  3. 解析原始JSON，按以下规则处理字段：
     - Array + child String → 逗号分隔字符串（email_list, history_names）
     - Object + 多KV       → 展开每个KV为独立列（industryAll）
     - Object + 可能多条   → JSON字符串（staffList），额外提取total
     - Number时间戳        → DATETIME（≥1e10为毫秒级÷1000）
  4. 写入company_819_info表（ON DUPLICATE KEY UPDATE）

执行方式：
  python3 819-step2_data_parse.py [公司名]
  - 不指定：解析当天所有成功的调用记录
  - 指定公司名：只解析指定公司
"""

import pymysql
import json
import sys
import re
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional


INTERFACE_NAME = '819'

# 时间戳字段：API原始key → 转换为datetime后存入的DB列名
TIMESTAMP_FIELDS = {
    'estiblishTime': 'est_date',
    'fromTime': 'from_date',
    'toTime': 'to_date',
    'approvedTime': 'approval_date',
    'updateTimes': 'update_time',
    'cancelDate': 'cancel_date',
    'revokeDate': 'revoke_date',
}

# 字段重命名映射：API原始key → DB列名
FIELD_MAPPING = {
    'id': 'company_id',
    'type': 'legal_person_type',
    'orgNumber': 'org_code',
    'creditCode': 'social_credit_code',
    'actualCapital': 'paid_capital',
    'actualCapitalCurrency': 'paid_capital_currency',
    'companyOrgType': 'company_org_type',
    'base': 'province_short',
    'alias': 'company_alias',
    'name': 'company_name_api',
    'property3': 'property3',
    'BRNNumber': 'brn_number',
}


def load_config() -> Dict:
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"[FATAL] 加载配置文件失败: {e}")
        raise


CONFIG = load_config()


def get_db_connection() -> pymysql.Connection:
    mysql_config = CONFIG.get('mysql', {})
    return pymysql.connect(
        host=mysql_config.get('host', 'localhost'),
        port=mysql_config.get('port', 3306),
        user=mysql_config.get('user', 'root'),
        password=mysql_config.get('password', ''),
        database=mysql_config.get('database', 'powerlink'),
        charset=mysql_config.get('charset', 'utf8mb4')
    )


# ========== 数据转换工具 ==========

def camel_to_snake(name: str) -> str:
    """驼峰 → 下划线命名"""
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    return pattern.sub('_', name).lower()


def timestamp_to_datetime(ts: Any) -> Optional[str]:
    """
    BIGINT时间戳 → datetime字符串
    >=1e10 → 毫秒级(÷1000)；<1e10 → 秒级(直接转)
    """
    if ts is None or ts == '' or ts == 0:
        return None
    try:
        ts_num = float(ts)
        if ts_num >= 1e10:
            ts_seconds = int(ts_num // 1000)
        else:
            ts_seconds = int(ts_num)
        dt = datetime.fromtimestamp(ts_seconds)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError, OSError, OverflowError):
        return None


def array_to_string(arr: List) -> str:
    """Array[child String] → 逗号分隔字符串"""
    if not arr:
        return None
    return ','.join(str(item) for item in arr if item)


# ========== 数据库操作 ==========

def get_table_columns(table_name: str) -> List[str]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DESC {table_name}")
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"[WARNING] 获取表字段失败: {e}")
        return []
    finally:
        conn.close()


def get_today_success_records(company_name: str = None) -> List[tuple]:
    """
    从api_call_record读取当天成功记录并去重
    每个公司取create_time最近的一条，同时带出record id
    """
    today = datetime.now().strftime('%Y-%m-%d')
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            SELECT r.id, r.input_param, r.output_result, r.create_time
            FROM api_call_record r
            INNER JOIN (
                SELECT input_param, MAX(create_time) as max_ct
                FROM api_call_record
                WHERE interface_name = %s
                  AND status_code = 0
                  AND DATE(call_datetime) = %s
                GROUP BY input_param
            ) t ON r.input_param = t.input_param AND r.create_time = t.max_ct
            WHERE r.interface_name = %s
              AND r.status_code = 0
            """
            params = [INTERFACE_NAME, today, INTERFACE_NAME]

            if company_name:
                sql += " AND r.input_param = %s"
                params.append(company_name)

            sql += " ORDER BY r.input_param"
            cursor.execute(sql, params)
            results = cursor.fetchall()
            print(f"[INFO] 从api_call_record读取到 {len(results)} 条去重后的成功记录")
            return results
    except Exception as e:
        print(f"[ERROR] 读取调用记录失败: {e}")
        raise
    finally:
        conn.close()


# ========== 解析逻辑 ==========

def parse_and_insert(keyword: str, api_result: Dict, valid_columns: List[str], record_id: int = None):
    """
    解析API返回结果并写入company_819_info
    record_id: api_call_record.id，用于关联原始调用记录
    解析规则：
      1. Array + child String → 逗号分隔字符串
      2. Object + 多KV → 展开为独立列
      3. Object + 可能多条 → JSON字符串 + 提取total
      4. 时间戳字段 → datetime
      5. 简单字段 → 直接存入（驼峰→下划线命名）
    """
    result = api_result.get('result')
    if not result:
        print(f"[WARNING] result字段为空，跳过: {keyword}")
        return

    parsed = {}

    # ========== 0. 关联字段 ==========
    parsed['api_record_id'] = record_id
    parsed['company_name'] = keyword

    # ========== 1. Object + 多KV → 展开每个KV为独立列 ==========
    # industryAll: {category, categoryBig, ...} → industry_all_category, industry_all_category_big, ...
    industry_all = result.pop('industryAll', {})
    if isinstance(industry_all, dict):
        for k, v in industry_all.items():
            col_name = f'industry_all_{camel_to_snake(k)}'
            parsed[col_name] = v

    # ========== 2. Object + 可能多条 → JSON字符串 + 提取total ==========
    # staffList: {total: Number, result: [Array of Objects]} → staff_list_json(JSON) + staff_list_total(int)
    staff_list = result.pop('staffList', {})
    if isinstance(staff_list, dict):
        parsed['staff_list_total'] = staff_list.get('total')
        staff_result = staff_list.get('result')
        if staff_result:
            parsed['staff_list_json'] = json.dumps(staff_result, ensure_ascii=False)

    # ========== 3. Array + child String → 逗号分隔字符串 ==========
    # emailList: [string, string, ...] → email_list(逗号分隔)
    # historyNameList: [string, string, ...] → history_names(逗号分隔)
    # historyNames(分号字符串) 丢弃，用historyNameList替代
    result.pop('historyNames', None)

    email_list = result.pop('emailList', None)
    if isinstance(email_list, list):
        parsed['email_list'] = array_to_string(email_list)

    history_name_list = result.pop('historyNameList', None)
    if isinstance(history_name_list, list):
        parsed['history_names'] = array_to_string(history_name_list)

    # ========== 4. 时间戳字段 → datetime ==========
    for api_key, db_col in TIMESTAMP_FIELDS.items():
        if api_key in result:
            parsed[db_col] = timestamp_to_datetime(result.pop(api_key))

    # ========== 5. 字段重命名 ==========
    for api_key, db_col in FIELD_MAPPING.items():
        if api_key in result:
            parsed[db_col] = result.pop(api_key)

    # ========== 6. 剩余简单字段 → 驼峰转下划线后直接存入 ==========
    for k, v in result.items():
        col_name = camel_to_snake(k)
        # 只保留数据库中存在的列，跳过未知字段
        if col_name in valid_columns:
            # 如果值是dict/list且不在已知特殊处理中，转为JSON字符串
            if isinstance(v, dict):
                parsed[col_name] = json.dumps(v, ensure_ascii=False)
            elif isinstance(v, list):
                parsed[col_name] = array_to_string(v) if all(isinstance(x, str) for x in v) else json.dumps(v, ensure_ascii=False)
            else:
                parsed[col_name] = v

    # ========== 8. 构建动态SQL ==========
    columns = [k for k in parsed.keys() if k in valid_columns]

    if not columns:
        print(f"[WARNING] 没有有效字段可插入: {keyword}")
        return

    placeholders = ', '.join(['%s'] * len(columns))
    update_cols = [c for c in columns if c != 'company_name']
    update_clause = ', '.join([f"{col}=VALUES({col})" for col in update_cols])

    sql = f"""
    INSERT INTO company_819_info ({', '.join(columns)})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE {update_clause}
    """

    values = []
    for col in columns:
        val = parsed.get(col)
        if val is None:
            values.append(None)
        elif isinstance(val, (list, dict)):
            values.append(json.dumps(val, ensure_ascii=False))
        else:
            values.append(str(val))

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, values)
            conn.commit()
            print(f"[INFO] 解析入库成功: {keyword} (插入{len(columns)}个字段)")
    except Exception as e:
        print(f"[ERROR] 插入失败 {keyword}: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    print("=" * 60)
    print(f"【Step2】天眼查{INTERFACE_NAME}接口 - 数据解析")
    print("=" * 60)
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("解析规则:")
    print("  Array+child String → 逗号分隔字符串")
    print("  Object+多KV       → 展开为独立列")
    print("  Object+可能多条   → JSON字符串+提取total")
    print("  Number时间戳      → datetime(≥1e10为毫秒)")
    print()

    try:
        valid_columns = get_table_columns('company_819_info')
        print(f"[INFO] 数据库表字段数: {len(valid_columns)} 个")

        target_company = sys.argv[1] if len(sys.argv) > 1 else None
        if target_company:
            print(f"[INFO] 解析指定公司: {target_company}")

        records = get_today_success_records(target_company)

        if not records:
            print("[WARNING] 没有找到可解析的数据")
            print("提示：先执行 819-step1_api_fetch.py 拉取数据")
            return

        success_count = 0
        failed_count = 0

        for i, (record_id, company_name, result_json, create_time) in enumerate(records, 1):
            print(f"\n[{i}/{len(records)}] {company_name} (record_id={record_id})")
            print("-" * 60)

            try:
                api_result = json.loads(result_json)
                parse_and_insert(company_name, api_result, valid_columns, record_id=record_id)
                success_count += 1
            except Exception as e:
                print(f"[ERROR] 解析失败: {e}")
                traceback.print_exc()
                failed_count += 1

        print("\n" + "=" * 60)
        print("解析完成！")
        print("-" * 60)
        print(f"总计公司数: {len(records)}")
        print(f"  SUCCESS: {success_count}")
        print(f"  FAILED:  {failed_count}")
        print("=" * 60)

    except Exception as e:
        print(f"[FATAL] 任务执行失败: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()