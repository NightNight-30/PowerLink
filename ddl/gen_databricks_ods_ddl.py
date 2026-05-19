#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL DDL → Databricks ODS层DDL 转换脚本

从 api_call_record.sql 读取MySQL建表语句，自动转换为Databricks/Spark SQL格式。

转换规则（注意事项）：
  1. 表名加 ods_pl_ 前缀（PowerLink项目缩写）
  2. VARCHAR(n) / TEXT / LONGTEXT / JSON → STRING
     - Spark SQL无VARCHAR，统一STRING，长度由Parquet自动处理
     - JSON→STRING：Databricks无原生JSON类型，存原始字符串，后续可用from_json()解析
  3. DATETIME → TIMESTAMP
     - Spark SQL的TIMESTAMP带时区信息，MySQL DATETIME不带
     - 注意：若数据从MySQL导入时不含时区，TIMESTAMP会按UTC解释，可能与MySQL时区不一致
     - 建议：导入时明确指定时区，或使用session时区配置
  4. BIGINT / INT / DECIMAL(24,4) → 保持不变
     - DECIMAL精度和标度完全保留，不做截断
  5. 去掉 AUTO_INCREMENT / PRIMARY KEY / UNIQUE KEY / INDEX
     - ODS层原始数据不建约束，数据完整性由ETL保证
     - Parquet不依赖B-tree索引，查询优化通过分区/排序/Z-order实现
  6. DEFAULT CURRENT_TIMESTAMP → 去掉
     - Databricks不支持CURRENT_TIMESTAMP作为列默认值（需用INSERT时赋值或generated column）
  7. NOT NULL → 去掉
     - ODS层原始数据允许NULL，严格约束在后续DWD/DWS层加
  8. USING PARquet 替代 ENGINE=InnoDB
  9. customer_info 表跳过（配置/种子表，非接口数据）
  10. 字段名与MySQL完全一致，便于数据追溯和血缘分析

执行方式：
  python3 gen_databricks_ods_ddl.py
  输出: databricks_ods_ddl.sql (同目录下)
"""

import re
import sys


# ========== 类型映射 ==========
# MySQL → Databricks/Spark SQL
TYPE_MAP = {
    'BIGINT':       'BIGINT',
    'INT':          'INT',
    'TINYINT':      'INT',
    'SMALLINT':     'INT',
    'DECIMAL':      'DECIMAL',      # 精度标度保留，如 DECIMAL(24,4)
    'VARCHAR':      'STRING',
    'CHAR':         'STRING',
    'TEXT':         'STRING',
    'LONGTEXT':     'STRING',
    'MEDIUMTEXT':   'STRING',
    'JSON':         'STRING',
    'DATETIME':     'TIMESTAMP',
    'DATE':         'DATE',
    'TIMESTAMP':    'TIMESTAMP',
}

# 需要跳过的表（非接口数据表）
SKIP_TABLES = ['customer_info']

# 表名前缀
TABLE_PREFIX = 'ods_pl_'

# 存储格式
USING_FORMAT = 'PARquet'


def convert_mysql_type(mysql_type: str) -> str:
    """将MySQL数据类型转换为Databricks/Spark SQL类型"""
    # 提取基础类型名（去掉括号内的参数）
    # 如 VARCHAR(200) → 基础类型 VARCHAR, 参数 200
    match = re.match(r'(\w+)(?:\((.+)\))?', mysql_type.strip())
    if not match:
        return 'STRING'  # 未知类型兜底

    base_type = match.group(1).upper()
    params = match.group(2)

    spark_base = TYPE_MAP.get(base_type, 'STRING')

    # DECIMAL保留精度标度，如 DECIMAL(24,4)
    if base_type == 'DECIMAL' and params:
        return f'DECIMAL({params})'

    # 其他类型不需要长度参数（STRING无VARCHAR那样的长度限制）
    return spark_base


def parse_mysql_ddl(sql_content: str) -> list:
    """解析MySQL DDL文件，提取所有CREATE TABLE语句"""
    tables = []

    # 匹配 CREATE TABLE IF NOT EXISTS table_name (...) ENGINE=...
    pattern = re.compile(
        r'CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\n\)\s*ENGINE=InnoDB.*?COMMENT\s*=\s*\'(.*?)\'',
        re.DOTALL
    )

    for match in pattern.finditer(sql_content):
        table_name = match.group(1)
        body = match.group(2)
        table_comment = match.group(3)

        if table_name in SKIP_TABLES:
            continue

        # 解析列定义（排除 INDEX / KEY / UNIQUE 行）
        columns = []
        for line in body.split('\n'):
            line = line.strip()
            # 跳过空行、索引行、注释行
            if not line or line.startswith('INDEX') or line.startswith('UNIQUE KEY') \
               or line.startswith('KEY') or line.startswith('--'):
                continue

            # 解析列：name TYPE [NOT NULL] [DEFAULT xxx] [AUTO_INCREMENT] [PRIMARY KEY] COMMENT 'xxx'
            # 宽松匹配，允许列名后有空格
            col_match = re.match(
                r'(\w+)\s+([\w()]+(?:\(\d+(?:,\d+)?\))?)'
                r'(?:\s+NOT NULL)?'
                r'(?:\s+DEFAULT\s+\S+)?'
                r'(?:\s+AUTO_INCREMENT)?'
                r'(?:\s+PRIMARY KEY)?'
                r'\s+COMMENT\s+\'(.*?)\'',
                line
            )

            # 没有COMMENT的列（如id），单独处理
            if not col_match:
                col_match_simple = re.match(
                    r'(\w+)\s+BIGINT\s+AUTO_INCREMENT\s+PRIMARY KEY',
                    line
                )
                if col_match_simple:
                    col_name = col_match_simple.group(1)
                    columns.append((col_name, 'BIGINT', '主键ID'))
                continue

            col_name = col_match.group(1)
            mysql_type = col_match.group(2)
            col_comment = col_match.group(3)

            spark_type = convert_mysql_type(mysql_type)
            columns.append((col_name, spark_type, col_comment))

        # 解析数据关系（从注释推断）
        if '1:1' in table_comment or table_comment.startswith('企业基本信息') \
           or table_comment.startswith('上市公司') or table_comment.startswith('组织机构') \
           or table_comment.startswith('企业规模') or table_comment.startswith('付款指数'):
            relation = '1:1'
        elif '1:N' in table_comment or '调用记录' in table_comment:
            relation = '1:N' if '调用记录' not in table_comment else ''
        else:
            relation = ''

        tables.append({
            'mysql_name': table_name,
            'db_name': TABLE_PREFIX + table_name,
            'comment': table_comment,
            'columns': columns,
            'relation': relation,
        })

    return tables


def generate_databricks_ddl(tables: list) -> str:
    """生成Databricks ODS层DDL"""
    lines = [
        '-- ============================================',
        '-- PowerLink ODS层建表DDL (Databricks)',
        '-- 表名前缀: ods_pl_ (PowerLink项目缩写)',
        '-- 存储格式: PARquet',
        '-- 字段名与MySQL一致,便于数据追溯',
        '-- 类型映射: VARCHAR/TEXT/LONGTEXT/JSON→STRING, DATETIME→TIMESTAMP, DECIMAL/BIGINT/INT保持不变',
        '-- ODS层无约束(无PK/UK/索引),数据完整性由ETL保证',
        '-- ============================================',
        '',
    ]

    for i, table in enumerate(tables, 1):
        # 表注释补充数据关系
        comment = table['comment']
        if table['relation']:
            comment += f',{table["relation"]}关系'

        lines.append(f'-- {i}. {comment.split("(")[0].strip()}')
        lines.append(f'CREATE TABLE IF NOT EXISTS {table["db_name"]} (')

        # 列定义
        col_lines = []
        for col_name, spark_type, col_comment in table['columns']:
            # 对齐：类型列固定宽度
            type_str = spark_type
            if spark_type == 'DECIMAL(24,4)':
                type_str = 'DECIMAL(24,4)'
            # 计算对齐宽度
            name_pad = max(28 - len(col_name), 0)
            type_pad = max(14 - len(type_str), 0)
            col_line = f'  {col_name}{" " * name_pad}{type_str}{" " * type_pad}COMMENT \'{col_comment}\''
            col_lines.append(col_line)

        lines.append(',\n'.join(col_lines))
        lines.append(f') USING {USING_FORMAT}')
        lines.append(f"COMMENT '{comment}';")
        lines.append('')

    return '\n'.join(lines)


def main():
    input_file = 'api_call_record.sql'
    output_file = 'databricks_ods_ddl.sql'

    print('=' * 60)
    print('MySQL DDL → Databricks ODS层DDL 转换')
    print('=' * 60)

    # 读取MySQL DDL
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            mysql_ddl = f.read()
    except FileNotFoundError:
        print(f'[FATAL] 未找到MySQL DDL文件: {input_file}')
        print('请在ddl/目录下运行此脚本，或指定正确路径')
        sys.exit(1)

    # 解析
    tables = parse_mysql_ddl(mysql_ddl)
    print(f'[INFO] 解析到 {len(tables)} 张表')
    for t in tables:
        print(f'  {t["mysql_name"]} → {t["db_name"]} ({len(t["columns"])}个字段)')

    # 生成
    databricks_ddl = generate_databricks_ddl(tables)

    # 写入
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(databricks_ddl)

    print(f'\n[SUCCESS] Databricks DDL已生成: {output_file}')
    print(f'[INFO] 共 {len(tables)} 张表, {sum(len(t["columns"]) for t in tables)} 个字段')

    # 类型转换统计
    type_conversions = {}
    for t in tables:
        for _, spark_type, _ in t['columns']:
            type_conversions[spark_type] = type_conversions.get(spark_type, 0) + 1
    print('\n[INFO] 类型分布:')
    for dtype, count in sorted(type_conversions.items()):
        print(f'  {dtype}: {count}个字段')

    # 注意事项提醒
    print('\n' + '=' * 60)
    print('⚠️  转换注意事项:')
    print('=' * 60)
    print('1. DATETIME→TIMESTAMP: Spark TIMESTAMP带时区,MySQL DATETIME不带')
    print('   导入时注意时区一致性,建议指定session时区')
    print('2. JSON→STRING: Databricks无原生JSON类型,存原始字符串')
    print('   后续可用from_json()解析为结构化数据')
    print('3. VARCHAR→STRING: Spark SQL无VARCHAR长度限制')
    print('   长度由Parquet自动处理,不影响存储')
    print('4. 去掉DEFAULT CURRENT_TIMESTAMP: Databricks不支持列级默认时间')
    print('   需在INSERT时赋值或使用generated column')
    print('5. 去掉NOT NULL约束: ODS层允许NULL,严格约束在DWD/DWS层加')
    print('6. 去掉AUTO_INCREMENT/PRIMARY KEY/UNIQUE KEY/INDEX:')
    print('   ODS层原始数据不建约束,完整性由ETL保证')


if __name__ == '__main__':
    main()