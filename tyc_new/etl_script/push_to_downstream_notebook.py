# -*- coding: utf-8 -*-
# 装 pymysql (推送用 pymysql 单连接批量插入, 避开 Spark JDBC 新连接不稳定)
%pip install pymysql -q

"""【Notebook版】下游数据推送 (Databricks → MySQL via pymysql 批量插入)

每天 ETL 跑完后,由 jobs 编排调用本 notebook,把指定 Delta 表的 dt 分区全量数据
overwrite 到下游 MySQL 整表。

流程(对配置里每个 task 循环):
  1. Spark read 上游 Delta 表 dt 分区
  2. 查 information_schema 判断下游表是否存在
  3. 不存在 → 读 DDL 文件用 pymysql 执行建表
  4. 字段校验(上游 Spark schema vs 下游 information_schema.columns)
  5. pymysql 单连接: TRUNCATE + executemany 批量 INSERT
  6. 写审计日志到 ods_downstream_push_audit_log

为什么用 pymysql 不用 Spark JDBC write:
  - Spark JDBC write 每次 .write.jdbc 开多个新连接(numPartitions), 新连接被 SCC relay 限流
  - pymysql 单连接复用, TRUNCATE + executemany 批量插入, 稳定不挂
  - pymysql 的 executemany 自动做 multi-value INSERT 重写, 批量效率够用

参数(jobs 编排传入 widget):
  - dt: 推送的数据分区日期(yyyyMMdd),如 20260707

配置文件:
  /Workspace/Shared/powerlink_warehouse/tyc_new/config/downstream_push_config.json
  (模板见 downstream_push_config.example.json)
"""

import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 纯 Python JDBC 库(不依赖 spark._jvm,Spark Connect 模式下可用)
try:
    import pymysql  # MySQL
except ImportError:
    pymysql = None
try:
    import pymssql  # SQL-Server
except ImportError:
    pymssql = None

from common.config_loader import load_config
from common.spark_utils import get_spark
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType,
    TimestampType, IntegerType,
)


# ========== 配置 ==========

CONFIG_PATH = os.environ.get(
    'DOWNSTREAM_PUSH_CONFIG_PATH',
    '/Workspace/Shared/powerlink_warehouse/tyc_new/config/downstream_push_config.json'
)

spark = get_spark()

# 读 widget 参数 dt(jobs 编排传入); 没有则用昨天
try:
    dbutils  # noqa  Databricks 内置
    PUSH_DT = dbutils.widgets.get("dt")
except Exception:
    PUSH_DT = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

print("=" * 60)
print("【下游数据推送 Databricks → MySQL/SQL-Server】")
print(f"推送分区: dt={PUSH_DT}")
print("=" * 60)


# ========== pymysql 工具(纯 Python, 不依赖 spark._jvm, Spark Connect 兼容) ==========
# 注意: Databricks 默认 Spark Connect 模式下 spark._jvm 不可用,
#       全部 DB 操作(建表/查字段/批量插入)都用 pymysql, 不用 Spark JDBC write
# (原 Spark JDBC write 因新连接被 SCC relay 限流不稳定, 改用 pymysql 单连接复用)


def _parse_jdbc_url(jdbc_url: str, downstream_type: str) -> Tuple[str, int, str]:
    """解析 JDBC URL → (host, port, db_name)
    MySQL:      jdbc:mysql://host:port/dbname?params
    SQL-Server: jdbc:sqlserver://host:port;databaseName=dbname;params
    """
    if downstream_type == 'mysql':
        # jdbc:mysql://host:port/dbname?params
        after_proto = jdbc_url.split('//', 1)[1]  # host:port/dbname?params
        host_port_db = after_proto.split('?', 1)[0]  # host:port/dbname
        host_port, db = host_port_db.split('/', 1)
        if ':' in host_port:
            host, port = host_port.split(':')
        else:
            host, port = host_port, 3306
        return host, int(port), db
    elif downstream_type == 'mssql':
        # jdbc:sqlserver://host:port;databaseName=dbname;params
        after_proto = jdbc_url.split('//', 1)[1]  # host:port;databaseName=dbname;params
        parts = after_proto.split(';')
        host_port = parts[0]
        if ':' in host_port:
            host, port = host_port.split(':')
        else:
            host, port = host_port, 1433
        db = None
        for part in parts[1:]:
            if part.lower().startswith('databasename='):
                db = part.split('=', 1)[1]
                break
        if not db:
            raise ValueError(f"SQL-Server JDBC URL 缺少 databaseName 参数: {jdbc_url}")
        return host, int(port), db
    raise ValueError(f"不支持的 downstream_type: {downstream_type}")


def _get_py_conn(jdbc_url: str, user: str, password: str, downstream_type: str):
    """用纯 Python 库(pymysql/pymssql)拿 DB 连接,执行 raw SQL"""
    host, port, db = _parse_jdbc_url(jdbc_url, downstream_type)
    if downstream_type == 'mysql':
        if pymysql is None:
            raise ImportError(
                "pymysql 未安装。请在 notebook 第一个 cell 跑: %pip install pymysql"
            )
        return pymysql.connect(
            host=host, port=port, user=user, password=password,
            database=db, charset='utf8mb4', autocommit=True,
        )
    elif downstream_type == 'mssql':
        if pymssql is None:
            raise ImportError(
                "pymssql 未安装。请在 notebook 第一个 cell 跑: %pip install pymssql"
            )
        return pymssql.connect(
            host=host, port=port, user=user, password=password, database=db,
        )
    raise ValueError(f"不支持的 downstream_type: {downstream_type}")


def _extract_db_name(jdbc_url: str, downstream_type: str) -> str:
    """从 JDBC URL 提取 database 名(保留原签名,内部走 _parse_jdbc_url)"""
    _, _, db = _parse_jdbc_url(jdbc_url, downstream_type)
    return db


def check_table_exists(jdbc_url: str, user: str, password: str,
                       downstream_type: str, db_name: str, table_name: str) -> bool:
    """查 information_schema 判断表是否存在"""
    conn = _get_py_conn(jdbc_url, user, password, downstream_type)
    try:
        with conn.cursor() as cur:
            sql = (
                f"SELECT 1 FROM information_schema.tables "
                f"WHERE table_schema = %s AND table_name = %s LIMIT 1"
            )
            cur.execute(sql, (db_name, table_name))
            return cur.fetchone() is not None
    finally:
        conn.close()


def execute_ddl(jdbc_url: str, user: str, password: str,
                downstream_type: str, ddl_sql: str) -> None:
    """执行建表 DDL(CREATE TABLE IF NOT EXISTS,可能含多条 ; 分隔)"""
    conn = _get_py_conn(jdbc_url, user, password, downstream_type)
    try:
        with conn.cursor() as cur:
            for piece in ddl_sql.split(';'):
                piece = piece.strip()
                if piece:
                    cur.execute(piece)
        # pymysql autocommit=True 已自动提交; mssql pymssql 默认 autocommit 也 True
    finally:
        conn.close()


def get_downstream_columns(jdbc_url: str, user: str, password: str,
                           downstream_type: str, db_name: str,
                           table_name: str) -> List[Tuple[str, str]]:
    """查下游表字段列表 [(name_lower, type_lower), ...] 按 ordinal_position 排序"""
    conn = _get_py_conn(jdbc_url, user, password, downstream_type)
    try:
        with conn.cursor() as cur:
            sql = (
                f"SELECT column_name, data_type FROM information_schema.columns "
                f"WHERE table_schema = %s AND table_name = %s "
                f"ORDER BY ordinal_position"
            )
            cur.execute(sql, (db_name, table_name))
            return [(row[0].lower(), row[1].lower()) for row in cur.fetchall()]
    finally:
        conn.close()


def pymysql_batch_insert(df, jdbc_url: str, user: str, password: str,
                         downstream_type: str, table_name: str,
                         batch_size: int = 5000) -> int:
    """用 pymysql 单连接批量插入 (替代 Spark JDBC write)

    流程:
      1. 开一个 pymysql 连接 (复用单连接, 不开新连接)
      2. TRUNCATE 目标表
      3. df.toLocalIterator() 逐分区拉到 driver, 按 batch_size 攒批 executemany
      4. 关闭连接

    pymysql 的 executemany 自动把多条 INSERT 重写成 multi-value INSERT,
    批量效率够用 (不需要 rewriteBatchedStatements 参数, 那是 Java JDBC 的)

    toLocalIterator 逐分区拉数据, 不 OOM (大表也安全)
    """
    if downstream_type != 'mysql':
        raise ValueError(
            f"pymysql 批量插入仅支持 mysql, 收到: {downstream_type}"
            f" (SQL-Server 需用 pymssql, 但当前只实现了 mysql 路径)"
        )

    # 列名用反引号转义 (防保留字冲突, 如 `rank`)
    cols = [f"`{c}`" for c in df.columns]
    col_str = ", ".join(cols)
    placeholder = ", ".join(["%s"] * len(cols))
    insert_sql = f"INSERT INTO `{table_name}` ({col_str}) VALUES ({placeholder})"

    conn = _get_py_conn(jdbc_url, user, password, downstream_type)
    try:
        with conn.cursor() as cur:
            # TRUNCATE 先清表 (autocommit=True, DDL 自动提交)
            cur.execute(f"TRUNCATE TABLE `{table_name}`")

            total = 0
            batch = []
            # toLocalIterator: 逐分区拉到 driver, 内存友好
            for row in df.toLocalIterator():
                batch.append(tuple(row))
                if len(batch) >= batch_size:
                    cur.executemany(insert_sql, batch)
                    total += len(batch)
                    batch = []
            # 插剩余不足一批的
            if batch:
                cur.executemany(insert_sql, batch)
                total += len(batch)

            print(f"  pymysql 批量插入: {total} 行, batch_size={batch_size}")
            return total
    finally:
        conn.close()


# ========== 字段校验 ==========

# Spark 类型 → MySQL/SQL-Server 兼容类型(下游 data_type 关键字)
SPARK_TYPE_COMPAT = {
    'string':    {'varchar', 'text', 'char', 'longtext', 'mediumtext', 'tinytext'},
    'int':       {'int', 'integer', 'mediumint', 'smallint', 'bigint'},
    'long':      {'bigint', 'int', 'integer'},
    'short':     {'smallint', 'int', 'integer', 'bigint'},
    'date':      {'date', 'datetime', 'timestamp'},
    'timestamp': {'datetime', 'timestamp', 'date'},
    'decimal':   {'decimal', 'numeric', 'double', 'float'},
    'double':    {'double', 'float', 'decimal'},
    'boolean':   {'tinyint', 'bit', 'boolean'},
}


def validate_schema(upstream_fields: List[Tuple[str, str]],
                    downstream_fields: List[Tuple[str, str]],
                    skip_downstream_cols: Optional[set] = None) -> None:
    """对比上下游字段名+顺序+类型兼容性

    upstream_fields:   [(name, spark_type), ...] 从 df.schema 拿
    downstream_fields: [(name, db_type), ...]    从 information_schema.columns 拿
    skip_downstream_cols: 下游存在但上游没有的字段(如自增主键 id),校验时跳过
    """
    skip = skip_downstream_cols or {'id'}
    ds_filtered = [(n, t) for n, t in downstream_fields if n not in skip]

    if len(upstream_fields) != len(ds_filtered):
        raise ValueError(
            f"字段数量不一致: 上游 {len(upstream_fields)} 个, "
            f"下游 {len(ds_filtered)} 个(已排除 {skip})"
        )

    for i, (up_name, up_type) in enumerate(upstream_fields):
        ds_name, ds_type = ds_filtered[i]
        if up_name.lower() != ds_name:
            raise ValueError(
                f"第 {i+1} 个字段名不一致: 上游 '{up_name}' vs 下游 '{ds_name}'"
            )
        # 类型兼容性
        spark_simple = up_type.lower().split('(')[0].strip()
        compat_set = SPARK_TYPE_COMPAT.get(spark_simple, set())
        if compat_set and ds_type not in compat_set:
            raise ValueError(
                f"字段 '{up_name}' 类型不兼容: 上游 Spark {up_type} "
                f"→ 下游 {ds_type} (期望之一: {compat_set})"
            )


# ========== 审计日志 ==========

AUDIT_LOG_DDL = """
CREATE TABLE IF NOT EXISTS powerlink_uat.pw_ods.ods_downstream_push_audit_log (
    task_name       STRING      COMMENT '任务名',
    dt              STRING      COMMENT '推送分区日期(yyyyMMdd)',
    upstream_table  STRING      COMMENT '上游 Delta 表全名',
    downstream_table STRING     COMMENT '下游表名',
    row_count       BIGINT      COMMENT '推送行数',
    table_created   BOOLEAN     COMMENT '本次是否新建了下游表',
    start_time      TIMESTAMP   COMMENT '开始时间',
    end_time        TIMESTAMP   COMMENT '结束时间',
    duration_sec    INT         COMMENT '耗时(秒)',
    status          STRING      COMMENT 'success / failed / skipped / no_data',
    error_msg       STRING      COMMENT '失败时的错误信息'
) USING DELTA
PARTITIONED BY (dt)
LOCATION 'abfss://powerlink@powerlink.dfs.core.chinacloudapi.cn/powerlink/pw_ods/ods_downstream_push_audit_log'
COMMENT '下游数据推送审计日志'
"""


def ensure_audit_log_table(audit_cfg: Dict) -> Optional[str]:
    """确保审计日志 Delta 表存在(返回表全名或 None)"""
    if not audit_cfg.get('enabled', True):
        return None
    table = audit_cfg['table']
    spark.sql(AUDIT_LOG_DDL)
    return table


def write_audit_log(table: Optional[str], record: Dict) -> None:
    if not table:
        return
    # 显式 schema: 避免类型推断不匹配 Delta 表 (duration_sec INT 不能推断成 LongType)
    schema = StructType([
        StructField('task_name', StringType(), True),
        StructField('dt', StringType(), True),
        StructField('upstream_table', StringType(), True),
        StructField('downstream_table', StringType(), True),
        StructField('row_count', LongType(), True),
        StructField('table_created', BooleanType(), True),
        StructField('start_time', TimestampType(), True),
        StructField('end_time', TimestampType(), True),
        StructField('duration_sec', IntegerType(), True),  # INT 不是 LongType
        StructField('status', StringType(), True),
        StructField('error_msg', StringType(), True),
    ])
    df = spark.createDataFrame([record], schema=schema)
    df.write.mode("append").saveAsTable(table)


# ========== 主流程 ==========

def run_push_task(task: Dict, dt: str, audit_log_table: Optional[str]) -> Dict:
    """跑单个推送任务,返回审计记录"""
    task_name = task['name']
    up = task['upstream']
    down = task['downstream']
    wo = task.get('write_options', {})

    log = {
        'task_name': task_name, 'dt': dt,
        'upstream_table': up['table'], 'downstream_table': down['table'],
        'row_count': 0, 'table_created': False,
        'start_time': datetime.now(), 'end_time': None,
        'duration_sec': 0, 'status': 'success', 'error_msg': '',
    }

    print(f"\n[任务] {task_name}")
    print(f"  上游: {up['table']} dt={dt}")
    print(f"  下游: {down['type']} {down['table']}")

    try:
        if not task.get('enabled', True):
            print("  跳过(enabled=false)")
            log['status'] = 'skipped'
            return log

        # 1. Spark 读上游 dt 分区
        df = spark.read.table(up['table']).filter(F.col(up['dt_field']) == dt)
        row_count = df.count()
        log['row_count'] = row_count
        print(f"  上游行数: {row_count}")

        if row_count == 0:
            print(f"  上游 dt={dt} 无数据,跳过推送")
            log['status'] = 'no_data'
            log['end_time'] = datetime.now()
            return log

        # 2. 判断下游表是否存在
        db_name = _extract_db_name(down['jdbc_url'], down['type'])
        table_exists = check_table_exists(
            down['jdbc_url'], down['user'], down['password'],
            down['type'], db_name, down['table']
        )
        print(f"  下游表存在: {table_exists}")

        # 3. 不存在则建表
        if not table_exists:
            ddl_path = down['ddl_path']
            if not os.path.isabs(ddl_path):
                ddl_path = os.path.join(
                    '/Workspace/Shared/powerlink_warehouse/tyc_new', ddl_path
                )
            with open(ddl_path, 'r', encoding='utf-8') as f:
                ddl_sql = f.read()
            print(f"  执行建表 DDL: {ddl_path}")
            execute_ddl(
                down['jdbc_url'], down['user'], down['password'],
                down['type'], ddl_sql
            )
            log['table_created'] = True
            print("  建表完成")

        # 4. 字段校验
        upstream_fields = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
        downstream_fields = get_downstream_columns(
            down['jdbc_url'], down['user'], down['password'],
            down['type'], db_name, down['table']
        )
        # dt 是上游 Delta 表的分区字段,Spark read 时会作为普通列出现;
        # 下游表也有 dt 字段(DDL 已建),所以校验时不用排除
        validate_schema(upstream_fields, downstream_fields, skip_downstream_cols={'id'})
        print(f"  字段校验通过: 上游 {len(upstream_fields)} 字段 vs 下游 {len(downstream_fields)} 字段(含 id)")

        # 5. pymysql 单连接批量插入 (替代 Spark JDBC write)
        # Spark JDBC write 每次开 numPartitions 个新连接, 新连接被 SCC relay 限流会挂
        # pymysql 单连接复用: TRUNCATE + executemany, 稳定不挂
        batch_size = wo.get('batchsize', 5000)
        inserted = pymysql_batch_insert(
            df, down['jdbc_url'], down['user'], down['password'],
            down['type'], down['table'], batch_size
        )
        print(f"  推送完成: {inserted} 行 → {down['table']}")

    except Exception as e:
        log['status'] = 'failed'
        log['error_msg'] = str(e)[:2000]
        print(f"  ✗ 失败: {e}")
    finally:
        log['end_time'] = datetime.now()
        log['duration_sec'] = int((log['end_time'] - log['start_time']).total_seconds())
        # 审计日志写失败不影响任务结果(别让 finally 抛异常掩盖推送成功)
        try:
            write_audit_log(audit_log_table, log)
        except Exception as audit_e:
            print(f"  审计日志写入失败(不影响推送结果): {audit_e}")

    return log


def main():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        push_cfg = json.load(f)

    audit_log_table = ensure_audit_log_table(push_cfg.get('audit_log', {}))

    print(f"\n加载配置: {CONFIG_PATH}")
    print(f"任务数: {len(push_cfg['tasks'])}")
    print(f"审计日志表: {audit_log_table or '(未启用)'}")

    results = []
    for task in push_cfg['tasks']:
        try:
            log = run_push_task(task, PUSH_DT, audit_log_table)
            results.append(log)
        except Exception as e:
            # 单任务异常不影响其他任务
            print(f"[任务 {task.get('name')}] 严重错误: {e}")
            results.append({
                'task_name': task.get('name', '?'), 'dt': PUSH_DT,
                'status': 'failed', 'error_msg': str(e)[:2000],
            })

    # 汇总
    print("\n" + "=" * 60)
    print("【推送汇总】")
    success = sum(1 for r in results if r['status'] == 'success')
    failed = sum(1 for r in results if r['status'] == 'failed')
    skipped = sum(1 for r in results if r['status'] in ('skipped', 'no_data'))
    print(f"成功 {success} / 失败 {failed} / 跳过 {skipped}")
    for r in results:
        flag = '✓' if r['status'] == 'success' else '✗' if r['status'] == 'failed' else '-'
        print(f"  {flag} {r['task_name']} | {r['status']} | rows={r.get('row_count', 0)}")

    if failed > 0:
        raise SystemExit(f"有 {failed} 个任务失败,详见上面日志")


main()
