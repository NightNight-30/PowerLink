# ============================================================
# Cell 1: 配置 + 安装依赖
# ============================================================
# COMMAND ----------
%pip install jaydebeapi JPype1

# COMMAND ----------
import os
from common.config_loader import load_config, get_manual_load_config

CONFIG = load_config()
_ml = get_manual_load_config(CONFIG)
if not _ml:
    raise RuntimeError("config.json 缺 manual_load 段, 请参考 config.json.example 补全")

# --- Databricks Catalog/Schema ---
CATALOG = _ml["catalog"]
SCHEMA = _ml["schema"]
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# --- Delta 表名前缀 ---
TABLE_PREFIX = _ml["table_prefix"]

# --- 文件路径 ---
VOLUME_BASE = _ml["volume_base"]
UPLOAD_DIR = VOLUME_BASE + _ml["upload_dir"]
PROCESSED_DIR = VOLUME_BASE + _ml["processed_dir"]
JAR_DIR = VOLUME_BASE + _ml["jar_dir"]

UCANACCESS_JAR = JAR_DIR + _ml["ucanaccess_jar"]
UCANACCESS_DEPS = [JAR_DIR + j for j in _ml["ucanaccess_deps"]]
ALL_JARS_VOLUME = [UCANACCESS_JAR] + UCANACCESS_DEPS

# JVM 加载 JAR 需要本地文件系统路径，Volume 是 FUSE 挂载，JVM 无法直接加载 class
# 解决方案：将 JAR 复制到 driver 本地 /tmp/jars/ 目录
LOCAL_JAR_DIR = "/tmp/jars/"
ALL_JARS_LOCAL = [f"{LOCAL_JAR_DIR}{jar.split('/')[-1]}" for jar in ALL_JARS_VOLUME]

def strip_dbfs_uri(path):
    """剥离 dbutils.fs.ls() 返回的 dbfs: URI 前缀"""
    if path.startswith("dbfs:"):
        return path[5:]  # dbfs:/Volumes/xxx → /Volumes/xxx
    return path

def copy_jars_to_local():
    """将 Volume 上的 JAR 复制到 driver 本地 /tmp/jars/ 目录"""
    import subprocess
    import shutil
    # 先清理残留目录（上次 OOM/异常中断可能留下脏文件或权限错乱的文件，导致 cp cannot stat）
    shutil.rmtree(LOCAL_JAR_DIR, ignore_errors=True)
    os.makedirs(LOCAL_JAR_DIR, exist_ok=True)
    for volume_jar, local_jar in zip(ALL_JARS_VOLUME, ALL_JARS_LOCAL):
        volume_path_clean = strip_dbfs_uri(volume_jar)
        # 双保险：再尝试删一次目标文件（rmtree 已清但保险起见）
        try:
            os.remove(local_jar)
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"  [warn] 清理目标文件失败: {local_jar} - {e}")
        # 先验证源文件在 Volume 上是否存在
        try:
            jar_info = dbutils.fs.ls(volume_path_clean)
            src_size = jar_info[0].size if jar_info else 0
            src_ok = src_size > 0
        except Exception as e:
            src_ok = False
            src_size = 0
            print(f"  [源文件检查失败] {volume_path_clean}: {e}")
        if not src_ok:
            print(f"  [源文件不存在或为空] {volume_path_clean}")
            print(f"    请确认 Volume 路径 catalog/schema 段是否正确，或手动上传 JAR 到该路径")
            raise FileNotFoundError(f"源 JAR 不存在或为空: {volume_path_clean}")

        result = subprocess.run(
            ["cp", volume_path_clean, local_jar],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"  FAIL cp {volume_path_clean} → {local_jar}")
            print(f"    returncode: {result.returncode}")
            print(f"    stderr: {result.stderr.strip() or '(空)'}")
            print(f"    stdout: {result.stdout.strip() or '(空)'}")
            raise RuntimeError(f"cp 失败 (rc={result.returncode}): {volume_path_clean} → {local_jar}: {result.stderr.strip()}")
        dest_size = os.path.getsize(local_jar)
        print(f"  OK cp {volume_path_clean} → {local_jar} ({dest_size/1024:.1f} KB)")
    print("JAR 复制完成")

# --- Metadata 表 ---
META_TABLE = f"{FULL_SCHEMA}.{_ml['meta_table']}"

# --- 写入模式 ---
WRITE_MODE = _ml["write_mode"]

# --- 强制重新加载 ---
# False: 仅处理 meta 表中未记录或 MD5 有变更的文件（增量模式），处理后归档到 processed/
# True:  忽略 meta 表记录，强制处理上传目录中的所有文件（全量重跑），不归档文件
#       适用场景：手动删了 Delta 表需要重建、数据更新需要重新导入
# 走环境变量 ACCESS_FORCE_RELOAD=true 开启 (jobs 调度时指定, 默认 false)
FORCE_RELOAD = os.environ.get("ACCESS_FORCE_RELOAD", "false").lower() in ("1", "true", "yes", "on")

# --- UCanAccess JDBC 连接参数 ---
JDBC_DRIVER = "net.ucanaccess.jdbc.UcanaccessDriver"

# --- JVM 堆大小 (GB) ---
# driver 内存加大后, JVM 堆给到 12GB, pandas+Spark 留 10GB+
JVM_HEAP_GB = int(_ml["jvm_heap_gb"])

# --- 启用 PySpark Arrow 优化 (pandas → Spark DataFrame 转换更快更省内存) ---
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
# memory=true: hsqldb mirror 走 JVM 内存,不写磁盘。
# 之前用 memory=false 想省 JVM 堆,但 UCanAccess 会把 mirror 写到 accdb 同目录,
# Volume 是只读挂载会报 Input/output error。mirror= 参数实测没生效。
# 361MB accdb → mirror 约 1-2GB,JVM 堆 4GB + executor 堆 8GB+ 都够用。
JDBC_PARAMS = _ml["jdbc_params"]

# --- processed/ 目录保留文件数 ---
PROCESSED_KEEP = int(_ml.get("processed_keep", 3))

print(f"配置加载完成: catalog={CATALOG} schema={SCHEMA} volume_base={VOLUME_BASE}")
print(f"FORCE_RELOAD={FORCE_RELOAD} (env ACCESS_FORCE_RELOAD)")
print(f"JVM_HEAP_GB={JVM_HEAP_GB}, WRITE_MODE={WRITE_MODE}, JDBC_PARAMS={JDBC_PARAMS}")

# ============================================================
# Cell 2: 环境准备（创建目录、meta表、复制JAR到本地）
# ============================================================
# COMMAND ----------
import os
import shutil

# 创建 Schema
print(f"确保 Schema 存在: {FULL_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA}")

# 确保 Volume 存在
volume_path = "/Volumes/powerlink/default/env/"
try:
    dbutils.fs.ls(volume_path)
    print(f"Volume 已存在: {volume_path}")
except Exception:
    print("Volume 可能不存在，请先创建: CREATE VOLUME IF NOT EXISTS powerlink.default.env;")

# 创建子目录（dbutils.fs 用 Volume 路径）
for dir_path in [UPLOAD_DIR, PROCESSED_DIR, JAR_DIR]:
    print(f"创建目录: {dir_path}")
    dbutils.fs.mkdirs(dir_path)

# 创建 metadata 表
print(f"创建 metadata 表: {META_TABLE}")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {META_TABLE} (
    filename STRING,
    md5 STRING,
    processed_at TIMESTAMP,
    table_names STRING,
    table_counts STRING,
    status STRING
)
USING DELTA
""")

# 兼容旧表: 若已存在但缺 table_counts 列(老版本创建)则补列
try:
    cols = [r.col_name for r in spark.sql(f"DESCRIBE TABLE {META_TABLE}").collect()]
    if "table_counts" not in cols:
        print("meta 表缺 table_counts 列, 执行 ALTER TABLE ADD COLUMNS...")
        spark.sql(f"ALTER TABLE {META_TABLE} ADD COLUMNS (table_counts STRING)")
except Exception as e:
    print(f"[WARN] 检查/补列 table_counts 失败(忽略): {e}")

# 验证 JayDeBeApi（只检查安装状态，不 import，避免 JVM 提前启动）
print("\n验证 JayDeBeApi...")
import importlib.util
if importlib.util.find_spec("jaydebeapi") is not None:
    print("JayDeBeApi 已安装")
else:
    print("JayDeBeApi 未安装！请在 Cell 1 中运行: %pip install jaydebeapi JPype1")

# 验证 Volume 上的 JAR 文件
print("\n验证 Volume 上 JAR 文件...")
for jar in ALL_JARS_VOLUME:
    try:
        dbutils.fs.ls(jar)
        print(f"  OK {jar}")
    except Exception:
        print(f"  MISSING {jar} - 请手动上传到 Volume")

# 复制 JAR 到 driver 本地 /tmp/jars/
print("\n复制 JAR 到 driver 本地目录...")
copy_jars_to_local()

# 验证本地 JAR 内容（0字节说明有问题）
print("\n验证本地 JAR 内容...")
jar_ok = True
for jar in ALL_JARS_LOCAL:
    if os.path.exists(jar):
        size_bytes = os.path.getsize(jar)
        jar_name = jar.split('/')[-1]
        if size_bytes < 1000:
            print(f"  WARNING {jar} 大小仅 {size_bytes} 字节！可能是空文件")
            jar_ok = False
        else:
            size_kb = size_bytes / 1024
            print(f"  OK {jar} ({size_kb:.1f} KB)")
    else:
        print(f"  MISSING {jar}")

# 列出上传目录中的文件
print(f"\n当前上传目录中的 Access 文件:")
try:
    files = dbutils.fs.ls(UPLOAD_DIR)
    access_files = [f for f in files if f.name.endswith('.accdb') or f.name.endswith('.mdb')]
    if access_files:
        for f in access_files:
            print(f"  {f.name}  ({f.size / 1024 / 1024:.1f} MB)")
    else:
        print("  (暂无文件)")
except Exception:
    print("  (目录为空或不存在)")

print("\n=== 环境准备完成 ===")

# ============================================================
# Cell 3: 读取 .accdb 文件并写入 Delta 表
# ============================================================
# COMMAND ----------
import hashlib
import re
import json
import subprocess
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F

def get_file_md5(filepath):
    """用 subprocess 调 md5sum 算 MD5,不进 Spark,不进 Python 大内存。

    之前尝试:
    - spark.read.binaryFile + wholeFile=true: 400M 文件 executor OOM (exit 52)
    - dbutils.fs.open: 'RemoteFsHandler' object has no attribute 'open' (新 DBR 不支持)
    改用 subprocess + md5sum,driver 进程只持 64KB 缓冲,恒定内存。
    """
    try:
        local_path = strip_dbfs_uri(filepath)
        # md5sum 流式读,不把整个文件加载到内存
        result = subprocess.run(
            ["md5sum", local_path],
            capture_output=True, text=True, check=True
        )
        # 输出格式: "d41d8cd98f00b204e9800998ecf8427e  /path/to/file"
        return result.stdout.split()[0]
    except Exception as e:
        print(f"  [MD5 计算失败] {filepath}: {e}")
        return ""


def copy_accdb_to_local(filepath):
    """用 shell cp 将 .accdb 从 Volume 复制到 /tmp/"""
    local_path = strip_dbfs_uri(filepath)
    filename = local_path.split('/')[-1]
    local_file = f"/tmp/{filename}"
    subprocess.run(["cp", local_path, local_file], check=True)
    size_mb = os.path.getsize(local_file) / 1024 / 1024
    print(f"  复制 Access 文件到本地: {local_path} → {local_file} ({size_mb:.1f} MB)")
    return local_file


def list_new_files():
    if FORCE_RELOAD:
        # 全量模式：处理上传目录中所有 accdb/mdb 文件，忽略 meta 表
        print("  FORCE_RELOAD=True，忽略 meta 表记录")
        all_files = dbutils.fs.ls(UPLOAD_DIR)
        access_files = [f for f in all_files
                        if f.name.endswith('.accdb') or f.name.endswith('.mdb')]
        return [{
            'path': f.path,
            'filename': f.name,
            'md5': get_file_md5(f.path),
            'size': f.size,
        } for f in access_files]

    # 增量模式：取每个 filename 最新一条 success 记录的 MD5
    # 同名文件重新上传会在 meta 表留下多条记录，必须按 processed_at desc 取最新一条，避免字典覆盖导致误判
    try:
        latest_md5_rows = spark.sql(f"""
            SELECT filename, md5
            FROM (
                SELECT filename, md5, processed_at,
                       ROW_NUMBER() OVER (PARTITION BY filename ORDER BY processed_at DESC) AS rn
                FROM {META_TABLE}
                WHERE status = 'success'
            ) WHERE rn = 1
        """).collect()
        processed = {row.filename: row.md5 for row in latest_md5_rows}
    except Exception:
        processed = {}

    all_files = dbutils.fs.ls(UPLOAD_DIR)
    access_files = [f for f in all_files
                    if (f.name.endswith('.accdb') or f.name.endswith('.mdb'))
                    and not f.path.startswith(PROCESSED_DIR)]

    # 多个文件时只处理最新日期的；其他直接归档到 processed/，不写 meta（cleanup 会自动清理）
    if len(access_files) > 1:
        def extract_date(f):
            # 优先从文件名提取 YYYYMMDD；没日期则用 mtime 转成 YYYYMMDD（按上传时间）
            m = re.search(r'(\d{8})', f.name)
            if m:
                return m.group(1)
            from datetime import datetime
            return datetime.fromtimestamp(f.modificationTime / 1000).strftime("%Y%m%d")

        # 排序键：(文件名日期 or mtime 转日期) + mtime 时间戳（同日期时按上传时间细排）
        access_files.sort(key=lambda f: (extract_date(f), f.modificationTime), reverse=True)

        latest = access_files[0]
        older = access_files[1:]
        print(f"  UPLOAD_DIR 共 {len(access_files)} 个 Access 文件，只处理最新日期的: {latest.name} (日期={extract_date(latest)})")
        for f in older:
            print(f"    [归档非最新] {f.name} (日期={extract_date(f)}) → {PROCESSED_DIR}")
            dest = PROCESSED_DIR + f.name
            dbutils.fs.mv(f.path, dest)
        access_files = [latest]

    new_files = []
    for f in access_files:
        md5 = get_file_md5(f.path)
        if f.name not in processed:
            print(f"  [新文件] {f.name} (md5={md5[:8]}...)")
            new_files.append({
                'path': f.path,
                'filename': f.name,
                'md5': md5,
                'size': f.size,
            })
        elif processed[f.name] != md5:
            print(f"  [MD5变更] {f.name}: {processed[f.name][:8]}... -> {md5[:8]}...")
            new_files.append({
                'path': f.path,
                'filename': f.name,
                'md5': md5,
                'size': f.size,
            })
        else:
            print(f"  [跳过] {f.name} (md5={md5[:8]}... 未变更)")
    return new_files


import contextlib

def print_memory_usage(label=""):
    """打印 driver 当前 RSS + 峰值 RSS + JVM 堆使用，便于监控内存累积/释放"""
    # 当前 RSS (用 psutil 或 /proc/self/status)
    rss_now = None
    try:
        import psutil
        rss_now = psutil.Process().memory_info().rss / 1024 / 1024 / 1024
    except ImportError:
        try:
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        kb = int(line.split()[1])
                        rss_now = kb / 1024 / 1024
                        break
        except Exception:
            pass
    except Exception:
        pass
    if rss_now is not None:
        print(f"  [Python RSS{label}] 当前={rss_now:.2f}GB", end="")
    else:
        print(f"  [Python RSS{label}] 当前=读取失败", end="")

    # 峰值 RSS (ru_maxrss 只增不减，但能看到本次跑批最高水位)
    try:
        import resource
        peak_gb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024
        print(f" | 峰值={peak_gb:.2f}GB", end="")
    except Exception:
        pass

    # JVM 堆
    try:
        import jpype
        if jpype.isJVMStarted():
            Runtime = jpype.JClass("java.lang.Runtime")
            runtime = Runtime.getRuntime()
            jvm_used_gb = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024 / 1024
            jvm_max_gb = runtime.maxMemory() / 1024 / 1024 / 1024
            print(f" | JVM 堆={jvm_used_gb:.2f}/{jvm_max_gb:.2f}GB", end="")
    except Exception as e:
        print(f" | JVM 堆=读取失败({e})", end="")
    print()


def release_memory():
    """文件处理完后释放累积的内存：Spark 缓存 + JVM GC + Python GC"""
    try:
        spark.catalog.clearCache()
    except Exception:
        pass
    try:
        import jpype
        if jpype.isJVMStarted():
            System = jpype.JClass("java.lang.System")
            System.gc()
    except Exception:
        pass
    import gc
    gc.collect()


@contextlib.contextmanager
def open_access_connection(filepath):
    """打开 Access 文件,yield (table_names, conn)。

    用 jaydebeapi 在 driver 进程内启 jpype JVM 连 Access。
    driver 能访问 /Volumes/ FUSE 挂载,executor 不能 — 所以不能用 spark.read.jdbc。
    pandas 路径: pd.read_sql 在 driver 上读,数据进 Python 内存,但 driver 16GB 够用。
    """
    volume_path = strip_dbfs_uri(filepath)
    jdbc_url = f"jdbc:ucanaccess://{volume_path}{JDBC_PARAMS}"
    print(f"  JDBC URL: {jdbc_url}")

    import jpype
    if not jpype.isJVMStarted():
        classpath = os.pathsep.join(ALL_JARS_LOCAL)
        jvm_path = jpype.getDefaultJVMPath()
        print(f"  启动 JVM: {jvm_path} (-Xmx{JVM_HEAP_GB}g)")
        print(f"  Classpath: {classpath}")
        jpype.startJVM(jvm_path, f"-Xmx{JVM_HEAP_GB}g", classpath=classpath)
    else:
        print("  JVM 已启动，动态加载 JAR...")
        for jar in ALL_JARS_LOCAL:
            jpype.addClassPath(jar)

    import jaydebeapi
    conn = jaydebeapi.connect(JDBC_DRIVER, jdbc_url)

    try:
        metadata = conn.jconn.getMetaData()
        tables_result = metadata.getTables(None, None, None, ["TABLE"])
        table_names = []
        while tables_result.next():
            table_names.append(tables_result.getString("TABLE_NAME"))

        print(f"  发现 {len(table_names)} 个表: {table_names}")
        yield table_names, conn
    finally:
        try:
            conn.close()
        except Exception:
            pass


def process_one_file(file_info):
    filepath = file_info['path']
    filename = file_info['filename']
    md5 = file_info['md5']
    size_mb = file_info['size'] / 1024 / 1024

    print(f"\n处理文件: {filename} ({size_mb:.1f} MB)")
    print_memory_usage("-开始")

    import gc
    try:
        # pandas 路径: pd.read_sql 在 driver 上读,数据进 Python 内存。
        # spark.read.jdbc 走不通 (executor 看不到 /Volumes/,/dbfs/tmp/ 也不可用)。
        # 内存优化: astype(str) 向量化替代 map(lambda) 行级调用; del+gc 每表后释放。
        table_counts_map = {}  # {orig_table_name: row_cnt} 用于写 meta, 供预警脚本做近两次成功解析差异对比
        with open_access_connection(filepath) as (table_names, conn):
            for table in table_names:
                try:
                    pdf = pd.read_sql(f"SELECT * FROM [{table}]", conn)
                except Exception as e:
                    print(f"    {table}: 读取失败 - {e}")
                    continue
                print(f"    {table}: {len(pdf)} 行, {len(pdf.columns)} 列")

                # 清洗表名：Delta 表名只允许 ASCII 字母+数字+下划线
                table_name_raw = str(table).lower()
                table_name_clean = re.sub(r'[^a-zA-Z0-9_]', '_', table_name_raw)
                table_name_clean = re.sub(r'_+', '_', table_name_clean).strip('_')
                if table_name_clean != table_name_raw:
                    print(f"  表名清洗: {table_name_raw} -> {table_name_clean}")
                delta_table_name = f"{FULL_SCHEMA}.{TABLE_PREFIX}{table_name_clean}"
                print(f"  写入 Delta 表: {delta_table_name}")

                # 清洗列名：Delta 不允许列名含 ' ,;{}()\n\t=/#'
                invalid_chars = r'[ ,;{}()\n\t=/#]'
                rename_map = {}
                for col in pdf.columns:
                    new_col = re.sub(invalid_chars, '_', str(col))
                    new_col = re.sub(r'_+', '_', new_col).strip('_')
                    if new_col != str(col):
                        rename_map[str(col)] = new_col
                if rename_map:
                    pdf = pdf.rename(columns=rename_map)
                    print(f"  列名清洗: {len(rename_map)} 列需要重命名")

                # 处理重复列名（Delta 不允许重复列名）
                seen = {}
                final_rename = {}
                for col in pdf.columns:
                    if col in seen:
                        seen[col] += 1
                        final_rename[col] = f"{col}_{seen[col]}"
                    else:
                        seen[col] = 1
                if final_rename:
                    pdf = pdf.rename(columns=final_rename)
                    print(f"  重复列名处理: {final_rename}")

                # 清洗数据类型：非数值列强制转 string，处理 Java 对象/混合类型
                # astype(str) 向量化操作,比 map(lambda x: str(x)) 快且省内存
                numeric_dtypes = ['int64', 'int32', 'float64', 'float32', 'bool']
                for col in pdf.columns:
                    if str(pdf[col].dtype) not in numeric_dtypes:
                        pdf[col] = pdf[col].astype(str).where(pdf[col].notna(), None)

                # 全 NULL 列填充空字符串：Parquet 会优化掉全 NULL 列导致 schema 不匹配
                all_null_cols = []
                for col in pdf.columns:
                    if pdf[col].isna().all():
                        pdf[col] = ""
                        all_null_cols.append(col)
                if all_null_cols:
                    print(f"  全 NULL 列填充空字符串: {all_null_cols}")

                # 写入前类型检查
                print(f"  写入前类型检查:")
                for col in pdf.columns:
                    print(f"    {col}: {pdf[col].dtype} (非空={pdf[col].notna().sum()}, 空={pdf[col].isna().sum()})")

                # 先记录行数，再用 sdf 创建+写入；写完立即释放 pandas，避免双倍内存
                row_cnt = len(pdf)
                table_counts_map[str(table)] = row_cnt
                sdf = spark.createDataFrame(pdf, samplingRatio=0.1)
                del pdf
                gc.collect()

                sdf.write.mode(WRITE_MODE).option("overwriteSchema", "true").saveAsTable(delta_table_name)
                print(f"  OK {delta_table_name}: {row_cnt} 行已写入")
                del sdf
                release_memory()

        # with 块退出后连接已关闭、本地副本已删；写 meta + 归档
        table_counts_json = json.dumps(table_counts_map, ensure_ascii=False)
        meta_data = [(filename, md5, datetime.now(), ",".join(str(t) for t in table_names), table_counts_json, "success")]
        meta_sdf = spark.createDataFrame(meta_data, ["filename", "md5", "processed_at", "table_names", "table_counts", "status"])
        meta_sdf.write.mode("append").saveAsTable(META_TABLE)

        # 归档文件（无论哪种模式，处理完都归档到 processed/，避免 UPLOAD_DIR 残留文件冲突）
        dest = PROCESSED_DIR + filename
        dbutils.fs.mv(filepath, dest)
        print(f"  已归档到: {dest}")
        if FORCE_RELOAD:
            print(f"  (FORCE_RELOAD 模式: 下次想重跑需把文件从 processed/ 移回 UPLOAD_DIR 或重新上传)")
        print_memory_usage("-结束")
        release_memory()
        return True

    except Exception as e:
        print(f"  FAIL: {e}")
        meta_data = [(filename, md5, datetime.now(), "", "", f"failed: {str(e)[:200]}")]
        meta_sdf = spark.createDataFrame(meta_data, ["filename", "md5", "processed_at", "table_names", "table_counts", "status"])
        meta_sdf.write.mode("append").saveAsTable(META_TABLE)
        return False


def cleanup_processed_dir(keep=3):
    """清理 processed/ 目录，只保留最近修改时间的 keep 个文件，多余删除"""
    try:
        files = dbutils.fs.ls(PROCESSED_DIR)
    except Exception:
        print(f"  processed 目录为空或不存在，跳过清理")
        return 0

    access_files = [f for f in files
                    if f.name.endswith('.accdb') or f.name.endswith('.mdb')]
    if len(access_files) <= keep:
        print(f"  processed 目录共 {len(access_files)} 个文件，<= {keep}，无需清理")
        return 0

    # modificationTime 是毫秒，按时间倒序
    sorted_files = sorted(access_files, key=lambda f: f.modificationTime, reverse=True)
    to_keep = sorted_files[:keep]
    to_delete = sorted_files[keep:]

    print(f"  processed 目录共 {len(access_files)} 个文件，保留最近 {keep} 个，删除 {len(to_delete)} 个:")
    for f in to_keep:
        print(f"    [保留] {f.name} (mtime={datetime.fromtimestamp(f.modificationTime/1000).strftime('%Y-%m-%d %H:%M:%S')})")
    for f in to_delete:
        print(f"    [删除] {f.name} (mtime={datetime.fromtimestamp(f.modificationTime/1000).strftime('%Y-%m-%d %H:%M:%S')})")
        dbutils.fs.rm(f.path, recurse=True)
    return len(to_delete)


# --- 主流程 ---
print("=" * 60)
print("Access (.accdb) → Databricks Delta 加载任务")
print(f"模式: {'FORCE_RELOAD (全量重跑)' if FORCE_RELOAD else '增量模式'}")
print("=" * 60)

new_files = list_new_files()

if not new_files:
    print("\n没有新的或变更的 Access 文件需要处理。")
else:
    print(f"\n发现 {len(new_files)} 个待处理文件:")
    for f in new_files:
        print(f"  {f['filename']} ({f['size'] / 1024 / 1024:.1f} MB)")

    success_count = 0
    fail_count = 0
    for file_info in new_files:
        ok = process_one_file(file_info)
        if ok:
            success_count += 1
        else:
            fail_count += 1

    print(f"\n{'=' * 60}")
    print(f"处理完成: 成功 {success_count}, 失败 {fail_count}")
    print(f"{'=' * 60}")

# 清理 processed/ 目录，只保留最近 N 个文件
print(f"\n清理 processed/ 目录 (保留最近 {PROCESSED_KEEP} 个):")
deleted = cleanup_processed_dir(keep=PROCESSED_KEEP)
if deleted:
    print(f"  共删除 {deleted} 个旧文件")

print("\n历史处理记录:")
display(spark.table(META_TABLE))