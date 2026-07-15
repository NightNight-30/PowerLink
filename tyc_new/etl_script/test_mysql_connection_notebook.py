# -*- coding: utf-8 -*-
# 装 pymysql (executor 侧持久连接测试需要, Databricks %pip 会对整个 notebook 生效含 executor)
%pip install pymysql -q

"""【Databricks -> MySQL 直连测试 notebook】

测试 Databricks 集群能否直连下游 MySQL,分三层逐步排查:
  1. TCP 端口连通性 (Python socket, 不依赖 JDBC 驱动, 最快定位网络层)
  2. JDBC 连接 + SELECT 1 (PySpark read.jdbc, 验证驱动+认证+SSL)
  3. 读表测试 (SELECT COUNT(*), 验证读权限)

用法:
  - 在 Databricks workspace 导入此 notebook
  - compute 处于 Running 状态运行
  - 顶部 widget 填 MySQL 连接参数 (host/port/database/user/password/test_table)
  - 看输出判断哪层通/不通

MySQL JDBC 驱动 (com.mysql.cj.jdbc.Driver)
Databricks runtime 默认不自带(跟 SQL Server 驱动不一样), 需在 cluster 装驱动:
  - Cluster → Libraries → Install new → Maven
  - 坐标: com.mysql:mysql-connector-j:9.0.0  (驱动版本要 >= MySQL server 版本)
  - MySQL 8.4 server 必须用 8.4.0+ 或 9.0.0+ 驱动, 8.0.x 驱动不兼容 8.4 的 handshake 格式
    (报 Communications link failure / no packets from server, 实际是协议层解析失败)
  - 装完重启 cluster 再跑本测试
  - UC allowlist: CREATE ARTIFACT ALLOWLIST 'maven:com.mysql:mysql-connector-j:*';

结果判断:
  - TCP 不通 -> 网络层问题 (MySQL 私网 IP 路由不到 / NSG / 防火墙)
  - TCP 通但 JDBC 失败 -> 认证/SSL/驱动/权限问题
  - 全通 -> 可以用 push_to_downstream_notebook.py 的 Spark JDBC 模式直推

注意:
  - useSSL=false 跳过 SSL 握手(测试用, 生产应配 SSL)
  - serverTimezone=Asia/Shanghai 避免时区报错
  - password widget 明文显示, 生产应改用 dbutils.secrets.get(scope, key)
"""

import socket
import time
from datetime import datetime


# SCC relay 可能间歇性丢连接, 失败时重试
# retry 间隔短一点(不用等 relay cooldown, 真正问题是新连接被限流, 重试只是多给次机会)
JDBC_RETRY_COUNT = 3
JDBC_RETRY_DELAY_SEC = 5


# ========== 参数 (widget) ==========

dbutils.widgets.text("host", "", "MySQL 主机 (FQDN 或 IP)")
dbutils.widgets.text("port", "3306", "端口")
dbutils.widgets.text("database", "", "库名")
dbutils.widgets.text("user", "", "账号")
dbutils.widgets.text("password", "", "密码")
dbutils.widgets.text("test_table", "", "测试表名 (可选, 形如 ads_pw_credit_metric_df)")

HOST = dbutils.widgets.get("host").strip()
PORT = int(dbutils.widgets.get("port") or "3306")
DATABASE = dbutils.widgets.get("database").strip()
USER = dbutils.widgets.get("user").strip()
PASSWORD = dbutils.widgets.get("password")
TEST_TABLE = dbutils.widgets.get("test_table").strip()

# 生产环境密码应从 Databricks Secrets 读:
# PASSWORD = dbutils.secrets.get(scope="mysql", key="password")


print("=" * 60)
print("【Databricks -> MySQL 直连测试】")
print(f"  时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  目标: {HOST}:{PORT}/{DATABASE}")
print(f"  账号: {USER}")
print(f"  测试表: {TEST_TABLE or '(未指定)'}")
print("=" * 60)

if not HOST or not DATABASE or not USER or not PASSWORD:
    print("\n[ERROR] 请填全 host / database / user / password 四个参数")
    dbutils.notebook.exit("missing params")


# ========== 1. TCP 端口连通性 ==========

def test_tcp(host, port, timeout=10):
    """测 TCP 端口是否能通(最快定位网络层问题)"""
    print(f"\n[1/3] TCP 端口测试: {host}:{port} (超时 {timeout}s)")
    start = time.time()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        elapsed = time.time() - start
        print(f"  [OK] TCP 通 (耗时 {elapsed:.3f}s)")
        return True
    except socket.timeout:
        print(f"  [FAIL] TCP 超时 - 网络不通或防火墙拦截")
        return False
    except ConnectionRefusedError:
        print(f"  [FAIL] 连接被拒 - 端口未开或 MySQL 未监听 {port}")
        return False
    except socket.gaierror as e:
        print(f"  [FAIL] DNS 解析失败: {e}")
        return False
    except Exception as e:
        print(f"  [FAIL] {type(e).__name__}: {e}")
        return False
    finally:
        try:
            sock.close()
        except Exception:
            pass


# ========== 1b. executor 侧 TCP 端口连通性 ==========

def test_tcp_from_executor(host, port, timeout=10):
    """从 Spark executor/server 跑 TCP + 读 MySQL 握手包 (Spark Connect 兼容, 用 UDF)
    Spark Connect 模式下 spark.sparkContext 不可用, 用 udf + spark.range 跑在 server 侧
    raw socket 只做 TCP 三次握手, 不代表 MySQL 会发握手包
    本测试 TCP connect 后 recv 读 MySQL Initial Handshake Packet (服务端先发)
    如果 TCP 通但 recv 超时, 说明 MySQL 没回握手包 (或返回路径被 SCC relay 切)
    """
    print(f"\n[1b/3] executor 侧 TCP + MySQL 握手包测试: {host}:{port} (超时 {timeout}s)")
    try:
        from pyspark.sql.functions import udf, lit
        from pyspark.sql.types import StringType

        def _socket_and_handshake_udf(host_str, port_int):
            import socket
            import time as _time
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                start = _time.time()
                sock.connect((host_str, int(port_int)))
                tcp_elapsed = _time.time() - start
                # MySQL 协议: 服务端先发 Initial Handshake Packet, 读前 100 字节
                data = sock.recv(100)
                sock.close()
                if len(data) > 0:
                    # MySQL 包头: 3字节payload长度 + 1字节sequence_id
                    # 第5字节(data[4])才是协议版本(通常 10 = HandshakeV10)
                    proto_ver = data[4] if len(data) > 4 else -1
                    return f"OK tcp={tcp_elapsed:.3f}s handshake={len(data)}B proto_ver={proto_ver}"
                else:
                    return f"FAIL tcp={tcp_elapsed:.3f}s 但 MySQL 没回握手包 (连接被关, 0 字节)"
            except socket.timeout:
                return "FAIL TCP 通但读 MySQL 握手包超时 (服务端没发包 或 返回路径被切)"
            except Exception as e:
                return f"FAIL {type(e).__name__}: {e}"

        socket_udf = udf(_socket_and_handshake_udf, StringType())
        result_df = spark.range(1).select(socket_udf(lit(host), lit(port)).alias("result"))
        result = result_df.collect()[0]["result"]
        if result.startswith("OK"):
            print(f"  [OK] executor 侧 TCP + MySQL 握手包都通 ({result})")
            return True
        else:
            print(f"  [FAIL] {result}")
            if "没回握手包" in result or "超时" in result:
                print("  -> 判断: TCP 握手通但 MySQL 不发握手包 (或返回路径被 SCC relay 切)")
                print("     这是 mysql_protocol 层问题, 不是纯网络层")
                print("     排查: SCC relay 是否做 DPI / MySQL host_cache / max_connect_errors")
            return False
    except Exception as e:
        print(f"  [FAIL] executor 测试任务失败: {type(e).__name__}: {e}")
        return False


# ========== 2. JDBC 连接 + SELECT 1 ==========

def make_jdbc_url(host, port, database):
    """MySQL JDBC URL
    注意: characterEncoding 填 Java 字符集名(UTF-8), 不是 MySQL 字符集名(utf8mb4)
          MySQL 服务端是 utf8mb4, 驱动内部会自动映射 UTF-8 → utf8mb4
    allowPublicKeyRetrieval=true: MySQL 8 默认 caching_sha2_password, useSSL=false 时
        cache miss 需取服务器公钥加密密码, 默认 false 会失败(连接被关, 报 Communications link failure)
    """
    return (f"jdbc:mysql://{host}:{port}/{database}"
            f"?useUnicode=true&characterEncoding=UTF-8"
            f"&useSSL=false&allowPublicKeyRetrieval=true"
            f"&serverTimezone=Asia/Shanghai"
            f"&connectTimeout=15&socketTimeout=60")


def test_jdbc(host, port, database, user, password):
    """测 JDBC 能否连上 + 跑 SELECT 1, 返回 (ok, category)
    SCC relay 间歇性丢连接, 失败重试 JDBC_RETRY_COUNT 次
    """
    print(f"\n[2/3] JDBC 连接测试: SELECT 1 (重试 {JDBC_RETRY_COUNT} 次)")
    jdbc_url = make_jdbc_url(host, port, database)
    print(f"  URL: {jdbc_url}")
    print(f"  driver: com.mysql.cj.jdbc.Driver")
    last_category = "other"
    for attempt in range(1, JDBC_RETRY_COUNT + 1):
        try:
            df = (spark.read
                  .format("jdbc")
                  .option("driver", "com.mysql.cj.jdbc.Driver")
                  .option("url", jdbc_url)
                  .option("user", user)
                  .option("password", password)
                  .option("query", "SELECT 1 AS test_col")
                  .option("fetchsize", 1)
                  .load())
            row = df.first()
            print(f"  [OK] JDBC 通 (尝试 {attempt}/{JDBC_RETRY_COUNT}), 返回: {row}")
            return True, "ok"
        except Exception as e:
            err = str(e)[:2000]
            print(f"  [FAIL 尝试 {attempt}/{JDBC_RETRY_COUNT}] {type(e).__name__}")
            print(f"  错误: {err[:500]}")
            low = err.lower()
            # 1. 驱动缺失(优先判断, 别让 ssl 误匹配 classloader)
            if "classnotfoundexception" in low or "driver not found" in low or "classnotfound" in low:
                print("  -> 判断: 驱动缺失 (cluster 没装 MySQL JDBC JAR)")
                print("     修复: Cluster → Libraries → Install new → Maven → com.mysql:mysql-connector-j:9.0.0")
                print("     装完重启 cluster 再跑")
                return False, "driver_missing"
            # 1b. 字符集配置错
            if "unsupported character encoding" in low:
                print("  -> 判断: 字符集配置错 (characterEncoding 应填 UTF-8, 不是 utf8mb4)")
                return False, "encoding_config"
            # 2a. MySQL 协议层(TCP 通但 MySQL 不响应)
            if "communications link" in low:
                print("  -> 判断: MySQL 协议层 (TCP 通, MySQL 不响应) 或驱动版本不兼容")
                print("     重点检查: JDBC 驱动版本是否 >= MySQL server 版本")
                print("     (MySQL 8.4 server 必须用 mysql-connector-j:8.4.0+ 或 9.0.0+, 8.0.x 会报这个错)")
                if attempt < JDBC_RETRY_COUNT:
                    print(f"     等 {JDBC_RETRY_DELAY_SEC}s 重试...")
                    time.sleep(JDBC_RETRY_DELAY_SEC)
                last_category = "mysql_protocol"
                continue
            # 其他错误不重试(认证/SSL 等重试也没用)
            # 2b. 真网络层
            if any(k in low for k in ("connection refused", "timed out", "unknownhost", "unknown host", "socket")):
                print("  -> 判断: 网络层 (TCP/路由/DNS)")
                return False, "network"
            # 3. 认证
            if any(k in low for k in ("access denied", "denied for user", "authentication")):
                print("  -> 判断: 认证 (账号/密码错 或 host 不在白名单)")
                return False, "auth"
            # 4. SSL/TLS
            if any(k in low for k in ("ssl handshake", "ssl exception", "tls protocol", "certificate")):
                print("  -> 判断: SSL/TLS (useSSL=false 已设, 仍报错可能是驱动版本)")
                return False, "ssl"
            # 5. 库不存在
            if "unknown database" in low:
                print(f"  -> 判断: 库名错 ({database} 不存在)")
                return False, "db_missing"
            # 6. MySQL 8 caching_sha2
            if "public key" in low or "rsa" in low or "caching_sha2" in low:
                print("  -> 判断: MySQL 8 caching_sha2_password 需 allowPublicKeyRetrieval=true")
                print("     可在 JDBC URL 加 &allowPublicKeyRetrieval=true")
                return False, "auth_plugin"
            print("  -> 判断: 其他 (看错误细节)")
            return False, "other"
    print(f"  [FAIL] 重试 {JDBC_RETRY_COUNT} 次都失败")
    return False, last_category


# ========== 3. 读表测试 (可选) ==========

def test_read_table(host, port, database, user, password, table):
    """测能不能读指定表, 返回 (ok_or_None, category)
    None 表示跳过(未指定 test_table)
    SCC relay 间歇性丢连接, 失败重试 JDBC_RETRY_COUNT 次
    """
    if not table:
        print(f"\n[3/3] 跳过 (未指定 test_table)")
        return None, "skipped"
    print(f"\n[3/3] 读表测试: SELECT COUNT(*) FROM {table} (重试 {JDBC_RETRY_COUNT} 次)")
    jdbc_url = make_jdbc_url(host, port, database)
    last_category = "other"
    for attempt in range(1, JDBC_RETRY_COUNT + 1):
        try:
            df = (spark.read
                  .format("jdbc")
                  .option("driver", "com.mysql.cj.jdbc.Driver")
                  .option("url", jdbc_url)
                  .option("user", user)
                  .option("password", password)
                  .option("query", f"SELECT COUNT(*) AS cnt FROM {table}")
                  .option("fetchsize", 1)
                  .load())
            row = df.first()
            print(f"  [OK] {table} 行数: {row.cnt} (尝试 {attempt}/{JDBC_RETRY_COUNT})")
            return True, "ok"
        except Exception as e:
            err = str(e)[:2000]
            print(f"  [FAIL 尝试 {attempt}/{JDBC_RETRY_COUNT}] {type(e).__name__}: {err[:500]}")
            low = err.lower()
            if "communications link" in low:
                print("  -> 判断: MySQL 协议层 / SCC relay 丢包")
                if attempt < JDBC_RETRY_COUNT:
                    print(f"     等 {JDBC_RETRY_DELAY_SEC}s 重试...")
                    time.sleep(JDBC_RETRY_DELAY_SEC)
                last_category = "mysql_protocol"
                continue
            if "classnotfoundexception" in low or "driver not found" in low:
                return False, "driver_missing"
            if "access denied" in low or "denied for user" in low:
                return False, "auth"
            if "unknown table" in low or "doesn't exist" in low or "not exist" in low:
                return False, "table_missing"
            return False, "other"
    print(f"  [FAIL] 重试 {JDBC_RETRY_COUNT} 次都失败")
    return False, last_category


# ========== 2b. pymysql 持久连接测试 (复用单连接跑多查询) ==========

def test_pymysql_persistent(host, port, database, user, password, table, timeout=10):
    """用 pymysql 在 executor 侧开一个持久连接, 跑多个查询
    Spark JDBC 每次 read.jdbc 都开新连接, 新连接被 relay 限流就挂
    pymysql 复用单连接跑多个查询, 避开"新连接失败"问题
    如果这个通了 + Spark JDBC 读表失败, 说明推送应该用 pymysql 批量插入, 不用 Spark JDBC
    """
    print(f"\n[2b] pymysql 持久连接测试 (executor 侧, 复用单连接跑多查询)")
    try:
        from pyspark.sql.functions import udf, lit
        from pyspark.sql.types import StringType

        def _pymysql_test_udf(host_s, port_i, user_s, pwd_s, db_s, tbl_s):
            try:
                import pymysql
            except ImportError:
                return "SKIP pymysql 未安装 (executor 无外网装不了, 但 JDBC 通的话不需要 pymysql)"
            try:
                conn = pymysql.connect(
                    host=host_s, port=int(port_i), user=user_s, password=pwd_s,
                    database=db_s, connect_timeout=timeout, read_timeout=30,
                    charset="utf8mb4"
                )
                results = []
                # 查询1: SELECT 1
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 AS test_col")
                    results.append(f"SELECT 1 = {cur.fetchone()[0]}")
                # 查询2: SELECT COUNT(*) 同一连接
                if tbl_s:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT COUNT(*) AS cnt FROM {tbl_s}")
                        results.append(f"COUNT(*) = {cur.fetchone()[0]}")
                conn.close()
                return "OK " + "; ".join(results)
            except Exception as e:
                return f"FAIL {type(e).__name__}: {str(e)[:200]}"

        pymysql_udf = udf(_pymysql_test_udf, StringType())
        result_df = spark.range(1).select(
            pymysql_udf(lit(host), lit(port), lit(user), lit(password),
                       lit(database), lit(table)).alias("result")
        )
        result = result_df.collect()[0]["result"]
        if result.startswith("OK"):
            print(f"  [OK] {result}")
            print("  -> pymysql 单连接跑多个查询都通, 避开了 Spark JDBC 新连接问题")
            print("  -> 如果 Spark JDBC 读表失败但 pymysql 通, 推送改用 pymysql 批量插入")
            return True
        else:
            print(f"  [FAIL] {result}")
            return False
    except Exception as e:
        print(f"  [FAIL] pymysql 测试任务失败: {type(e).__name__}: {e}")
        return False


# ========== 主流程 ==========

CONCLUSION_TEXT = {
    "driver_missing":  "驱动缺失: cluster 没装 MySQL JDBC JAR",
    "encoding_config": "字符集配置错: characterEncoding 应填 UTF-8(不是 utf8mb4)",
    "mysql_protocol":  "MySQL 协议层: TCP 通但 MySQL 不响应(检查驱动版本>=server版本 / TRUNCATE host_cache / max_connect_errors)",
    "network":         "网络层: 路由/DNS/防火墙",
    "auth":            "认证: 账号/密码错 或 host 不在白名单",
    "ssl":             "SSL/TLS: 驱动版本或证书",
    "db_missing":      "库名错",
    "table_missing":  "表不存在或读权限问题",
    "auth_plugin":     "MySQL 8 caching_sha2_password 需 allowPublicKeyRetrieval=true",
    "skipped":         "跳过(未指定 test_table)",
    "other":           "其他异常 (看上面错误细节)",
}

tcp_ok = test_tcp(HOST, PORT)

# driver 通的话再测 executor 侧, 区分 driver/executor 网络差异
executor_tcp_ok = None
if tcp_ok:
    executor_tcp_ok = test_tcp_from_executor(HOST, PORT)

if not tcp_ok:
    print("\n" + "=" * 60)
    print("【结论】TCP 不通, JDBC 大概率也不通 (仍会试一次确认错误类型)")
    print("排查方向:")
    print(f"  1. {HOST} 是公网 IP/FQDN 还是私网 IP?")
    print("     - 私网 IP (10.x/172.16-31.x/192.168.x): Databricks managed VNet 路由不到,")
    print("       需 VNet-injected workspace + peering, 或 VPN/Azure Relay")
    print("     - 公网 IP: 检查 MySQL 防火墙/NSG 是否放行 Databricks 出站")
    print("       (SCC 无公共 IP 模式, 出站走 relay, 源 IP 不固定)")
    print(f"  2. 端口 {PORT} 是否对 Databricks 开放?")
    print("  3. MySQL 在 Azure VM -> 检查 NSG 入站规则")
    print("  4. MySQL 在 on-prem -> 检查公司防火墙 + 端口转发")
    print("  5. MySQL bind-address 是否只监听 127.0.0.1 (需改 0.0.0.0)")
    print("=" * 60)
    # 仍试 JDBC 看具体错误类型
    test_jdbc(HOST, PORT, DATABASE, USER, PASSWORD)
else:
    jdbc_ok, category = test_jdbc(HOST, PORT, DATABASE, USER, PASSWORD)
    read_ok, read_category = (None, "skipped")
    if jdbc_ok:
        read_ok, read_category = test_read_table(HOST, PORT, DATABASE, USER, PASSWORD, TEST_TABLE)
    # pymysql 持久连接测试 (复用单连接跑多查询, 对比 Spark JDBC 新连接模式)
    pymysql_ok = test_pymysql_persistent(HOST, PORT, DATABASE, USER, PASSWORD, TEST_TABLE)
    print("\n" + "=" * 60)
    if jdbc_ok and (read_ok is True or read_ok is None):
        print("【结论】直连 OK! Databricks 能直接连 MySQL。")
        print("下一步: 可以用 push_to_downstream_notebook.py 的 Spark JDBC 模式直推")
        print("       (不用 blob 中转, 不用 host 侧 pull 脚本)")
    elif jdbc_ok and read_ok is False:
        print(f"【结论】JDBC SELECT 1 通, 但读表失败: {CONCLUSION_TEXT.get(read_category, read_category)}")
        print(f"  (SELECT 1 通说明驱动+网络+认证都没问题, 读表失败是 {CONCLUSION_TEXT.get(read_category, read_category)})")
        print("  排查: 表是否存在 / 读权限 / 表数据量过大触发超时")
        if pymysql_ok:
            print("  [关键] pymysql 持久连接测试通! 说明 MySQL 没问题, 是 Spark JDBC 新连接被限流")
            print("  -> 推送方案: 改用 pymysql 批量插入 (单连接复用), 不用 Spark JDBC write")
            print("  -> 或用 blob 中转方案 A (大表更稳)")
    else:
        print(f"【结论】TCP 通但 JDBC 失败: 网络没问题, 问题在 {CONCLUSION_TEXT.get(category, category)}")
        if executor_tcp_ok is False:
            print("  [关键] server 侧 executor: TCP 通但 MySQL 不发握手包!")
            print("  -> MySQL 协议层问题: TCP 握手通, 但 MySQL 不回 Initial Handshake Packet")
            print("  -> 可能原因: SCC relay 做 DPI / MySQL host_cache 阻塞 / max_connect_errors 累积")
            print("  -> 实操: 在 MySQL 跑 TRUNCATE TABLE performance_schema.host_cache; 再试")
            print("  -> 如果还不行: 走 blob 中转方案 A, 不再死磕直连")
        elif executor_tcp_ok is True and category == "mysql_protocol":
            print("  [关键] server 侧 TCP + MySQL 握手包都通, 但 JDBC 收不到响应")
            print("  -> 网络和 MySQL 协议层都没问题, 是 JDBC 驱动层问题")
            print("  -> 可尝试: 换驱动版本 com.mysql:mysql-connector-j:9.0.0 (匹配 MySQL 8.4)")
            print("  -> 或加 &enabledTLSProtocols=TLSv1.2,TLSv1.3 看是否 TLS 版本问题")
        if pymysql_ok:
            print("  [备选] pymysql 持久连接测试通! 可改用 pymysql 批量插入推送")
        print("具体排查方向看上面 [2/3] 输出的 -> 判断 行")
    print("=" * 60)
