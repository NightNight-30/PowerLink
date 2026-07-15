# -*- coding: utf-8 -*-
# 装 paramiko (SFTP 客户端, 纯 Python, Spark Connect 兼容)
%pip install paramiko -q

"""【Notebook版】方案B: Databricks 主动 SFTP 拉取应用端服务器的 Access 文件到 UC Volume

Databricks notebook SSH 到应用端服务器, 扫描指定目录的 .accdb 文件,
流式计算远端文件 MD5 (不全量下载), 查 access_load_meta 表去重,
新文件 SFTP get 到 /tmp/ 再 cp 到 Volume。
后续由 notebook_merged.py 自动解析 (已有 MD5 去重逻辑, 重复跑不会重处理)。

前置:
  1. 应用端服务器开 SSH (22) 给 Databricks VNet (跟之前开 3306 一样找客户开)
  2. 配置 SSH 用户 + 私钥 (推荐) 或密码
  3. Databricks compute 装 paramiko (本脚本 %pip install 已处理)

配置:
  改下文 SSH_HOST / SSH_USER / SSH_PASSWORD 或 SSH_PRIVATE_KEY_PATH / REMOTE_DIR

编排:
  先跑本脚本拉文件到 Volume, 再跑 notebook_merged.py 解析
  (或在 Jobs 里串成两步: pull_access → notebook_merged)
"""

import os
import hashlib
import io
import sys

try:
    import paramiko
except ImportError:
    print("[ERROR] paramiko 未装。notebook 第一个 cell 跑: %pip install paramiko")
    sys.exit(1)

from datetime import datetime
from common.spark_utils import get_spark
from pyspark.sql import functions as F


# ========== 配置 ==========

SSH_HOST = "<server IP>"
SSH_PORT = 22
SSH_USER = "<user>"
SSH_PASSWORD = "<password>"           # 二选一
SSH_PRIVATE_KEY_PATH = None            # /Workspace/Shared/.../id_rsa (推荐, 优先于密码)

REMOTE_DIR = "/path/to/app/server/uploads"  # 服务器上 .accdb 所在目录
FILE_PATTERN = ".accdb"

VOLUME_DIR = "/Volumes/powerlink/default/env/manual/access_uploads"

META_TABLE = "powerlink.pw_manual.access_load_meta"


# ========== Spark ==========
spark = get_spark()


def get_existing_md5_set() -> set:
    """从 access_load_meta 表查已处理文件的 MD5 集合"""
    try:
        df = spark.table(META_TABLE).select(F.col("md5"))
        return {row.md5 for row in df.collect()}
    except Exception as e:
        # 首次运行表不存在, 当作空集合
        print(f"[WARN] 读 meta 表失败 (可能是首次运行): {e}")
        return set()


# ========== SFTP 工具 ==========

def get_sftp_client():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print(f"连接 {SSH_HOST}:{SSH_PORT} (user={SSH_USER})...")
    if SSH_PRIVATE_KEY_PATH:
        key = paramiko.RSAKey.from_private_key_file(SSH_PRIVATE_KEY_PATH)
        client.connect(SSH_HOST, SSH_PORT, SSH_USER, pkey=key, timeout=15)
    else:
        client.connect(SSH_HOST, SSH_PORT, SSH_USER, SSH_PASSWORD, timeout=15)
    print("SSH 连接成功")
    return client.open_sftp()


def list_remote_files(sftp, remote_dir):
    """列出服务器上所有 .accdb 文件"""
    files = []
    for entry in sftp.listdir_attr(remote_dir):
        if entry.filename.endswith(FILE_PATTERN) and entry.st_size > 0:
            files.append(entry.filename)
    return sorted(files)


def compute_remote_md5(sftp, remote_path: str) -> str:
    """流式读远端文件计算 MD5 (不全量下载到本地)"""
    h = hashlib.md5()
    with sftp.open(remote_path, 'rb') as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def download_to_volume(sftp, remote_path: str, volume_path: str):
    """SFTP get 到 /tmp/ 再 dbutils.fs.cp 到 Volume (Volume 是 FUSE 挂载, SFTP 不能直接写)"""
    filename = os.path.basename(remote_path)
    tmp_path = f"/tmp/{filename}"
    print(f"  SFTP 下载到 {tmp_path} ...")
    sftp.get(remote_path, tmp_path)

    # dbutils.fs.cp 从本地 file:// 到 Volume
    print(f"  cp 到 Volume {volume_path} ...")
    dbutils.fs.cp(f"file://{tmp_path}", volume_path)
    os.remove(tmp_path)


# ========== 主流程 ==========

def main():
    print("=" * 60)
    print("【Access 文件 SFTP 拉取 Databricks Volume】")
    print(f"服务器: {SSH_HOST}:{SSH_PORT}")
    print(f"远端目录: {REMOTE_DIR}")
    print(f"目标 Volume: {VOLUME_DIR}")
    print(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    existing_md5 = get_existing_md5_set()
    print(f"已处理 MD5 数: {len(existing_md5)}")

    sftp = get_sftp_client()
    try:
        remote_files = list_remote_files(sftp, REMOTE_DIR)
        print(f"\n扫描到 {len(remote_files)} 个 {FILE_PATTERN} 文件")

        if not remote_files:
            print("无文件, 退出")
            return

        stats = {'new': 0, 'skip': 0, 'fail': 0}
        for filename in remote_files:
            remote_path = f"{REMOTE_DIR}/{filename}"
            print(f"\n[{filename}]")
            try:
                md5 = compute_remote_md5(sftp, remote_path)
                print(f"  MD5: {md5}")

                if md5 in existing_md5:
                    print(f"  跳过: MD5 已在 {META_TABLE} 中")
                    stats['skip'] += 1
                    continue

                volume_path = f"{VOLUME_DIR}/{filename}"
                download_to_volume(sftp, remote_path, volume_path)
                print(f"  拉取完成")
                stats['new'] += 1
                existing_md5.add(md5)  # 加入本次已处理集合, 同次不重拉
            except Exception as e:
                print(f"  ✗ 失败: {e}")
                stats['fail'] += 1
    finally:
        sftp.close()
        print("\nSSH 连接已关闭")

    print("\n" + "=" * 60)
    print(f"完成。新拉取 {stats['new']} / 跳过 {stats['skip']} / 失败 {stats['fail']}")
    print("下一步: 运行 notebook_merged.py 解析 Volume 里的新文件")


main()
