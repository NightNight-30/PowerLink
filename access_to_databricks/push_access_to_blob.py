#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""方案A: 应用端服务器推送 Access (.accdb) 文件到 Azure Blob (Databricks UC Volume 背后存储)

在应用端服务器上运行。扫描本地 .accdb 文件,上传到 abfss 容器对应 Volume 子目录。
Databricks 侧 notebook_merged.py 继续从 Volume 扫描, 自动处理 + MD5 去重。

依赖:
    pip install azure-storage-blob

配置:
    改下文 AZURE_CONNECTION_STRING / CONTAINER_NAME / BLOB_PREFIX / LOCAL_DIR

执行:
    python push_access_to_blob.py
"""

import os
import hashlib
import sys

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    print("[ERROR] 缺依赖: pip install azure-storage-blob")
    sys.exit(1)


# ========== 配置 ==========

AZURE_CONNECTION_STRING = "<storage connection string>"  # Azure Portal → 存储账号 → 访问密钥
CONTAINER_NAME = "powerlinkprod"  # UC Volume 容器 (DESCRIBE VOLUME powerlink.default.env 输出 abfss://powerlinkprod@...)
BLOB_PREFIX = "env/manual/access_uploads/"  # 容器内路径前缀 (对应 /Volumes/powerlink/default/env/manual/access_uploads/)
LOCAL_DIR = "/path/to/app/server/uploads"  # 应用端 .accdb 文件所在目录
FILE_PATTERN = ".accdb"  # 只处理 .accdb (Excel 用 .xlsx, 另写脚本)


# ========== 工具函数 ==========

def compute_md5(file_path: str) -> str:
    h = hashlib.md5()
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def blob_exists_with_md5(blob_service: BlobServiceClient, container: str,
                         blob_name: str, md5: str) -> bool:
    """检查 blob 是否已存在且 MD5 一致 (避免重复上传相同文件)

    注意: 这只是优化, 不替代 Databricks 侧 access_load_meta 表的 MD5 去重。
    即便这里漏判, Databricks notebook_merged.py 还会再查 meta 表。
    """
    try:
        blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
        if not blob_client.exists():
            return False
        props = blob_client.get_blob_properties()
        return props.metadata.get('md5', '') == md5
    except Exception as e:
        print(f"  [WARN] 查 blob 元数据失败: {e}")
        return False


def upload_file(blob_service: BlobServiceClient, container: str,
                blob_name: str, file_path: str, md5: str) -> None:
    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)
    with open(file_path, 'rb') as f:
        blob_client.upload_blob(
            f, overwrite=True,
            metadata={'md5': md5, 'source_filename': os.path.basename(file_path)},
        )
    print(f"  上传完成: {blob_name} (md5={md5})")


# ========== 主流程 ==========

def main():
    if not os.path.isdir(LOCAL_DIR):
        print(f"[ERROR] 本地目录不存在: {LOCAL_DIR}")
        sys.exit(1)

    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

    files = sorted(f for f in os.listdir(LOCAL_DIR) if f.endswith(FILE_PATTERN))
    print("=" * 60)
    print(f"扫描 {LOCAL_DIR}: {len(files)} 个 {FILE_PATTERN} 文件")
    print("=" * 60)

    if not files:
        print("无文件, 退出")
        return

    stats = {'new': 0, 'skip': 0, 'fail': 0}
    for filename in files:
        local_path = os.path.join(LOCAL_DIR, filename)
        blob_name = BLOB_PREFIX + filename
        print(f"\n[{filename}]")
        try:
            md5 = compute_md5(local_path)
            print(f"  MD5: {md5}")

            if blob_exists_with_md5(blob_service, CONTAINER_NAME, blob_name, md5):
                print(f"  跳过: blob 已存在且 MD5 一致")
                stats['skip'] += 1
                continue

            upload_file(blob_service, CONTAINER_NAME, blob_name, local_path, md5)
            stats['new'] += 1
        except Exception as e:
            print(f"  ✗ 失败: {e}")
            stats['fail'] += 1

    print("\n" + "=" * 60)
    print(f"完成。新上传 {stats['new']} / 跳过 {stats['skip']} / 失败 {stats['fail']}")
    print("Databricks 侧 notebook_merged.py 会自动处理 Volume 里的新文件")


if __name__ == '__main__':
    main()
