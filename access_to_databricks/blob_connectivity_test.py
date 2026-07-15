#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最小连通性测试: 在 MySQL VM 上验证 Azure Blob 凭证 + 容器路径全对

成功条件: 上传 _connectivity_test.txt 到 manual/access_uploads/, Azure Portal 能看到。
失败排查: AuthenticationFailed → key 错; ResourceNotFound → 容器名错; 网络错 → 之前 nc 就不通了。

依赖: pip3 install azure-storage-blob
"""

import sys
from datetime import datetime

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    print("[ERROR] 缺依赖: pip3 install azure-storage-blob")
    sys.exit(1)


# ========== 配置 (从 Azure Portal / DESCRIBE VOLUME 拿) ==========

# 存储账号名 (应该跟 abfss URL 里 @ 后面 . 之前的部分一致)
ACCOUNT_NAME = "powerlinkprod"

# AccountKey (Azure Portal → 存储账号 → 访问密钥 → key1)
ACCOUNT_KEY = "<your_account_key_here>"

# 容器名 (DESCRIBE VOLUME 输出 abfss://<container>@<storage>.dfs... 里的 <container>)
# powerlink.default.env → abfss://powerlinkprod@powerlinkprod.dfs.core.chinacloudapi.cn/env
# 容器 = powerlinkprod, Volume 根路径在容器内 = /env/
CONTAINER_NAME = "powerlinkprod"

# 测试上传的 blob 路径 (对应 /Volumes/powerlink/default/env/manual/access_uploads/)
# 容器内完整路径前缀 = env/manual/access_uploads/
TEST_BLOB = "env/manual/access_uploads/_connectivity_test.txt"


# ========== 跑测试 ==========

def main():
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={ACCOUNT_NAME};"
        f"AccountKey={ACCOUNT_KEY};"
        f"EndpointSuffix=core.chinacloudapi.cn"
    )
    print(f"连接字符串: AccountName={ACCOUNT_NAME}, EndpointSuffix=core.chinacloudapi.cn")
    print(f"容器: {CONTAINER_NAME}")
    print(f"测试 blob: {TEST_BLOB}")
    print("-" * 60)

    try:
        svc = BlobServiceClient.from_connection_string(conn_str)
    except Exception as e:
        print(f"[FAIL] 构造 BlobServiceClient 失败: {e}")
        sys.exit(1)

    # 1. 列容器 (验证容器名 + 认证)
    try:
        container_client = svc.get_container_client(CONTAINER_NAME)
        blobs = list(container_client.list_blobs(name_starts_with="env/manual/access_uploads/"))
        print(f"[OK] 认证通过, 容器存在")
        print(f"     当前 env/manual/access_uploads/ 下已有 {len(blobs)} 个 blob")
    except Exception as e:
        print(f"[FAIL] 列容器失败: {e}")
        print("       常见原因: AccountKey 错 / CONTAINER_NAME 错 / 没有权限")
        sys.exit(1)

    # 2. 上传一个测试文件
    try:
        bc = svc.get_blob_client(container=CONTAINER_NAME, blob=TEST_BLOB)
        content = f"connectivity test from MySQL VM at {datetime.now().isoformat()}\n"
        bc.upload_blob(content, overwrite=True)
        print(f"[OK] 上传成功: {TEST_BLOB}")
    except Exception as e:
        print(f"[FAIL] 上传失败: {e}")
        sys.exit(1)

    # 3. 回读验证
    try:
        downloaded = bc.download_blob().readall().decode()
        print(f"[OK] 回读内容一致: {downloaded.strip()}")
    except Exception as e:
        print(f"[FAIL] 回读失败: {e}")
        sys.exit(1)

    print("-" * 60)
    print(f"全链路通。去 Databricks 跑 dbutils.fs.ls('/Volumes/powerlink_prod/default/env/manual/access_uploads/') 应该能看到 _connectivity_test.txt")

if __name__ == '__main__':
    main()
