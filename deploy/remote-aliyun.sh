#!/bin/bash
# 阿里云远程执行封装(密钥认证)
set -e
cd "$(dirname "$0")"
CMD="${1:?用法: remote-aliyun.sh \"命令\" [超时秒]}"
TO="${2:-60}"
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -i ~/.ssh/id_aliyun ubuntu@112.124.6.125 "$CMD"
