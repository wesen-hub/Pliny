#!/bin/bash
# scp 传输到腾讯云(expect + 密码)
# 用法: ./scp-tencent.sh 本地文件 远端路径
set -e
cd "$(dirname "$0")"
SRC="${1:?用法: scp-tencent.sh 本地文件 远端路径}"
DST="${2:?用法: scp-tencent.sh 本地文件 远端路径}"

SRC="$SRC" DST="$DST" expect <<'EOF'
set timeout 1800
spawn scp -o StrictHostKeyChecking=no $env(SRC) ubuntu@10.66.0.3:$env(DST)
expect "password:" { send "Qwert1234\r" }
expect eof
EOF
