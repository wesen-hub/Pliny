#!/bin/bash
# 腾讯云远程执行封装(expect + ssh)
# 用法: ./remote-tencent.sh "要执行的命令" [超时秒]
set -e
cd "$(dirname "$0")"
CMD="${1:?用法: remote-tencent.sh \"命令\" [超时秒]}"
TO="${2:-60}"

CMD="$CMD" TO="$TO" expect <<'EOF'
set timeout $env(TO)
spawn ssh -o StrictHostKeyChecking=no ubuntu@111.230.154.203 $env(CMD)
expect "password:" { send "Qwert1234\r" }
expect eof
EOF
