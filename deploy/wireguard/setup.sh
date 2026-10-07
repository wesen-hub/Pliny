#!/bin/bash
# WireGuard 三机组网: 本机生成密钥与三份 wg0.conf + 云机安装脚本
# 用法: ./setup.sh [家宽公网IP(可选, 云机 Endpoint 兜底)]
set -e
cd "$(dirname "$0")"
GEN=gen
mkdir -p "$GEN"

WG_PORT=51820
MAC_IP=10.66.0.1
ALI_IP=10.66.0.2
TENCENT_IP=10.66.0.3
# 云机公网 IP(从部署现场确认; 阿里云 ECS 弹性公网 / 腾讯云 CVM 公网)
ALI_PUB=112.124.6.125
TENCENT_PUB=111.230.154.203
HOME_PUB="${1:-}"

# 1. 生成三对密钥
for h in mac aliyun tencent; do
  [ -f "$GEN/$h.key" ] || { umask 077; wg genkey > "$GEN/$h.key"; }
  wg pubkey < "$GEN/$h.key" > "$GEN/$h.pub"
done
MAC_KEY=$(cat $GEN/mac.key);     MAC_PUBKEY=$(cat $GEN/mac.pub)
ALI_KEY=$(cat $GEN/aliyun.key);  ALI_PUBKEY=$(cat $GEN/aliyun.pub)
TEN_KEY=$(cat $GEN/tencent.key); TEN_PUBKEY=$(cat $GEN/tencent.pub)

# 2. Mac 配置(对两云机 Peer; 家宽 NAT 侧 keepalive 保活)
cat > "$GEN/wg0-mac.conf" <<EOF
[Interface]
PrivateKey = $MAC_KEY
Address = $MAC_IP/24
ListenPort = $WG_PORT

[Peer] # aliyun
PublicKey = $ALI_PUBKEY
Endpoint = $ALI_PUB:$WG_PORT
AllowedIPs = $ALI_IP/32
PersistentKeepalive = 25

[Peer] # tencent
PublicKey = $TEN_PUBKEY
Endpoint = $TENCENT_PUB:$WG_PORT
AllowedIPs = $TENCENT_IP/32
PersistentKeepalive = 25
EOF

# 2b. Mac 纯 wg 格式(去 Address 行, 供 wg setconf 手动流程使用; Address 由 ifconfig 单独配)
grep -v '^Address' "$GEN/wg0-mac.conf" > "$GEN/wg0-mac-pure.conf"

# 3. 阿里云配置(Mac 家宽侧不填 Endpoint, 靠首包学习 NAT 映射)
cat > "$GEN/wg0-aliyun.conf" <<EOF
[Interface]
PrivateKey = $ALI_KEY
Address = $ALI_IP/24
ListenPort = $WG_PORT

[Peer] # mac
PublicKey = $MAC_PUBKEY
AllowedIPs = $MAC_IP/32

[Peer] # tencent
PublicKey = $TEN_PUBKEY
Endpoint = $TENCENT_PUB:$WG_PORT
AllowedIPs = $TENCENT_IP/32
PersistentKeepalive = 25
EOF

# 4. 腾讯云配置
cat > "$GEN/wg0-tencent.conf" <<EOF
[Interface]
PrivateKey = $TEN_KEY
Address = $TENCENT_IP/24
ListenPort = $WG_PORT

[Peer] # mac
PublicKey = $MAC_PUBKEY
AllowedIPs = $MAC_IP/32

[Peer] # aliyun
PublicKey = $ALI_PUBKEY
Endpoint = $ALI_PUB:$WG_PORT
AllowedIPs = $ALI_IP/32
PersistentKeepalive = 25
EOF

# 5. 生成各机启动说明 + 云机安装脚本
cat > "$GEN/README.txt" <<EOF
==== WireGuard 三机组网(密钥已生成于 $GEN) ====

【Mac 本机】
  sudo cp $GEN/wg0-mac.conf /etc/wireguard/wg0.conf
  sudo wg-quick up wg0        # 每次开机: sudo wg-quick up wg0

【阿里云】
  scp $GEN/wg0-aliyun.conf ubuntu@$ALI_PUB:/tmp/wg0.conf
  scp $GEN/install-remote.sh ubuntu@$ALI_PUB:/tmp/
  ssh ubuntu@$ALI_PUB "sudo bash /tmp/install-remote.sh"

【腾讯云】
  scp $GEN/wg0-tencent.conf ubuntu@$TENCENT_PUB:/tmp/wg0.conf
  scp $GEN/install-remote.sh ubuntu@$TENCENT_PUB:/tmp/
  ssh ubuntu@$TENCENT_PUB "sudo bash /tmp/install-remote.sh"

【连通验证(三机互 ping)】
  ping -c2 $ALI_IP && ping -c2 $TENCENT_IP
  # 云机上: ping -c2 $MAC_IP $TENCENT_IP
EOF

# 云机安装脚本模板
cat > "$GEN/install-remote.sh" <<'EOF'
#!/bin/bash
set -e
if ! command -v wg >/dev/null; then
  apt-get update -qq && apt-get install -y -qq wireguard-tools
fi
mkdir -p /etc/wireguard
cp /tmp/wg0.conf /etc/wireguard/wg0.conf
chmod 600 /etc/wireguard/wg0.conf
# 系统 IP 转发(云机作为转发面时; host 网络容器流量)
sysctl -w net.ipv4.ip_forward=1
grep -q '^net.ipv4.ip_forward=1' /etc/sysctl.conf || echo 'net.ipv4.ip_forward=1' >> /etc/sysctl.conf
systemctl enable --now wg-quick@wg0 2>/dev/null || wg-quick up wg0
echo "==> WireGuard up:"; wg show
EOF

chmod +x "$GEN/install-remote.sh"
echo "==> 完成。请按 $GEN/README.txt 执行三机安装。"
cat "$GEN/README.txt"
