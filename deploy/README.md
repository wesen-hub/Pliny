# 最小集群三机部署编排

> 部署全过程踩过的问题（通用运维坑 + 设计阶段验证出的缺陷）见 [问题记录.md](./问题记录.md)。

> 对应 [docs/6-分布式设计/最小集群部署演练方案.md](../../docs/6-分布式设计/最小集群部署演练方案.md) v5。
> 三机 WireGuard 内网：Mac=10.66.0.1、阿里云=10.66.0.2、腾讯云=10.66.0.3。

## 一、网络模型

| 机器 | 容器网络 | 对外暴露 |
|---|---|---|
| Mac | bridge + 端口映射(0.0.0.0) | WireGuard 接口 10.66.0.1 |
| 阿里云 | **host**（容器直占主机端口） | 10.66.0.2 |
| 腾讯云 | **host** | 10.66.0.3 |

- 云机 host 网络：容器间跨机调用 = 直接 WireGuard IP:端口，无 NAT 开销。
- Mac bridge：容器映射到 0.0.0.0，云机经 10.66.0.1:端口 访问。

## 二、三机容器分布（51 容器 + 1 RDS）

| 分组 | Mac(10.66.0.1) | 阿里云(10.66.0.2) | 腾讯云(10.66.0.3) |
|---|---|---|---|
| TiKV×3 | tikv-1 | tikv-2 | tikv-3 |
| PD×3 | pd-1 | pd-2 | pd-3 |
| etcd×3(22379) | etcd-1 | etcd-2 | etcd-3 |
| Kafka×3(KRaft) | kafka-1 | kafka-2 | kafka-3 |
| Redis 1主1从 | — | redis-主 | redis-从 |
| Sentinel×3 | sentinel-1 | sentinel-2 | sentinel-3 |
| MinIO×4 | minio-1(9000)、minio-2(9002) | minio-3(9000) | minio-4(9000) |
| Nacos×3(cluster) | nacos-1 | nacos-2 | nacos-3 |
| TiDB server | tidb-server(4002) | — | — |
| TiCDC | — | ticdc(8300) | — |
| demo TiDB(源库) | demo-tidb(4000) | — | — |
| Gateway×2 | — | gateway-1(8088) | gateway-2(8088) |
| frontend(nginx)×2 | — | nginx-1(80) | nginx-2(80) |
| SeaTunnel×3 | seatunnel-1 | seatunnel-2 | seatunnel-3 |
| backend×2 | backend-1(8080) | backend-2(8080) | — |
| outlet-service×2 | outlet-1(8095) | — | outlet-2(8095) |
| subscription×2 | sub-1(8098) | sub-2(8098) | — |
| tagengine×2 | tag-1(8100) | — | tag-2(8100) |
| asset×2 | asset-1(8099) | asset-2(8099) | — |
| delivery×2 | deliv-1(8096) | — | deliv-2(8096) |
| dcp-worker×3 | worker-1(8097) | worker-2(8097) | worker-3(8097) |
| agent | agent | — | — |
| scheduler-console | — | — | 并入 nginx-2(8085) |
| MySQL 从库 | — | — | mysql-slave(3306) |
| **合计** | **19** | **16** | **16** |

元库主库 = 阿里云 RDS MySQL（X1 公网+白名单，rm-bp19av6aigxn7190c）。

## 三、关键端口（host 网络防冲突）

- etcd 用 **22379/22380**（避开同机 PD 的 2379/2380）
- Mac 双 MinIO：9000 与 9002（API），console 9100/9102
- Gateway 8088（避开 backend 8080）；scheduler-console 8085（避开 nginx 80）
- TiDB server 4002（避开 demo-tidb 4000）

## 四、启动顺序

1. **每机**：PD×3 → TiKV×3 → TiDB server/TiCDC/demo-tidb
2. etcd×3 → Kafka×3 → Redis主/从 → Sentinel×3 → MinIO×4 → Nacos×3
3. SeaTunnel×3 → dcp-worker×3 → 控制面 12 实例 → gateway×2 → nginx×2/console → agent
4. 健康检查通过后开始 H1-H16 演练

## 五、部署步骤

```bash
# 1. 本机构建全部集群镜像(应用/网关/前端/SeaTunnel 补丁)
./deploy/build-images.sh

# 2. 镜像打包传输到云机: build-images.sh 输出 tar, scp 后 docker load

# 3. WireGuard 组网(三机各自; 密钥由 deploy/wireguard/setup.sh 生成)

# 4. 本机
cp deploy/.env.example deploy/.env   # 填 RDS_HOST/RDS_PASSWORD
docker compose --env-file deploy/.env -f deploy/compose-mac.yml up -d

# 5. 云机(scp 上传 compose + .env + 镜像 tar 后)
docker compose --env-file .env -f compose-aliyun.yml up -d   # 阿里云
docker compose --env-file .env -f compose-tencent.yml up -d  # 腾讯云

# 6. MinIO 建 bucket: seatunnel-checkpoint; 腾讯云 mysql-slave 执行 CHANGE REPLICATION SOURCE
```

## 六、内存预算（limit 上限；demo 实际占用约其 50%）

| 机 | 合计 limit |
|---|---|
| Mac 16G | ≈11.5G(含 Docker Desktop 开销) |
| 阿里云 8G | ≈7.5G |
| 腾讯云 8G | ≈7.0G |

控制面单实例 -Xmx384m；TiKV 1G；Kafka heap 512m；SeaTunnel 768m；Nacos 512m。

## 七、安全组放行清单（云机控制台配置）

| 端口 | 放行范围 | 用途 |
|---|---|---|
| 51820/udp | 三机互放 + 家宽出口 | WireGuard |
| 80/tcp | 0.0.0.0/0 | 前端公网入口 |
| 其余全部 | 仅安全组内网/不放行 | 服务绑 WireGuard IP, 不暴露公网 |

云机 host 网络下 gateway 监听 0.0.0.0:8088 仅供同机 nginx 127.0.0.1 反代, 安全组不开 8088。

## 八、RDS 前置（用户控制台操作）

1. RDS 开公网地址（申请外网连接串）
2. 白名单加：腾讯云公网 IP、家宽出口 IP（RDS 主库只走公网连接）
3. 建库 `dcp`、`dcp_dst`、`nacos_config`（nacos_config 由 Nacos 首启自动初始化 schema）
4. 开启 binlog（ROW 格式，供 mysql-slave 复制）；建复制账号 `repl`

## 九、HA 演练结论（H1-H16 全部通过）

| # | 场景 | 演练动作 | 结果 |
|----|------|----------|------|
| H1 | 控制面实例宕机 | stop 某云 backend | 网关 lb:// 多上游自动切换，请求无感 |
| H2 | ShedLock 接管 | kill 持锁实例 | ≤2m 另一实例接管对账，locked_by 变化 |
| H3 | Kafka broker 宕机 | stop 1 broker | minISR=2 生产消费不中断，副本自动补齐 |
| H4 | TiKV 宕机 | stop 1 TiKV | TiDB 读写不受影响（3 副本多数派） |
| H5 | PD 宕机 | stop 1 PD | 2/3 存活，调度正常 |
| H6 | 元库主宕机 | RDS 重启主库 | JDBC 自动重连；从库数据跟随；异步窗口由补偿收敛 |
| H7 | etcd 宕机 | stop 1 etcd | 调度框架心跳/判死/迁移正常 |
| H8 | worker 宕机 | kill 1 worker | Kafka rebalance 秒级分区转移，消费不中断 |
| H9 | 整台云机关机 | 腾讯云整机断电 | 数据面/中间件全存活，控制面双实例接管 |
| H10 | 补偿机制闭环 | 断 worker 制造 activate 失败 | 巡检按 handover-pending 判据自动补交接 |
| H11 | 源库宕机断点续传 | kill demo TiDB → 重启 | TiCDC 从保存位点续拉，不丢不重 |
| H12 | Nacos 宕机 | stop 1 Nacos | 服务发现/配置下发不受影响（2/3 多数派） |
| H13 | 调度判死迁移 | kill worker → etcd 租约过期 | TaskSpec 判死 → 迁移存活实例继续执行 |
| H14 | SeaTunnel master 宕机 | kill Mac master | 1 秒接管；2.3.12 不自动恢复 job → savepoint/restore 显式恢复 |
| H15 | SeaTunnel task 宕机 | kill 腾讯云 task | 8 秒自动重部署 + MinIO checkpoint 恢复 + Kafka 断点续采 |
| H16 | 提交入口故障切换 | kill Mac 后异地提交 | hazelcast-client 三成员 failover，任一节点宕机不影响提交 |

### SeaTunnel 2.3.12 运维要点（H14-H16 实测）

1. **master 宕机**：秒级接管，但 RUNNING job 不自动恢复 → 日常需定期 savepoint，恢复命令：
   `seatunnel.sh -c job.conf -r {jobId}`（-c 与 -r 必须同时给；恢复后同一 jobId、checkpoint 编号连续）
2. **task 宕机**：JobMaster 自动 "Wait 3s and then restore the pipeline"，从 MinIO checkpoint 恢复（isRestored=true）；Kafka source 从 committed offset 续读不重不漏
3. **提交入口**：客户端走 `/opt/config/hazelcast-client.yaml`（三成员 mac.wg/10.66.0.2/10.66.0.3:5801），任一节点宕机自动 failover；引擎成员走 `hazelcast.yaml`（cluster-members 三节点）
4. **checkpoint 配置**（seatunnel.yaml）：`storage.type: hdfs`（SPI factoryIdentifier 只有 hdfs/localfile）+ `plugin-config.storage.type: s3` + `s3.bucket: s3a://seatunnel-checkpoint`（s3a 前缀才走 S3AFileSystem）
5. **内存**：容器 mem_limit 1536m——引擎 heap 192m + 提交/savepoint 客户端 JVM 同容器运行，768m 触发 cgroup OOM 杀客户端（exit 137）
6. **Mac 容器连通**：extra_hosts `mac.wg:host-gateway`（容器内直连 10.66.0.1 utun 黑洞）；云机侧不变
