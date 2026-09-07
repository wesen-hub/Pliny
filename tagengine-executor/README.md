# tagengine-executor —— 打标引擎 P0 批执行器(独立 Spark 工程)

与 `agent/` 平级的独立 Maven shade 工程(非 Spring), 实现打标引擎批执行(设计文档
`~/Desktop/数据采集平台V3文档/数据采集平台V3-打标引擎设计-2026-09-08.md` §12 细化)。
P0 仅批; 流/在线执行器 P1。

## 定位

- 平台侧把任务发布成 **task-json**(与后端 `tag_task`/`tag_rule` 结构对齐)落盘, 触发本作业。
- 执行流水线: 读数据集(湖表 `spark.table` / 联调 `spark.read` 抽象) → 派生字段装配
  → 动态 Fact(schema 类型)源码生成 + DRL 生成, Fact 源码与每资产 DRL 一并入 KieModule 由 KieBuilder 编译
  → broadcast → 逐分区
  StatefulKieSession 求值(平铺规则 → 规则流 Java 控制流) → 写 tag_fact(task_id 分区
  当前值覆盖, Iceberg time travel 留史)。
- 提交形态: spark-submit(运行时提供 spark + iceberg-spark-runtime; 见「运行」)。

## 目录结构

```
tagengine-executor/
├ pom.xml / Dockerfile / settings-docker.xml
├ README.md
└ src/main/java/com/dcp/tagjob/
  ├ TagBatchJob.java            主类: CLI/装配/编排/摘要(退出码 0/1/2)
  ├ PlanAssembler.java          driver 计划装配: 资产表/顺序/DRL 生成/Fact 源码生成/KieBase 编译校验
  ├ SmokeMain.java              引擎核心自检(无 Spark, Docker build 阶段执行)
  ├ config/                     task-json 模型 + CLI
  ├ plan/                       广播模型 BuildPlan/AssetPlan(纯 POJO)
  ├ fact/                       动态 Fact 源码生成(Java 源入 KieModule 由 KieBuilder 编译, 无 javax.tools)
  ├ drl/                        DrlGenerator(visual/table→DRL) + KieBaseFactory(编译工厂: KieModule 内联 Fact 源, 返回 KieBase + moduleClassLoader)
  ├ run/                        TagActions(动作约定) / RowSink / ExecutorRuntimeCache / PartitionEvaluator / MatrixAccumulator
  ├ spark/                      DatasetAssembly(读表/派生/schema 分类) + TagFactWriter(tag_fact 当前值写)
  └ util/                       Log
```

## 构建

```bash
cd tagengine-executor
docker build -t tagengine-executor:check .   # 多阶段: mvn package → SmokeMain 自检 → BUILD SUCCESS
# 产物: target/tagengine-executor.jar(drools+jacksion+本模块 shade; spark/iceberg 为 provided)
```

## 运行(task-json 示例见 examples/task-demo.json)

```bash
# 试跑(采样 1000 + 命中矩阵 JSON; 不写 tag_fact)
java -jar target/tagengine-executor.jar --task-json examples/task-demo.json --master local[*] --limit 1000 --matrix --matrix-out /tmp/matrix.json

# 正式批(写 tag_fact)
java -jar target/tagengine-executor.jar --task-json examples/task-demo.json --master local[*]

# spark-submit(集群/dev E2E; spark 由运行时提供, iceberg runtime 用 --packages)
spark-submit --master yarn \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1 \
  tagengine-executor.jar --task-json /path/task.json
```

参数: `--task-json`(必填) / `--master`(默认 local[*], 环境变量 SPARK_MASTER) /
`--iceberg-catalog|namespace|table`(读数据集覆盖, 环境变量 ICEBERG_*) /
`--fact-*`(tag_fact 覆盖) / `--limit N` 采样 / `--matrix` 试跑不写 / `--matrix-out <file>`。

## 动作约定(用户 DRL 的 API; 与前端「从可视化/决策表生成 DRL」同款)

```drl
rule "示例"
  when
    $f : Fact(net_amt > 50000 && status != "black")
  then
    tag($f, "订单/净支付额", "超限");   // 布尔标签第三参传 null: tag($f, "风控/黑名单", null)
    // halt();                          // 停止当前资产组求值(平铺阶段跳过剩余平铺规则; 规则流仍照跑)
end
```

约束:
- **手写 DRL 只写 rule 块**, 不要带 `package`/`import`/`function`(执行器会自动注入
  `package dcp.tagjob.asset.a{id}_v{ver}` + Fact import + `tag/halt` function 头; 包内 dialect 为 mvel)。
- Fact 字段名 = 数据集字段 ∪ 派生字段名(须 Java 标识符; schema 类型驱动: 数值列 long/double,
  布尔 boolean, 字符串及日期时间序列化为 string —— 日期时间按 ISO 字符串比较/contains)。
- `tag`/`halt` 为保留函数名; 规则名自定义(资产按包隔离, 不冲突)。
- 决策表 = 排他组(activation-group + salience, 行序越靠前越优先 + 兜底最低)。
- 可视化语义(与设计 §3/UI「组间: 全部满足」一致): 组内按组 logic(AND/OR)合并, 组间全部满足(AND)。
  注: 前端 mock「从可视化生成 DRL」把组间拼成 `||`, 与 UI 提示矛盾; 执行器以设计文档 + UI 提示为准。
- 条件操作符: `== != > >= < <= in like null`(like=不区分大小写 contains; in 集合转等值链)。

## 语义决策(§11 已拍决策的执行器落地)

| 主题 | 实现 |
|---|---|
| 执行序 | bindings 数组序, 但规则先跑 → 规则流后跑(§3b); 平铺规则内 halt 跳剩余, 规则流照跑 |
| 终止粒度 | 规则流 stop/end 只结束当前流(§11①), 继续下一个绑定流; 嵌套流终止只作用于嵌套流 |
| hit/miss 分支 | next/stop/end/jump[+N]: jump 为**相对前跳 N 步**(UI 默认 1; 越界=流自然结束) |
| 决策表排他 | 同表同 activation-group(资产品质名) + 行序 salience 递减 + 兜底最低 salience 恒真 |
| halt()/停止 | TagActions: 行级 halt 标志 + 尽力 `KieSession.halt()`(同 drools.halt()); 每次 fire 后消费 |
| 命中定义 | 资产命中 = 该资产至少执行一条动作(tag 或 stop/halt 动作); 流命中 = 链内至少一条动作(§3) |
| agenda 过滤 | 按资产包名(package)过滤 —— 手写 DRL 规则名不可预测, 包名精确放行 |
| 动态 Fact | 简单名固定 Fact, 包 = `dcp.tagjob.fact.t{taskId}_v{taskVer}`(包级隔离防冲突); 包装类型可判空; Java 源码与资产 DRL 一并入 KieModule 由 KieBuilder 编译(driver/executor 同一 KieBaseFactory 路径), 实例化用 moduleClassLoader —— 无 TCCL/无 javax.tools |
| Fact 装配真相 | datasetFields 与真实 schema 交集校验(缺失→任务失败); 派生后 schema 分类为真相(§12) |
| 写 tag_fact | task_id 分区: 不存在→writeTo.create(partitioned); 存在→DELETE 本任务旧分区→append(iceberg); 非事务湖回退过滤重写; (entity,tag) 去重 |

## 与设计文档的偏差(切片③编码时定)

1. 手写 DRL 与生成 DRL 统一注入包头; Fact 简单名固定为 `Fact`(文档建议 `Fact{taskVer}_{hash}`
   命名, 冲突风险改由**包级隔离**解决 —— 手写 DRL 才能原样直跑 `Fact(...)`/`tag($f,...)`)。
2. 派生时间窗: 本期不做跨天窗口回看, 聚合=全表/近 N 天 `groupBy(entityKey).agg(func(field))` 实现,
   时间窗仅做 `windowColumn >= now-days` 预过滤, 留 TODO(E2E 细化)。
3. 可视化组间语义按 §3「组间全满足」实现(前端生成 DRL mock 拼 `||`, 见上表注)。
4. Iceberg 依赖按 provided 接入; 模块**零 iceberg import**(spark.table/catalog + DataSource 抽象 +
   SQL DELETE), 运行时由 spark-submit `--packages` 提供 provider —— 接湖只换运行时装配, 不动代码。
5. 试跑/命中矩阵仅 CLI 侧(后端试跑接口接线 TODO)。

## 遗留 TODO(E2E 与后续)

- [ ] E2E: 本地 dev Spark+Iceberg 组合跑通全链路(读湖表→求值→tag_fact 分区覆盖 + time travel 验证)。
- [ ] 平台触发接线: 后端发布 → task-json 落盘 → spark-submit; run 审计回写(mysql-connector-j 未引, pom 留位)。
- [ ] 试跑接口: 后端 API 调 `--matrix`(本期 CLI 已完成, 输出 JSON 待后端消费)。
- [ ] 流式输出: 大分区标签缓冲内存简化(PartitionEvaluator 单分区收集, 见代码注释)。
- [ ] decimal 精度: Fact 统一降级 double(打标档位够用); 精确口径(如金额累加比对)后续按 BigDecimal 事实列扩展。
- [ ] 时间窗聚合细化(跨天回看/水印); 实体多行同名标签的 (entity,tag) 冲突后写覆盖审计(§11⑧)。
- [ ] tag_fact 在线层/位图、注册表值域校验落在写前(后端 publish 已拦同 tag 冲突, 执行器按执行序后写覆盖)。
