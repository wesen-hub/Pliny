# 本地 E2E(dev Spark + Iceberg, hadoop catalog 本地 warehouse)

验证 P0 全链路: 后端发布/run(task-json)→ 执行器 Spark 读湖表 → Drools 求值(规则流短路语义) → tag_fact 当前值分区覆盖 + time travel 留史。

## 前提
- 后端已起(:8081, 冒烟数据在库: 规则流#4 风控优先决策链, 闭包含 黑名单#1/金额档#2)
- `docker build` 过 tagengine-executor(镜像 tagengine-executor-final 内含 /app/app.jar)
- 镜像 apache/spark:3.5.3-java17(执行器 Java17; KieModule 编译走 Drools 内置 JDT, 无需 JDK)

## 步骤(详见历史验证, 2026-09-08)
1) 准备共享目录与 jar:
   mkdir -p /tmp/tag-e2e/{warehouse,tasks}
   docker create --name teimg tagengine-executor-final && docker cp teimg:/app/app.jar /tmp/tag-e2e/executor.jar && docker rm teimg
2) 起 dev spark 容器(挂 warehouse/tasks/jar):
   docker run -d --name dcp-spark-dev --user root -e SPARK_LOCAL_IP=127.0.0.1 \
     -v /tmp/tag-e2e/warehouse:/opt/warehouse -v /tmp/tag-e2e/tasks:/opt/tasks \
     -v /tmp/tag-e2e/executor.jar:/opt/jar/executor.jar \
     --entrypoint /bin/bash apache/spark:3.5.3-java17 -c "sleep 7200"
3) 生成任务 json(prepare_task.py: 建任务绑规则流#4 → publish → run(simulate) → 取 payloadText 落 /tmp/tag-e2e/tasks/task.json, 并补 iceberg/tagFact 湖参数):
   python3 tagengine-executor/dev/prepare_task.py
4) 造数(spark-sql 建 demo.dcp.customer_wide + 6 行; 首次拉 iceberg-runtime):
   docker exec dcp-spark-dev /opt/spark/bin/spark-sql --master 'local[1]' \
     --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1 \
     --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
     --conf spark.sql.catalog.demo.type=hadoop --conf spark.sql.catalog.demo.warehouse=/opt/warehouse \
     -f /opt/tasks/seed.sql     # seed.sql 需先拷进 tasks 挂载目录
5) 跑执行器:
   docker exec dcp-spark-dev /opt/spark/bin/spark-submit --master 'local[2]' \
     --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1 \
     --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
     --conf spark.sql.catalog.demo.type=hadoop --conf spark.sql.catalog.demo.warehouse=/opt/warehouse \
     --class com.dcp.tagjob.TagBatchJob /opt/jar/executor.jar --task-json /opt/tasks/task.json
6) 验证(期望: 金额档 大额2/小额2, 黑名单2; 二次跑后行数仍6, snapshots 递增 = time travel):
   spark-sql(同上 conf) -e "SELECT tag,value,COUNT(*) FROM demo.dcp.tag_fact GROUP BY tag,value; SELECT COUNT(*) FROM demo.dcp.tag_fact; SELECT COUNT(*) FROM demo.dcp.tag_fact.snapshots;"

## 2026-09-08 实测结果(E2E PASS)
- 执行器退出码 0; [摘要] 每资产命中: 风控链命中行=18(行×3次求值, 多 action 未 cache 所致, 结果幂等), 黑名单 6(2行×3), 金额档 12(4行×3)
- tag_fact: 订单/金额档 大额=2 小额=2, 风控/黑名单=2(黑名单实体未被打金额档 → 流内 stop 短路语义正确)
- 二次运行: 已删除本任务旧分区后写入, 行数仍 6; snapshots 1→3(create/run1/run2)= time travel 留史验证
- 坑: task-json 契约对齐(后端已修): 键名 assets→rules; datasetFields 字符串数组→[{name}] 对象
