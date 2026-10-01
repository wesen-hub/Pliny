#!/bin/bash
# ① 位点透传补丁一键应用脚本(设计文档 §2.4 路径 1 位点即序号):
# 给 5 个 CDC connector 接线自研 converter 对(PositionMetadataConverter + SnapshotFlagMetadataConverter),
# 使源端位点 "type:k1:v1..." 与快照标志以行尾两列透传给 CdcSeqTransform。
#
# 注入点(实测 SeaTunnel 2.3.12 字节码):
#   mysql/pg/oracle/sqlserver: createDebeziumDeserializationSchema 的 Builder.build() 前
#     → setMetadataConverters([POSITION, SNAPSHOT])   (metadata 列行尾、不进 schema)
#   mongodb: MongoDBConnectorDeserializationSchema.emit 开头
#     → row = PositionAppender.append(row, record)    (行尾追加同构两列)
#
# 自举: connector-cdc-base 缺两个 converter 类时自动编译注入(幂等)。
#
# 用法: bash patch_cdc_position.sh [dist目录, 默认本脚本同级的 dist/]
# 每个 jar 自动备份 <jar>.bak(已存在不覆盖, 重复执行安全: 幂等注入点不重复)
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
DIST="${1:-$DIR/dist}"
PATCH_SRC="$DIR/patch-src"
ASM_JAR="$HOME/.m2/repository/org/ow2/asm/asm/9.5/asm-9.5.jar"
ST_API="$HOME/.m2/repository/org/apache/seatunnel/seatunnel-api/2.3.12/seatunnel-api-2.3.12.jar"
CONNECT_API="$HOME/.m2/repository/org/apache/kafka/connect-api/3.1.1/connect-api-3.1.1.jar"
CDC_BASE="$DIST/connector-cdc-base-2.3.12.jar"

echo "== 编译 ASM 补丁主类 =="
mkdir -p "$PATCH_SRC/classes"
javac -encoding UTF-8 -cp "$ASM_JAR" -d "$PATCH_SRC/classes" "$PATCH_SRC/PatchCdcPosition.java"

echo "== 自举: connector-cdc-base 注入 converter 对(幂等) =="
# 注意: 用 grep -c 而非 grep -q —— grep -q 匹配即退出会提前关闭管道,
# unzip 收 SIGPIPE(141) 在 pipefail 下会让 `! pipeline` 误判为缺失
if ! unzip -l "$CDC_BASE" 2>/dev/null | grep -c "debezium/PositionMetadataConverter.class" >/dev/null; then
    mkdir -p "$DIR/cdc-patch-src/classes"
    javac -encoding UTF-8 -cp "$ST_API:$CONNECT_API:$CDC_BASE" -d "$DIR/cdc-patch-src/classes" \
        "$DIR/cdc-patch-src/org/apache/seatunnel/connectors/cdc/debezium/PositionMetadataConverter.java" \
        "$DIR/cdc-patch-src/org/apache/seatunnel/connectors/cdc/debezium/SnapshotFlagMetadataConverter.java"
    cp "$CDC_BASE" "$CDC_BASE.bak"
    python3 - "$CDC_BASE" "$DIR/cdc-patch-src/classes" <<'PYEOF'
import sys, zipfile, shutil, os
jar, classes = sys.argv[1], sys.argv[2]
names = ["org/apache/seatunnel/connectors/cdc/debezium/PositionMetadataConverter.class",
         "org/apache/seatunnel/connectors/cdc/debezium/SnapshotFlagMetadataConverter.class"]
tmp = jar + ".tmp"
with zipfile.ZipFile(jar, "r") as zin, zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zout:
    have = zin.namelist()
    for item in zin.infolist():
        zout.writestr(item, zin.read(item.filename))
    for cls in names:
        if cls not in have:
            zout.write(os.path.join(classes, cls), cls)
shutil.move(tmp, jar)
print("injected converter pair into connector-cdc-base")
PYEOF
else
    echo "converter pair already in connector-cdc-base, skip"
fi

echo "== 编译 PositionAppender(依赖已注入的 converter 对) =="
javac -encoding UTF-8 -cp "$ST_API:$CONNECT_API:$CDC_BASE" -d "$PATCH_SRC/classes" "$PATCH_SRC/PositionAppender.java"

# 幂等: 存在 .bak 时先恢复(重复执行 = 在原始 jar 上重新打补丁, 不会重复注入)
for J in "$DIST"/connector-cdc-{mysql,postgres,oracle,sqlserver,mongodb}-2.3.12.jar; do
    if [ -f "$J.bak" ]; then
        cp "$J.bak" "$J"
        echo "restored from backup: $J"
    fi
done

echo "== builder 模式: mysql/postgres/oracle/sqlserver =="
java -cp "$PATCH_SRC/classes:$ASM_JAR" PatchCdcPosition \
    "$DIST/connector-cdc-mysql-2.3.12.jar" builder \
    org/apache/seatunnel/connectors/seatunnel/cdc/mysql/source/MySqlIncrementalSource
java -cp "$PATCH_SRC/classes:$ASM_JAR" PatchCdcPosition \
    "$DIST/connector-cdc-postgres-2.3.12.jar" builder \
    org/apache/seatunnel/connectors/seatunnel/cdc/postgres/source/PostgresIncrementalSource
java -cp "$PATCH_SRC/classes:$ASM_JAR" PatchCdcPosition \
    "$DIST/connector-cdc-oracle-2.3.12.jar" builder \
    org/apache/seatunnel/connectors/seatunnel/cdc/oracle/source/OracleIncrementalSource
java -cp "$PATCH_SRC/classes:$ASM_JAR" PatchCdcPosition \
    "$DIST/connector-cdc-sqlserver-2.3.12.jar" builder \
    org/apache/seatunnel/connectors/seatunnel/cdc/sqlserver/source/SqlServerIncrementalSource

echo "== mongo-emit 模式: mongodb(重写 emit + 追加 PositionAppender) =="
java -cp "$PATCH_SRC/classes:$ASM_JAR" PatchCdcPosition \
    "$DIST/connector-cdc-mongodb-2.3.12.jar" mongo-emit \
    org/apache/seatunnel/connectors/seatunnel/cdc/mongodb/sender/MongoDBConnectorDeserializationSchema
python3 - "$DIST/connector-cdc-mongodb-2.3.12.jar" "$PATCH_SRC/classes" <<'PYEOF'
import sys, zipfile, shutil, os
jar, classes = sys.argv[1], sys.argv[2]
cls = "com/dcp/seatunnel/cdc/PositionAppender.class"
src = os.path.join(classes, cls)
if not os.path.exists(src):
    print("MISSING", cls); sys.exit(2)
tmp = jar + ".tmp"
with zipfile.ZipFile(jar, "r") as zin, zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zout:
    names = zin.namelist()
    for item in zin.infolist():
        data = zin.read(item.filename)
        if item.filename == cls:
            data = open(src, "rb").read()
        zout.writestr(item, data)
    if cls not in names:
        zout.write(src, cls)
shutil.move(tmp, jar)
print("appended", cls)
PYEOF

echo "== 完成 =="
ls -la "$DIST" | grep "connector-cdc"
