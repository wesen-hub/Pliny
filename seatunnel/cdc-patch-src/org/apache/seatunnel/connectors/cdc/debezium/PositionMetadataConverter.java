package org.apache.seatunnel.connectors.cdc.debezium;

import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;
import java.util.Map;

/**
 * 位点透传补丁(设计方案 §3.1 connector 补丁层, 路径 1 位点即序号)。
 *
 * <p>{@link MetadataConverter#read(SourceRecord)} 从 sourceOffset 提取源端位点, 输出
 * 统一字符串 {@code "type:k1:v1:..."}, 下游 CdcSeqTransform 按源类型解析并编码
 * {@code seq = f(位点)}(四判据: 源端生成/内容绑定/单主流内单调/逐行可得)。
 *
 * <p>{@link #merge(MetadataConverter[])} 是 ASM 补丁(PatchCdcBasePosition)在
 * {@code SeaTunnelRowDebeziumDeserializeSchema$Builder.build()} 的注入助手:
 * metadataConverters 为 null 时注入 [本类, SnapshotFlagMetadataConverter],
 * 非 null 时尾部追加(保留上游已有 converter)。行末多出两列: __src_pos / __snapshot。
 */
public final class PositionMetadataConverter implements MetadataConverter, Serializable {

    private static final long serialVersionUID = 1L;

    public static final PositionMetadataConverter INSTANCE = new PositionMetadataConverter();

    private PositionMetadataConverter() {
    }

    @Override
    public Object read(SourceRecord record) {
        if (record == null || record.sourceOffset() == null) {
            return null;
        }
        Map<String, ?> off = record.sourceOffset();
        // MySQL: file / pos (/ row)
        if (off.containsKey("file") && off.containsKey("pos")) {
            return "mysql:" + off.get("file") + ":" + off.get("pos") + ":" + val(off, "row");
        }
        // PostgreSQL: lsn(如 1/16B37420)
        if (off.containsKey("lsn")) {
            return "pg:" + off.get("lsn");
        }
        // Oracle: commit_scn (/ lcr_position)
        if (off.containsKey("commit_scn")) {
            return "oracle:" + off.get("commit_scn") + ":" + val(off, "lcr_position");
        }
        // SQLServer: commit_lsn (/ event_serial_no)
        if (off.containsKey("commit_lsn")) {
            return "sqlserver:" + off.get("commit_lsn") + ":" + val(off, "event_serial_no");
        }
        // MongoDB: ts_ms(毫秒级位点)/ ord(同毫秒内序号)
        // ⑥ 满位源治理 TODO(实跑确认): SeaTunnel 2.3.12 mongo CDC sourceOffset 实际键形态
        // (Debezium resume token 可能无 ts_ms, 需实跑打印 sourceOffset 后校正本分支;
        //  ts_ms 缺失时本分支输出键空哨兵 "mongo::ord", 严格位点模式 fail-fast、兼容模式回退计数器)
        if (off.containsKey("ts_ms")) {
            Object ord = off.get("ord") != null ? off.get("ord") : off.get("h");
            return "mongo:" + off.get("ts_ms") + ":" + (ord == null ? "" : ord);
        }
        // TiDB/兼容: ts(TSO 天然 64 位有序; ⑥ 满位源治理在 PositionSeq 裁剪平移)
        // TODO(实跑确认): TiDB CDC sourceOffset 的 TSO 键名(可能为 tso/ts/commitTs)
        if (off.containsKey("ts")) {
            return "tidb:" + off.get("ts");
        }
        // 未知位点体系 → 原样透传, 下游按不变式 5 fail-fast
        return "unknown:" + off;
    }

    /** 取 sourceOffset 键值, 缺失时返回空串(不参与拼接语义) */
    private static Object val(Map<String, ?> off, String key) {
        Object v = off.get(key);
        return v == null ? "" : v;
    }

    /** ASM 注入助手: null/空 → [POSITION, SNAPSHOT]; 非 null → 尾部追加 */
    public static MetadataConverter[] merge(MetadataConverter[] arr) {
        if (arr == null || arr.length == 0) {
            return new MetadataConverter[]{INSTANCE, SnapshotFlagMetadataConverter.INSTANCE};
        }
        MetadataConverter[] out = new MetadataConverter[arr.length + 2];
        System.arraycopy(arr, 0, out, 0, arr.length);
        out[arr.length] = INSTANCE;
        out[arr.length + 1] = SnapshotFlagMetadataConverter.INSTANCE;
        return out;
    }
}
