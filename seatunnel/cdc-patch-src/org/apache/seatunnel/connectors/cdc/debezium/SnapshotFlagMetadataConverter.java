package org.apache.seatunnel.connectors.cdc.debezium;

import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;

/**
 * 快照标志透传(设计方案 §4.4 批/流模板分派依据)。
 *
 * <p>Debezium 快照阶段 sourceOffset.snapshot = true / "true" / "last" → 返回 Boolean.TRUE,
 * 否则 FALSE。与 {@link PositionMetadataConverter} 一起被 ASM 补丁注入
 * {@code SeaTunnelRowDebeziumDeserializationConverters}, 行末第二列 __snapshot;
 * 下游 CdcSeqTransform 写入 CdcRecord.snapshot, 写入端据此分派批/流模板。
 */
public final class SnapshotFlagMetadataConverter implements MetadataConverter, Serializable {

    private static final long serialVersionUID = 1L;

    public static final SnapshotFlagMetadataConverter INSTANCE = new SnapshotFlagMetadataConverter();

    private SnapshotFlagMetadataConverter() {
    }

    @Override
    public Object read(SourceRecord record) {
        if (record == null || record.sourceOffset() == null) {
            return Boolean.FALSE;
        }
        Object snap = record.sourceOffset().get("snapshot");
        if (snap instanceof Boolean) {
            return snap;
        }
        String s = snap == null ? null : String.valueOf(snap);
        return Boolean.valueOf("true".equals(s) || "last".equals(s));
    }
}
