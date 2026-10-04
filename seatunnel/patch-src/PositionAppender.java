package com.dcp.seatunnel.cdc;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.debezium.BeforeImageMetadataConverter;
import org.apache.seatunnel.connectors.cdc.debezium.PositionMetadataConverter;
import org.apache.seatunnel.connectors.cdc.debezium.SnapshotFlagMetadataConverter;

/**
 * Mongo CDC 位点追加器(① 位点透传补丁, Mongo 专链)。
 *
 * <p>Mongo connector 不走 {@code SeaTunnelRowDebeziumDeserializeSchema}(自研
 * {@code MongoDBConnectorDeserializationSchema}), 官方 converter 无处接线;
 * ASM 补丁在 {@code emit(SourceRecord, SeaTunnelRow, Collector)} 开头调用本类,
 * 把位点字符串/快照标志/before 行追加为行尾三列——
 * 与 Jdbc 系 metadata 列同构(行尾、不进 schema、值为 null 表示无位点),
 * 下游 CdcSeqTransform 按 {@code values[names.length] / values[names.length+1] / values[names.length+2]} 读取。
 */
public final class PositionAppender {

    private PositionAppender() {
    }

    /** 行尾追加位点列 + 快照标志列 + before 行列; 位点缺失时补 null(与官方 metadata 列语义一致) */
    public static SeaTunnelRow append(SeaTunnelRow row, SourceRecord record) {
        Object pos = PositionMetadataConverter.INSTANCE.read(record);
        Object snap = SnapshotFlagMetadataConverter.INSTANCE.read(record);
        Object before = BeforeImageMetadataConverter.INSTANCE.read(record);
        int n = row.getFields().length;
        SeaTunnelRow out = new SeaTunnelRow(n + 3);
        for (int i = 0; i < n; i++) {
            out.setField(i, row.getField(i));
        }
        out.setField(n, pos == null ? null : String.valueOf(pos));
        out.setField(n + 1, snap == null ? Boolean.FALSE : snap);
        out.setField(n + 2, before == null ? null : String.valueOf(before));
        return out;
    }
}
