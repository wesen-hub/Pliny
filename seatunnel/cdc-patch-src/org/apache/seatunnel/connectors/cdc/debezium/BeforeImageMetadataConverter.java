package org.apache.seatunnel.connectors.cdc.debezium;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;

/**
 * before 行透传(设计方案 §4.4 主键修改检测, G1 方案A)。
 *
 * <p>Debezium UPDATE 事件 Envelope.before 携带旧行(MySQL 全列 before image),
 * 而 2.3.12 SeaTunnelRowDebeziumDeserializeSchema 不发出 before → 主键修改无法在
 * 转换层感知。本 converter 把 before 行整体 JSON 化(字段值统一字符串化, 与目标
 * VARCHAR 承载口径一致), 行末第三列 __src_before; 下游 CdcSeqTransform 解析 JSON
 * 按 business_key_fields 拼出 beforeKey, beforeKey ≠ businessKey → 主键被修改
 * → 写 sync_alert 告警 + 抛异常停任务(checkpoint 保留, watchdog 不自动 restore)。
 *
 * <p>before 为 null(INSERT/快照行/无 before image 的 connector)→ 返回 null(不检测)。
 * 与 {@link PositionMetadataConverter} / {@link SnapshotFlagMetadataConverter} 一起
 * 被 ASM 补丁注入, 行末三列: __src_pos / __snapshot / __src_before。
 */
public final class BeforeImageMetadataConverter implements MetadataConverter, Serializable {

    private static final long serialVersionUID = 1L;

    public static final BeforeImageMetadataConverter INSTANCE = new BeforeImageMetadataConverter();

    private BeforeImageMetadataConverter() {
    }

    @Override
    public Object read(SourceRecord record) {
        if (record == null || !(record.value() instanceof Struct value)) {
            return null;
        }
        Object before = value.get("before");
        if (!(before instanceof Struct struct)) {
            return null;
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Field f : struct.schema().fields()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"').append(esc(f.name())).append("\":");
            Object v = struct.get(f);
            if (v == null) {
                sb.append("null");
            } else {
                sb.append('"').append(esc(String.valueOf(v))).append('"');
            }
        }
        sb.append('}');
        return sb.toString();
    }

    /** JSON 字符串值转义(最小实现, 仅字段名/字符串值用) */
    private static String esc(String s) {
        StringBuilder o = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> o.append("\\\"");
                case '\\' -> o.append("\\\\");
                case '\n' -> o.append("\\n");
                case '\r' -> o.append("\\r");
                case '\t' -> o.append("\\t");
                default -> {
                    if (c < 0x20) {
                        o.append(String.format("\\u%04x", (int) c));
                    } else {
                        o.append(c);
                    }
                }
            }
        }
        return o.toString();
    }
}
