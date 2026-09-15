package com.dcp.outlet.integration;

import com.dcp.outlet.sdk.Op;
import com.dcp.outlet.worker.DecodedMessage;
import com.dcp.outlet.worker.MessageDecoder;
import com.dcp.outlet.worker.RawMessage;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 极简解码器(内存/联调用):文本格式 {@code WATERMARK|tso} 或 {@code EVENT|tso|stream|OP|k:v,k:v}。
 *
 * <p>生产实现 = canal-json 解析(只取需要字段)。
 */
public class SdkEventDecoder implements MessageDecoder {

    @Override
    public DecodedMessage decode(RawMessage raw) {
        String s = new String(raw.payload(), StandardCharsets.UTF_8);
        String[] parts = s.split("\\|", -1);
        if (parts[0].equals("WATERMARK")) {
            return DecodedMessage.watermark(raw.partition(), raw.offset(), Long.parseLong(parts[1]));
        }
        long tso = Long.parseLong(parts[1]);
        String stream = parts[2];
        Op op = Op.valueOf(parts[3]);
        Map<String, Object> row = parseRow(parts.length > 4 ? parts[4] : "");
        Map<String, Object> key = new LinkedHashMap<>();
        key.put("id", row.get("id"));
        return DecodedMessage.event(raw.partition(), raw.offset(), tso, stream, op, key,
                new LinkedHashMap<>(), row);
    }

    private Map<String, Object> parseRow(String s) {
        Map<String, Object> row = new LinkedHashMap<>();
        if (s.isEmpty()) return row;
        for (String kv : s.split(",")) {
            int i = kv.indexOf(':');
            if (i > 0) row.put(kv.substring(0, i), kv.substring(i + 1));
        }
        return row;
    }

    /** 便捷构造函数(测试用):生成一条事件 payload。 */
    public static byte[] eventPayload(long tso, String stream, Op op, String kv) {
        return ("EVENT|" + tso + "|" + stream + "|" + op.name() + "|" + kv).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] watermarkPayload(long tso) {
        return ("WATERMARK|" + tso).getBytes(StandardCharsets.UTF_8);
    }
}
