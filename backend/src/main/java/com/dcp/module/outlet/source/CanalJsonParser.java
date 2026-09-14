package com.dcp.module.outlet.source;

import com.dcp.module.outlet.model.ChangeEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * canal-json 解析器(设计 §5.3 事件格式 / §6.6 增量段)。
 *
 * <p>解析 TiCDC → Kafka 的 L1 内部流报文。实测样例见《新集群部署与冒烟记录-2026-09-14》§2:
 * <pre>
 * {"type":"INSERT","table":"t_outlet_probe","data":[{"id":"1","name":"alpha"}],"_tidb":{"commitTs":469072829392879624}}
 * {"type":"UPDATE","table":"t_outlet_probe","data":[{...new...}],"old":[{...before...}],"_tidb":{"commitTs":...}}
 * {"type":"DELETE","table":"t_outlet_probe","data":[{...被删行...}],"_tidb":{"commitTs":...}}
 * {"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":469072825711329284}}
 * {"type":"CREATE","table":"t_outlet_probe",...,"_tidb":{"commitTs":...}}   // DDL
 * </pre>
 *
 * <p>关键语义(实测确认):{@code data} = **变更后整行**;{@code old} = 变更前值(仅 UPDATE);
 * {@code _tidb.commitTs} = **事件自带位点**(消费方无需回表);WATERMARK 表示
 * "所有 commitTs &lt; watermarkTs 的事件均已发出"。
 *
 * <p>本类**纯函数、无 IO**,便于单测锁定协议解析。
 */
@Slf4j
@Component
public class CanalJsonParser {

    private final ObjectMapper mapper = new ObjectMapper();

    /** 解析结果:要么是一条水位事件,要么是 0..N 条行变更(DDL/心跳等不产出事件) */
    public record Parsed(Type type, Long watermarkTs, List<ChangeEvent> events, String ddlSql) {
        public enum Type { ROW, WATERMARK, DDL, IGNORED }
    }

    public Parsed parse(String message) {
        if (message == null || message.isBlank()) {
            return new Parsed(Parsed.Type.IGNORED, null, List.of(), null);
        }
        JsonNode root;
        try {
            root = mapper.readTree(message);
        } catch (Exception e) {
            log.warn("[outlet] canal-json 解析失败,已忽略: {}", message.length() > 200 ? message.substring(0, 200) + "..." : message);
            return new Parsed(Parsed.Type.IGNORED, null, List.of(), null);
        }

        String type = text(root, "type");
        JsonNode tidb = root.path("_tidb");
        Long commitTs = tidb.hasNonNull("commitTs") ? tidb.get("commitTs").asLong() : null;

        if ("TIDB_WATERMARK".equalsIgnoreCase(type)) {
            Long wm = tidb.hasNonNull("watermarkTs") ? tidb.get("watermarkTs").asLong() : null;
            return new Parsed(Parsed.Type.WATERMARK, wm, List.of(), null);
        }
        if ("DDL".equalsIgnoreCase(type) || root.path("isDdl").asBoolean(false)) {
            return new Parsed(Parsed.Type.DDL, null, List.of(), text(root, "sql"));
        }
        if (!"INSERT".equalsIgnoreCase(type) && !"UPDATE".equalsIgnoreCase(type) && !"DELETE".equalsIgnoreCase(type)) {
            return new Parsed(Parsed.Type.IGNORED, null, List.of(), null);
        }
        if (commitTs == null) {
            // 没有 commitTs 就没有位点 → 不可用于消费(设计依赖"事件自带位点")
            log.warn("[outlet] 事件缺少 _tidb.commitTs,已忽略: type={}", type);
            return new Parsed(Parsed.Type.IGNORED, null, List.of(), null);
        }

        String table = firstNonNull(text(root, "table"), text(root, "tableName"));
        List<ChangeEvent> events = new ArrayList<>();
        JsonNode data = root.path("data");
        JsonNode old = root.path("old");
        long ts = root.hasNonNull("ts") ? root.get("ts").asLong() : 0L;

        if (data.isArray()) {
            for (int i = 0; i < data.size(); i++) {
                ChangeEvent e = new ChangeEvent();
                e.setOp(type.toUpperCase());
                e.setStreamId(table);
                Map<String, Object> row = toMap(data.get(i));
                e.setRow(row);
                if ("UPDATE".equalsIgnoreCase(type) && old.isArray() && old.size() > i) {
                    e.setBefore(toMap(old.get(i)));
                }
                e.setKey(pickKey(root, row));
                e.setTso(commitTs);
                e.setTs(ts);
                events.add(e);
            }
        }
        return new Parsed(Parsed.Type.ROW, null, events, null);
    }

    /** 主键:优先 packet 里的 pkNames,否则回退 id */
    private Map<String, Object> pickKey(JsonNode root, Map<String, Object> row) {
        Map<String, Object> key = new LinkedHashMap<>();
        JsonNode pkNames = root.path("pkNames");
        if (pkNames.isArray() && !pkNames.isEmpty()) {
            for (JsonNode n : pkNames) {
                String col = n.asText();
                if (row.containsKey(col)) {
                    key.put(col, row.get(col));
                }
            }
        }
        if (key.isEmpty() && row.containsKey("id")) {
            key.put("id", row.get("id"));
        }
        return key;
    }

    private Map<String, Object> toMap(JsonNode node) {
        Map<String, Object> m = new LinkedHashMap<>();
        if (node == null || !node.isObject()) {
            return m;
        }
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            JsonNode v = e.getValue();
            if (v == null || v.isNull()) {
                m.put(e.getKey(), null);
            } else if (v.isNumber()) {
                m.put(e.getKey(), v.numberValue());
            } else if (v.isBoolean()) {
                m.put(e.getKey(), v.booleanValue());
            } else if (v.isObject() || v.isArray()) {
                m.put(e.getKey(), v.toString());
            } else {
                m.put(e.getKey(), v.asText());
            }
        }
        return m;
    }

    private String text(JsonNode root, String field) {
        JsonNode n = root.path(field);
        return n.isMissingNode() || n.isNull() ? null : n.asText();
    }

    private String firstNonNull(String a, String b) {
        return a != null && !a.isBlank() ? a : b;
    }
}
