package com.dcp.seatunnel.transform;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 字段映射执行器(采集端共享规则引擎, 设计文档 §7/向导 Step3 语义)。
 *
 * <p>输入: 源行字段(CDC 行, 值已字符串化) + 全局 seq + rowKind;
 * 输出: 模型字段 Map(只含配置声明的 modelField, 加工链按序应用)。
 *
 * <p>支持的来源(kind, 互斥):
 * <pre>
 *   field-map   modelField ← 源字段 srcField(直连)
 *   compute     Aviator 表达式(可引用源字段 + 前置模型字段, 拓扑序)
 *   uuid        生成 UUID
 *   system      系统生成: modelField 名为 seq → 全局 seq; 否则当前时间戳
 * </pre>
 *
 * <p>支持的加工链步骤(steps, 可叠加按序): null-fill / case / truncate / mask
 * / json-parse / date / cast / trim / replace / enum / split。
 *
 * <p>约定: DELETE 事件(仅 PK 列)同样走映射 —— key 列直连可取到值,
 * 其余模型字段为 null; 消费端 DELETE 只看 key 列。
 *
 * <p>纯 Java 无框架依赖(SeaTunnel 插件 + 可独立单测)。
 */
public class FieldMappingExecutor implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper OM = new ObjectMapper();

    /** 单条映射配置 */
    public static class Mapping implements Serializable {
        private static final long serialVersionUID = 1L;
        public String modelField;
        public Map<String, Object> source = new LinkedHashMap<>();
        public List<Map<String, Object>> steps = new ArrayList<>();
    }

    private final List<Mapping> mappings;
    /** 是否启用映射(配置为空 = 直通) */
    private final boolean enabled;
    private transient com.googlecode.aviator.AviatorEvaluatorInstance aviator;

    public FieldMappingExecutor(String fieldMappingJson) {
        this.mappings = new ArrayList<>();
        boolean has = false;
        if (fieldMappingJson != null && !fieldMappingJson.isBlank()) {
            try {
                List<Map<String, Object>> list = OM.readValue(fieldMappingJson,
                        OM.getTypeFactory().constructCollectionType(List.class, Map.class));
                for (Map<String, Object> m : list) {
                    Mapping mm = new Mapping();
                    mm.modelField = String.valueOf(m.get("modelField"));
                    @SuppressWarnings("unchecked")
                    Map<String, Object> src = (Map<String, Object>) m.get("source");
                    if (src != null) {
                        mm.source = src;
                    }
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> steps = (List<Map<String, Object>>) m.get("steps");
                    if (steps != null) {
                        mm.steps = steps;
                    }
                    mappings.add(mm);
                    if (mm.source.get("kind") != null && !"null".equals(String.valueOf(mm.source.get("kind")))) {
                        has = true;
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException("field_mapping JSON 解析失败: " + e.getMessage(), e);
            }
        }
        this.enabled = has;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * 应用映射: 源行 → 模型字段。
     *
     * @param sourceFields 源行字段(已字符串化; DELETE 行仅 PK 列)
     * @param seq          全局 seq(system 类型字段用)
     * @return 模型字段 Map(modelField → 值)
     */
    public Map<String, Object> apply(Map<String, Object> sourceFields, long seq) {
        Map<String, Object> out = new LinkedHashMap<>();
        if (!enabled) {
            return sourceFields;
        }
        for (Mapping m : mappings) {
            String kind = String.valueOf(m.source.getOrDefault("kind", ""));
            Object value = null;
            switch (kind) {
                case "field-map" -> {
                    String srcField = String.valueOf(m.source.getOrDefault("srcField", ""));
                    value = sourceFields.get(srcField);
                }
                case "compute" -> {
                    String expr = String.valueOf(m.source.getOrDefault("expr", ""));
                    if (!expr.isBlank()) {
                        value = eval(expr, sourceFields, out);
                    }
                }
                case "uuid" -> value = genUuid(m);
                case "system" -> value = "seq".equals(m.modelField) ? seq : now();
                default -> {
                    // 未定义来源: 直通同名源字段(容错)
                    value = sourceFields.get(m.modelField);
                }
            }
            // 加工链按序应用
            value = applySteps(value, m.steps);
            out.put(m.modelField, value);
        }
        return out;
    }

    /** UUID 生成(支持策略: v4 随机默认 / v7 时间有序 / snowflake 趋势递增) */
    private static final java.util.concurrent.atomic.AtomicLong SNOW_SEQ = new java.util.concurrent.atomic.AtomicLong();
    private static final long SNOW_WORKER = (long) (Math.random() * 1024); // 无协调器: 随机 worker 位, 单实例唯一

    private String genUuid(Mapping m) {
        String strategy = String.valueOf(m.source.getOrDefault("strategy", "v4"));
        switch (strategy) {
            case "v7" -> {
                long ms = System.currentTimeMillis();
                long hi = (ms << 16) | 0x7000L | (SNOW_SEQ.incrementAndGet() & 0x0fffL); // 48位时间 + ver7 + rand_a
                long lo = 0x8000000000000000L | (ThreadLocalRandom.current().nextLong() & 0x3fffffffffffffffL);
                return new UUID(hi, lo).toString();
            }
            case "snowflake" -> {
                long ts = System.currentTimeMillis() & 0x1FFFFFFFFFFL; // 41位
                long seq = SNOW_SEQ.incrementAndGet() & 0xFFFL;
                return Long.toHexString((ts << 22) | (SNOW_WORKER << 12) | seq);
            }
            default -> { return UUID.randomUUID().toString(); } // v4
        }
    }

    /** Aviator 表达式求值(env = 源字段 + 已算出的前置模型字段) */
    private Object eval(String expr, Map<String, Object> sourceFields, Map<String, Object> out) {
        if (aviator == null) {
            aviator = com.googlecode.aviator.AviatorEvaluator.getInstance();
        }
        Map<String, Object> env = new LinkedHashMap<>(sourceFields);
        env.putAll(out);
        try {
            return aviator.execute(expr, env);
        } catch (Exception e) {
            throw new IllegalStateException("compute 表达式求值失败: " + expr + " -> " + e.getMessage(), e);
        }
    }

    // ---------- 加工链 ----------

    private Object applySteps(Object value, List<Map<String, Object>> steps) {
        Object v = value;
        for (Map<String, Object> s : steps) {
            String kind = String.valueOf(s.getOrDefault("kind", ""));
            @SuppressWarnings("unchecked")
            Map<String, Object> p = (Map<String, Object>) s.getOrDefault("params", Map.of());
            switch (kind) {
                case "null-fill" -> {
                    if (v == null || String.valueOf(v).isBlank()) {
                        v = String.valueOf(p.getOrDefault("fillValue", ""));
                    }
                }
                case "case" -> {
                    String t = String.valueOf(p.getOrDefault("caseType", "upper"));
                    v = "lower".equals(t) ? String.valueOf(v).toLowerCase() : String.valueOf(v).toUpperCase();
                }
                case "truncate" -> {
                    int len = ((Number) p.getOrDefault("length", 20)).intValue();
                    String sv = String.valueOf(v);
                    v = sv.length() > len ? sv.substring(0, len) : sv;
                }
                case "mask" -> v = mask(String.valueOf(v), String.valueOf(p.getOrDefault("maskType", "custom")));
                case "trim" -> v = v == null ? null : String.valueOf(v).trim();
                case "replace" -> {
                    String find = String.valueOf(p.getOrDefault("find", ""));
                    String to = String.valueOf(p.getOrDefault("to", ""));
                    v = String.valueOf(v).replace(find, to);
                }
                case "cast" -> {
                    // 目标分表统一 VARCHAR 承载, cast 保持字符串(前端类型对拍用)
                }
                case "json-parse" -> {
                    try {
                        String path = String.valueOf(p.getOrDefault("path", ""));
                        com.fasterxml.jackson.databind.JsonNode node = OM.readTree(String.valueOf(v));
                        if (path.startsWith("$.")) {
                            node = node.at(path.substring(1));
                        } else if (!path.isBlank() && !"null".equals(path)) {
                            node = node.at(path);
                        }
                        v = node.isMissingNode() ? null : node.asText();
                    } catch (Exception e) {
                        v = null;
                    }
                }
                case "date" -> {
                    String fmt = String.valueOf(p.getOrDefault("format", ""));
                    if (!fmt.isBlank() && !"null".equals(fmt)) {
                        v = formatDate(String.valueOf(v), fmt);
                    }
                }
                case "enum" -> v = enumMap(String.valueOf(v), String.valueOf(p.getOrDefault("dict", "")));
                case "split" -> {
                    // split 生成新字段(独立于 modelField, 直接追加到输出由调用方处理) —— 单值场景保留首段
                    String sep = String.valueOf(p.getOrDefault("sep", ","));
                    String sv = String.valueOf(v);
                    String[] parts = sv.split(java.util.regex.Pattern.quote(sep));
                    v = parts.length > 0 ? parts[0] : v;
                }
                default -> {
                }
            }
        }
        return v;
    }

    private static String mask(String v, String type) {
        if (v == null) {
            return null;
        }
        return switch (type) {
            case "phone" -> v.length() >= 11 ? v.substring(0, 3) + "****" + v.substring(v.length() - 4) : v;
            case "idcard" -> v.length() >= 14 ? v.substring(0, 4) + "**********" + v.substring(v.length() - 4) : v;
            case "amount" -> v;
            default -> v.length() > 5 ? v.substring(0, 2) + "***" : v;
        };
    }

    /** 枚举映射: '1→已支付, 2→已完成' 或 '1/2/3→普通/银/金' */
    private static String enumMap(String v, String dict) {
        if (v == null || dict == null || dict.isBlank()) {
            return v;
        }
        for (String item : dict.split(",")) {
            item = item.trim();
            int arrow = item.indexOf('→');
            if (arrow < 0) {
                continue;
            }
            String keys = item.substring(0, arrow).trim();
            String val = item.substring(arrow + 1).trim();
            for (String k : keys.split("/")) {
                if (k.trim().equals(v)) {
                    return val;
                }
            }
        }
        return v;
    }

    /** 日期格式转换(MySQL 风格 %Y/%m/%d/%H/%i/%s → Java pattern) */
    private static String formatDate(String v, String fmt) {
        try {
            String javaFmt = fmt
                    .replace("%Y", "yyyy").replace("%m", "MM").replace("%d", "dd")
                    .replace("%H", "HH").replace("%i", "mm").replace("%s", "ss");
            DateTimeFormatter out = DateTimeFormatter.ofPattern(javaFmt);
            // 输入按常见格式解析(默认 yyyy-MM-dd HH:mm:ss / yyyy-MM-dd)
            String sv = String.valueOf(v).trim();
            LocalDateTime dt;
            try {
                dt = LocalDateTime.parse(sv, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            } catch (Exception e1) {
                try {
                    dt = LocalDateTime.parse(sv, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
                } catch (Exception e2) {
                    dt = java.time.LocalDate.parse(sv, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
                            .atStartOfDay();
                }
            }
            return dt.format(out);
        } catch (Exception e) {
            return String.valueOf(v);
        }
    }

    private static String now() {
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now());
    }
}
