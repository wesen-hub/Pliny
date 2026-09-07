package com.dcp.seatunnel.transform;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 行级转换执行器(采集端, 向导 Step4)。
 *
 * <p>范围(2026-08-31 定稿): 仅两种节点, 均为 SeaTunnel 天然支持的逐行语义 ——
 * <pre>
 *   filter       条件过滤: expr(Aviator, 基于映射后模型字段) + mode(keep=保留匹配 / drop=丢弃匹配)
 *   filter-kind  按操作类型过滤: kinds ⊆ [INSERT, UPDATE, DELETE], 只保留指定类型
 * </pre>
 *
 * <p>节点按配置顺序执行, 任一不通过 → 该行丢弃(map 返回 null, 不进 Kafka)。
 * dedup / 跨表 Join / Groovy 不在范围内(设计已去掉)。
 */
public class RowChainExecutor implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper OM = new ObjectMapper();

    public static class Node implements Serializable {
        private static final long serialVersionUID = 1L;
        public String type;                 // filter | filter-kind
        public boolean enabled = true;
        public Map<String, Object> params = new LinkedHashMap<>();
    }

    private final List<Node> nodes;
    private final boolean enabled;
    private transient com.googlecode.aviator.AviatorEvaluatorInstance aviator;

    public RowChainExecutor(String rowChainJson) {
        this.nodes = new ArrayList<>();
        boolean has = false;
        if (rowChainJson != null && !rowChainJson.isBlank()) {
            try {
                List<Map<String, Object>> list = OM.readValue(rowChainJson,
                        OM.getTypeFactory().constructCollectionType(List.class, Map.class));
                for (Map<String, Object> m : list) {
                    Node n = new Node();
                    n.type = String.valueOf(m.getOrDefault("type", ""));
                    Object en = m.get("enabled");
                    n.enabled = en == null || Boolean.parseBoolean(String.valueOf(en));
                    @SuppressWarnings("unchecked")
                    Map<String, Object> params = (Map<String, Object>) m.get("params");
                    if (params != null) {
                        n.params = params;
                    }
                    nodes.add(n);
                    if (n.enabled && !"null".equals(n.type) && !n.type.isBlank()) {
                        has = true;
                    }
                }
            } catch (Exception e) {
                throw new IllegalStateException("row_chain JSON 解析失败: " + e.getMessage(), e);
            }
        }
        this.enabled = has;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /**
     * 判断行是否通过行级转换链。
     *
     * @param rowKind INSERT | UPDATE | DELETE
     * @param fields  映射后的模型字段
     * @return true = 通过(保留), false = 丢弃
     */
    public boolean pass(String rowKind, Map<String, Object> fields) {
        if (!enabled) {
            return true;
        }
        for (Node n : nodes) {
            if (!n.enabled) {
                continue;
            }
            switch (n.type) {
                case "filter" -> {
                    String expr = String.valueOf(n.params.getOrDefault("expr", ""));
                    String mode = String.valueOf(n.params.getOrDefault("mode", "keep"));
                    if (expr.isBlank() || "null".equals(expr)) {
                        continue;
                    }
                    boolean matched = eval(expr, fields);
                    // keep=保留匹配(drop 时不匹配才保留); drop=丢弃匹配
                    if ("drop".equals(mode)) {
                        if (matched) {
                            return false;
                        }
                    } else if (!matched) {
                        return false;
                    }
                }
                case "filter-kind" -> {
                    @SuppressWarnings("unchecked")
                    List<String> kinds = (List<String>) n.params.getOrDefault("kinds", List.of());
                    if (!kinds.contains(rowKind)) {
                        return false;
                    }
                }
                default -> {
                    // 未知节点类型: 忽略(向前兼容)
                }
            }
        }
        return true;
    }

    /** Aviator 表达式求值(env = 模型字段) */
    private boolean eval(String expr, Map<String, Object> fields) {
        if (aviator == null) {
            aviator = com.googlecode.aviator.AviatorEvaluator.getInstance();
        }
        try {
            // 规范化: ① SeaTunnel HOCON 解析会吞掉单引号空串 → 转双引号;
            // ② Aviator 布尔运算符是 && / ||, 兼容用户习惯的 and / or(词边界替换)
            String normalized = expr.replace("''", "\"\"").replace("'", "\"")
                    .replaceAll("(?i)\\band\\b", "&&")
                    .replaceAll("(?i)\\bor\\b", "||");
            Object v = aviator.execute(normalized, fields);
            if (v == null) {
                return false;
            }
            if (v instanceof Boolean b) {
                return b;
            }
            if (v instanceof Number num) {
                return num.doubleValue() != 0;
            }
            return !String.valueOf(v).isBlank();
        } catch (Exception e) {
            throw new IllegalStateException("filter 表达式求值失败: " + expr + " -> " + e.getMessage(), e);
        }
    }
}
