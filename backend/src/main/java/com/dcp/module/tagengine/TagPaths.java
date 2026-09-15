package com.dcp.module.tagengine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * 标签路径工具:`tag_dict`(邻接表 id/parent_id/name)→ `path → id` 映射。
 *
 * <p>为什么需要:标签树的**路径是派生的**(父链拼接,不落库),而打标规则与结果要**存节点 id**
 * (存 id 而非路径 —— 改名/移动时不再失配)。因此平台在执行前把这份映射下发给执行器
 * (task-json / job-json 的 `tagIds`),执行器写 `tag_fact_view` / `tag_notify` 时顺带写 `tag_dict_id`。
 *
 * <p>集中在这里,避免「路径推导」逻辑在后端与两个执行器里各抄一份(它们已经抄了 6 个 kie 类,别再增加)。
 */
public final class TagPaths {

    private TagPaths() {
    }

    /**
     * @param rows {@code SELECT id, parent_id, name FROM tag_dict} 的结果
     * @return path → id;父链缺失(孤儿)时该节点 path 退化为自身名字,与 TagService.buildPath 语义一致
     */
    public static Map<String, Long> pathToId(List<Map<String, Object>> rows) {
        Map<Long, String> name = new HashMap<>();
        Map<Long, Long> parent = new HashMap<>();
        for (Map<String, Object> r : rows) {
            if (r.get("id") == null) continue;
            long id = ((Number) r.get("id")).longValue();
            name.put(id, String.valueOf(r.get("name")));
            parent.put(id, r.get("parent_id") == null ? 0L : ((Number) r.get("parent_id")).longValue());
        }
        Map<String, Long> out = new LinkedHashMap<>();
        for (Long id : name.keySet()) {
            List<String> chain = new ArrayList<>();
            Long cur = id;
            Set<Long> guard = new HashSet<>();
            while (cur != null && cur != 0 && guard.add(cur) && name.containsKey(cur)) {
                chain.add(0, name.get(cur));
                cur = parent.get(cur);
            }
            out.put(String.join("/", chain), id);
        }
        return out;
    }

    /**
     * 把动作标签的**节点 id 固化进发布快照**:`visual.actions[].tagId` / `table.tagId`。
     *
     * <p>发布即冻结 —— 标签日后改名/移动时,已发布任务仍指向同一节点(路径会变,id 不会)。
     * <p>DRL 手写模式的标签路径在源码里,不做改写(执行时由运行时 tagIds 兼底)。
     */
    public static void freezeIds(JsonNode snap, Map<String, Long> pathToId) {
        if (snap == null || pathToId == null) return;
        for (JsonNode a : snap.path("assets")) {
            JsonNode def = a.path("def");
            String mode = def.path("mode").asText("visual");
            if ("visual".equals(mode)) {
                for (JsonNode ac : def.path("visual").path("actions")) {
                    if ("tag".equals(ac.path("type").asText()) && ac instanceof ObjectNode on) {
                        Long id = pathToId.get(ac.path("tag").asText(""));
                        if (id != null) on.put("tagId", id);
                    }
                }
            } else if ("table".equals(mode)) {
                JsonNode t = def.path("table");
                if (t instanceof ObjectNode on) {
                    Long id = pathToId.get(t.path("tag").asText(""));
                    if (id != null) on.put("tagId", id);
                }
            }
        }
    }

    /**
     * 执行器用的 `路径→id`:优先取快照里固化的 `tagId`(发布时冻结),再用实时标签树补齐
     * (drl 手写路径 / 旧快照)。快照优先 —— 这样标签改名后执行仍落到同一节点。
     */
    public static Map<String, Long> idsFromSnapshot(JsonNode snap, Map<String, Long> live) {
        Map<String, Long> out = new LinkedHashMap<>();
        if (snap != null) {
            for (JsonNode a : snap.path("assets")) {
                JsonNode def = a.path("def");
                String mode = def.path("mode").asText("visual");
                if ("visual".equals(mode)) {
                    for (JsonNode ac : def.path("visual").path("actions")) {
                        if (ac.hasNonNull("tagId")) out.put(ac.path("tag").asText(""), ac.path("tagId").asLong());
                    }
                } else if ("table".equals(mode)) {
                    JsonNode t = def.path("table");
                    if (t.hasNonNull("tagId")) out.put(t.path("tag").asText(""), t.path("tagId").asLong());
                }
            }
        }
        if (live != null) live.forEach(out::putIfAbsent);
        return out;
    }
}
