package com.dcp.sync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 同步任务字段映射结构校验规则(2026-10-04 新方案: 列裁剪白名单的注册级强校验)。
 *
 * <p>UI 建任务(SyncTaskService.save)与直建/脚本(CdcStService.register)共用同一套规则,
 * 保证"映射非空 + 输出字段集合法"在任何注册路径都成立。
 *
 * <ul>
 *   <li>field_mapping 非空(强校验: 空映射 = 全列直通, 洞穿写者列裁判前提)</li>
 *   <li>modelField 不撞保留名(seq / CdcRecord 顶层字段 / `_` 前缀)</li>
 *   <li>D2: 同一任务内, 同一源表多列 → 同一 modelField 拒绝(同源行必撞死配置);
 *       完全重复的映射行拒绝; 跨源表同 modelField 放行(业务允许, 主键区分行, 撞行 LWW)</li>
 *   <li>kind 基本约束: field-map 需 srcField; compute 表达式交给表达式校验器</li>
 * </ul>
 */
public final class SyncMappingRules {

    /** CdcRecord 顶层字段 + 系统保留列: modelField 禁止同名(缺陷 6a: 补 beforeKey) */
    public static final Set<String> RESERVED_MODEL_FIELDS = Set.of(
            "seq", "logicalTable", "taskId", "sourceDb", "sourceTable", "datasourceId",
            "businessKey", "beforeKey", "keyColumns", "rowKind", "snapshot", "positionMode", "fields");

    private SyncMappingRules() {
    }

    /** 解析 field_mapping JSON(非法 JSON 抛 IllegalArgumentException) */
    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> parseMapping(String json) {
        if (json == null || json.isBlank()) {
            return List.of();
        }
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().readValue(json, List.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("field_mapping JSON 解析失败: " + e.getMessage(), e);
        }
    }

    /**
     * 结构校验(不依赖 DB 与源表字段信息; 源字段存在性由任务启动时对探测源列集核对)。
     *
     * @return 问题列表(空 = 通过); 调用方抛异常或返回给前端
     */
    public static List<String> validateStructure(List<Map<String, Object>> fieldMapping) {
        List<String> issues = new ArrayList<>();
        if (fieldMapping == null || fieldMapping.isEmpty()) {
            issues.add("字段映射不能为空(强校验: 空映射会全列直通, 破坏写者列裁判前提)");
            return issues;
        }
        // modelField → 已见 (srcTable, srcField)(D2: 同源表多列→同 modelField 拒绝; 跨源表放行)
        Map<String, String[]> byModel = new HashMap<>();
        // 完全重复行检测: srcTable|srcField|modelField
        Set<String> exactSeen = new HashSet<>();
        for (Map<String, Object> row : fieldMapping) {
            String modelField = trim(row.get("modelField"));
            Map<String, Object> source = row.get("source") instanceof Map<?, ?> m
                    ? (Map<String, Object>) m : Map.of();
            String kind = trim(source.get("kind"));
            String srcTable = trim(source.get("srcTable"));
            String srcField = trim(source.get("srcField"));
            String expr = trim(source.get("expr"));
            if (modelField.isEmpty()) {
                issues.add("映射缺少 modelField(目标字段名)");
                continue;
            }
            if (RESERVED_MODEL_FIELDS.contains(modelField)) {
                issues.add("modelField " + modelField + " 与系统保留字段冲突, 请改名");
                continue;
            }
            if (modelField.startsWith("_")) {
                issues.add("modelField " + modelField + " 禁止以 _ 开头(_ 前缀保留给系统列)");
                continue;
            }
            if (kind.isBlank() || "null".equals(kind)) {
                issues.add("字段 " + modelField + " 未定义来源(kind)");
                continue;
            }
            if ("field-map".equals(kind) && srcField.isBlank()) {
                issues.add("字段 " + modelField + " 为 field-map 但缺少 srcField");
                continue;
            }
            if ("compute".equals(kind) && expr.isBlank()) {
                issues.add("字段 " + modelField + " 为 compute 但缺少 expr");
                continue;
            }
            // D2: 任务内重复声明边界(仅对 field-map 按源表维度判定; compute/uuid/system 生成列天然唯一)
            String[] prev = byModel.get(modelField);
            if (prev != null) {
                if (srcTable.equals(prev[0]) && !srcField.equals(prev[1])) {
                    issues.add("同一源表 " + srcTable + " 的多个列(" + prev[1] + ", " + srcField
                            + ")映射到同一目标列 " + modelField
                            + ": 同源行必撞(死配置), 请改用 compute 表达式显式表达");
                }
                // 跨源表同 modelField = 业务允许, 放行
            } else {
                byModel.put(modelField, new String[]{srcTable, srcField});
            }
            String exactKey = srcTable + "|" + srcField + "|" + modelField;
            if (!exactSeen.add(exactKey)) {
                issues.add("映射重复: " + (srcTable.isEmpty() ? "" : srcTable + ".") + srcField
                        + " → " + modelField + " 声明了多次");
            }
        }
        return issues;
    }

    private static String trim(Object v) {
        return v == null ? "" : String.valueOf(v).trim();
    }
}
