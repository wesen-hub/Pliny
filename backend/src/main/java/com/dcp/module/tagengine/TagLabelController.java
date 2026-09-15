package com.dcp.module.tagengine;

import com.dcp.common.ApiResponse;
import com.dcp.module.tagengine.perm.DataSecurityService;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 标签服务 API(P1; 查询面 = 投影表 tag_fact_view) + 数据权限(P3): 标签级 L0-L3
 * L0=不可见(剔除) L1=结构可见(无数据) L2=可圈选(仅计数, 不附实体明细) L3=全量明细。
 */
@RestController
@RequestMapping("/api/tagengine/labels")
public class TagLabelController {

    private final TagEngineService engine;
    private final DataSecurityService security;

    public TagLabelController(TagEngineService engine, DataSecurityService security) {
        this.engine = engine;
        this.security = security;
    }

    @GetMapping("/summary")
    public ApiResponse<List<Map<String, Object>>> summary() {
        List<Map<String, Object>> rows = engine.labelSummary();
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            int lv = security.tagLevel(String.valueOf(r.get("tag")));
            if (lv <= DataSecurityService.HIDDEN) continue;         // L0 剔除
            if (lv == DataSecurityService.META) r.put("entities", null);  // L1 见结构不见数据
            out.add(r);
        }
        return ApiResponse.ok(out);
    }

    @GetMapping("/circle")
    public ApiResponse<Map<String, Object>> circle(@RequestParam String tagPath,
                                                   @RequestParam(required = false) Long tagDictId,
                                                   @RequestParam(required = false) String value,
                                                   @RequestParam(defaultValue = "100") int limit) {
        int lv = security.tagLevel(tagPath);
        if (lv < DataSecurityService.MASKED) {
            return ApiResponse.error(403, "标签不可圈选(当前可见等级 L" + lv + ", 圈选需 L2+): " + tagPath);
        }
        Map<String, Object> m;
        try {
            m = engine.labelCircle(tagPath, tagDictId, value, Math.min(Math.max(limit, 1), 1000));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
        if (lv == DataSecurityService.MASKED) {
            // L2: 可圈选计数与标签值, 不下钻实体明细
            Object total = m.get("count");
            Map<String, Object> slim = new LinkedHashMap<>();
            slim.put("tagPath", tagPath);
            slim.put("value", m.get("value"));
            slim.put("total", total);
            slim.put("entities", new ArrayList<>());
            return ApiResponse.ok(slim);
        }
        return ApiResponse.ok(m);
    }

    @GetMapping("/entity/{entityKey}")
    public ApiResponse<Map<String, Object>> entity(@PathVariable String entityKey) {
        try {
            Map<String, Object> m = engine.labelEntity(entityKey);
            List<Map<String, Object>> labels = (List<Map<String, Object>>) m.get("labels");
            List<Map<String, Object>> visible = new ArrayList<>();
            if (labels != null) {
                for (Map<String, Object> row : labels) {
                    if (security.tagLevel(String.valueOf(row.get("tag"))) >= DataSecurityService.MASKED) {
                        visible.add(row);
                    }
                }
            }
            m.put("labels", visible);
            m.put("count", visible.size());
            return ApiResponse.ok(m);
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }
}
