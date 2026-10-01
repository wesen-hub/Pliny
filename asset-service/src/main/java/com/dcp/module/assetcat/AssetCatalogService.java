package com.dcp.module.assetcat;

import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * 数据资产目录服务(2026-09-09 P0): 人工业务打标(资产×tag_path 绑定) + 按标签查资产 + 标签树。
 * 自动标签(分层/来源/类型)不落库——由血缘图(/api/lineage/graph)实时推导, 目录前端已实现;
 * 本模块只负责"人工业务标签"(如 业务域/电商、项目/营销), 与 tag_dict 字典树设计同构但按需即存(路径即标签, 支持 / 层级聚合)。
 */
@Service("catalogTagService")
public class AssetCatalogService {

    private final JdbcTemplate jdbc;

    public AssetCatalogService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static String s(Object o) { return o == null ? "" : String.valueOf(o); }

    /** 打标(幂等): 支持任意层级路径 */
    public void tag(String table, String path, String who) {
        String t = s(table).trim(), p = normalize(s(path));
        if (t.isEmpty() || p.isEmpty()) throw new BizException(ErrorCode.PARAM_ERROR, "table/path 必填");
        jdbc.update("INSERT IGNORE INTO asset_tag_binding(asset_table, tag_path, create_by) VALUES(?,?,?)", t, p, who);
        audit("TAG", t + " ← " + p);
    }

    /** 去标 */
    public void untag(String table, String path, String who) {
        jdbc.update("DELETE FROM asset_tag_binding WHERE asset_table=? AND tag_path=?", s(table), normalize(s(path)));
        audit("UNTAG", s(table) + " 移除 " + s(path));
    }

    /** 某资产的业务标签 */
    public List<String> tagsOf(String table) {
        return jdbc.queryForList("SELECT tag_path FROM asset_tag_binding WHERE asset_table=? ORDER BY tag_path", s(table))
                .stream().map(r -> s(r.get("tag_path"))).toList();
    }

    /** 按标签查资产(tag 或其后代路径, LIKE 'tag/%') */
    public List<String> assetsByTag(String tag) {
        String p = normalize(s(tag));
        if (p.isEmpty()) return List.of();
        return jdbc.queryForList("SELECT DISTINCT asset_table FROM asset_tag_binding WHERE tag_path=? OR tag_path LIKE ? ORDER BY asset_table",
                        p, p + "/%")
                .stream().map(r -> s(r.get("asset_table"))).toList();
    }

    /** 业务标签树(按 / 层级聚合 + 资产计数) */
    public List<Map<String, Object>> tagTree() {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT tag_path, COUNT(DISTINCT asset_table) c FROM asset_tag_binding GROUP BY tag_path ORDER BY tag_path");
        return treeFromRows(rows);
    }

    private List<Map<String, Object>> treeFromRows(List<Map<String, Object>> rows) {
        Map<String, Map<String, Object>> node = new LinkedHashMap<>();
        Map<String, List<String>> child = new HashMap<>();
        for (Map<String, Object> r : rows) {
            String[] segs = s(r.get("tag_path")).split("/");
            String acc = "";
            for (int i = 0; i < segs.length; i++) {
                String parent = acc;
                acc = acc.isEmpty() ? segs[i] : acc + "/" + segs[i];
                if (!node.containsKey(acc)) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", "biz:" + acc);
                    m.put("name", segs[i]);
                    m.put("count", 0);
                    m.put("full", acc);
                    node.put(acc, m);
                }
                if (i == segs.length - 1) node.get(acc).put("count", ((Number) r.get("c")).intValue());
                child.computeIfAbsent(parent, k -> new ArrayList<>()).add(acc);
            }
        }
        List<Map<String, Object>> roots = new ArrayList<>();
        for (Map<String, Object> m : node.values()) {
            String full = (String) m.get("full");
            if (full.contains("/")) continue;
            roots.add(build(m, full, node, child));
        }
        return roots;
    }

    private Map<String, Object> build(Map<String, Object> self, String full,
                                      Map<String, Map<String, Object>> node, Map<String, List<String>> child) {
        Map<String, Object> out = new LinkedHashMap<>(self);
        List<Map<String, Object>> cs = new ArrayList<>();
        int sum = 0;
        for (String k : child.getOrDefault(full, List.of())) {
            Map<String, Object> c = build(node.get(k), k, node, child);
            sum += ((Number) c.getOrDefault("count", 0)).intValue();
            cs.add(c);
        }
        if (!cs.isEmpty()) {
            out.put("children", cs);
            // 父节点计数 = 子孙叶子绑定合计(更直观的目录聚合数)
            int own = ((Number) out.getOrDefault("count", 0)).intValue();
            out.put("count", own > 0 ? own : sum);
        }
        return out;
    }

    private static String normalize(String p) {
        String x = s(p).trim().replaceAll("^/+", "").replaceAll("/+$", "");
        return x.replaceAll("/{2,}", "/");
    }

    @Autowired
    private com.dcp.module.system.service.AuditService auditService;

    private void audit(String op, String detail) {
        try { if (auditService != null) auditService.record("asset-catalog", op, "asset", null, detail); } catch (Exception ignore) { }
    }
}
