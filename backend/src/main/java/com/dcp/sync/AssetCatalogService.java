package com.dcp.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;

/**
 * 数据资产编目(可维护): 多层级目录节点 + 多维度挂载 + 资产元数据。
 * - 维度: biz(业务域)/src(来源系统)/sec(密级)/custom(自定义)
 * - src 维度按来源系统自动分组(不落库); biz/sec/custom 由用户维护节点与挂载
 * - 资产 = 逻辑表; 资产元数据(asset_meta)可编辑(展示名/域/分层/密级/负责人/标签)
 * 注: 手动目录节点接口已停用(2026-09-09 标签驱动目录替代), 本 service 保留供 TagService 依赖。
 */
@Service
public class AssetCatalogService {
    private static final Logger log = LoggerFactory.getLogger(AssetCatalogService.class);
    private final JdbcTemplate metaJdbc;
    private final AssetService assetService;
    private final ObjectMapper om = new ObjectMapper();

    public AssetCatalogService(JdbcTemplate jdbcTemplate, AssetService assetService) {
        this.metaJdbc = jdbcTemplate;
        this.assetService = assetService;
        initDefaultNodes();
    }

    /** 默认维度根节点(幂等) */
    private void initDefaultNodes() {
        ensureRoot("biz", "业务域");
        ensureRoot("src", "来源系统");
        ensureRoot("sec", "密级");
        ensureRoot("custom", "自定义");
        // 密级默认子节点
        ensureChild("sec", 1, "公开");
        ensureChild("sec", 2, "内部");
        ensureChild("sec", 3, "机密");
        ensureChild("sec", 4, "绝密");
    }

    private void ensureRoot(String dim, String name) {
        Integer n = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM asset_catalog_node WHERE dim = ? AND parent_id = 0", Integer.class, dim);
        if (n == null || n == 0) {
            metaJdbc.update("INSERT INTO asset_catalog_node (dim, parent_id, name, sort) VALUES (?, 0, ?, 0)",
                    dim, name);
        }
    }

    private void ensureChild(String dim, int sort, String name) {
        Integer parent = metaJdbc.queryForObject(
                "SELECT id FROM asset_catalog_node WHERE dim = ? AND parent_id = 0 LIMIT 1", Integer.class, dim);
        if (parent == null) return;
        Integer n = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM asset_catalog_node WHERE dim = ? AND parent_id = ? AND name = ?",
                Integer.class, dim, parent, name);
        if (n == null || n == 0) {
            metaJdbc.update("INSERT INTO asset_catalog_node (dim, parent_id, name, sort) VALUES (?, ?, ?, ?)",
                    dim, parent, name, sort);
        }
    }

    /** 目录树(某维度): 节点树 + 挂载资产; src 维度按来源系统自动分组 */
    public Map<String, Object> tree(String dim) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("id", "root-" + dim);
        root.put("label", dimName(dim));
        root.put("dim", dim);
        root.put("children", new ArrayList<>());
        List<Map<String, Object>> assets = assetService.list();
        if ("src".equals(dim)) {
            // 按来源系统自动分组(不落库): 每组一个虚拟节点
            Map<String, List<Map<String, Object>>> bySrc = new LinkedHashMap<>();
            for (Map<String, Object> a : assets) {
                List<Map<String, Object>> srcs = (List<Map<String, Object>>) a.get("sources");
                String key;
                if (srcs == null || srcs.isEmpty()) {
                    key = "批任务/无来源";
                } else {
                    key = srcs.stream().map(s -> String.valueOf(s.get("dsName"))).distinct()
                            .reduce((x, y) -> x + "+" + y).orElse("未知来源");
                }
                bySrc.computeIfAbsent(key, k -> new ArrayList<>()).add(a);
            }
            List<Map<String, Object>> children = (List<Map<String, Object>>) root.get("children");
            bySrc.forEach((k, arr) -> {
                Map<String, Object> node = new LinkedHashMap<>();
                node.put("id", "src-" + k.hashCode());
                node.put("label", k);
                node.put("count", arr.size());
                node.put("virtual", true);
                node.put("children", arr.stream().map(a -> assetNode(a)).toList());
                children.add(node);
            });
            return root;
        }
        // 静态节点树(biz/sec/custom): 节点 + 挂载资产
        List<Map<String, Object>> nodes = metaJdbc.queryForList(
                "SELECT id, parent_id, name, sort FROM asset_catalog_node WHERE dim = ? ORDER BY sort, id", dim);
        Map<Long, Map<String, Object>> nodeMap = new LinkedHashMap<>();
        for (Map<String, Object> nd : nodes) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", nd.get("id"));
            m.put("parent_id", nd.get("parent_id"));
            m.put("label", nd.get("name"));
            m.put("dim", dim);
            m.put("children", new ArrayList<>());
            nodeMap.put(((Number) nd.get("id")).longValue(), m);
        }
        // 挂载关系
        Map<Long, List<Map<String, Object>>> bindings = new HashMap<>();
        if (!nodeMap.isEmpty()) {
            List<Map<String, Object>> binds = metaJdbc.queryForList(
                    "SELECT b.node_id, b.table_name FROM asset_node_binding b " +
                            "JOIN asset_catalog_node n ON n.id = b.node_id WHERE n.dim = ?", dim);
            for (Map<String, Object> b : binds) {
                long nid = ((Number) b.get("node_id")).longValue();
                String tbl = String.valueOf(b.get("table_name"));
                Map<String, Object> asset = assets.stream().filter(a -> tbl.equals(a.get("table")))
                        .findFirst().orElse(null);
                if (asset == null) continue;
                bindings.computeIfAbsent(nid, k -> new ArrayList<>()).add(assetNode(asset));
            }
        }
        // 组装树: 先挂父子层级, 再把挂载资产追加进节点
        Map<String, Object> uncategorized = null;
        List<Map<String, Object>> children = (List<Map<String, Object>>) root.get("children");
        for (Map<String, Object> nd : nodeMap.values()) {
            long id = ((Number) nd.get("id")).longValue();
            long pid = ((Number) nd.get("parent_id")).longValue();
            if (pid == 0) {
                children.add(nd);
            } else {
                Map<String, Object> parent = nodeMap.get(pid);
                if (parent != null) {
                    ((List<Map<String, Object>>) parent.get("children")).add(nd);
                }
            }
            List<Map<String, Object>> assetsOfNode = bindings.get(id);
            if (assetsOfNode != null) {
                ((List<Map<String, Object>>) nd.get("children")).addAll(assetsOfNode);
            }
        }
        // 未挂载资产 → "未分类"虚拟节点
        Set<String> bound = new HashSet<>();
        bindings.values().forEach(l -> l.forEach(a -> bound.add(String.valueOf(a.get("table")))));
        List<Map<String, Object>> unbound = assets.stream()
                .filter(a -> !bound.contains(String.valueOf(a.get("table")))).toList();
        if (!unbound.isEmpty()) {
            Map<String, Object> uc = new LinkedHashMap<>();
            uc.put("id", "uncategorized-" + dim);
            uc.put("label", "未分类");
            uc.put("count", unbound.size());
            uc.put("virtual", true);
            uc.put("children", unbound.stream().map(this::assetNode).toList());
            children.add(uc);
        }
        return root;
    }

    private Map<String, Object> assetNode(Map<String, Object> asset) {
        Map<String, Object> n = new LinkedHashMap<>();
        n.put("id", "asset-" + asset.get("table"));
        n.put("table", asset.get("table"));
        n.put("label", String.valueOf(asset.get("name")));
        n.put("count", asset.get("shardCount"));
        n.put("status", asset.get("status"));
        n.put("sensitive", asset.get("sensitive"));
        return n;
    }

    private String dimName(String dim) {
        return switch (dim) {
            case "biz" -> "业务域";
            case "src" -> "来源系统";
            case "sec" -> "密级";
            case "custom" -> "自定义";
            default -> dim;
        };
    }

    // ---- 节点维护 ----

    public long createNode(String dim, long parentId, String name, int sort) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("节点名不能为空");
        Integer exists = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM asset_catalog_node WHERE dim = ? AND parent_id = ? AND name = ?",
                Integer.class, dim, parentId, name);
        if (exists != null && exists > 0) throw new IllegalArgumentException("同层级已存在同名节点: " + name);
        KeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO asset_catalog_node (dim, parent_id, name, sort) VALUES (?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, dim);
            ps.setLong(2, parentId);
            ps.setString(3, name);
            ps.setInt(4, sort);
            return ps;
        }, kh);
        return kh.getKey().longValue();
    }

    public void renameNode(long id, String name) {
        metaJdbc.update("UPDATE asset_catalog_node SET name = ? WHERE id = ?", name, id);
    }

    @Transactional
    public void deleteNode(long id) {
        metaJdbc.update("DELETE FROM asset_node_binding WHERE node_id IN " +
                "(SELECT id FROM (SELECT id FROM asset_catalog_node WHERE id = ? OR parent_id = ?) t)", id, id);
        metaJdbc.update("DELETE FROM asset_catalog_node WHERE id = ? OR parent_id = ?", id, id);
    }

    // ---- 挂载维护 ----

    public void bind(long nodeId, String tableName) {
        metaJdbc.update("INSERT IGNORE INTO asset_node_binding (node_id, table_name) VALUES (?, ?)",
                nodeId, tableName);
    }

    public void unbind(long nodeId, String tableName) {
        metaJdbc.update("DELETE FROM asset_node_binding WHERE node_id = ? AND table_name = ?",
                nodeId, tableName);
    }

    // ---- 资产元数据维护 ----

    public void saveMeta(String table, Map<String, Object> meta) {
        String displayName = str(meta.get("displayName"));
        String description = str(meta.get("description"));
        String domain = str(meta.get("domain"));
        String layer = str(meta.get("layer"));
        int secretLevel = meta.get("secretLevel") == null ? 2 : ((Number) meta.get("secretLevel")).intValue();
        String owner = str(meta.get("owner"));
        String quality = str(meta.get("quality"));
        String tags = meta.get("tags") == null ? null : toJson(meta.get("tags"));
        Integer n = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM asset_meta WHERE table_name = ?", Integer.class, table);
        if (n == null || n == 0) {
            metaJdbc.update("INSERT INTO asset_meta (table_name, display_name, description, domain, layer, " +
                            "secret_level, owner, quality, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    table, displayName, description, domain, layer, secretLevel, owner, quality, tags);
        } else {
            metaJdbc.update("UPDATE asset_meta SET display_name = ?, description = ?, domain = ?, layer = ?, " +
                            "secret_level = ?, owner = ?, quality = ?, tags = ? WHERE table_name = ?",
                    displayName, description, domain, layer, secretLevel, owner, quality, tags, table);
        }
    }

    public Map<String, Object> metaOf(String table) {
        try {
            return metaJdbc.queryForMap("SELECT * FROM asset_meta WHERE table_name = ?", table);
        } catch (Exception e) {
            return Map.of();
        }
    }

    /** 资产列表增强: 合并可维护元数据(展示名/域/分层/密级/负责人/标签) */
    public List<Map<String, Object>> assetsWithMeta() {
        List<Map<String, Object>> assets = assetService.list();
        for (Map<String, Object> a : assets) {
            Map<String, Object> meta = metaOf(String.valueOf(a.get("table")));
            a.put("displayName", meta.getOrDefault("display_name", ""));
            a.put("description", meta.getOrDefault("description", ""));
            a.put("domain", meta.getOrDefault("domain", ""));
            a.put("layer", meta.getOrDefault("layer", ""));
            a.put("secretLevel", meta.getOrDefault("secret_level", 2));
            a.put("owner", meta.getOrDefault("owner", ""));
            a.put("quality", meta.getOrDefault("quality", ""));
            // tags 为 JSON 列, 解析为 List(path 数组); 解析失败保持 null
            Object tags = meta.getOrDefault("tags", null);
            if (tags instanceof String s && !s.isBlank()) {
                try {
                    a.put("tags", om.readValue(s, List.class));
                } catch (Exception e) {
                    a.put("tags", null);
                }
            } else {
                a.put("tags", null);
            }
        }
        return assets;
    }

    private String str(Object o) {
        return o == null ? null : String.valueOf(o).isBlank() ? null : String.valueOf(o);
    }

    private String toJson(Object o) {
        try {
            return om.writeValueAsString(o);
        } catch (Exception e) {
            return null;
        }
    }
}
