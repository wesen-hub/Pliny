package com.dcp.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;

/**
 * 标签体系(多级 tag_dict):
 * - 标签字典树可维护(增删改), path 由层级推导(如 业务域/交易/订单)
 * - 资产打标: asset_meta.tags 存标签 path 数组(前端树选择)
 * - 系统标签自动统计: 来源系统/{dsName}、分层/{layer}、密级/{密级名} 从资产属性实时派生
 * - 挂载资产数: 手动标签查 asset_meta.tags 匹配(含子孙继承); 系统标签按规则实时算
 */
@Service
public class TagService {
    private static final Logger log = LoggerFactory.getLogger(TagService.class);
    private final JdbcTemplate metaJdbc;
    private final AssetCatalogService catalog;

    /**
     * 挂载资产数缓存:树结构**每次实时读**(新增标签立刻可见),但"哪些资产命中哪些标签"依赖整份资产目录,
     * 算一次要 4N+1 次查询 → 按短 TTL 缓存。新增标签不在缓存里 → 自然取 0,无需失效;资产元数据最多旧 TTL。
     */
    private static final long ASSET_COUNT_TTL_MS = 30_000L;
    private volatile Map<String, Integer> assetCountCache;
    private volatile long assetCountAt;

    public TagService(JdbcTemplate jdbcTemplate, AssetCatalogService catalog) {
        this.metaJdbc = jdbcTemplate;
        this.catalog = catalog;
        initDefaults();
    }

    /** 默认标签树(幂等): 6 个一级 + 常用二级 */
    private void initDefaults() {
        long biz = ensure("业务域", 0, "业务标签", "表级", "模型定义", 1);
        ensure("交易", biz, "业务标签", "表级", "模型定义", 1);
        ensure("财务", biz, "业务标签", "表级", "模型定义", 2);
        ensure("客户", biz, "业务标签", "表级", "模型定义", 3);
        long src = ensure("来源系统", 0, "系统标签", "表级", "采集自动继承", 2);
        ensure("MySQL", src, "系统标签", "表级", "采集自动继承", 1);
        ensure("PostgreSQL", src, "系统标签", "表级", "采集自动继承", 2);
        ensure("Oracle", src, "系统标签", "表级", "采集自动继承", 3);
        ensure("MongoDB", src, "系统标签", "表级", "采集自动继承", 4);
        long sec = ensure("密级", 0, "敏感标签", "表级", "定级审批", 3);
        ensure("公开", sec, "敏感标签", "表级", "定级审批", 1);
        ensure("内部", sec, "敏感标签", "表级", "定级审批", 2);
        ensure("机密", sec, "敏感标签", "表级", "定级审批", 3);
        ensure("绝密", sec, "敏感标签", "表级", "定级审批", 4);
        long layer = ensure("分层", 0, "业务标签", "表级", "模型定义", 4);
        ensure("ODS", layer, "业务标签", "表级", "模型定义", 1);
        ensure("DWD", layer, "业务标签", "表级", "模型定义", 2);
        ensure("DWS", layer, "业务标签", "表级", "模型定义", 3);
        ensure("ADS", layer, "业务标签", "表级", "模型定义", 4);
        long sens = ensure("敏感字段", 0, "敏感标签", "字段级", "ML 检测", 5);
        ensure("手机号", sens, "敏感标签", "字段级", "ML 检测", 1);
        ensure("金额", sens, "敏感标签", "字段级", "ML 检测", 2);
        ensure("自定义标签", 0, "自定义标签", "表级", "手动", 6);
    }

    private long ensure(String name, long parentId, String category, String granularity, String source, int sort) {
        Integer n = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM tag_dict WHERE parent_id = ? AND name = ?", Integer.class, parentId, name);
        if (n != null && n > 0) {
            return metaJdbc.queryForObject(
                    "SELECT id FROM tag_dict WHERE parent_id = ? AND name = ? LIMIT 1", Long.class, parentId, name);
        }
        KeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO tag_dict (parent_id, name, category, granularity, source, sort) VALUES (?, ?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setLong(1, parentId);
            ps.setString(2, name);
            ps.setString(3, category);
            ps.setString(4, granularity);
            ps.setString(5, source);
            ps.setInt(6, sort);
            return ps;
        }, kh);
        return kh.getKey().longValue();
    }

    // ---- 标签树 ----

    /** 标签树: 节点 + path + 挂载资产数(系统标签实时派生/手动标签匹配 asset_meta.tags, 含子孙) */
    public List<Map<String, Object>> tree() {
        List<Map<String, Object>> tags = metaJdbc.queryForList(
                "SELECT id, parent_id, name, category, granularity, source, sort, status FROM tag_dict ORDER BY sort, id");
        Map<Long, Map<String, Object>> nodeMap = new LinkedHashMap<>();
        for (Map<String, Object> t : tags) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", t.get("id"));
            m.put("parent_id", t.get("parent_id"));
            m.put("tag", t.get("name"));
            m.put("category", t.get("category"));
            m.put("granularity", t.get("granularity"));
            m.put("source", t.get("source"));
            m.put("status", t.get("status"));
            m.put("level", 1);
            m.put("path", "");
            m.put("children", new ArrayList<>());
            nodeMap.put(((Number) t.get("id")).longValue(), m);
        }
        // 组装层级:父节点缺失(孤儿——通常是父被删/被移动)时**挂到根并打 orphan 标记**,不再静默丢节点
        List<Map<String, Object>> roots = new ArrayList<>();
        for (Map<String, Object> nd : nodeMap.values()) {
            long pid = ((Number) nd.get("parent_id")).longValue();
            Map<String, Object> parent = pid == 0 ? null : nodeMap.get(pid);
            if (parent != null) {
                ((List<Map<String, Object>>) parent.get("children")).add(nd);
            } else {
                if (pid != 0) nd.put("orphan", true);   // 父不存在 → 挂根但标出来
                roots.add(nd);
            }
        }
        // 计算 path + 资产数(资产侧走缓存,不再每节点重扫资产)
        Map<String, Integer> counts = assetCountsCached(tags);
        for (Map<String, Object> nd : nodeMap.values()) {
            String path = buildPath(nodeMap, ((Number) nd.get("id")).longValue());
            nd.put("path", path);
            nd.put("level", path.split("/").length);
            nd.put("assets", counts.getOrDefault(path, 0));
        }
        return roots;
    }

    /** 单次查全表标签(路径/计数共用),避免同一请求里重复查 tag_dict */
    private List<Map<String, Object>> loadTags() {
        return metaJdbc.queryForList("SELECT id, parent_id, name FROM tag_dict ORDER BY sort, id");
    }

    /** 资产数(按 path)带短 TTL 缓存 */
    private Map<String, Integer> assetCountsCached(List<Map<String, Object>> tags) {
        long now = System.currentTimeMillis();
        Map<String, Integer> c = assetCountCache;
        if (c != null && now - assetCountAt < ASSET_COUNT_TTL_MS) return c;
        synchronized (this) {
            if (assetCountCache != null && System.currentTimeMillis() - assetCountAt < ASSET_COUNT_TTL_MS) {
                return assetCountCache;
            }
            Map<String, Integer> fresh = countAssetsByPath(assetTagPaths(catalog.assetsWithMeta(), tags));
            assetCountCache = fresh;
            assetCountAt = System.currentTimeMillis();
            return fresh;
        }
    }

    private String buildPath(Map<Long, Map<String, Object>> nodeMap, long id) {
        return buildPath(nodeMap, id, new HashSet<>());
    }

    /** 父链求路径;guard 防父链成环时无限递归(脏数据兜底) */
    private String buildPath(Map<Long, Map<String, Object>> nodeMap, long id, Set<Long> guard) {
        Map<String, Object> nd = nodeMap.get(id);
        if (nd == null || !guard.add(id)) return "";
        long pid = ((Number) nd.get("parent_id")).longValue();
        String name = String.valueOf(nd.get("tag"));
        if (pid == 0) return name;
        String parent = buildPath(nodeMap, pid, guard);
        return parent.isEmpty() ? name : parent + "/" + name;
    }

    /** 每个资产 → 标签 path 集合(手动 tags + 系统标签命中), 统一前缀匹配实现父子聚合。
     *  tags 由调用方传入(同一请求只查一次 tag_dict)。 */
    private Map<String, Set<String>> assetTagPaths(List<Map<String, Object>> assets,
                                                   List<Map<String, Object>> tagsIn) {
        Map<String, Set<String>> map = new HashMap<>();
        // 全部标签 path(含层级)
        Map<Long, Map<String, Object>> tagMap = new LinkedHashMap<>();
        List<Map<String, Object>> tags = tagsIn;
        for (Map<String, Object> t : tags) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", t.get("id"));
            m.put("parent_id", t.get("parent_id"));
            m.put("tag", t.get("name"));
            tagMap.put(((Number) t.get("id")).longValue(), m);
        }
        List<String> allPaths = new ArrayList<>();
        for (Map<String, Object> t : tags) {
            allPaths.add(buildPath(tagMap, ((Number) t.get("id")).longValue()));
        }
        for (Map<String, Object> a : assets) {
            String table = String.valueOf(a.get("table"));
            Set<String> paths = new HashSet<>();
            // 系统标签命中
            for (String p : allPaths) {
                if (assetMatchesTag(a, p)) paths.add(p);
            }
            // 手动标签(asset_meta.tags, 存 path)
            Object manual = a.get("tags");
            if (manual instanceof List<?> list) {
                for (Object t : list) paths.add(String.valueOf(t));
            }
            map.put(table, paths);
        }
        return map;
    }

    /** 资产是否命中某标签 path(含系统标签规则 + 手动标签, 不含子孙继承) */
    private boolean assetMatchesTag(Map<String, Object> asset, String path) {
        String[] secNames = {"", "公开", "内部", "机密", "绝密"};
        if (path.startsWith("来源系统/")) {
            String name = path.substring("来源系统/".length());
            for (Object s : (List<?>) asset.getOrDefault("sources", List.of())) {
                Map<?, ?> src = (Map<?, ?>) s;
                if (String.valueOf(src.get("dsName")).contains(name)
                        || String.valueOf(src.get("dsType")).equalsIgnoreCase(name)) {
                    return true;
                }
            }
            return false;
        }
        if (path.startsWith("分层/")) {
            String name = path.substring("分层/".length());
            return name.equals(asset.get("layer"));
        }
        if (path.startsWith("密级/")) {
            String name = path.substring("密级/".length());
            int sec = asset.get("secretLevel") instanceof Number n ? n.intValue() : 2;
            return sec >= 1 && sec <= 4 && name.equals(secNames[sec]);
        }
        // 手动标签: asset_meta.tags 含该 path
        Object tags = asset.get("tags");
        if (tags instanceof List<?> list) {
            return list.stream().anyMatch(t -> String.valueOf(t).equals(path));
        }
        return false;
    }

    /**
     * 每个标签 path 的资产数(含子孙继承)。
     * <p>做法:遍历一次资产的命中 path,对**它自身及其所有祖先 path** 各登记一次该资产(去重),
     * 这样 countAssets 从"每节点重扫全部资产"(O(节点×资产×路径))降为**单次遍历**(O(资产×路径×深度))。
     */
    private Map<String, Integer> countAssetsByPath(Map<String, Set<String>> assetPaths) {
        Map<String, Set<String>> hit = new HashMap<>();
        for (Map.Entry<String, Set<String>> e : assetPaths.entrySet()) {
            String table = e.getKey();
            for (String p : e.getValue()) {
                String cur = p;
                Set<String> guard = new HashSet<>();
                while (cur != null && !cur.isEmpty() && guard.add(cur)) {
                    hit.computeIfAbsent(cur, k -> new HashSet<>()).add(table);
                    int i = cur.lastIndexOf('/');
                    cur = i < 0 ? null : cur.substring(0, i);
                }
            }
        }
        Map<String, Integer> out = new HashMap<>();
        hit.forEach((k, v) -> out.put(k, v.size()));
        return out;
    }

    /** 某标签(含子孙)下的资产列表 */
    public List<Map<String, Object>> assetsOfTag(String path) {
        List<Map<String, Object>> all = catalog.assetsWithMeta();
        Map<String, Set<String>> assetPaths = assetTagPaths(all, loadTags());
        return all.stream().filter(a -> assetPaths.getOrDefault(String.valueOf(a.get("table")), Set.of()).stream()
                .anyMatch(p -> p.equals(path) || p.startsWith(path + "/"))).toList();
    }

    // ---- 标签维护 ----

    /** 移动标签(改父节点 = 调整目录层级) */
    public void move(long id, long newParentId) {
        if (id == newParentId) {
            throw new IllegalArgumentException("不能移动到自身");
        }
        Integer n = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM tag_dict WHERE id = ?", Integer.class, id);
        if (n == null || n == 0) {
            throw new IllegalArgumentException("标签不存在: " + id);
        }
        // 防环: 新父不能是自己的子孙
        if (isDescendant(id, newParentId)) {
            throw new IllegalArgumentException("不能移动到自己的子孙节点下");
        }
        Integer dup = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM tag_dict WHERE parent_id = ? AND name = (SELECT name FROM tag_dict WHERE id = ?)",
                Integer.class, newParentId, id);
        if (dup != null && dup > 0) {
            throw new IllegalArgumentException("目标层级已存在同名标签");
        }
        metaJdbc.update("UPDATE tag_dict SET parent_id = ? WHERE id = ?", newParentId, id);
    }

    private boolean isDescendant(long ancestor, long candidate) {
        Long pid = metaJdbc.queryForObject(
                "SELECT parent_id FROM tag_dict WHERE id = ?", Long.class, candidate);
        while (pid != null && pid > 0) {
            if (pid == ancestor) {
                return true;
            }
            pid = metaJdbc.queryForObject(
                    "SELECT parent_id FROM tag_dict WHERE id = ?", Long.class, pid);
        }
        return false;
    }

    /**
     * 动态子集: 按标签组合筛选资产(目录的"查询子集"能力)。
     *
     * @param include 标签 path 列表(AND: 全部命中; OR: 任一命中; 含子孙继承)
     * @param op      AND | OR
     * @param exclude 排除标签 path 列表(命中即剔除)
     */
    public List<Map<String, Object>> subset(List<String> include, String op, List<String> exclude) {
        List<Map<String, Object>> all = catalog.assetsWithMeta();
        Map<String, Set<String>> assetPaths = assetTagPaths(all, loadTags());
        List<String> excludes = exclude == null ? List.of() : exclude;
        boolean and = !"OR".equalsIgnoreCase(op);
        return all.stream().filter(a -> {
            Set<String> paths = assetPaths.getOrDefault(String.valueOf(a.get("table")), Set.of());
            // 命中判定: path 等于某标签 或 是其子孙
            boolean hitInclude = include == null || include.isEmpty() || (and
                    ? include.stream().allMatch(t -> matches(paths, t))
                    : include.stream().anyMatch(t -> matches(paths, t)));
            if (!hitInclude) {
                return false;
            }
            // 排除: 命中任一排除标签则剔除
            return excludes.stream().noneMatch(t -> matches(paths, t));
        }).toList();
    }

    private boolean matches(Set<String> paths, String tagPath) {
        return paths.stream().anyMatch(p -> p.equals(tagPath) || p.startsWith(tagPath + "/"));
    }

    public long create(String name, long parentId, String category, String granularity, String source) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("标签名不能为空");
        Integer n = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM tag_dict WHERE parent_id = ? AND name = ?", Integer.class, parentId, name);
        if (n != null && n > 0) throw new IllegalArgumentException("同层级已存在同名标签: " + name);
        KeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO tag_dict (parent_id, name, category, granularity, source) VALUES (?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setLong(1, parentId);
            ps.setString(2, name);
            ps.setString(3, category == null ? "自定义标签" : category);
            ps.setString(4, granularity == null ? "表级" : granularity);
            ps.setString(5, source);
            return ps;
        }, kh);
        return kh.getKey().longValue();
    }

    public void update(long id, String name, String category, String granularity, String source, String status) {
        if (name != null && !name.isBlank()) {
            metaJdbc.update("UPDATE tag_dict SET name = ? WHERE id = ?", name, id);
        }
        if (category != null) metaJdbc.update("UPDATE tag_dict SET category = ? WHERE id = ?", category, id);
        if (granularity != null) metaJdbc.update("UPDATE tag_dict SET granularity = ? WHERE id = ?", granularity, id);
        if (source != null) metaJdbc.update("UPDATE tag_dict SET source = ? WHERE id = ?", source, id);
        if (status != null) metaJdbc.update("UPDATE tag_dict SET status = ? WHERE id = ?", status, id);
    }

    public void delete(long id) {
        metaJdbc.update("DELETE FROM tag_dict WHERE id = ? OR parent_id = ?", id, id);
    }
}
