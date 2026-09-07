package com.dcp.module.tagengine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;

/**
 * 打标引擎 V2 —— 标签注册表(tag_registry):
 * 载体 = tag_dict 树(身份/层级) + 注册表声明(可计算/可打标属性), 树节点即标签, 注册表一行挂一个节点。
 * 声明列: value_type(结构)/cardinality/timing(时效)/compute(计算模式, 默认由 timing 推导)/store(存储, 推导)/ttl/domain(取值域)。
 */
@Slf4j
@Component
public class TagRegistryService {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final Set<String> VALUE_TYPES = Set.of("boolean", "enum1", "enumN", "number", "score", "datetime");
    private static final Set<String> CARDINALITIES = Set.of("single", "multi");
    private static final Set<String> TIMINGS = Set.of("static", "slow", "accumulate", "sliding", "instant");

    private final JdbcTemplate metaJdbc;

    public TagRegistryService(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    @PostConstruct
    public void ensureSchema() {
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS tag_registry ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "tag_dict_id BIGINT NOT NULL COMMENT 'tag_dict 树节点(标签身份)',"
                + "value_type VARCHAR(16) NOT NULL COMMENT 'boolean|enum1|enumN|number|score|datetime',"
                + "cardinality VARCHAR(8) NOT NULL DEFAULT 'single' COMMENT 'single|multi',"
                + "timing VARCHAR(16) NOT NULL COMMENT 'static|slow|accumulate|sliding|instant',"
                + "compute VARCHAR(16) NOT NULL COMMENT 'batch|stream|online (由 timing 推导, 可覆盖)',"
                + "store VARCHAR(16) NOT NULL COMMENT 'fact|online|bitmap',"
                + "ttl_days INT NULL COMMENT 'sliding/instant 必填',"
                + "domain_json TEXT NULL COMMENT '取值域(JSON array), enum/数值档位校验用',"
                + "descr VARCHAR(512),"
                + "status TINYINT NOT NULL DEFAULT 1,"
                + "create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
                + "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                + "UNIQUE KEY uk_tag (tag_dict_id))");
        log.info("[tagengine] schema ensured: tag_registry");
    }

    /* ---------- 工具 ---------- */

    private static String jstr(Object v) {
        try {
            return OM.writeValueAsString(v == null ? "[]" : v);
        } catch (Exception e) {
            throw new IllegalArgumentException("JSON 序列化失败: " + e.getMessage());
        }
    }

    private static JsonNode jnode(String json) {
        try {
            return OM.readTree(json == null || json.isBlank() ? "{}" : json);
        } catch (Exception e) {
            throw new IllegalArgumentException("JSON 非法");
        }
    }

    private static String s(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    /** 全量 tag_dict → path 前缀(父链/name) */
    private Map<Long, String> dictPaths() {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id,parent_id,name FROM tag_dict");
        Map<Long, String> name = new HashMap<>();
        Map<Long, Long> parent = new HashMap<>();
        for (Map<String, Object> r : rows) {
            name.put(((Number) r.get("id")).longValue(), s(r.get("name")));
            parent.put(((Number) r.get("id")).longValue(), ((Number) r.get("parent_id")).longValue());
        }
        Map<Long, String> out = new HashMap<>();
        for (Long id : name.keySet()) {
            List<String> chain = new ArrayList<>();
            Long cur = id;
            Set<Long> guard = new HashSet<>();
            while (cur != null && cur != 0 && guard.add(cur) && name.containsKey(cur)) {
                chain.add(0, name.get(cur));
                cur = parent.get(cur);
            }
            out.put(id, String.join("/", chain));
        }
        return out;
    }

    private Map<String, Object> rowToMap(Map<String, Object> r, Map<Long, String> paths) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", r.get("id"));
        m.put("tagDictId", r.get("tag_dict_id"));
        m.put("tagPath", paths.get(((Number) r.get("tag_dict_id")).longValue()));
        m.put("valueType", r.get("value_type"));
        m.put("cardinality", r.get("cardinality"));
        m.put("timing", r.get("timing"));
        m.put("compute", r.get("compute"));
        m.put("store", r.get("store"));
        m.put("ttlDays", r.get("ttl_days"));
        m.put("domain", jnode((String) r.get("domain_json")));
        m.put("descr", r.get("descr"));
        m.put("status", r.get("status"));
        return m;
    }

    private void validate(Map<String, Object> body) {
        long tagDictId = body.get("tagDictId") instanceof Number n ? n.longValue() : Long.parseLong(s(body.get("tagDictId")));
        Integer exist = metaJdbc.queryForObject("SELECT COUNT(*) FROM tag_dict WHERE id=?", Integer.class, tagDictId);
        if (exist == null || exist == 0) throw new IllegalArgumentException("tag_dict 节点不存在 id=" + tagDictId);
        String vt = s(body.get("valueType"));
        String timing = s(body.get("timing"));
        if (!VALUE_TYPES.contains(vt)) throw new IllegalArgumentException("valueType 须为 boolean|enum1|enumN|number|score|datetime");
        if (!TIMINGS.contains(timing)) throw new IllegalArgumentException("timing 须为 static|slow|accumulate|sliding|instant");
        String card = s(body.getOrDefault("cardinality", "single"));
        if (!CARDINALITIES.contains(card)) throw new IllegalArgumentException("cardinality 须为 single|multi");
        /* 默认推导: compute/store 由 timing; 可显式覆盖 */
        Map<String, String[]> infer = new HashMap<>();
        infer.put("static", new String[]{"batch", "fact"});
        infer.put("slow", new String[]{"batch", "fact"});
        infer.put("accumulate", new String[]{"batch", "fact"});
        infer.put("sliding", new String[]{"stream", "fact+online"});
        infer.put("instant", new String[]{"online", "online"});
        String[] def = infer.get(timing);
        String compute = s(body.getOrDefault("compute", def[0]));
        String store = s(body.getOrDefault("store", def[1]));
        if (!Set.of("batch", "stream", "online").contains(compute)) throw new IllegalArgumentException("compute 须为 batch|stream|online");
        if (!Set.of("fact", "online", "bitmap", "fact+online").contains(store)) throw new IllegalArgumentException("store 须为 fact|online|bitmap|fact+online");
        if (Set.of("sliding", "instant").contains(timing)) {
            Number ttl = body.get("ttlDays") instanceof Number num ? num : null;
            if (ttl == null || ttl.intValue() <= 0) throw new IllegalArgumentException("sliding/instant 标签必填 ttlDays");
        }
    }

    private Object[] bindParams(Map<String, Object> body) {
        long tagDictId = body.get("tagDictId") instanceof Number n ? n.longValue() : Long.parseLong(s(body.get("tagDictId")));
        String timing = s(body.get("timing"));
        Map<String, String[]> infer = new HashMap<>();
        infer.put("static", new String[]{"batch", "fact"});
        infer.put("slow", new String[]{"batch", "fact"});
        infer.put("accumulate", new String[]{"batch", "fact"});
        infer.put("sliding", new String[]{"stream", "fact+online"});
        infer.put("instant", new String[]{"online", "online"});
        String[] d = infer.get(timing);
        return new Object[]{
                tagDictId,
                s(body.get("valueType")),
                s(body.getOrDefault("cardinality", "single")),
                timing,
                s(body.getOrDefault("compute", d[0])),
                s(body.getOrDefault("store", d[1])),
                body.get("ttlDays") == null ? null : ((Number) body.get("ttlDays")).intValue(),
                jstr(body.getOrDefault("domain", new ArrayList<String>())),
                s(body.getOrDefault("descr", "")),
                body.getOrDefault("status", 1) instanceof Number num ? num.intValue() : 1
        };
    }

    /* ---------- API ---------- */

    public List<Map<String, Object>> list() {
        Map<Long, String> paths = dictPaths();
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id,tag_dict_id,value_type,cardinality,timing,compute,store,ttl_days,domain_json,descr,status FROM tag_registry ORDER BY id");
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : rows) out.add(rowToMap(r, paths));
        return out;
    }

    public Map<String, Object> create(Map<String, Object> body) {
        validate(body);
        Object[] p = bindParams(body);
        Long tagDictId = (Long) p[0];
        Integer dup = metaJdbc.queryForObject("SELECT COUNT(*) FROM tag_registry WHERE tag_dict_id=?", Integer.class, tagDictId);
        if (dup != null && dup > 0) throw new IllegalArgumentException("该标签节点已注册, 用 PUT 更新声明");
        KeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO tag_registry(tag_dict_id,value_type,cardinality,timing,compute,store,ttl_days,domain_json,descr,status)"
                            + " VALUES(?,?,?,?,?,?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
            for (int i = 0; i < p.length; i++) ps.setObject(i + 1, p[i]);
            return ps;
        }, kh);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", kh.getKey());
        out.put("tagDictId", tagDictId);
        log.info("[tagengine] registry created tag_dict_id={} #{}", tagDictId, out.get("id"));
        return out;
    }

    public Map<String, Object> update(long id, Map<String, Object> body) {
        List<Map<String, Object>> rows = metaJdbc.queryForList("SELECT id,tag_dict_id FROM tag_registry WHERE id=?", id);
        if (rows.isEmpty()) throw new IllegalArgumentException("注册记录不存在 id=" + id);
        body.putIfAbsent("tagDictId", rows.get(0).get("tag_dict_id"));
        validate(body);
        Object[] p = bindParams(body);
        metaJdbc.update("UPDATE tag_registry SET value_type=?,cardinality=?,timing=?,compute=?,store=?,ttl_days=?,domain_json=?,descr=?,status=? WHERE id=?",
                p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], id);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("tagDictId", p[0]);
        return out;
    }

    public void delete(long id) {
        int n = metaJdbc.update("DELETE FROM tag_registry WHERE id=?", id);
        if (n == 0) throw new IllegalArgumentException("注册记录不存在 id=" + id);
    }
}
