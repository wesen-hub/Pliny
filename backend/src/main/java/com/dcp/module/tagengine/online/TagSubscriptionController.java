package com.dcp.module.tagengine.online;

import com.dcp.common.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.web.bind.annotation.*;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;

/**
 * 标签订阅 + 变更推送(P1.2④; 设计 §17.7): 订阅 = 标签条件(tagPath + 可选精确值);
 * 命中实体 = 标签服务圈选(tag_fact_view 实时); push 底座 = 执行端标签变更审计 tag_notify(批/流/在线三态写入,
 * 每 entity+tag+task 保留最新一次), 订阅命中统计/变更流由此查询。
 *
 * <pre>
 *   GET    /api/tagengine/subs                订阅列表(含各订阅命中实体数/最近变更时间)
 *   POST   /api/tagengine/subs                新建 {name, tagPath, tagValue?}
 *   PUT    /api/tagengine/subs/{id}           更新(name/tagValue/status)
 *   DELETE /api/tagengine/subs/{id}
 *   GET    /api/tagengine/notify              变更审计流(可按 tagPath/taskMode 过滤, 时间倒序)
 * </pre>
 */
@Slf4j
@RestController
@RequestMapping("/api/tagengine")
public class TagSubscriptionController {

    private final JdbcTemplate metaJdbc;

    public TagSubscriptionController(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ApiResponse<Void> onBadArg(IllegalArgumentException e) {
        return ApiResponse.error(400, e.getMessage());
    }

    private static String s(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    private void ensureSchema() {
        // 表由 TagEngineService.ensureSchema 建; 此处兜底幂等(独立部署时)
        try {
            metaJdbc.execute("CREATE TABLE IF NOT EXISTS tag_subscription ("
                    + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                    + "name VARCHAR(128) NOT NULL,"
                    + "tag_path VARCHAR(512) NOT NULL,"
                    + "tag_value VARCHAR(128) NULL,"
                    + "status TINYINT NOT NULL DEFAULT 1,"
                    + "create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
                    + "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "UNIQUE KEY uk_sub (tag_path, tag_value))");
        } catch (DataAccessException ignored) {
        }
    }

    /** 注册表路径集合(校验订阅 tagPath 必须已注册) */
    private Set<String> registeredPaths() {
        Map<Long, String> name = new HashMap<>();
        Map<Long, Long> parent = new HashMap<>();
        for (Map<String, Object> r : metaJdbc.queryForList("SELECT id,parent_id,name FROM tag_dict")) {
            long id = ((Number) r.get("id")).longValue();
            name.put(id, String.valueOf(r.get("name")));
            parent.put(id, ((Number) r.get("parent_id")).longValue());
        }
        Set<String> paths = new LinkedHashSet<>();
        for (Map<String, Object> r : metaJdbc.queryForList("SELECT tag_dict_id FROM tag_registry")) {
            long id = ((Number) r.get("tag_dict_id")).longValue();
            List<String> chain = new ArrayList<>();
            Long cur = id;
            Set<Long> guard = new HashSet<>();
            while (cur != null && cur != 0 && guard.add(cur) && name.containsKey(cur)) {
                chain.add(0, name.get(cur));
                cur = parent.get(cur);
            }
            paths.add(String.join("/", chain));
        }
        return paths;
    }

    @GetMapping("/subs")
    public ApiResponse<List<Map<String, Object>>> listSubs() {
        ensureSchema();
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT s.id,s.name,s.tag_path,s.tag_value,s.status,s.create_time,s.update_time,"
                        + " (SELECT COUNT(DISTINCT n.entity) FROM tag_notify n"
                        + "   WHERE n.tag_path=s.tag_path AND (s.tag_value IS NULL OR s.tag_value='' OR n.tag_value=s.tag_value)) AS hit_entities,"
                        + " (SELECT MAX(n.occur_ts) FROM tag_notify n"
                        + "   WHERE n.tag_path=s.tag_path AND (s.tag_value IS NULL OR s.tag_value='' OR n.tag_value=s.tag_value)) AS latest_at"
                        + " FROM tag_subscription s ORDER BY s.id DESC");
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            Map<String, Object> m = new LinkedHashMap<>(r);
            out.add(m);
        }
        return ApiResponse.ok(out);
    }

    @PostMapping("/subs")
    public ApiResponse<Map<String, Object>> createSub(@RequestBody Map<String, Object> body) {
        ensureSchema();
        String name = s(body.get("name")).trim();
        String path = s(body.get("tagPath")).trim();
        if (name.isEmpty()) throw new IllegalArgumentException("订阅名必填");
        if (!registeredPaths().contains(path)) {
            throw new IllegalArgumentException("订阅标签未注册: " + path + " —— 标签须先在注册表注册并被任务认领");
        }
        String val = s(body.get("tagValue")).trim();
        KeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO tag_subscription(name,tag_path,tag_value) VALUES(?,?,?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setString(2, path);
            ps.setString(3, val.isEmpty() ? null : val);
            return ps;
        }, kh);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", kh.getKey());
        log.info("[tagengine] 订阅创建 #{} {}", out.get("id"), name);
        return ApiResponse.ok(out);
    }

    @PutMapping("/subs/{id}")
    public ApiResponse<Map<String, Object>> updateSub(@PathVariable long id, @RequestBody Map<String, Object> body) {
        ensureSchema();
        List<Map<String, Object>> rows = metaJdbc.queryForList("SELECT id FROM tag_subscription WHERE id=?", id);
        if (rows.isEmpty()) throw new IllegalArgumentException("订阅不存在 id=" + id);
        String name = body.containsKey("name") ? s(body.get("name")).trim() : null;
        String path = body.containsKey("tagPath") ? s(body.get("tagPath")).trim() : null;
        if (path != null && !registeredPaths().contains(path)) {
            throw new IllegalArgumentException("订阅标签未注册: " + path);
        }
        String val = body.containsKey("tagValue") ? s(body.get("tagValue")).trim() : null;
        Integer status = body.get("status") instanceof Number n ? n.intValue()
                : body.containsKey("status") ? Integer.parseInt(s(body.get("status"))) : null;
        if (name != null) metaJdbc.update("UPDATE tag_subscription SET name=? WHERE id=?", name, id);
        if (path != null || val != null) {
            metaJdbc.update("UPDATE tag_subscription SET tag_path=COALESCE(?,tag_path), tag_value=? WHERE id=?",
                    path, (val == null || val.isEmpty()) ? null : val, id);
        }
        if (status != null) metaJdbc.update("UPDATE tag_subscription SET status=? WHERE id=?", status, id);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        return ApiResponse.ok(out);
    }

    @DeleteMapping("/subs/{id}")
    public ApiResponse<Map<String, Object>> deleteSub(@PathVariable long id) {
        ensureSchema();
        int n = metaJdbc.update("DELETE FROM tag_subscription WHERE id=?", id);
        if (n == 0) throw new IllegalArgumentException("订阅不存在 id=" + id);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("deleted", true);
        return ApiResponse.ok(out);
    }

    /** 变更审计流(时间倒序; tagPath/taskMode 过滤) */
    @GetMapping("/notify")
    public ApiResponse<Map<String, Object>> listNotify(@RequestParam(required = false) String tagPath,
                                                       @RequestParam(required = false) String taskMode,
                                                       @RequestParam(defaultValue = "50") int limit) {
        StringBuilder sql = new StringBuilder(
                "SELECT entity,tag_path,tag_value,task_id,task_mode,occur_ts FROM tag_notify WHERE 1=1");
        List<Object> args = new ArrayList<>();
        if (tagPath != null && !tagPath.isBlank()) {
            sql.append(" AND tag_path=?");
            args.add(tagPath);
        }
        if (taskMode != null && !taskMode.isBlank()) {
            sql.append(" AND task_mode=?");
            args.add(taskMode);
        }
        sql.append(" ORDER BY occur_ts DESC LIMIT ").append(Math.min(Math.max(limit, 1), 200));
        List<Map<String, Object>> rows = metaJdbc.queryForList(sql.toString(), args.toArray());
        return ApiResponse.ok(Map.of("list", rows, "total", rows.size()));
    }
}
