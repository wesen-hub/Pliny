package com.dcp.module.tagengine.online;

import com.dcp.common.ApiResponse;
import com.dcp.module.tagengine.TagPaths;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.web.bind.annotation.*;
import org.springframework.security.access.prepost.PreAuthorize;

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
    private final com.dcp.module.tagengine.perm.DataSecurityService security;
    @org.springframework.beans.factory.annotation.Autowired
    private com.dcp.module.system.service.AuditService auditService;

    public TagSubscriptionController(JdbcTemplate metaJdbc, com.dcp.module.tagengine.perm.DataSecurityService security) {
        this.metaJdbc = metaJdbc;
        this.security = security;
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
                    + "tag_dict_id BIGINT NULL COMMENT '标签节点 id(与 tag_path 双写, 以 id 为准)',"
                    + "tag_value VARCHAR(128) NULL,"
                    + "owner VARCHAR(64) NULL COMMENT '订阅属主(创建用户); 投递=属主实时权限快照',"
                    + "channel VARCHAR(16) NOT NULL DEFAULT '站内' COMMENT '站内|站内+webhook',"
                    + "webhook_url VARCHAR(512) NULL,"
                    + "freq VARCHAR(16) NOT NULL DEFAULT 'real-time' COMMENT 'real-time|daily',"
                    + "ttl_days INT NULL COMMENT '到期回收(预留)',"
                    + "status TINYINT NOT NULL DEFAULT 1,"
                    + "create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
                    + "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "UNIQUE KEY uk_sub (tag_path, tag_value))");
        } catch (DataAccessException ignored) {
        }
        try { metaJdbc.execute("ALTER TABLE tag_subscription ADD COLUMN tag_dict_id BIGINT NULL"); } catch (DataAccessException ignored) { }
    }

    /** 可订阅的标签路径集合 = **tag_dict 全部节点路径**
     *  (原为"已注册"路径;去注册表后:节点存在即可订阅, 标签树就是唯一真相源) */
    private Set<String> registeredPaths() {
        return new LinkedHashSet<>(TagPaths.pathToId(
                metaJdbc.queryForList("SELECT id,parent_id,name FROM tag_dict")).keySet());
    }

    @GetMapping("/subs")
    public ApiResponse<List<Map<String, Object>>> listSubs() {
        ensureSchema();
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT s.id,s.name,s.tag_path,s.tag_dict_id,s.tag_value,s.owner,s.status,s.create_time,s.update_time,"
                        + " (SELECT COUNT(DISTINCT n.entity) FROM tag_notify n"
                        + "   WHERE ((s.tag_dict_id IS NOT NULL AND n.tag_dict_id=s.tag_dict_id)"
                        + "          OR (s.tag_dict_id IS NULL AND n.tag_path=s.tag_path))"
                        + "     AND (s.tag_value IS NULL OR s.tag_value='' OR n.tag_value=s.tag_value)) AS hit_entities,"
                        + " (SELECT MAX(n.occur_ts) FROM tag_notify n"
                        + "   WHERE ((s.tag_dict_id IS NOT NULL AND n.tag_dict_id=s.tag_dict_id)"
                        + "          OR (s.tag_dict_id IS NULL AND n.tag_path=s.tag_path))"
                        + "     AND (s.tag_value IS NULL OR s.tag_value='' OR n.tag_value=s.tag_value)) AS latest_at"
                        + " FROM tag_subscription s ORDER BY s.id DESC");
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            if (security.tagLevel(String.valueOf(r.get("tag_path"))) <= 0) continue;   // L0 订阅不可见
            Map<String, Object> m = new LinkedHashMap<>(r);
            m.put("ownerLevel", security.tagLevel(String.valueOf(r.get("tag_path"))));  // 当前登录视角可见等级
            m.put("ownerReadable", security.tagLevel(String.valueOf(r.get("tag_path"))) >= 2);
            out.add(m);
        }
        return ApiResponse.ok(out);
    }

    @PostMapping("/subs")
    @PreAuthorize("@perm.has('tag:sub:manage')")
    public ApiResponse<Map<String, Object>> createSub(@RequestBody Map<String, Object> body) {
        ensureSchema();
        String name = s(body.get("name")).trim();
        String path = s(body.get("tagPath")).trim();
        if (name.isEmpty()) throw new IllegalArgumentException("订阅名必填");
        if (!registeredPaths().contains(path)) {
            throw new IllegalArgumentException("订阅标签不存在于标签树: " + path + " —— 请在标签树中先建好该节点");
        }
        String val = s(body.get("tagValue")).trim();
        if (security.tagLevel(path) < 2) {
            throw new IllegalArgumentException("订阅标签当前不可圈选(可见等级 L" + security.tagLevel(path)
                    + ", 需 L2+) —— 订阅=属主实时权限快照, 标签须对属主可圈选: " + path);
        }
        String owner = com.dcp.security.SecurityUtils.currentUsername();
        String channel = s(body.getOrDefault("channel", "站内"));
        String webhook = s(body.get("webhookUrl"));
        String freq = s(body.getOrDefault("freq", "real-time"));
        KeyHolder kh = new GeneratedKeyHolder();
        final String fPath = path;
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO tag_subscription(name,tag_path,tag_dict_id,tag_value,owner,channel,webhook_url,freq)"
                            + " VALUES(?,?,?,?,?,?,?,?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setString(2, fPath);
            Long tid = TagPaths.pathToId(metaJdbc.queryForList("SELECT id,parent_id,name FROM tag_dict")).get(fPath);
            if (tid == null) ps.setNull(3, java.sql.Types.BIGINT); else ps.setLong(3, tid);
            ps.setString(4, val.isEmpty() ? null : val);
            ps.setString(5, owner);
            ps.setString(6, channel);
            ps.setString(7, webhook.isEmpty() ? null : webhook);
            ps.setString(8, freq);
            return ps;
        }, kh);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", kh.getKey());
        log.info("[tagengine] 订阅创建 #{} {}", out.get("id"), name);
        return ApiResponse.ok(out);
    }

    @PutMapping("/subs/{id}")
    @PreAuthorize("@perm.has('tag:sub:manage')")
    public ApiResponse<Map<String, Object>> updateSub(@PathVariable long id, @RequestBody Map<String, Object> body) {
        ensureSchema();
        List<Map<String, Object>> rows = metaJdbc.queryForList("SELECT id FROM tag_subscription WHERE id=?", id);
        if (rows.isEmpty()) throw new IllegalArgumentException("订阅不存在 id=" + id);
        String name = body.containsKey("name") ? s(body.get("name")).trim() : null;
        String path = body.containsKey("tagPath") ? s(body.get("tagPath")).trim() : null;
        if (path != null && !registeredPaths().contains(path)) {
            throw new IllegalArgumentException("订阅标签不存在于标签树: " + path);
        }
        String val = body.containsKey("tagValue") ? s(body.get("tagValue")).trim() : null;
        Integer status = body.get("status") instanceof Number n ? n.intValue()
                : body.containsKey("status") ? Integer.parseInt(s(body.get("status"))) : null;
        if (name != null) metaJdbc.update("UPDATE tag_subscription SET name=? WHERE id=?", name, id);
        if (path != null || val != null) {
            Long tid = path != null
                    ? TagPaths.pathToId(metaJdbc.queryForList("SELECT id,parent_id,name FROM tag_dict")).get(path) : null;
            metaJdbc.update("UPDATE tag_subscription SET tag_path=COALESCE(?,tag_path), tag_dict_id=COALESCE(?,tag_dict_id), tag_value=? WHERE id=?",
                    path, tid, (val == null || val.isEmpty()) ? null : val, id);
        }
        if (status != null) metaJdbc.update("UPDATE tag_subscription SET status=? WHERE id=?", status, id);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        return ApiResponse.ok(out);
    }

    @DeleteMapping("/subs/{id}")
    @PreAuthorize("@perm.has('tag:sub:manage')")
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
        List<Map<String, Object>> visible = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            if (security.tagLevel(String.valueOf(r.get("tag_path"))) >= 2) visible.add(r);
        }
        return ApiResponse.ok(Map.of("list", visible, "total", visible.size()));
    }

    /** 订阅实体明细导出: 数据出口 —— 需属主对标签 L3(完全可读), 动作写审计 */
    @PostMapping("/subs/{id}/export")
    @PreAuthorize("@perm.has('label:export')")
    public ApiResponse<Map<String, Object>> export(@PathVariable long id) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id,name,tag_path FROM tag_subscription WHERE id=?", id);
        if (rows.isEmpty()) throw new IllegalArgumentException("订阅不存在 id=" + id);
        String tag = String.valueOf(rows.get(0).get("tag_path"));
        int lv = security.tagLevel(tag);
        if (lv < 3) {
            throw new IllegalArgumentException("导出实体明细需标签 L3(完全可读), 当前可见等级 L" + lv + ": " + tag);
        }
        Number total = metaJdbc.queryForObject(
                "SELECT COUNT(DISTINCT entity) FROM tag_fact_view WHERE tag=?", Number.class, tag);
        long n = total == null ? 0 : total.longValue();
        if (auditService != null) {
            auditService.record("tagengine", "SUB_EXPORT", "sub", id,
                    "导出订阅实体明细: " + rows.get(0).get("name") + " tag=" + tag + " 行数=" + n);
        }
        return ApiResponse.ok(Map.of("subId", id, "tag", tag, "exported", n));
    }
}
