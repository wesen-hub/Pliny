package com.dcp.module.tagengine.perm;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.dcp.common.ApiResponse;
import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.module.system.entity.SysRole;
import com.dcp.module.system.entity.SysUser;
import com.dcp.module.system.mapper.SysRoleMapper;
import com.dcp.module.system.mapper.SysUserMapper;
import com.dcp.module.system.mapper.SysUserRoleMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * 数据权限策略管理(2026-09-08 设计): data_policy(scope/dataset/column/tag × role/user × L0-3) + row_rule。
 * 管理页(Permission.vue 4 Tab)数据源; 写入后清 DataSecurityService 缓存。
 */
@RestController
@RequestMapping("/api/data-policy")
@RequiredArgsConstructor
public class DataPolicyController {

    private final JdbcTemplate metaJdbc;
    private final DataSecurityService dataSecurity;
    private final SysRoleMapper roleMapper;
    private final SysUserMapper userMapper;
    private final SysUserRoleMapper userRoleMapper;

    private static String s(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    /* ---------- 主体/资源目录(管理页) ---------- */

    @GetMapping("/subjects")
    @PreAuthorize("@perm.has('system:data-policy:manage')")
    public ApiResponse<Map<String, Object>> subjects() {
        List<Map<String, Object>> roles = new ArrayList<>();
        for (SysRole r : roleMapper.selectList(null)) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", r.getId());
            m.put("code", r.getRoleCode());
            m.put("name", r.getRoleName());
            roles.add(m);
        }
        List<Map<String, Object>> users = new ArrayList<>();
        for (SysUser u : userMapper.selectList(null)) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", u.getId());
            m.put("username", u.getUsername());
            List<com.dcp.module.system.entity.SysUserRole> ur = userRoleMapper.selectList(
                    new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<com.dcp.module.system.entity.SysUserRole>()
                    .eq(com.dcp.module.system.entity.SysUserRole::getUserId, u.getId()));
            List<String> codes = new ArrayList<>();
            for (var x : ur) {
                SysRole rr = roleMapper.selectById(x.getRoleId());
                if (rr != null) codes.add(rr.getRoleCode());
            }
            m.put("roles", codes);
            users.add(m);
        }
        return ApiResponse.ok(Map.of("roles", roles, "users", users));
    }

    @GetMapping("/resources")
    @PreAuthorize("@perm.has('system:data-policy:manage')")
    public ApiResponse<Map<String, Object>> resources() {
        // 数据集(资产目录 ∪ 打标任务用到的数据面表, 附资产密级)
        // 注: 两表列 collation 不一致(asset_meta utf8mb4_0900_ai_ci vs tag_task utf8mb4_unicode_ci),
        // UNION 会报 MySQL 1271 Illegal mix of collations → 显式 COLLATE 统一
        List<Map<String, Object>> datasets = metaJdbc.queryForList(
                "SELECT table_name COLLATE utf8mb4_unicode_ci AS name, secret_level AS secret FROM asset_meta WHERE table_name IS NOT NULL"
                        + " UNION SELECT DISTINCT dataset COLLATE utf8mb4_unicode_ci AS name, NULL AS secret FROM tag_task WHERE dataset IS NOT NULL"
                        + " ORDER BY name");
        // 标签树(已注册标签路径 + 中间分类节点)
        Map<Long, String> name = new HashMap<>();
        Map<Long, Long> parent = new HashMap<>();
        for (Map<String, Object> r : metaJdbc.queryForList("SELECT id,parent_id,name FROM tag_dict")) {
            name.put(((Number) r.get("id")).longValue(), s(r.get("name")));
            parent.put(((Number) r.get("id")).longValue(), ((Number) r.get("parent_id")).longValue());
        }
        Set<String> tagPaths = new LinkedHashSet<>();
        Set<String> groupPaths = new LinkedHashSet<>();
        for (Long id : name.keySet()) {
            List<String> chain = new ArrayList<>();
            Long cur = id;
            Set<Long> guard = new HashSet<>();
            while (cur != null && cur != 0 && guard.add(cur) && name.containsKey(cur)) {
                chain.add(0, name.get(cur));
                cur = parent.get(cur);
            }
            String path = String.join("/", chain);
            if (chain.size() > 1) groupPaths.add(path.substring(0, path.lastIndexOf('/')));
            // 仅收录 registry 已注册叶(打标引擎标签资产)
            Integer reg = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM tag_registry tr JOIN tag_dict td ON td.id=tr.tag_dict_id WHERE td.id=?",
                    Integer.class, id);
            if (reg != null && reg > 0) tagPaths.add(path);
        }
        // 分类(scope): 业务域/密级/来源 分类根及其子
        List<Map<String, Object>> scopes = new ArrayList<>();
        for (String gp : groupPaths) {
            if (gp.startsWith("业务域/") || gp.startsWith("密级/") || gp.startsWith("来源系统/")) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("path", gp);
                m.put("dim", gp.startsWith("业务域") ? "biz" : gp.startsWith("密级") ? "sec" : "src");
                scopes.add(m);
            }
        }
        return ApiResponse.ok(Map.of("datasets", datasets, "tags", new ArrayList<>(tagPaths),
                "groups", groupPaths, "scopes", scopes));
    }

    /* ---------- 策略 CRUD ---------- */

    @GetMapping
    @PreAuthorize("@perm.has('system:data-policy:manage')")
    public ApiResponse<List<Map<String, Object>>> list(@RequestParam String subjectType,
                                                       @RequestParam String subjectId,
                                                       @RequestParam(required = false) String resType) {
        StringBuilder sql = new StringBuilder(
                "SELECT id,res_type,dim,res_path,subject_type,subject_id,level,eff,enabled,update_by,update_time"
                        + " FROM data_policy WHERE subject_type=? AND subject_id=?");
        List<Object> args = new ArrayList<>(List.of(subjectType, subjectId));
        if (resType != null && !resType.isBlank()) {
            sql.append(" AND res_type=?");
            args.add(resType);
        }
        sql.append(" ORDER BY res_type,res_path");
        return ApiResponse.ok(metaJdbc.queryForList(sql.toString(), args.toArray()));
    }

    @PostMapping
    @PreAuthorize("@perm.has('system:data-policy:manage')")
    public ApiResponse<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        String resType = s(body.get("resType"));
        String resPath = s(body.get("resPath"));
        String st = s(body.get("subjectType"));
        String sid = s(body.get("subjectId"));
        int level = body.get("level") instanceof Number n ? n.intValue() : -1;
        if (!List.of("scope", "dataset", "column", "tag").contains(resType) || resPath.isBlank()
                || !List.of("role", "user").contains(st) || sid.isBlank() || level < 0 || level > 3) {
            throw new BizException(ErrorCode.PARAM_ERROR, "参数非法");
        }
        String eff = "deny".equals(s(body.get("eff"))) ? "deny" : "grant";
        // 幂等: 同键 upsert
        Integer n = metaJdbc.queryForObject("SELECT COUNT(*) FROM data_policy WHERE res_type=? AND res_path=?"
                + " AND subject_type=? AND subject_id=?", Integer.class, resType, resPath, st, sid);
        if (n != null && n > 0) {
            metaJdbc.update("UPDATE data_policy SET level=?, eff=?, dim=?, update_by=? WHERE res_type=? AND res_path=?"
                            + " AND subject_type=? AND subject_id=?",
                    level, eff, body.get("dim") == null ? null : s(body.get("dim")),
                    com.dcp.security.SecurityUtils.currentUsername(), resType, resPath, st, sid);
        } else {
            metaJdbc.update("INSERT INTO data_policy(res_type,dim,res_path,subject_type,subject_id,level,eff,update_by)"
                            + " VALUES(?,?,?,?,?,?,?,?)",
                    resType, body.get("dim") == null ? null : s(body.get("dim")), resPath, st, sid, level, eff,
                    com.dcp.security.SecurityUtils.currentUsername());
        }
        dataSecurity.clearCache();
        return ApiResponse.ok(Map.of("ok", true, "resPath", resPath, "level", level));
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("@perm.has('system:data-policy:manage')")
    public ApiResponse<Map<String, Object>> delete(@PathVariable long id) {
        int n = metaJdbc.update("DELETE FROM data_policy WHERE id=?", id);
        if (n == 0) throw new BizException(ErrorCode.NOT_FOUND, "策略不存在");
        dataSecurity.clearCache();
        return ApiResponse.ok(Map.of("deleted", true));
    }

    /* ---------- 行规则 CRUD ---------- */

    @GetMapping("/row-rule")
    @PreAuthorize("@perm.has('system:data-policy:manage')")
    public ApiResponse<List<Map<String, Object>>> rowRules(@RequestParam(required = false) String resType,
                                                           @RequestParam(required = false) String resPath) {
        StringBuilder sql = new StringBuilder(
                "SELECT id,name,res_type,res_path,kind,cond_json,list_text,tag_ref_path,note FROM row_rule WHERE 1=1");
        List<Object> args = new ArrayList<>();
        if (resType != null && !resType.isBlank()) { sql.append(" AND res_type=?"); args.add(resType); }
        if (resPath != null && !resPath.isBlank()) { sql.append(" AND res_path=?"); args.add(resPath); }
        return ApiResponse.ok(metaJdbc.queryForList(sql.toString(), args.toArray()));
    }

    @PostMapping("/row-rule")
    @PreAuthorize("@perm.has('system:data-policy:manage')")
    public ApiResponse<Map<String, Object>> createRowRule(@RequestBody Map<String, Object> body) {
        String kind = s(body.get("kind"));
        if (!List.of("cond", "list", "tag_ref").contains(kind)) throw new BizException(ErrorCode.PARAM_ERROR, "kind 非法");
        metaJdbc.update("INSERT INTO row_rule(name,res_type,res_path,kind,cond_json,list_text,tag_ref_path,note)"
                        + " VALUES(?,?,?,?,?,?,?,?)",
                s(body.get("name")), s(body.get("resType")), s(body.get("resPath")), kind,
                body.get("condJson") == null ? null : body.get("condJson").toString(),
                body.get("listText") == null ? null : body.get("listText").toString(),
                s(body.get("tagRefPath")), s(body.getOrDefault("note", "")));
        return ApiResponse.ok(Map.of("ok", true));
    }

    @DeleteMapping("/row-rule/{id}")
    @PreAuthorize("@perm.has('system:data-policy:manage')")
    public ApiResponse<Map<String, Object>> deleteRowRule(@PathVariable long id) {
        int n = metaJdbc.update("DELETE FROM row_rule WHERE id=?", id);
        if (n == 0) throw new BizException(ErrorCode.NOT_FOUND, "行规则不存在");
        return ApiResponse.ok(Map.of("deleted", true));
    }
}
