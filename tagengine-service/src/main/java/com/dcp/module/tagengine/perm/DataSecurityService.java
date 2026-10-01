package com.dcp.module.tagengine.perm;

import com.dcp.security.SecurityUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据权限判定(2026-09-08 设计文档): 资源(scope/dataset/column/tag) × L0-L3 × 主体(role/user)。
 * 判定链: ①deny(L0) 短路 ②user 显式 ③role 显式 ④scope(标签路径前缀最长匹配) ⑤资产密级兜底(dataset) ⑥默认 L3(开放)。
 * ADMIN 恒 L3。列继承表; 行规则(row_rule)供表数据/标签圈选附加(见 TagLabelService 消费)。
 * 本期边界: 平台查询面; 引擎/湖直读=可信通道。
 */
@Slf4j
@Service
public class DataSecurityService {

    public static final int HIDDEN = 0, META = 1, MASKED = 2, FULL = 3;

    private final JdbcTemplate metaJdbc;
    private final Map<String, Integer> cache = new ConcurrentHashMap<>();
    private volatile long cacheVersion = 0;

    public DataSecurityService(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    /** 策略变更后调用(DataPolicyController) */
    public void clearCache() {
        cacheVersion++;
        cache.clear();
    }

    public boolean isAdmin() {
        return SecurityUtils.roles().contains("ADMIN");
    }

    private List<String> roles() {
        List<String> r = SecurityUtils.roles();
        return r == null ? List.of() : r;
    }

    private String username() {
        try {
            return SecurityUtils.currentUsername();
        } catch (Exception e) {
            return null;
        }
    }

    private String key(String type, String path) {
        List<String> rs = new ArrayList<>(roles());
        Collections.sort(rs);
        return type + "|" + path + "|" + cacheVersion + "|" + String.join(",", rs) + "|" + username();
    }

    /* ---------- 数据集/列 ---------- */

    /** 表级: deny→user→role→资产密级兜底→L3 */
    public int tableLevel(String dataset) {
        if (dataset == null || dataset.isBlank()) return FULL;
        if (isAdmin()) return FULL;
        Integer hit = cache.get(key("ds", dataset));
        if (hit != null) return hit;
        int level = resolve("dataset", dataset, null);
        if (level == HIDDEN) return cachePut("ds", dataset, HIDDEN);
        // 资产密级兜底(asset_meta.secret_level: 1公开→3, 2内部→2, 3机密→1, 4绝密→0)
        if (level == FULL) {
            Integer sec = null;
            try {
                sec = metaJdbc.queryForObject(
                        "SELECT secret_level FROM asset_meta WHERE table_name=? LIMIT 1", Integer.class, dataset);
            } catch (Exception ignore) {
            }
            if (sec != null) {
                level = switch (sec) {
                    case 1 -> FULL;
                    case 2 -> MASKED;
                    case 3 -> META;
                    default -> HIDDEN;   // 绝密: 仅显式授权
                };
            }
        }
        return cachePut("ds", dataset, level);
    }

    /** 列级: 列显式策略优先; 否则继承表级(列只可能更紧: 显式列策略 < 表级时用列) */
    public int columnLevel(String dataset, String column) {
        if (isAdmin()) return FULL;
        Integer hit = cache.get(key("col", dataset + "." + column));
        if (hit != null) return hit;
        int col = resolve("column", dataset + "." + column, null);
        int tbl = tableLevel(dataset);
        int level = col == FULL ? tbl : Math.min(col, tbl);
        return cachePut("col", dataset + "." + column, level);
    }

    /** 表行规则(cond/list/tag_ref; 平台表直查出口用) */
    public List<Map<String, Object>> rowRules(String resType, String resPath) {
        return metaJdbc.queryForList(
                "SELECT id,name,kind,cond_json,list_text,tag_ref_path FROM row_rule"
                        + " WHERE res_type=? AND res_path=? AND enabled=1", resType, resPath);
    }

    /* ---------- 标签 ---------- */

    /** 标签级: deny→user→role→scope(路径前缀, 如 业务域/风控)→L3 */
    public int tagLevel(String tagPath) {
        if (tagPath == null || tagPath.isBlank()) return FULL;
        if (isAdmin()) return FULL;
        Integer hit = cache.get(key("tag", tagPath));
        if (hit != null) return hit;
        int level = resolve("tag", tagPath, null);
        if (level == FULL) {
            // scope 兜底: biz 业务域(标签路径前缀最长匹配)
            List<Map<String, Object>> scopes = metaJdbc.queryForList(
                    "SELECT res_path,level FROM data_policy WHERE res_type='scope' AND enabled=1"
                            + " AND subject_type='role' AND eff='grant' AND level<3");
            int best = FULL;
            String bestPath = "";
            for (Map<String, Object> s : scopes) {
                String p = String.valueOf(s.get("res_path"));
                if (tagPath.startsWith(p + "/") && p.length() > bestPath.length()) {
                    best = ((Number) s.get("level")).intValue();
                    bestPath = p;
                }
            }
            if (bestPath.isEmpty()) {
                // 无 role scope 时再看 user 级 scope
                List<Map<String, Object>> us = metaJdbc.queryForList(
                        "SELECT res_path,level FROM data_policy WHERE res_type='scope' AND enabled=1"
                                + " AND subject_type='user' AND subject_id=? AND eff='grant' AND level<3",
                        username());
                for (Map<String, Object> s : us) {
                    String p = String.valueOf(s.get("res_path"));
                    if (tagPath.startsWith(p + "/") && p.length() > bestPath.length()) {
                        best = ((Number) s.get("level")).intValue();
                    }
                }
            }
            level = best;
        }
        return cachePut("tag", tagPath, level);
    }

    /** 标签可圈选(≥L2)? */
    public boolean tagReadable(String tagPath) {
        return tagLevel(tagPath) >= MASKED;
    }

    /* ---------- 判定核心 ---------- */

    /** 显式策略判定: 查 user 级与 role 级(deny 短路, user>role) */
    private int resolve(String resType, String resPath, String dim) {
        // ① user deny / user grant
        Integer userLevel = queryPolicy(resType, resPath, dim, "user", username());
        if (userLevel != null) return userLevel;
        // ② role deny → 0(任一角色 deny); role grant 取最高
        int best = FULL;
        for (String role : roles()) {
            Integer lv = queryPolicy(resType, resPath, dim, "role", role);
            if (lv != null) {
                if (lv == HIDDEN) return HIDDEN;   // deny 优先
                best = Math.min(best, lv);         // 多角色取最严
            }
        }
        return best;
    }

    /** 单条策略(user 或 role); eff=deny → level 恒 0 */
    private Integer queryPolicy(String resType, String resPath, String dim, String st, String sid) {
        try {
            List<Map<String, Object>> rows = metaJdbc.queryForList(
                    "SELECT level,eff FROM data_policy WHERE res_type=? AND res_path=? AND enabled=1"
                            + " AND subject_type=? AND subject_id=?",
                    resType, resPath, st, sid);
            if (rows.isEmpty()) return null;
            Map<String, Object> r = rows.get(0);
            int lv = ((Number) r.get("level")).intValue();
            return "deny".equals(r.get("eff")) ? HIDDEN : lv;
        } catch (Exception e) {
            return null;
        }
    }

    private int cachePut(String type, String path, int level) {
        cache.put(key(type, path), level);
        return level;
    }
}
