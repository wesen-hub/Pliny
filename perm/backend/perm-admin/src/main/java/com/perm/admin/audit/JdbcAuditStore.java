package com.perm.admin.audit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;

/**
 * JDBC 审计存储（perm.storage=jdbc，落 perm_audit 表；正式形态查询入口）
 * matchedPolicies 序列化为 JSON 存 TEXT；查询按时间倒序（id 倒序）。
 */
@Component
@ConditionalOnProperty(name = "perm.storage", havingValue = "jdbc")
public class JdbcAuditStore implements AuditStore {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper = new ObjectMapper();

    public JdbcAuditStore(JdbcTemplate jdbcTemplate) {
        this.jdbc = jdbcTemplate;
    }

    @Override
    public void save(AuditRecord r) {
        try {
            jdbc.update(
                    "INSERT INTO perm_audit (ts, user_id, resource_id, action, decision, mode, matched_policies) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    Timestamp.from(r.ts()), r.userId(), r.resourceId(), r.action(),
                    r.decision(), r.mode(), mapper.writeValueAsString(r.matchedPolicies()));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("审计序列化失败", e);
        }
    }

    @Override
    public List<AuditRecord> recent(int limit) {
        return jdbc.query(
                "SELECT ts, user_id, resource_id, action, decision, mode, matched_policies "
                        + "FROM perm_audit ORDER BY id DESC LIMIT ?",
                (rs, i) -> {
                    try {
                        return new AuditRecord(
                                rs.getTimestamp("ts").toInstant(),
                                rs.getString("user_id"),
                                rs.getString("resource_id"),
                                rs.getString("action"),
                                rs.getString("decision"),
                                rs.getString("mode"),
                                mapper.readValue(rs.getString("matched_policies"),
                                        mapper.getTypeFactory().constructCollectionType(List.class, String.class)));
                    } catch (JsonProcessingException e) {
                        throw new IllegalStateException("审计反序列化失败", e);
                    }
                },
                limit);
    }
}
