package com.perm.admin.publisher;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.perm.common.conflict.ConflictReport;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;

/**
 * MySQL 发布流水（perm.storage=jdbc 时生效）
 * perm_publication 表存档：发布后 planVersion 为主键，conflicts 存 WARNING/INFO 报告 JSON。
 */
@Component
@ConditionalOnProperty(name = "perm.storage", havingValue = "jdbc")
public class JdbcPublicationHistory implements PublicationHistory {

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public JdbcPublicationHistory(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    @Override
    public void add(Publication publication) {
        String conflictsJson = null;
        if (publication.conflictReport() != null) {
            try {
                conflictsJson = objectMapper.writeValueAsString(publication.conflictReport());
            } catch (Exception e) {
                throw new IllegalStateException("冲突报告序列化失败", e);
            }
        }
        jdbc.update("""
                        INSERT INTO perm_publication (version, change_type, policy_ids, operator, ts, conflicts)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                publication.version(), publication.changeType(),
                writeJson(publication.policyIds()), publication.operator(),
                Timestamp.from(publication.ts()), conflictsJson);
    }

    @Override
    public List<Publication> all() {
        return jdbc.query(
                "SELECT version, change_type, policy_ids, operator, ts, conflicts "
                        + "FROM perm_publication ORDER BY version DESC",
                (rs, i) -> new Publication(
                        rs.getInt("version"),
                        rs.getString("change_type"),
                        readJson(rs.getString("policy_ids"), new TypeReference<List<String>>() {
                        }),
                        rs.getString("operator"),
                        rs.getTimestamp("ts").toInstant(),
                        readConflicts(rs.getString("conflicts"))));
    }

    private ConflictReport readConflicts(String json) {
        if (json == null) {
            return null;
        }
        try {
            return objectMapper.readValue(json, ConflictReport.class);
        } catch (Exception e) {
            throw new IllegalStateException("冲突报告反序列化失败", e);
        }
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 序列化失败", e);
        }
    }

    private <T> T readJson(String json, TypeReference<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 反序列化失败", e);
        }
    }
}
