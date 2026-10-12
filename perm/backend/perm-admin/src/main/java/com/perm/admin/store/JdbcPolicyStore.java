package com.perm.admin.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.perm.admin.model.PolicyRecord;
import com.perm.admin.model.PolicyStatus;
import com.perm.common.compile.PolicyCompiler;
import com.perm.common.dto.PolicyDsl;
import com.perm.common.model.Policy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * MySQL 策略存储（perm.storage=jdbc 时生效）
 * perm_policy 存 DSL JSON + 生命周期 + 元信息；perm_meta 存 planVersion（原子递增）。
 * 读回时 DSL → 编译重建 Policy（与内存实现语义一致）。
 */
@Component
@ConditionalOnProperty(name = "perm.storage", havingValue = "jdbc")
public class JdbcPolicyStore implements PolicyStore {

    private static final String PLAN_VERSION_KEY = "plan_version";

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final PolicyCompiler compiler;
    private final ReentrantLock publishLock = new ReentrantLock();

    public JdbcPolicyStore(JdbcTemplate jdbc, ObjectMapper objectMapper, PolicyCompiler compiler) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.compiler = compiler;
    }

    @Override
    public PolicyRecord get(String policyId) {
        List<PolicyRecord> rows = jdbc.query(
                "SELECT policy_id, dsl_json, status, created_by, created_at, updated_at, published_version "
                        + "FROM perm_policy WHERE policy_id = ?",
                (rs, i) -> mapRow(rs), policyId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    @Override
    public List<PolicyRecord> all() {
        return jdbc.query(
                "SELECT policy_id, dsl_json, status, created_by, created_at, updated_at, published_version "
                        + "FROM perm_policy",
                (rs, i) -> mapRow(rs));
    }

    @Override
    @Transactional
    public PolicyRecord put(PolicyRecord record) {
        PolicyDsl dsl = record.request();
        String json;
        try {
            json = objectMapper.writeValueAsString(dsl);
        } catch (Exception e) {
            throw new IllegalStateException("策略 DSL 序列化失败", e);
        }
        jdbc.update("""
                        INSERT INTO perm_policy
                            (policy_id, dsl_json, status, created_by, created_at, updated_at, published_version)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON DUPLICATE KEY UPDATE
                            dsl_json = VALUES(dsl_json), status = VALUES(status),
                            created_by = VALUES(created_by), created_at = VALUES(created_at),
                            updated_at = VALUES(updated_at), published_version = VALUES(published_version)
                        """,
                record.policy().policyId(), json, record.status().name(), record.createdBy(),
                Timestamp.from(record.createdAt()), Timestamp.from(record.updatedAt()),
                record.publishedVersion());
        return record;
    }

    @Override
    public PolicyRecord remove(String policyId) {
        PolicyRecord existing = get(policyId);
        if (existing != null) {
            jdbc.update("DELETE FROM perm_policy WHERE policy_id = ?", policyId);
        }
        return existing;
    }

    @Override
    public int planVersion() {
        Integer v = jdbc.queryForObject(
                "SELECT v FROM perm_meta WHERE k = ?", Integer.class, PLAN_VERSION_KEY);
        return v == null ? 1 : v;
    }

    /** 原子版本递增（UPDATE 行锁串行化，多实例安全） */
    @Override
    public int nextVersion() {
        jdbc.update("UPDATE perm_meta SET v = v + 1 WHERE k = ?", PLAN_VERSION_KEY);
        return planVersion();
    }

    @Override
    public ReentrantLock publishLock() {
        return publishLock;
    }

    private PolicyRecord mapRow(java.sql.ResultSet rs) throws java.sql.SQLException {
        PolicyDsl dsl;
        try {
            dsl = objectMapper.readValue(rs.getString("dsl_json"), PolicyDsl.class);
        } catch (Exception e) {
            throw new IllegalStateException("策略 DSL 反序列化失败: " + rs.getString("policy_id"), e);
        }
        Policy policy = compiler.compile(dsl);
        return new PolicyRecord(
                policy,
                dsl,
                PolicyStatus.valueOf(rs.getString("status")),
                rs.getString("created_by"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant(),
                rs.getInt("published_version"));
    }
}
