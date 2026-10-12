package com.perm.admin.audit;

import java.util.List;

/**
 * 审计存储（R-A 消费端落点）
 * 实现：InMemoryAuditStore（memory 模式）/ JdbcAuditStore（jdbc 模式落 perm_audit 表）。
 */
public interface AuditStore {

    void save(AuditRecord record);

    List<AuditRecord> recent(int limit);
}
