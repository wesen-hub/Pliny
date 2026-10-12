package com.perm.admin.audit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 审计消费链路测试（纯单元：内存存储 + 监听器消息解析）
 * 覆盖：JSON 消息解析落库 / 解析失败跳过不中断 / 最近查询
 */
class AuditEventListenerTest {

    @Test
    @DisplayName("消费 perm.audit 消息：解析落库（与 PDP 投递 payload 契约一致）")
    void parseAndSave() {
        InMemoryAuditStore store = new InMemoryAuditStore();
        AuditEventListener listener = new AuditEventListener(store);
        String message = "{\"ts\":1759100000000,\"userId\":\"lisi\",\"resourceId\":\"sdm.sales_order\","
                + "\"action\":\"select\",\"decision\":\"PERMIT\",\"mode\":\"sync\","
                + "\"matchedPolicies\":[\"pol_1\",\"pol_2\"]}";

        listener.onAudit(message);

        List<AuditRecord> records = store.recent(10);
        assertEquals(1, records.size());
        AuditRecord r = records.get(0);
        assertEquals("lisi", r.userId());
        assertEquals("PERMIT", r.decision());
        assertEquals("sync", r.mode());
        assertEquals(List.of("pol_1", "pol_2"), r.matchedPolicies());
        assertEquals(Instant.ofEpochMilli(1759100000000L), r.ts());
    }

    @Test
    @DisplayName("非法消息：仅告警跳过，不中断消费，不影响已有流水")
    void parseFailureSkipped() {
        InMemoryAuditStore store = new InMemoryAuditStore();
        AuditEventListener listener = new AuditEventListener(store);
        listener.onAudit("{not-valid-json");
        listener.onAudit("{\"ts\":1759100000000,\"userId\":\"lisi\",\"resourceId\":\"sdm.sales_order\","
                + "\"action\":\"select\",\"decision\":\"PERMIT\",\"mode\":\"sync\",\"matchedPolicies\":[]}");

        List<AuditRecord> records = store.recent(10);
        assertEquals(1, records.size());
        assertEquals("lisi", records.get(0).userId());
    }

    @Test
    @DisplayName("InMemoryAuditStore：recent 返回最近 N 条（插入序）")
    void recentLimits() {
        InMemoryAuditStore store = new InMemoryAuditStore();
        for (int i = 0; i < 5; i++) {
            store.save(new AuditRecord(Instant.now(), "u" + i, "r", "select", "PERMIT", "sync", List.of()));
        }
        assertEquals(3, store.recent(3).size());
        assertTrue(store.recent(10).size() == 5);
    }
}
