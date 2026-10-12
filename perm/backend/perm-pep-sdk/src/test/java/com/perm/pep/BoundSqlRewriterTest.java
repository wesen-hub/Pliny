package com.perm.pep;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BoundSqlRewriter 绑定/注入测试（复用 perm-common 组件）
 * 覆盖：绑定注入 / DENY 拒绝 / 掩码应用
 */
class BoundSqlRewriterTest {

    private VerifiedArtifact permitWithRegion() {
        return new VerifiedArtifact("PERMIT", null, 2,
                "(`region` = :p0 AND `status` IN (:list1))",
                List.of(new VerifiedArtifact.Placeholder(":p0", "subject.region", "SCALAR", null),
                        new VerifiedArtifact.Placeholder(":list1", "subject.allowedStatus", "LIST", null)),
                List.of("id", "order_no", "region", "amount"),
                List.of(new VerifiedArtifact.Mask("amount", "HASH")),
                null, List.of(), List.of("pol_region_001"));
    }

    private PepContext ctx() {
        return PepContext.builder()
                .userId("lisi")
                .roles(List.of("regional_manager"))
                .attributes(Map.of("region", "华东", "allowedStatus", List.of("PAID", "SHIPPED")))
                .resourceId("sdm.sales_order")
                .action("select")
                .build();
    }

    @Test
    @DisplayName("行谓词绑定并注入 WHERE")
    void bindAndInject() {
        BoundSql bound = BoundSqlRewriter.rewrite(permitWithRegion(), ctx(),
                "SELECT id, order_no, region, amount FROM sales_order WHERE created_at > ?");
        assertTrue(bound.sql().contains("WHERE (created_at > ?) AND ((`region` = ? AND `status` IN (?, ?)))"));
        assertEquals(2, bound.bindValues().size()); // 一个 SCALAR + 一个 LIST（列表整体为一项）
        assertEquals("华东", bound.bindValues().get(0));
        assertEquals(List.of("PAID", "SHIPPED"), bound.bindValues().get(1));
    }

    @Test
    @DisplayName("无业务 WHERE 时自动补 WHERE")
    void injectWithoutWhere() {
        BoundSql bound = BoundSqlRewriter.rewrite(permitWithRegion(), ctx(),
                "SELECT id FROM sales_order");
        assertTrue(bound.sql().contains("WHERE (`region` = ? AND `status` IN (?, ?))"));
    }

    @Test
    @DisplayName("DENY 产物直接拒绝（不产生改写）")
    void denyThrows() {
        VerifiedArtifact deny = new VerifiedArtifact("DENY", "命中黑名单策略 pol_deny_002", 1,
                null, List.of(), null, List.of(), null, List.of(), List.of("pol_deny_002"));
        assertThrows(PepDeniedException.class,
                () -> BoundSqlRewriter.rewrite(deny, ctx(), "SELECT 1"));
    }

    @Test
    @DisplayName("掩码应用（HASH/NULL）")
    void maskApplier() {
        List<VerifiedArtifact.Mask> masks = List.of(
                new VerifiedArtifact.Mask("amount", "HASH"),
                new VerifiedArtifact.Mask("phone", "NULL"));
        Map<String, Object> row = Map.of("id", 1, "amount", "12888.00", "phone", "13800000000");
        Map<String, Object> masked = MaskApplier.applyRow(row, masks);
        assertEquals(1, masked.get("id"));
        assertEquals("5f79fcce6dba9f1f", MaskApplier.applyValue("12888.00", "HASH"));
        assertNull(masked.get("phone"));
        assertEquals(16, String.valueOf(masked.get("amount")).length()); // SHA-256 截断 8 字节 hex
    }
}
