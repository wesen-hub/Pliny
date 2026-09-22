package com.dcp.outlet.control;

import com.dcp.outlet.control.support.InMemoryLogicCatalog;

/**
 * 逻辑目录端口 + 值对象的离线自检(内核纯度:零框架,不碰数据库/Spring)。
 *
 * <p>覆盖:LogicInfo 默认状态 / builder、同版本禁止覆盖、版本查询 / 全量 / 精确查找、状态迁移、审计。
 */
public final class LogicCatalogSelfCheck {

    private static int passed, failed;

    public static void main(String[] args) {
        // ---------- 值对象 ----------
        LogicInfo li = LogicInfo.builder("es-sync", "1.0.0").build();
        check("LogicInfo 默认 status=active", li.status(), "active");
        check("LogicInfo active()=true", li.active(), true);

        LogicInfo full = LogicInfo.builder("es-sync", "1.0.0")
                .status(LogicInfo.STATUS_DISABLED).artifactHash("abc").jarSize(123L)
                .publishedBy("wesen").preflight("通过").description("d").build();
        check("LogicInfo 全字段", full.status() + "|" + full.artifactHash() + "|" + full.publishedBy()
                + "|" + full.jarSize() + "|" + full.preflight(), "disabled|abc|wesen|123|通过");
        check("LogicInfo disabled → active()=false", full.active(), false);

        // ---------- 内存实现:登记 / 查询 ----------
        InMemoryLogicCatalog c = new InMemoryLogicCatalog();
        c.register("es-sync", "1.0.0");
        c.register("es-sync", "2.0.0");
        c.register("tag-eval", "1.0.0");
        check("registered 命中", c.registered("es-sync", "1.0.0"), true);
        check("registered 未命中", c.registered("es-sync", "9.9.9"), false);
        check("find 命中", c.find("es-sync", "1.0.0").isPresent(), true);
        check("find 未命中", c.find("es-sync", "9.9.9").isEmpty(), true);
        check("versionsOf 数量", c.versionsOf("es-sync").size(), 2);
        check("all 数量", c.all().size(), 3);

        // ---------- 同版本禁止覆盖 ----------
        boolean dup = false;
        try {
            c.register(LogicInfo.builder("es-sync", "1.0.0").build());
        } catch (LogicPublishException e) {
            dup = e.getMessage().contains("禁止覆盖");
        }
        check("同版本重复注册 → LogicPublishException", dup, true);

        // ---------- 状态迁移 ----------
        c.setStatus("es-sync", "1.0.0", LogicInfo.STATUS_ARCHIVED);
        check("setStatus 后状态", c.find("es-sync", "1.0.0").get().status(), "archived");
        check("setStatus 不动其他版本", c.find("es-sync", "2.0.0").get().status(), "active");

        // ---------- 审计 ----------
        c.audit("es-sync", "1.0.0", "publish", "wesen", "sha=abc");
        c.audit("es-sync", "1.0.0", "archive", "wesen", "delete → archived");
        check("audit 记录条数", c.auditCount(), 2);

        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
