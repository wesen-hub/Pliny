package com.dcp.seatunnel.transform;

/**
 * 位点体系离线自检(路径 1 位点即序号, 设计文档 §2.4; main 直跑, 无需引擎/真实源)。
 *
 * <p>覆盖:
 * <ul>
 *   <li>编码公式: 六源各一例期望值(重放恒定: 同一位点两次编码同号)</li>
 *   <li>守卫-位宽溢出: 分量超映射表上限 → fail-fast</li>
 *   <li>守卫-未知格式: 键缺失/非数值/未知 type → fail-fast</li>
 *   <li>② 逐源键名映射表: 键数不足/键空 → fail-fast; 合法位点通过</li>
 *   <li>④ 快照级号 S=f(起点位点): 两次编码同号(全批同号/重放恒定)</li>
 *   <li>⑤ 灰度开关三态 + ③ 混发/体系重置守卫: 经 CdcSeqTransform 实例化路径验证</li>
 * </ul>
 */
public final class PositionCheck {

    private static int passed;
    private static int failed;

    public static void main(String[] args) {
        // ① 编码公式(期望值手工推导, 与设计文档 §2.4 公式一一对应)
        eq("mysql 编码", encode("mysql", "mysql:123:456789:2"),
                (123L << 44) | (456789L << 12) | 2L);
        eq("mysql 重放恒定", encode("mysql", "mysql:123:456789:2"),
                encode("mysql", "mysql:123:456789:2"));
        eq("mysql row 缺省", encode("mysql", "mysql:123:456789"), (123L << 44) | (456789L << 12));
        eq("pg 编码", encode("pg", "pg:3/16B6C50"), ((3L << 32) | 0x16B6C50L) << 10);
        eq("oracle 编码", encode("oracle", "oracle:58001234:7"), (58001234L << 12) | 7L);
        eq("sqlserver 编码", encode("sqlserver", "sqlserver:00000025:00000001:002f:0003"),
                0x00000025L * 1_000_000L + 1L);
        eq("mongo 编码", encode("mongo", "mongo:1728000000000:42"), 1728000000000L * 1_000_000L + 42L);
        // ⑥ tidb TSO 裁剪平移: (physical - TSO_EPOCH_MS)<<18 | logical
        long tso = 4613546412488327174L;
        eq("tidb TSO 裁剪", encode("tidb", "tidb:" + tso),
                ((tso >>> 18) - PositionSeq.TSO_EPOCH_MS) << 18 | (tso & ((1L << 18) - 1)));
        eq("tidb TSO 裁剪重放恒定", encode("tidb", "tidb:" + tso),
                encode("tidb", "tidb:" + tso));

        // ③ 守卫-位宽溢出: file ≥ 2^19 串位符号位 / pos ≥ 2^32 越界
        throwIf("mysql file 位宽溢出", () -> encode("mysql", "mysql:524288:1:0"));
        throwIf("mysql pos 位宽溢出", () -> encode("mysql", "mysql:1:4294967296:0"));
        throwIf("mysql row 位宽溢出", () -> encode("mysql", "mysql:1:1:4096"));
        throwIf("pg logid 位宽溢出", () -> encode("pg", "pg:2097152/0"));
        throwIf("oracle scn 位宽溢出", () -> encode("oracle", "oracle:2251799813685248:0"));
        throwIf("mongo ord 位宽溢出", () -> encode("mongo", "mongo:1728000000000:1048576"));
        // ⑥ tidb TSO 裁剪: physical 早于纪元基线 → fail-fast(TSO 为正且 physical&lt;2^45, 移位不溢出, 唯一真实边界是纪元)
        throwIf("tidb TSO 早于裁剪纪元", () -> encode("tidb", "tidb:" + (PositionSeq.TSO_EPOCH_MS - 1 << 18)));

        // ③ 守卫-未知格式 fail-fast
        throwIf("位点为空", () -> encode("mysql", null));
        throwIf("位点非数值", () -> encode("mysql", "mysql:abc:1:0"));
        throwIf("未知 type", () -> encode("mysql", "redis:1:2"));
        throwIf("未知体系", () -> encode("mysql", "unknown:whatever"));
        throwIf("sqlserver lsn 长度不足", () -> encode("sqlserver", "sqlserver:0000"));

        // ② 逐源键名映射表
        throwIf("键数不足", () -> SourcePositionSpec.checkLayout("mysql", "mysql:1:2"));
        throwIf("键值空", () -> SourcePositionSpec.checkLayout("pg", "pg:"));
        if (noThrow(() -> SourcePositionSpec.checkLayout("mysql", "mysql:1:2:3"))) {
            ok("映射表校验通过");
        } else {
            fail("映射表校验通过");
        }

        // ④ 快照级号 S=f(起点位点): 同号两次(重放恒定)
        eq("快照级号 S", encode("mysql", "mysql:999:123456789:0"),
                (999L << 44) | (123456789L << 12));

        // ⑤ 灰度开关 + ③ 混发守卫: 经 CdcSeqTransform 同包直连验证(见 PositionCheckTransform 断言)
        seqPolicyCheck();

        System.out.println("PositionCheck: " + passed + " 通过, " + failed + " 失败");
        if (failed > 0) {
            System.exit(1);
        }
    }

    /**
     * ⑤/③ 发号策略级自检: 直接驱动 CdcSeqTransform.resolveSeq(同包可见)。
     * 严格位点模式(emit_position=true)断言:
     * 增量行有位点→位点编码; 位点空+未配快照起点→混发 fail-fast; 位点空+配快照起点→快照级号 S;
     * 体系重置(seq 大幅回退)→ 告警不丢数据。
     */
    private static void seqPolicyCheck() {
        // 构造最小 config(仅 source_type/emit_position/snapshot_start_position/阈值), 其余选项取默认
        java.util.Map<String, Object> opts = new java.util.HashMap<>();
        opts.put("logical_table", "t");
        opts.put("business_key_fields", java.util.List.of("id"));
        opts.put("key_columns", java.util.List.of("id"));
        opts.put("seq_db_url", "jdbc:mysql://x/d");
        opts.put("seq_db_user", "u");
        opts.put("seq_db_password", "p");
        opts.put("source_type", "mysql");
        opts.put("emit_position", true);
        opts.put("snapshot_start_position", "mysql:999:123456789:0");
        opts.put("seq_reset_threshold", 1_000_000_000_000L);
        org.apache.seatunnel.api.configuration.ReadonlyConfig cfg =
                org.apache.seatunnel.api.configuration.ReadonlyConfig.fromMap(opts);

        // 输入 CatalogTable: 单列 id LONG(无 metadata 列声明, 位点列由补丁行尾透传)
        org.apache.seatunnel.api.table.catalog.PhysicalColumn idCol =
                org.apache.seatunnel.api.table.catalog.PhysicalColumn.of("id",
                        org.apache.seatunnel.api.table.type.BasicType.LONG_TYPE, (Long) null, true, null, null);
        org.apache.seatunnel.api.table.catalog.TableSchema schema =
                org.apache.seatunnel.api.table.catalog.TableSchema.builder()
                        .columns(new java.util.ArrayList<>(java.util.List.of(idCol))).build();
        org.apache.seatunnel.api.table.catalog.CatalogTable table =
                org.apache.seatunnel.api.table.catalog.CatalogTable.of(
                        org.apache.seatunnel.api.table.catalog.TableIdentifier.of("dcp", "d", "t"), schema,
                        new java.util.HashMap<String, String>(), new java.util.ArrayList<String>(), null);

        CdcSeqTransformConfig config = new CdcSeqTransformConfig(java.util.List.of(table), cfg);
        CdcSeqTransform transform = new CdcSeqTransform(config);
        transform.open();

        try {
            eq("严格位点: 增量行位点编码",
                    transform.resolveSeq("mysql:123:456789:2", false), encode("mysql", "mysql:123:456789:2"));
            eq("严格位点: 快照行(位点空)→快照级号 S",
                    transform.resolveSeq(null, true), (999L << 44) | (123456789L << 12));
            // ④ 快照行带非空位点也必须走 S(快照阶段位点不逐行单调, 不能作序号)
            eq("严格位点: 快照行带位点仍走快照级号 S",
                    transform.resolveSeq("mysql:7:8:9", true), (999L << 44) | (123456789L << 12));
            // ③ 体系重置: 历史峰值已推到 mysql:123:456789, 现遇 file=1 的小号 → 告警且值不变
            long resetSeq = transform.resolveSeq("mysql:1:456789:0", false);
            eq("体系重置: 新 seq 原样返回(告警不丢数据)", resetSeq, (1L << 44) | (456789L << 12));
        } catch (Exception e) {
            fail("严格位点 resolveSeq 抛异常: " + e);
        }

        // ⑥ mongo 满位源治理: ts_ms 缺失键空哨兵 → 严格位点模式 fail-fast(混发守卫);
        // 兼容模式回退计数器号段需连元库, 不在此离线自检覆盖(逻辑见 resolveSeq 兼容分支)
        java.util.Map<String, Object> optsM = new java.util.HashMap<>(opts);
        optsM.put("source_type", "mongo");
        CdcSeqTransformConfig configM = new CdcSeqTransformConfig(java.util.List.of(table),
                org.apache.seatunnel.api.configuration.ReadonlyConfig.fromMap(optsM));
        CdcSeqTransform transformM = new CdcSeqTransform(configM);
        transformM.open();
        try {
            transformM.resolveSeq("mongo::5", false);
            fail("mongo 键空哨兵 fail-fast");
        } catch (IllegalStateException e) {
            ok("mongo 键空哨兵 fail-fast(严格位点: " + e.getMessage() + ")");
        } catch (Exception e) {
            fail("mongo 键空哨兵: 异常类型不符: " + e);
        }
        transformM.close();

        // 混发 fail-fast: 快照起点未配置时位点空必须失败
        java.util.Map<String, Object> opts2 = new java.util.HashMap<>(opts);
        opts2.remove("snapshot_start_position");
        org.apache.seatunnel.api.configuration.ReadonlyConfig cfg2 =
                org.apache.seatunnel.api.configuration.ReadonlyConfig.fromMap(opts2);
        CdcSeqTransformConfig config2 = new CdcSeqTransformConfig(java.util.List.of(table), cfg2);
        CdcSeqTransform transform2 = new CdcSeqTransform(config2);
        transform2.open();
        try {
            transform2.resolveSeq(null, true);
            fail("混发 fail-fast: 位点空且无快照起点");
        } catch (IllegalStateException e) {
            ok("混发 fail-fast: 位点空且无快照起点(fail-fast: " + e.getMessage() + ")");
        } catch (Exception e) {
            fail("混发 fail-fast: 异常类型不符: " + e);
        }

        transform.close();
        transform2.close();
    }

    private static long encode(String sourceType, String position) {
        return PositionSeq.encode(sourceType, position);
    }

    private static void eq(String name, long actual, long expected) {
        if (actual == expected) {
            ok(name);
        } else {
            fail(name + ": 期望 " + expected + " 实际 " + actual);
        }
    }

    private static void ok(String name) {
        passed++;
        System.out.println("  [PASS] " + name);
    }

    private static void fail(String name) {
        failed++;
        System.out.println("  [FAIL] " + name);
    }

    private static void throwIf(String name, Runnable r) {
        try {
            r.run();
            fail(name + ": 未抛异常");
        } catch (IllegalStateException e) {
            ok(name + "(fail-fast: " + e.getMessage() + ")");
        }
    }

    private static boolean noThrow(Runnable r) {
        try {
            r.run();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }
}
