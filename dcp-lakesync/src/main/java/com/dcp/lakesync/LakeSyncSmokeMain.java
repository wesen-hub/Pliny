package com.dcp.lakesync;

import com.dcp.lakesync.config.LakeJobConfig;

/**
 * 无 Spark 核心自检(Docker 构建期执行): 配置解析 + 增量谓词构建 + 标识符引用 —— 纯函数验证, 不启 Spark。
 */
public final class LakeSyncSmokeMain {

    private static int failures = 0;

    public static void main(String[] args) {
        check("全量(无游标)谓词为 null", newJob(0, -1, -1).pullPredicate() == null);
        check("增量谓词 OR 下推", "`_seq_a` > 41 OR `_seq_b` > 27".equals(newJob(0, 41, 27).pullPredicate()));
        LakeJobConfig c = newJob(1, -1, 5);
        check("混载: 任一列未同步(游标-1) → 整轮全量(防漏未同步列历史)", c.pullPredicate() == null);
        LakeJobConfig bad = new LakeJobConfig();
        boolean threw = false;
        try {
            LakeJobConfig.load("/nonexistent.json");
        } catch (IllegalArgumentException e) {
            threw = true;
        }
        check("缺失文件抛配置错误", threw);
        if (failures == 0) {
            System.out.println("LAKESYNC SMOKE PASS: 配置解析/谓词/引用 自检通过");
            System.exit(0);
        }
        System.out.println("LAKESYNC SMOKE FAIL: " + failures + " 项未通过");
        System.exit(1);
    }

    private static LakeJobConfig newJob(long id, long curA, long curB) {
        LakeJobConfig c = new LakeJobConfig();
        c.logicalTable = "t_user_model";
        c.entityKeyColumns.add("user_id");
        c.dataSource.url = "jdbc:mysql://x/target_db";
        c.dataSource.table = "t_user_model";
        LakeJobConfig.SeqColumn a = new LakeJobConfig.SeqColumn();
        a.name = "_seq_a";
        a.cursor = curA;
        LakeJobConfig.SeqColumn b = new LakeJobConfig.SeqColumn();
        b.name = "_seq_b";
        b.cursor = curB;
        c.seqColumns.add(a);
        c.seqColumns.add(b);
        c.lake.catalog = "demo";
        c.lake.namespace = "dcp";
        c.lake.table = "t_user_model";
        c.lake.uri = "http://polaris:8181/api/catalog";
        return c;
    }

    private static void check(String name, boolean ok) {
        System.out.println((ok ? "PASS " : "FAIL ") + name);
        if (!ok) failures++;
    }
}
