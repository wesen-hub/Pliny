package com.dcp.filesync;

import com.dcp.filesync.core.RcloneEngine;
import com.dcp.filesync.util.Strategy;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * rclone 引擎纯函数单测(命令构造/远程路径/模式判定/引擎动作)——与迁出前 backend 行为对齐。
 */
class RcloneEngineTest {

    private final RcloneEngine engine = new RcloneEngine();

    @Test
    void fullOnceAlwaysFullRound() {
        assertTrue(Strategy.isFullRound("full_once", true));
        assertTrue(Strategy.isFullRound("full_once", false));
    }

    @Test
    void incrAlwaysIncremental() {
        assertFalse(Strategy.isFullRound("incr_scheduled", false));
        assertFalse(Strategy.isFullRound("incr_scheduled", true));
    }

    @Test
    void fullThenIncrDependsOnBaseline() {
        assertTrue(Strategy.isFullRound("full_then_incr", false));   // 无基线 → 全量
        assertFalse(Strategy.isFullRound("full_then_incr", true));   // 有基线 → 增量
    }

    @Test
    void engineOpMirrorByDefaultOnFull() {
        Map<String, Object> st = new LinkedHashMap<>();
        assertEquals("sync", Strategy.engineOp(true, st));    // 全量默认镜像删多余
        assertEquals("copy", Strategy.engineOp(false, st));   // 增量恒 copy
    }

    @Test
    void engineOpDegradesToCopyWhenDeleteExtrasFalse() {
        Map<String, Object> st = Map.of("deleteExtras", false);
        assertEquals("copy", Strategy.engineOp(true, st));
    }

    @Test
    void s3RemotePaths() {
        Map<String, String> src = new LinkedHashMap<>();
        src.put("type", "s3");
        src.put("bucket", "orders");
        src.put("path", "raw/2026/");
        assertEquals("dcpsrc:orders/raw/2026", RcloneEngine.srcRemote(src));

        Map<String, String> dst = new LinkedHashMap<>();
        dst.put("bucket", "datalake");
        dst.put("prefix", "orders/2026/");
        assertEquals("dcpdst:datalake/orders/2026", RcloneEngine.dstRemote(dst));
    }

    @Test
    void localSourceUsesAbsolutePath() {
        Map<String, String> src = new LinkedHashMap<>();
        src.put("type", "local");
        src.put("root", "/data");
        src.put("path", "app/");
        assertEquals("/data/app", RcloneEngine.srcRemote(src));
    }

    @Test
    void argsCarryJsonLogAndTransfers() {
        Path conf = Path.of("/tmp/fake.conf");
        Map<String, String> src = Map.of("type", "s3", "bucket", "b", "path", "");
        Map<String, String> dst = Map.of("bucket", "d", "prefix", "");
        Map<String, Object> st = new LinkedHashMap<>();
        st.put("includes", List.of("*.csv", "*.json"));
        st.put("excludes", List.of("*.tmp"));
        st.put("transfers", 16);
        st.put("checkers", 4);
        st.put("bwlimit", "10M");
        List<String> args = engine.buildRcloneArgs(conf, src, dst, "copy", st);
        assertTrue(args.get(0).equals("rclone") && args.get(1).equals("copy"));
        assertTrue(args.contains("--use-json-log"));
        assertTrue(args.contains("--include"));
        assertTrue(args.contains("*.csv"));
        assertTrue(args.contains("--exclude"));
        assertTrue(args.contains("*.tmp"));
        int i = args.indexOf("--transfers");
        assertEquals("16", args.get(i + 1));
        int j = args.indexOf("--bwlimit");
        assertEquals("10M", args.get(j + 1));
    }
}
