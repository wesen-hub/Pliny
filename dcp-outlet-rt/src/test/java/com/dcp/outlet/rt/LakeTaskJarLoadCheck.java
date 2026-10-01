package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.LoadedLogic;
import com.dcp.outlet.sdk.LogicRef;
import com.dcp.outlet.sdk.testkit.FakeInitContext;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 底座加载集成验证(P3 湖增量): JarLogicLoader 从 logic-repo 加载 dcp-lake-task fat jar
 * (lake-sync@1.0.0), 跑通 init → process fail-closed → close 生命周期。
 *
 * <p>断言口径:
 * <ul>
 *   <li>jar 加载 + SPI 定位成功(JarLogicLoader 从 logic-repo 解析 lake-sync@1.0.0;
 *       entityKeyColumns 显式给定 → 不依赖外部 MySQL);</li>
 *   <li>fail-closed 装配期: RESTCatalog.initialize(1.7.1) 会 eager GET /v1/config,
 *       REST 不可达时 init 即抛 RuntimeException(底座任务启动失败, 平台可感知)——
 *       比 process 期失败更早暴露; process 期湖写失败抛由 LakeLogicTest 覆盖;</li>
 *   <li>init fail-closed: 不传 entityKeyColumns 且元库不可达 → keys 回落为空 → 抛
 *       IllegalStateException(湖任务不能静默用空键写湖);</li>
 *   <li>湖写语义(写者裁判/字段级合并/多键文件整文件重写)由 dcp-lake-task 仓库内
 *       16 个单测覆盖(fake MetaStore/LakeOps + TestTables 真 Iceberg 写读)。</li>
 * </ul>
 *
 * <p>前置: dcp-lake-task 已 package 并部署到 logic-repo/lake-sync/1.0.0.jar + .sha256。
 */
class LakeTaskJarLoadCheck {

    private static final Path LOGIC_REPO = Path.of("logic-repo");

    /** 底座任务 params(与 backend 提交时组装同形状; 指向不可达元库/REST catalog, 只验装载与 fail-closed) */
    private static Map<String, Object> params(boolean withKeys) {
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("logicalTable", "orders");
        p.put("jdbcUrl", "jdbc:mysql://127.0.0.1:3306/dcp?connectTimeout=1000");
        p.put("user", "root");
        p.put("password", "x");
        if (withKeys) {
            p.put("entityKeyColumns", List.of("id"));
        }
        Map<String, Object> lake = new LinkedHashMap<>();
        lake.put("catalog", "demo");
        lake.put("namespace", "dcp");
        lake.put("table", "orders");
        lake.put("uri", "http://127.0.0.1:8181/api/catalog");
        lake.put("warehouse", "demo");
        // 不带 credential: RESTCatalog.initialize 不触发 token fetch(不连网);
        // 带 credential 会在 init 就 POST oauth 换 token(集成验证要验的是 init 成功 + process fail-closed)
        p.put("lake", lake);
        return p;
    }

    @Test
    void jarLoadsAndFailsClosedWhenRestUnreachable() throws Exception {
        LogicRef ref = LogicRef.of("lake-sync", "1.0.0");
        JarLogicLoader loader = new JarLogicLoader(new FileLogicRepository(LOGIC_REPO));
        LoadedLogic loaded = loader.load(ref);
        assertNotNull(loaded, "logic-repo 缺少 lake-sync/1.0.0.jar(先 package dcp-lake-task 并部署)");
        ConsumerLogic logic = loaded.logic();

        // RESTCatalog.initialize(1.7.1) eager GET /v1/config → REST 不可达 init 即抛(fail-closed 装配期)
        RuntimeException ex = assertThrows(RuntimeException.class, () ->
                logic.init(FakeInitContext.of(params(true))));
        assertNotNull(ex.getMessage());

        logic.close();
        loaded.close();
    }

    @Test
    void initFailsClosedWithoutKeysWhenMetaUnreachable() throws Exception {
        LogicRef ref = LogicRef.of("lake-sync", "1.0.0");
        JarLogicLoader loader = new JarLogicLoader(new FileLogicRepository(LOGIC_REPO));
        LoadedLogic loaded = loader.load(ref);
        assertNotNull(loaded);
        ConsumerLogic logic = loaded.logic();

        // 不传 entityKeyColumns + 元库不可达 → keys 回落为空 → init 显式拒绝(不静默用空键写湖)
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                logic.init(FakeInitContext.of(params(false))));
        assertTrue(ex.getMessage().contains("未配置业务键"), "实际: " + ex.getMessage());

        loaded.close();
    }
}
