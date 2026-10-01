package com.dcp.outlet.adapters;

import com.dcp.outlet.control.FailureSpec;
import com.dcp.outlet.control.TaskRecord;
import com.dcp.outlet.worker.CreateUnitCmd;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务级指令编解码往返自检:**编码 → 查询串 → 解码**,字段一个都不能丢(两平面靠它对话)。
 * 彻底版:一个任务 = 框架一个 TaskSpec,全部分区起点表整包透传;执行器按框架分配的
 * {@code ctx.partId} 取本分区起点单分区消费(unitId 由执行器覆写为 taskId#partId)。
 */
public final class TaskCommandCodecCheck {

    private static int passed, failed;

    public static void main(String[] args) {
        Map<Integer, Long> starts = new LinkedHashMap<>();
        starts.put(0, 31785L);
        starts.put(1, 0L);
        starts.put(2, 40001L);
        Map<Integer, Long> tsos = new LinkedHashMap<>();
        tsos.put(0, 469104200000000000L);
        tsos.put(1, 469104200000000000L);
        tsos.put(2, 469104200000000000L);

        TaskRecord t = new TaskRecord("t1").name("编解码自检").consumerType("custom")
                .stream("rt_sync_c").logic("es-sync", "1.0.0")
                .contractRef("ct-sub-rt_sync_c").startPolicy("snapshot")
                .params(Map.of("esUrl", "http://es:9200", "index", "crm_rt_sync_c", "note", "a b&c=d"))
                .failure(new FailureSpec(7, 1500L, "dlq", "continue"))
                .consumerGroup("task-t1");

        Map<String, String> kv = TaskCommandCodec.encode(t, "dcp.cdc.target_db.rt_sync_c",
                "dcp-outlet-kafka:19092", "rt_sync_c", List.of(0, 1, 2), starts, tsos);
        check("编码含全部分区", kv.get("partition.0") + kv.get("partition.1") + kv.get("partition.2"), "012");
        check("编码含起始 offset", kv.get("startOffset.0"), "31785");
        check("编码含会话过滤边界 T0", kv.get("startTso.2"), "469104200000000000");
        check("编码含业务参数", kv.get("param.index"), "crm_rt_sync_c");
        check("编码含契约引用", kv.get("contractRef"), "ct-sub-rt_sync_c");
        check("编码含流所在 Kafka(随指令透传)", kv.get("kafkaBootstrap"), "dcp-outlet-kafka:19092");

        String query = TaskCommandCodec.toQuery(kv);
        Map<String, String> parsed = TaskCommandCodec.fromQuery(query);
        CreateUnitCmd cmd = TaskCommandCodec.decode(parsed);

        check("往返:unitId = taskId(执行器侧覆写 taskId#partId)", cmd.unitId(), "t1");
        check("往返:taskId", cmd.taskId(), "t1");
        check("往返:topic", cmd.topic(), "dcp.cdc.target_db.rt_sync_c");
        check("往返:stream", cmd.stream(), "rt_sync_c");
        check("往返:流所在 Kafka", cmd.kafkaBootstrap(), "dcp-outlet-kafka:19092");
        check("往返:契约引用(计算面据此拉视图)", cmd.contractRef(), "ct-sub-rt_sync_c");
        check("往返:consumerGroup", cmd.consumerGroup(), "task-t1");
        check("往返:logic", cmd.logic().toString(), "es-sync@1.0.0");
        check("往返:全部分区(有序)", cmd.partitions(), List.of(0, 1, 2));
        check("往返:每分区起点", cmd.startOffsets().get(2), 40001L);
        check("往返:会话过滤边界 T0", cmd.startTsos().get(0), 469104200000000000L);
        check("往返:业务参数含特殊字符", cmd.params().get("note"), "a b&c=d");
        check("往返:失败策略", cmd.failure().maxRetries(), 7);
        check("往返:onFailure 映射为 SKIP", cmd.failure().onFailure().name(), "SKIP");

        System.out.println();
        System.out.println("  查询串示例(前 200 字符):" + query.substring(0, Math.min(200, query.length())));
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
