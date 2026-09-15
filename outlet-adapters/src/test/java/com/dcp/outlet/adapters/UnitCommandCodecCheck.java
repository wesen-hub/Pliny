package com.dcp.outlet.adapters;

import com.dcp.outlet.control.DispatchRequest;
import com.dcp.outlet.control.FailureSpec;
import com.dcp.outlet.worker.CreateUnitCmd;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 指令编解码往返自检:**编码 → 查询串 → 解码**,字段一个都不能丢(两平面靠它对话)。 */
public final class UnitCommandCodecCheck {

    private static int passed, failed;

    public static void main(String[] args) {
        Map<Integer, Long> starts = new LinkedHashMap<>();
        starts.put(0, 31785L);
        starts.put(2, 40001L);
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("esUrl", "http://es:9200");
        params.put("index", "crm_rt_sync_c");
        params.put("note", "a b&c=d");                       // 特殊字符(URL 编码)

        DispatchRequest req = new DispatchRequest("req-1", "u-t1-0-1", "t1",
                "dcp.cdc.target_db.rt_sync_c", "rt_sync_c", List.of(0, 2), starts, "task-t1",
                "es-sync", "1.0.0", params,
                new FailureSpec(7, 1500L, "dlq", "continue"), 30_000L);

        Map<String, String> kv = UnitCommandCodec.encode(req);
        check("编码含分区", kv.containsKey("partition.0") && kv.containsKey("partition.2"), true);
        check("编码含起始 offset", kv.get("startOffset.0"), "31785");
        check("编码含业务参数", kv.get("param.index"), "crm_rt_sync_c");

        String query = UnitCommandCodec.toQuery(kv);
        Map<String, String> parsed = UnitCommandCodec.fromQuery(query);
        CreateUnitCmd cmd = UnitCommandCodec.decode(parsed);

        check("往返:unitId", cmd.unitId(), "u-t1-0-1");
        check("往返:taskId", cmd.taskId(), "t1");
        check("往返:topic", cmd.topic(), "dcp.cdc.target_db.rt_sync_c");
        check("往返:stream", cmd.stream(), "rt_sync_c");
        check("往返:consumerGroup", cmd.consumerGroup(), "task-t1");
        check("往返:logic", cmd.logic().toString(), "es-sync@1.0.0");
        check("往返:分区组(有序)", cmd.partitions(), List.of(0, 2));
        check("往返:每分区起点", cmd.startOffsets().get(2), 40001L);
        check("往返:业务参数含特殊字符", cmd.params().get("note"), "a b&c=d");
        check("往返:失败策略", cmd.failure().maxRetries(), 7);
        check("往返:onFailure 映射为 SKIP", cmd.failure().onFailure().name(), "SKIP");
        check("往返:租约 TTL 秒", cmd.leaseTtlSeconds(), 30);

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
