package com.dcp.outlet.rt;

import com.dcp.outlet.rt.TaskRecord;
import com.dcp.outlet.sdk.LogicRef;
import com.dcp.outlet.rt.CreateUnitCmd;
import com.dcp.outlet.rt.FailurePolicy;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 控制面 → 计算面的**任务级指令编解码**(on-the-wire 格式,彻底版)。
 *
 * <p>一个消费任务 = 框架一个 TaskSpec(partitions = 真实分区数),bizMeta 整包透传
 * **全部分区的起点表**;执行器按框架分配的 {@code ctx.partId} 取本分区起点单分区消费,
 * 防双消费由框架归属键唯一保证(不再有单元层/数据库层闸)。
 *
 * <p>用**扁平键值**而不是嵌套 JSON:两面都零 JSON 依赖,协议一眼可读、可直接 curl 联调。
 * 约定:
 * <pre>
 *   taskId / topic / stream / contractRef / consumerGroup / logicId / logicVersion / reqId
 *   partition.{p}   = 分区编号(全部分区)
 *   startOffset.{p} = 该分区起始 offset
 *   startTso.{p}    = 该分区会话过滤边界 T0
 *   param.{k}       = 业务参数(内核只透传)
 *   failure.maxRetries / failure.backoffMs / failure.onFailure(stop|continue)
 *   pollMillis
 * </pre>
 */
public final class TaskCommandCodec {

    private TaskCommandCodec() { }

    /** 任务交接(TaskRecord + 流 + 起点表)→ 线上键值。 */
    public static Map<String, String> encode(TaskRecord t, String topic, String kafkaBootstrap, String stream,
                                             List<Integer> partitions,
                                             Map<Integer, Long> startOffsets,
                                             Map<Integer, Long> startTsos) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("reqId", "task-" + t.taskId());
        m.put("taskId", t.taskId());
        m.put("topic", topic);
        if (kafkaBootstrap != null && !kafkaBootstrap.isBlank()) {
            m.put("kafkaBootstrap", kafkaBootstrap);   // 流所在 Kafka 随指令透传(缺 = 计算面 ns 环境)
        }
        m.put("stream", stream);
        if (t.contractRef() != null && !t.contractRef().isBlank()) {
            m.put("contractRef", t.contractRef());        // 计算面据此拉契约视图(缺 → fail-closed)
        }
        m.put("consumerGroup", t.consumerGroup());
        m.put("logicId", t.logicId());
        m.put("logicVersion", t.logicVersion());
        for (int p : partitions) m.put("partition." + p, String.valueOf(p));
        startOffsets.forEach((p, off) -> m.put("startOffset." + p, String.valueOf(off)));
        if (startTsos != null) startTsos.forEach((p, tso) -> m.put("startTso." + p, String.valueOf(tso)));
        t.params().forEach((k, v) -> m.put("param." + k, String.valueOf(v)));
        m.put("failure.maxRetries", String.valueOf(t.failure().maxRetries()));
        m.put("failure.backoffMs", String.valueOf(t.failure().backoffMillis()));
        m.put("failure.onFailure", t.failure().onFailure());
        return m;
    }

    /** 线上键值 → CreateUnitCmd(计算面执行器侧;调用方按 ctx.partId 过滤本分区起点)。 */
    public static CreateUnitCmd decode(Map<String, String> m) {
        CreateUnitCmd cmd = new CreateUnitCmd()
                .reqId(m.get("reqId"))
                .unitId(m.get("taskId"))           // 执行器侧按 ctx.partId 覆写为 taskId#partId
                .taskId(m.get("taskId"))
                .topic(m.get("topic"))
                .kafkaBootstrap(m.get("kafkaBootstrap"))
                .stream(m.get("stream"))
                .contractRef(m.get("contractRef"))
                .consumerGroup(m.get("consumerGroup"))
                .logic(LogicRef.of(m.get("logicId"), m.get("logicVersion")));

        List<Integer> partitions = new ArrayList<>();
        Map<Integer, Long> starts = new LinkedHashMap<>();
        Map<Integer, Long> tsos = new LinkedHashMap<>();
        Map<String, Object> params = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : m.entrySet()) {
            String k = e.getKey();
            if (k.startsWith("partition.")) partitions.add(Integer.parseInt(e.getValue()));
            else if (k.startsWith("startOffset.")) starts.put(Integer.parseInt(k.substring(12)), Long.parseLong(e.getValue()));
            else if (k.startsWith("startTso.")) tsos.put(Integer.parseInt(k.substring(9)), Long.parseLong(e.getValue()));
            else if (k.startsWith("param.")) params.put(k.substring(6), e.getValue());
        }
        partitions.sort(Integer::compareTo);
        cmd.partitions(partitions).startOffsets(starts).startTsos(tsos).params(params);
        cmd.failure(new FailurePolicy(
                Integer.parseInt(m.getOrDefault("failure.maxRetries", "5")),
                Long.parseLong(m.getOrDefault("failure.backoffMs", "2000")),
                "continue".equalsIgnoreCase(m.getOrDefault("failure.onFailure", "stop"))
                        ? FailurePolicy.OnFailure.SKIP : FailurePolicy.OnFailure.STOP));
        cmd.pollMillis(Long.parseLong(m.getOrDefault("pollMillis", "200")));
        return cmd;
    }

    /** 键值 → URL 查询串(HTTP 下发旧通道用;框架通道不经过此)。 */
    public static String toQuery(Map<String, String> m) {
        StringBuilder sb = new StringBuilder();
        m.forEach((k, v) -> {
            if (sb.length() > 0) sb.append('&');
            sb.append(enc(k)).append('=').append(enc(v));
        });
        return sb.toString();
    }

    /** URL 查询串 → 键值。 */
    public static Map<String, String> fromQuery(String query) {
        Map<String, String> m = new LinkedHashMap<>();
        if (query == null || query.isBlank()) return m;
        for (String pair : query.split("&")) {
            int i = pair.indexOf('=');
            if (i < 0) continue;
            m.put(dec(pair.substring(0, i)), dec(pair.substring(i + 1)));
        }
        return m;
    }

    private static String enc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8); }

    private static String dec(String s) { return URLDecoder.decode(s, StandardCharsets.UTF_8); }
}
