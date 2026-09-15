package com.dcp.outlet.adapters;

import com.dcp.outlet.control.DispatchRequest;
import com.dcp.outlet.sdk.LogicRef;
import com.dcp.outlet.worker.CreateUnitCmd;
import com.dcp.outlet.worker.FailurePolicy;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 控制面 → 计算面的**指令编解码**(on-the-wire 格式)。
 *
 * <p>用**扁平键值**而不是嵌套 JSON:两面都零 JSON 依赖,协议一眼可读、可直接 curl 联调。
 * 约定:
 * <pre>
 *   unitId / taskId / topic / stream / consumerGroup / logicId / logicVersion / reqId
 *   partition        = 可重复(分区组)
 *   startOffset.{p}  = 该分区起始 offset
 *   param.{k}        = 业务参数(内核只透传)
 *   failure.maxRetries / failure.backoffMs / failure.onFailure(stop|continue)
 *   pollMillis / leaseTtlSeconds / dispatchId
 * </pre>
 */
public final class UnitCommandCodec {

    private UnitCommandCodec() { }

    /** DispatchRequest(控制面) → 线上键值。 */
    public static Map<String, String> encode(DispatchRequest r) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("dispatchId", r.reqId());
        m.put("unitId", r.unitId());
        m.put("taskId", r.taskId());
        m.put("topic", r.topic());
        m.put("stream", r.stream());
        m.put("consumerGroup", r.consumerGroup());
        m.put("logicId", r.logicId());
        m.put("logicVersion", r.logicVersion());
        for (int p : r.partitions()) m.put("partition." + p, String.valueOf(p));
        r.startOffsets().forEach((p, off) -> m.put("startOffset." + p, String.valueOf(off)));
        r.params().forEach((k, v) -> m.put("param." + k, String.valueOf(v)));
        m.put("failure.maxRetries", String.valueOf(r.failure().maxRetries()));
        m.put("failure.backoffMs", String.valueOf(r.failure().backoffMillis()));
        m.put("failure.onFailure", r.failure().onFailure());
        m.put("leaseTtlSeconds", String.valueOf(r.leaseTtlMillis() / 1000));
        return m;
    }

    /** 线上键值 → CreateUnitCmd(计算面)。 */
    public static CreateUnitCmd decode(Map<String, String> m) {
        CreateUnitCmd cmd = new CreateUnitCmd()
                .reqId(m.get("dispatchId"))
                .unitId(m.get("unitId"))
                .taskId(m.get("taskId"))
                .topic(m.get("topic"))
                .stream(m.get("stream"))
                .consumerGroup(m.get("consumerGroup"))
                .logic(LogicRef.of(m.get("logicId"), m.get("logicVersion")));

        List<Integer> partitions = new ArrayList<>();
        Map<Integer, Long> starts = new LinkedHashMap<>();
        Map<String, Object> params = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : m.entrySet()) {
            String k = e.getKey();
            if (k.startsWith("partition.")) partitions.add(Integer.parseInt(e.getValue()));
            else if (k.startsWith("startOffset.")) starts.put(Integer.parseInt(k.substring(12)), Long.parseLong(e.getValue()));
            else if (k.startsWith("param.")) params.put(k.substring(6), e.getValue());
        }
        partitions.sort(Integer::compareTo);
        cmd.partitions(partitions).startOffsets(starts).params(params);
        cmd.failure(new FailurePolicy(
                Integer.parseInt(m.getOrDefault("failure.maxRetries", "5")),
                Long.parseLong(m.getOrDefault("failure.backoffMs", "2000")),
                "continue".equalsIgnoreCase(m.getOrDefault("failure.onFailure", "stop"))
                        ? FailurePolicy.OnFailure.SKIP : FailurePolicy.OnFailure.STOP));
        cmd.pollMillis(Long.parseLong(m.getOrDefault("pollMillis", "200")));
        return cmd;
    }

    /** 键值 → URL 查询串(控制面发起 HTTP 下发时用)。 */
    public static String toQuery(Map<String, String> m) {
        StringBuilder sb = new StringBuilder();
        m.forEach((k, v) -> {
            if (sb.length() > 0) sb.append('&');
            sb.append(enc(k)).append('=').append(enc(v));
        });
        return sb.toString();
    }

    /** URL 查询串 → 键值(计算面接收时用;Spring 也能直接绑定 Map)。 */
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
