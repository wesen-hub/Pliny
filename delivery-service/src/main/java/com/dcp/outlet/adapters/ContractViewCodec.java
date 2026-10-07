package com.dcp.outlet.adapters;

import com.dcp.outlet.worker.ContractView;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * 契约视图编解码(控制面 → 计算面)。
 *
 * <p>与指令协议同风格:**扁平键值**,零 JSON 依赖、可 curl 联调。
 * <pre>
 *   contractId / stream / rowCondition / maxLevel / consumerId
 *   allowedColumns = a,b,c              (已是"契约 ∩ 主体"的结果)
 *   maskRules      = phone=MASK_PHONE,v=MASK_FULL
 * </pre>
 *
 * <p><b>安全默认</b>:视图缺失或解码异常时**必须放大拒绝**,不能退回"全放行"。
 */
public final class ContractViewCodec {

    private ContractViewCodec() { }

    public static Map<String, String> encode(String contractId, String consumerId, String stream,
                                             Set<String> allowedColumns, Map<String, String> maskRules,
                                             int maxLevel, String rowCondition) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("contractId", contractId == null ? "" : contractId);
        m.put("consumerId", consumerId == null ? "" : consumerId);
        m.put("stream", stream == null ? "" : stream);
        m.put("maxLevel", String.valueOf(maxLevel));
        m.put("rowCondition", rowCondition == null ? "" : rowCondition);
        // null = 全放行(内部平台任务整行透传形态), 编码为 "*"(与 dcp-outlet-rt 计算面 codec 对齐)
        m.put("allowedColumns", allowedColumns == null ? "*" : String.join(",", allowedColumns));
        StringBuilder masks = new StringBuilder();
        if (maskRules != null) {
            maskRules.forEach((k, v) -> {
                if (masks.length() > 0) masks.append(',');
                masks.append(k).append('=').append(v);
            });
        }
        m.put("maskRules", masks.toString());
        return m;
    }

    public static String toQuery(Map<String, String> m) {
        StringBuilder sb = new StringBuilder();
        m.forEach((k, v) -> {
            if (sb.length() > 0) sb.append('&');
            sb.append(enc(k)).append('=').append(enc(v));
        });
        return sb.toString();
    }

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

    /** 解码为计算面可用的 {@link ContractView}。 */
    public static ContractView decode(Map<String, String> kv) {
        String stream = kv.getOrDefault("stream", "");
        Set<String> cols = new LinkedHashSet<>();
        String csv = kv.getOrDefault("allowedColumns", "");
        if (!csv.isBlank() && !"*".equals(csv)) {
            for (String c : csv.split(",")) if (!c.isBlank()) cols.add(c.trim());
        }
        Map<String, Set<String>> whitelist = new LinkedHashMap<>();
        // "*" = 全放行(null → DefaultContractFilter 不裁剪; 内部平台任务整行透传形态)
        whitelist.put(stream, "*".equals(csv) ? null : cols);

        Map<String, String> masks = new LinkedHashMap<>();
        String mr = kv.getOrDefault("maskRules", "");
        if (!mr.isBlank()) {
            for (String pair : mr.split(",")) {
                int i = pair.indexOf('=');
                if (i > 0) masks.put(pair.substring(0, i).trim(), pair.substring(i + 1).trim());
            }
        }
        int maxLevel = -1;
        try { maxLevel = Integer.parseInt(kv.getOrDefault("maxLevel", "-1")); } catch (NumberFormatException ignored) { }
        String cond = kv.getOrDefault("rowCondition", "");
        return new SimpleContractView(whitelist, masks, maxLevel, cond.isBlank() ? null : cond);
    }

    private static String enc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8); }
    private static String dec(String s) { return URLDecoder.decode(s, StandardCharsets.UTF_8); }
}
