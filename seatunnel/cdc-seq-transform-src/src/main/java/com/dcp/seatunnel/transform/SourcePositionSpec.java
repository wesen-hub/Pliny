package com.dcp.seatunnel.transform;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 逐源键名映射表 + 位宽上限表(路径 1 位点即序号, 设计文档 §2.4)。
 *
 * <p>补丁层(官方 PositionMetadataConverter)透传位点字符串 {@code "type:k1:v1:..."} 的格式契约:
 * <ul>
 *   <li>{@link #KEYS} 定义每源的键序列(与官方 converter 的拼接分支一一对应:
 *       mysql → file:pos:row、pg → lsn、oracle → commit_scn:lcr_position、
 *       sqlserver → commit_lsn:event_serial_no、mongo → ts_ms:ord、tidb → ts);</li>
 *   <li>{@link #LIMITS} 定义各键的数值上限(左闭右开), 超限编码会串位/溢出——
 *       位宽溢出守卫 fail-fast 的判定依据(守卫四件套之一)。</li>
 * </ul>
 *
 * <p>位宽推导(64 位 signed long, 各分量按位域拼接不重叠):
 * <pre>
 *   mysql     file&lt;2^19 (file&lt;&lt;44 后不占符号位) | pos&lt;2^32 | row&lt;2^12
 *   pg        logid&lt;2^21 (整体 &lt;&lt;10) | offset&lt;2^32
 *   oracle    scn&lt;2^51 | lcr&lt;2^12
 *   sqlserver lsn 前 8 hex(32bit 单调段) × 10^6 + serial&lt;2^20
 *   mongo     ts_ms&lt;2^53 (×10^6 后不越 2^63) + ord&lt;2^20
 *   tidb      ⑥ 满位源治理: TSO 裁剪后 physical&lt;2^46、logical&lt;2^18
 *             (TSO = 46bit physical 毫秒 + 18bit logical, 满 64 位, 见 PositionSeq.TSO_EPOCH_MS)
 * </pre>
 */
public final class SourcePositionSpec {

    /** 源类型 → 位点键序列(顺序即冒号分段顺序) */
    private static final Map<String, String[]> KEYS = new LinkedHashMap<>();
    /** 源类型 → 各键数值上限(左闭右开) */
    private static final Map<String, long[]> LIMITS = new LinkedHashMap<>();

    static {
        KEYS.put("mysql", new String[]{"file", "pos", "row"});
        KEYS.put("pg", new String[]{"lsn"});
        KEYS.put("oracle", new String[]{"commit_scn", "lcr_position"});
        KEYS.put("sqlserver", new String[]{"commit_lsn", "event_serial_no"});
        KEYS.put("mongo", new String[]{"ts_ms", "ord"});
        KEYS.put("tidb", new String[]{"ts"});

        LIMITS.put("mysql", new long[]{1L << 19, 1L << 32, 1L << 12});
        LIMITS.put("pg", new long[]{1L << 21, 1L << 32});
        LIMITS.put("oracle", new long[]{1L << 51, 1L << 12});
        LIMITS.put("sqlserver", new long[]{1L << 32, 1L << 20});
        LIMITS.put("mongo", new long[]{1L << 53, 1L << 20});
        // tidb: TSO 满位源, 裁剪平移后校验分量(physical 46bit / logical 18bit)
        LIMITS.put("tidb", new long[]{1L << 46, 1L << 18});
    }

    private SourcePositionSpec() {
    }

    /** 键序列(未知源返回空数组; 未知源在 PositionSeq 已 fail-fast, 这里仅作映射查询) */
    public static String[] keysOf(String sourceType) {
        String[] keys = KEYS.get(norm(sourceType));
        return keys == null ? new String[0] : keys;
    }

    /** 位宽上限表(未知源返回空数组) */
    public static long[] limitsOf(String sourceType) {
        long[] limits = LIMITS.get(norm(sourceType));
        return limits == null ? new long[0] : limits;
    }

    /** 校验位点字符串与映射表一致: 键名必须逐段匹配、键值必须为数值(不校验上限, 上限在编码处) */
    public static void checkLayout(String sourceType, String position) {
        String[] keys = keysOf(sourceType);
        if (keys.length == 0) {
            throw new IllegalStateException("未知源类型 " + sourceType + " 无键名映射表");
        }
        // 去掉 type 前缀: "mysql:file:pos:row"
        int cut = position.indexOf(':');
        String body = cut > 0 ? position.substring(cut + 1) : position;
        String[] parts = body.split(":", -1);
        if (parts.length < keys.length) {
            throw new IllegalStateException("位点键数不足: " + position + " (期望 " + keys.length + " 键: "
                    + String.join(":", keys) + ")");
        }
        for (int i = 0; i < keys.length; i++) {
            String part = parts[i];
            if (part == null || part.isBlank()) {
                throw new IllegalStateException("位点键 " + keys[i] + " 缺失: " + position);
            }
        }
    }

    /** 校验各键值在映射表位宽上限内(位宽溢出守卫, fail-fast) */
    public static void checkLimits(String sourceType, long[] values) {
        long[] limits = limitsOf(sourceType);
        if (limits.length == 0) {
            return; // tidb 满位源无分量约束
        }
        if (values.length < limits.length) {
            throw new IllegalStateException("位点分量不足: " + sourceType);
        }
        String[] keys = keysOf(sourceType);
        for (int i = 0; i < limits.length; i++) {
            if (values[i] < 0 || values[i] >= limits[i]) {
                throw new IllegalStateException("位宽溢出: " + sourceType + "." + keys[i]
                        + "=" + values[i] + " 超出上限 " + limits[i] + " (守卫: 位宽溢出, fail-fast)");
            }
        }
    }

    private static String norm(String sourceType) {
        return sourceType == null ? "" : sourceType.toLowerCase();
    }
}
