package com.dcp.seatunnel.transform;

/**
 * 位点编码器(设计方案 §2.4, 路径 1 位点即序号): seq = f(源端位点)。
 *
 * <p>输入为 connector 补丁层(PositionMetadataConverter)透传的位点字符串
 * {@code "type:k1:v1:..."}; 按源类型解析并按六条不变式约束:
 * 重放恒定(同一位点任何时刻编码相同)/ 单源单调 / fail-fast(格式未知立即失败, 不降级)。
 *
 * <p>编码公式(与设计文档 §2.4 一一对应):
 * <pre>
 *   mysql     fileNo&lt;&lt;44 | pos&lt;&lt;12 | row
 *   pg        (logid&lt;&lt;32 | offset) &lt;&lt; 10            lsn = "logid/offsethex"
 *   oracle    commit_scn&lt;&lt;12 | lcr_position
 *   sqlserver lsnNum*10^6 + serial                  lsn 三段 hex 取前 8 位(32bit, 单调)
 *   mongo     ts_ms*10^6 + ord                      (毫秒级平移, 单主流单调)
 *   tidb      (physical - TSO_EPOCH_MS)&lt;&lt;18 | logical   (⑥ 满位源治理: TSO 裁剪平移)
 * </pre>
 *
 * <p>守卫(四件套之一: 位宽溢出): 各源分量位宽上限见 {@link SourcePositionSpec#LIMITS},
 * 编码前逐分量校验、编码后校验结果为正(符号位被占即溢出), 超限一律 fail-fast。
 *
 * <p>⑥ 满位源治理:
 * <ul>
 *   <li>tidb TSO 满 64 位(46bit physical 时间戳 + 18bit logical), 直接透传数值域贴近
 *       Long.MAX 且与其它源数值域不可比 → 裁剪纪元基线平移, 三判据(重放恒定/单源单调/逐行可得)不变;
 *       {@link #TSO_EPOCH_MS} 基线 2020-01-01, 实测确认 TSO 取值后校正;</li>
 *   <li>mongo ts_ms/ord 取值形态待实跑确认(TODO: 实测 sourceOffset); ts_ms 缺失时
 *       converter 输出键空哨兵 → 严格位点模式 fail-fast、兼容模式回退计数器号段。</li>
 * </ul>
 */
public final class PositionSeq {

    private PositionSeq() {
    }

    /** 解析 "type:..." 位点字符串并按源编码; 未知源/格式 → fail-fast(不变式 5) */
    public static long encode(String sourceType, String position) {
        if (position == null || position.isBlank()) {
            throw new IllegalStateException("位点为空: 无法编码 seq(不变式 5 fail-fast)");
        }
        int cut = position.indexOf(':');
        String type = cut > 0 ? position.substring(0, cut) : position;
        String body = cut > 0 ? position.substring(cut + 1) : "";
        // 源类型归一(任务配置 source_type 与位点字符串 type 双保险, 以位点字符串为准)
        String st = (sourceType == null ? "" : sourceType).toLowerCase();
        switch (type) {
            case "mysql":
                return mysql(body);
            case "pg":
                return pg(body);
            case "oracle":
                return oracle(body);
            case "sqlserver":
                return sqlserver(body);
            case "mongo":
                return mongo(body);
            case "tidb":
                return tidb(body);
            case "unknown":
                throw new IllegalStateException("未知位点体系(" + body + "), seq 无法编码; "
                        + "源库类型=" + st + "(不变式 5 fail-fast)");
            default:
                throw new IllegalStateException("位点类型无法识别: " + type + "(" + position + ")");
        }
    }

    /** mysql:file:pos:row → fileNo<<44 | pos<<12 | row */
    static long mysql(String body) {
        String[] p = body.split(":", -1);
        if (p.length < 2) {
            throw new IllegalStateException("mysql 位点格式异常: " + body);
        }
        long file = parseLong(p[0], "mysql.file");
        long pos = parseLong(p[1], "mysql.pos");
        long row = p.length > 2 && !p[2].isEmpty() ? parseLong(p[2], "mysql.row") : 0L;
        SourcePositionSpec.checkLimits("mysql", new long[]{file, pos, row});
        return requirePositive((file << 44) | (pos << 12) | row, "mysql");
    }

    /** pg:lsn "logid/offsethex" → (logid<<32 | offset) << 10 */
    static long pg(String body) {
        int slash = body.indexOf('/');
        if (slash <= 0) {
            throw new IllegalStateException("pg lsn 格式异常: " + body);
        }
        long logid = parseLong(body.substring(0, slash), "pg.lsn.logid");
        long offset = Long.parseLong(body.substring(slash + 1), 16);
        SourcePositionSpec.checkLimits("pg", new long[]{logid, offset});
        return requirePositive(((logid << 32) | offset) << 10, "pg");
    }

    /** oracle:commit_scn:lcr_position → scn<<12 | lcr */
    static long oracle(String body) {
        String[] p = body.split(":", -1);
        if (p.length < 1) {
            throw new IllegalStateException("oracle 位点格式异常: " + body);
        }
        long scn = parseLong(p[0], "oracle.commit_scn");
        long lcr = p.length > 1 && !p[1].isEmpty() ? parseLong(p[1], "oracle.lcr_position") : 0L;
        SourcePositionSpec.checkLimits("oracle", new long[]{scn, lcr});
        return requirePositive((scn << 12) | lcr, "oracle");
    }

    /** sqlserver:commit_lsn:event_serial_no → lsnNum*10^6 + serial(lsn 三段 hex 取前 8 位) */
    static long sqlserver(String body) {
        String[] p = body.split(":", -1);
        if (p.length < 1) {
            throw new IllegalStateException("sqlserver 位点格式异常: " + body);
        }
        String hex = p[0].replace("0x", "").replace(":", "");
        if (hex.length() < 8) {
            throw new IllegalStateException("sqlserver lsn 长度不足: " + body);
        }
        long lsnNum = Long.parseLong(hex.substring(0, 8), 16);
        long serial = p.length > 1 && !p[1].isEmpty() ? parseLong(p[1], "sqlserver.event_serial_no") : 0L;
        SourcePositionSpec.checkLimits("sqlserver", new long[]{lsnNum, serial});
        return requirePositive(lsnNum * 1_000_000L + serial, "sqlserver");
    }

    /**
     * TiDB TSO 裁剪纪元基线(physical 毫秒, ⑥ 满位源治理): 2020-01-01T00:00:00Z。
     * TSO 的 physical 部分为 46bit 毫秒时间戳, 减掉基线后数值域平移至 2^53 内;
     * 基线为固定常量 → 重放恒定/单源单调不受影响。早于基线的 TSO fail-fast。
     */
    static final long TSO_EPOCH_MS = 1577836800000L;

    /** mongo:ts_ms:ord → ts_ms*10^6 + ord
     *  (⑥ 满位源治理 TODO: ts_ms/ord 取值需实跑确认 —— 见 {@code PositionMetadataConverter} mongo 分支;
     *   ts_ms 缺失时键空哨兵 → 严格位点 fail-fast / 兼容模式回退计数器) */
    static long mongo(String body) {
        String[] p = body.split(":", -1);
        if (p.length < 1) {
            throw new IllegalStateException("mongo 位点格式异常: " + body);
        }
        long tsMs = parseLong(p[0], "mongo.ts_ms");
        long ord = p.length > 1 && !p[1].isEmpty() ? parseLong(p[1], "mongo.ord") : 0L;
        SourcePositionSpec.checkLimits("mongo", new long[]{tsMs, ord});
        return requirePositive(tsMs * 1_000_000L + ord, "mongo");
    }

    /** tidb:ts → TSO 裁剪平移(⑥ 满位源治理): (physical - TSO_EPOCH_MS)&lt;&lt;18 | logical */
    static long tidb(String body) {
        long tso = parseLong(body.trim(), "tidb.ts");
        long physical = tso >>> 18;
        long logical = tso & ((1L << 18) - 1);
        SourcePositionSpec.checkLimits("tidb", new long[]{physical, logical});
        if (physical < TSO_EPOCH_MS) {
            throw new IllegalStateException("tidb TSO physical=" + physical + " 早于裁剪纪元 "
                    + TSO_EPOCH_MS + "(⑥ 满位源治理, fail-fast; 纪元基线需按实测 TSO 取值校正)");
        }
        return requirePositive(((physical - TSO_EPOCH_MS) << 18) | logical, "tidb");
    }

    private static long parseLong(String s, String what) {
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException(what + " 位点非数值: " + s + "(不变式 5 fail-fast)");
        }
    }

    /** 编码结果必须为正: 符号位被占即位宽溢出(守卫: 位宽溢出, fail-fast) */
    private static long requirePositive(long seq, String sourceType) {
        if (seq <= 0) {
            throw new IllegalStateException(sourceType + " 位点编码溢出(seq=" + seq
                    + "), 分量超出位宽上限(守卫: 位宽溢出, fail-fast)");
        }
        return seq;
    }
}
