package com.dcp.module.outlet;

/**
 * 变更流出口 · 错误码(设计 §6.9 / §6.4.2)
 *
 * <p>与 {@code ApiResponse.code} 共用数值空间:出口模块占用 6xxx 段。
 * 抛出方式:{@code throw new BizException(OutletErrorCode.POSITION_EXPIRED, "...")}。
 *
 * <p>设计约束(§6.4.2):起点/边界不合法一律**明确报错**,绝不静默从当前水位开始。
 */
public final class OutletErrorCode {

    private OutletErrorCode() {}

    /** 流不存在 / 该消费方契约未覆盖此流 */
    public static final int STREAM_NOT_FOUND = 6001;
    /** 契约不存在 / 未生效 / 已吊销 */
    public static final int CONTRACT_FORBIDDEN = 6002;
    /** T0 < changefeed.start_ts(该时点不在流的覆盖范围内)→ 必须重取边界 */
    public static final int BOUNDARY_BELOW_STREAM_START = 6003;
    /** T0 早于 GC safe point(历史版本已被回收)→ 必须重快照 */
    public static final int BOUNDARY_EXPIRED = 6004;
    /** T0 > latest(把未发生的时点当边界) */
    public static final int BOUNDARY_IN_FUTURE = 6005;
    /** 位点早于 earliest(保留窗口外)→ 消费方需重快照 */
    public static final int POSITION_EXPIRED = 6006;
    /** 位点 epoch 与当前流代次不一致(流被重建/topic 重建)→ 强制重快照 */
    public static final int EPOCH_MISMATCH = 6007;
    /** 超出配额(QPS / 日行数 / 并发)→ 429 语义 */
    public static final int QUOTA_EXCEEDED = 6008;
    /** 权限判定失败(列/行/密级均不可见,或未授权读该流) */
    public static final int FORBIDDEN = 6009;
    /** 出口内部错误(投影/规范化异常) */
    public static final int OUTLET_INTERNAL = 6099;
    /** 快照任务不存在(续拉时未提供 boundary 或未登记) */
    public static final int SNAPSHOT_JOB_NOT_FOUND = 6010;
    /** 快照任务已过期(超过 TTL / GC 窗口)→ 必须用新 T0 重来 */
    public static final int SNAPSHOT_JOB_EXPIRED = 6011;

    public static String message(int code) {
        return switch (code) {
            case STREAM_NOT_FOUND -> "流不存在或契约未覆盖该流";
            case CONTRACT_FORBIDDEN -> "契约无效、已过期或已吊销";
            case BOUNDARY_BELOW_STREAM_START -> "边界早于流的起点(不在覆盖范围内)";
            case BOUNDARY_EXPIRED -> "边界早于 GC safe point,历史版本已回收";
            case BOUNDARY_IN_FUTURE -> "边界晚于当前最新位点";
            case POSITION_EXPIRED -> "位点已超出保留窗口,请重做快照";
            case EPOCH_MISMATCH -> "位点代次(epoch)与当前流不一致,请重做快照";
            case QUOTA_EXCEEDED -> "超出契约配额";
            case FORBIDDEN -> "无权读取该数据";
            case SNAPSHOT_JOB_NOT_FOUND -> "快照任务不存在:续拉需带上 boundary(或先发起首页)";
            case SNAPSHOT_JOB_EXPIRED -> "快照任务已过期(超过 TTL),请用新的 T0 重新发起";
            default -> "出口内部错误";
        };
    }
}
