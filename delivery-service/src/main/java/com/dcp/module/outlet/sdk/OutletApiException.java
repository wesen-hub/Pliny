package com.dcp.module.outlet.sdk;

import com.dcp.module.outlet.OutletErrorCode;

/**
 * 出口调用失败(带业务码)。SDK 依据码决定"重试 / 重快照 / 直接失败"(设计 §6.8 失效处置)。
 */
public class OutletApiException extends RuntimeException {

    private final int code;

    public OutletApiException(int code, String message) {
        super("[" + code + "] " + message);
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    /** 位点超出保留窗口 → 必须重做快照(不可重试) */
    public boolean isPositionExpired() {
        return code == OutletErrorCode.POSITION_EXPIRED
                || code == OutletErrorCode.SNAPSHOT_JOB_EXPIRED
                || code == OutletErrorCode.BOUNDARY_EXPIRED;
    }

    /** 流代次变化 → 老位点不可信,必须重做快照 */
    public boolean isEpochMismatch() {
        return code == OutletErrorCode.EPOCH_MISMATCH;
    }

    /** 重快照即可恢复的一类 */
    public boolean needsResnapshot() {
        return isPositionExpired() || isEpochMismatch()
                || code == OutletErrorCode.SNAPSHOT_JOB_NOT_FOUND
                || code == OutletErrorCode.BOUNDARY_BELOW_STREAM_START
                || code == OutletErrorCode.BOUNDARY_IN_FUTURE;
    }

    /** 配额超限 → 退避后可重试 */
    public boolean isQuotaExceeded() {
        return code == OutletErrorCode.QUOTA_EXCEEDED;
    }

    /** 权限失败 → 重试无意义,直接失败并要求人工修契约 */
    public boolean isForbidden() {
        return code == OutletErrorCode.FORBIDDEN || code == OutletErrorCode.CONTRACT_FORBIDDEN;
    }
}
