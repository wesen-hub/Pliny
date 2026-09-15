package com.dcp.outlet.control;

/** 同分区同代已有活跃租约 —— 对应平台错误码 {@code LEASE_CONFLICT}。 */
public class LeaseConflictException extends RuntimeException {

    public LeaseConflictException(String topic, int partition, int epoch, String holder) {
        super("LEASE_CONFLICT:" + topic + "-" + partition + " epoch=" + epoch
                + " 已被 " + holder + " 持有;请先停旧单元");
    }
}
