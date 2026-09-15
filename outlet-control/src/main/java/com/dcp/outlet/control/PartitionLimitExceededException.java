package com.dcp.outlet.control;

/** 并发度超过分区总数 —— 对应平台错误码 {@code PARTITION_LIMIT_EXCEEDED}。 */
public class PartitionLimitExceededException extends RuntimeException {

    public PartitionLimitExceededException(int instances, int totalPartitions) {
        super("instances 必须在 1.." + totalPartitions + "(当前 " + instances
                + ");要更高并发请给表加分区或拆分任务");
    }
}
