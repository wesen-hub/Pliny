package com.dcp.outlet.control;

/**
 * 起点回扫超出上限 —— 对应平台错误码 {@code START_TOO_OLD}。
 *
 * <p>处理方式:**回到批阶段重做**(换新 T0),而不是继续深扫。
 * 深扫会把"批流交接"变成一个不可控的长任务。
 */
public class StartTooOldException extends RuntimeException {

    private final String topic;
    private final int partition;
    private final long tso;
    private final int scannedRows;

    public StartTooOldException(String topic, int partition, long tso, int scannedRows) {
        super("起点回扫超限:" + topic + "-" + partition + " tso=" + tso
                + " 已扫 " + scannedRows + " 条(超过上限),请重做批(换新 T0)");
        this.topic = topic;
        this.partition = partition;
        this.tso = tso;
        this.scannedRows = scannedRows;
    }

    public String topic() { return topic; }
    public int partition() { return partition; }
    public long tso() { return tso; }
    public int scannedRows() { return scannedRows; }
}
