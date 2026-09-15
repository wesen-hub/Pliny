package com.dcp.outlet.integration;

import com.dcp.outlet.worker.Heartbeat;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 心跳落地(内存版):记录每单元最近上报的 offset / tso —— 平台据此判 RUNNING 与停滞,
 * 并支撑"进度(offset → 反查 TSO → 滞后)"查询。生产实现 = 上报控制面 + Prometheus。
 */
public class LeaseBackedHeartbeat implements Heartbeat {

    /** 单元 → 最近上报。 */
    public static final class Report {
        public volatile long offset;
        public volatile long tso;
        public volatile long at;
        public volatile int count;
    }

    private final Map<String, Report> reports = new ConcurrentHashMap<>();

    @Override
    public void send(String unitId, int partition, long offset, long tso) {
        Report r = reports.computeIfAbsent(unitId, k -> new Report());
        r.offset = offset; r.tso = tso; r.at = System.currentTimeMillis(); r.count++;
    }

    public Report of(String unitId) { return reports.get(unitId); }

    public int size() { return reports.size(); }
}
