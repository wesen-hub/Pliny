package com.dcp.outlet.control;

/** 告警出口(生产实现 → 平台告警通道 / Prometheus Alertmanager)。 */
public interface AlertSink {

    void fire(Alert alert);
}
