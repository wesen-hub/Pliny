package com.dcp.outlet.control;

/** 最新 TSO 提供者(来自水位表):用于 {@code startPolicy=latest} 的"只从最新起"。 */
public interface LatestTsoProvider {

    long latestTso(String topic, int partition, int epoch);
}
