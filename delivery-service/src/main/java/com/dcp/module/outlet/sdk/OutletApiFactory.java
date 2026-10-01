package com.dcp.module.outlet.sdk;

/** 按消费方取 {@link OutletApi} 的工厂(进程内/HTTP 两种实现都适用)。 */
@FunctionalInterface
public interface OutletApiFactory {

    OutletApi forConsumer(String consumerId);
}
