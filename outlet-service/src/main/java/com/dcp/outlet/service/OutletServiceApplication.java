package com.dcp.outlet.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 数据消费平台 · 控制面独立服务(dcp-outlet-control)。
 *
 * <p>与平台主后端(dcp-backend)解耦:独立进程、独立端口、独立配置;
 * 复用内核(outlet-control)、适配层(outlet-adapters)与 SDK;计算面为 dcp-outlet-worker。
 */
@SpringBootApplication
@EnableScheduling
public class OutletServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OutletServiceApplication.class, args);
    }
}
