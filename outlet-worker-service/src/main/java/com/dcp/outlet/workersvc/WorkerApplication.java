package com.dcp.outlet.workersvc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 数据消费平台 · 计算面独立服务(dcp-outlet-worker)。
 *
 * <p>承载 1..N 个消费单元(可跨任务混布);与控制面通过 HTTP 交互:
 * 接收 {@code CreateUnit/StopUnit},上报心跳与续约。
 */
@SpringBootApplication
public class WorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }
}
