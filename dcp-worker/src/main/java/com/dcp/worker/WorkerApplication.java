package com.dcp.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** dcp-worker 纯宿主:统一调度框架的执行体服务(方案 N3, 一镜像一骨架多 ns)。 */
@SpringBootApplication(scanBasePackages = {"com.dcp.worker", "com.dcp.scheduler.rest"})
public class WorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }
}
