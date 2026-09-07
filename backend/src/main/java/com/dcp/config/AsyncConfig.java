package com.dcp.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/** 任务执行线程池（并发上限 20，对齐设计文档 §11.3） */
@Configuration
public class AsyncConfig {

    @Bean("engineExecutor")
    public ThreadPoolTaskExecutor engineExecutor(@Value("${dcp.engine.max-concurrency:20}") int maxConcurrency) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(maxConcurrency);
        executor.setMaxPoolSize(maxConcurrency);
        executor.setQueueCapacity(200);
        executor.setThreadNamePrefix("engine-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }
}
