package com.dcp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DataCollectPlatformApplication {
    public static void main(String[] args) {
        SpringApplication.run(DataCollectPlatformApplication.class, args);
    }
}
