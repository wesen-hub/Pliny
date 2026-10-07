package com.dcp.delivery;

import com.dcp.outlet.adapters.JdbcContractViewSource;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.sql.DataSource;

/**
 * 出口交付域独立服务(方案 N10)。
 *
 * <p>承载 backend/module/outlet(变更流出口 L3 六接口 + 批快照 + 消费方 SDK 运行时)
 * 与 outlet-service 的契约视图下发(/api/outlet/contracts);
 * 变更流数据面(/api/chg/**)为机器消费方 PULL, 鉴权走 consumerId+契约自校验(与 backend 同语义),
 * 无需用户 JWT 过滤链。
 */
@SpringBootApplication(scanBasePackages = "com.dcp")
@MapperScan("com.dcp.module.outlet.mapper")  // 收窄到 outlet mapper 包, 避免 com.dcp 下其他接口(如 BatchExecutor SPI)被误注册为 mapper
@EnableDiscoveryClient
@EnableScheduling
public class DeliveryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeliveryServiceApplication.class, args);
    }

    /**
     * 契约视图源(计算面拉取): 主体侧列级收窄在此接入(生产实现复用出口权限引擎,
     * 当前骨架形态为 permissive —— 契约白名单即最终授权)。
     */
    @Bean
    public JdbcContractViewSource contractViewSource(DataSource dataSource) {
        return new JdbcContractViewSource(dataSource, JdbcContractViewSource.SubjectColumnPolicy.permissive());
    }
}
