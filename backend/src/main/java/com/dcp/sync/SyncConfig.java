package com.dcp.sync;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.annotation.EnableKafka;

import javax.sql.DataSource;

/**
 * V3 同步链路配置(P1):
 * - @EnableKafka: 启用 @KafkaListener
 * - 目标分表 DataSource: 独立 bean(targetDataSource), 供 AtomicUpsertDao 与资产预览(AssetService)复用;
 *   注意 targetDataSource 不要命名为 dataSource, 避免与元库主数据源形成多候选
 *   (P0 踩过的坑: 元库表被建到了 target_db)。
 */
@Configuration
@EnableKafka
public class SyncConfig {

    @Bean
    public AtomicUpsertDao atomicUpsertDao(
            @Value("${dcp.sync.target.url}") String url,
            @Value("${dcp.sync.target.username:root}") String username,
            @Value("${dcp.sync.target.password:root123}") String password) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
        ds.setUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        return new AtomicUpsertDao(ds);
    }
}
