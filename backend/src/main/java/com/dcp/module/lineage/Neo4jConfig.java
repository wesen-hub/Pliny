package com.dcp.module.lineage;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * 血缘图存储 Neo4j(2026-09-09 方案 §5): 懒连接, Neo4j 不可用不影响平台主链路(血缘 API 降级 503)。
 */
@Configuration
public class Neo4jConfig {

    private static final Logger log = LoggerFactory.getLogger(Neo4jConfig.class);

    @Bean(destroyMethod = "close")
    public Driver neo4jDriver(@Value("${dcp.lineage.neo4j.uri:bolt://neo4j:7687}") String uri,
                              @Value("${dcp.lineage.neo4j.user:neo4j}") String user,
                              @Value("${dcp.lineage.neo4j.password:neo4j123}") String password) {
        try {
            Config cfg = Config.builder()
                    .withMaxConnectionPoolSize(8)
                    .withConnectionTimeout(3, java.util.concurrent.TimeUnit.SECONDS)
                    .build();
            return GraphDatabase.driver(uri, AuthTokens.basic(user, password), cfg);
        } catch (Exception e) {
            log.warn("Neo4j driver 初始化失败(血缘功能不可用): {}", e.getMessage());
            return null;
        }
    }
}
