package com.perm.admin.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * JDBC 数据源手动装配（perm.storage=jdbc 时生效）
 * 默认配置已排除 DataSourceAutoConfiguration（memory 模式零外部依赖），
 * jdbc profile 下此处显式创建 HikariCP 连接池 + JdbcTemplate + 事务管理器。
 * 连接参数读 application-jdbc.yml 的 spring.datasource.*，环境变量可覆盖（PERM_MYSQL_*）。
 */
@Configuration
@ConditionalOnProperty(name = "perm.storage", havingValue = "jdbc")
public class JdbcDataSourceConfig {

    @Bean
    public DataSource dataSource(
            @Value("${spring.datasource.url}") String url,
            @Value("${spring.datasource.username}") String username,
            @Value("${spring.datasource.password}") String password,
            @Value("${spring.datasource.driver-class-name}") String driverClassName) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName(driverClassName);
        config.setPoolName("perm-admin-pool");
        config.setMaximumPoolSize(5);
        return new HikariDataSource(config);
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /** @Transactional 支持（JdbcPolicyStore.put 发布原子性） */
    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}
