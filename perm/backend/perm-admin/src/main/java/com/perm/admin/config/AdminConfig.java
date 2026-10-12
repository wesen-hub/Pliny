package com.perm.admin.config;

import com.perm.common.compile.PolicyCompiler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 管理面 Bean 装配（perm-common 保持无 Spring 依赖，由各服务自行装配其组件）
 */
@Configuration
public class AdminConfig {

    @Bean
    public PolicyCompiler policyCompiler() {
        return new PolicyCompiler();
    }
}
