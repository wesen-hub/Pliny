package com.perm.demo;

import com.perm.pep.PermClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * PEP 业务接入演示（端口 18083）
 * 业务侧只需：装配 PermClient（SDK）→ 请求时构造 PepContext → authorize → rewrite。
 * 不依赖 PDP 内部实现；公钥由部署环境注入（作业镜像可内嵌公钥，R-Sec6）。
 */
@SpringBootApplication
public class PepDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(PepDemoApplication.class, args);
    }

    @Bean
    public PermClient permClient(
            @Value("${perm.pdp.url:http://localhost:8081}") String pdpUrl,
            @Value("${perm.security.ed25519-public-key:MCowBQYDK2VwAyEAw3xYYpGY/C4gbWd/bV6jZebqAPqGsD/993Y6kBPI/ag=}") String publicKey) {
        return PermClient.builder()
                .pdpBaseUrl(pdpUrl)
                .publicKeyBase64(publicKey)
                .build();
    }
}
