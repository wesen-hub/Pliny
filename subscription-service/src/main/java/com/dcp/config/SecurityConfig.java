package com.dcp.config;

import com.dcp.security.JwtAuthFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.util.List;

/**
 * subscription-service 安全配置(backend SecurityConfig 精简版)。
 *
 * <p>白名单: actuator + 订阅方 PULL 数据服务 API(schema/snapshot/changes/export/export-task,
 * Controller 内 X-Dcp-Token 自校验); 管理面一律 JWT + @perm.isAdmin()。
 * 无 /api/auth/login(登录仍在 backend, 这里只验证不签发)。
 */
@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http, JwtAuthFilter jwtAuthFilter) throws Exception {
        http.csrf(csrf -> csrf.disable())
                .cors(cors -> cors.configurationSource(corsConfigurationSource()))
                .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .exceptionHandling(ex -> ex
                        // token 缺失/无效/过期: 401（前端据此跳登录页）
                        .authenticationEntryPoint((req, resp, e) -> {
                            resp.setStatus(401);
                            resp.setContentType("application/json;charset=UTF-8");
                            resp.getWriter().write("{\"code\":401,\"message\":\"登录已过期, 请重新登录\"}");
                        })
                        // 已认证但权限不足: 403
                        .accessDeniedHandler((req, resp, e) -> {
                            resp.setStatus(403);
                            resp.setContentType("application/json;charset=UTF-8");
                            resp.getWriter().write("{\"code\":403,\"message\":\"无权限访问\"}");
                        }))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/api/actuator/**").permitAll()
                        // 订阅方 PULL 数据服务 API: X-Dcp-Token 在 Controller 自校验; 管理面仍走 JWT+isAdmin
                        .requestMatchers(HttpMethod.GET, "/api/data-sub/*/schema").permitAll()
                        .requestMatchers(HttpMethod.GET, "/api/data-sub/*/snapshot").permitAll()
                        .requestMatchers(HttpMethod.GET, "/api/data-sub/*/changes").permitAll()
                        .requestMatchers(HttpMethod.POST, "/api/data-sub/*/export").permitAll()
                        .requestMatchers(HttpMethod.GET, "/api/data-sub/export-task/*").permitAll()
                        .requestMatchers(HttpMethod.GET, "/api/data-sub/export-task/*/download").permitAll()
                        .requestMatchers(HttpMethod.OPTIONS, "/**").permitAll()
                        .anyRequest().authenticated())
                .addFilterBefore(jwtAuthFilter, UsernamePasswordAuthenticationFilter.class);
        return http.build();
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration cfg = new CorsConfiguration();
        cfg.setAllowedOriginPatterns(List.of("*"));
        cfg.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "OPTIONS"));
        cfg.setAllowedHeaders(List.of("*"));
        cfg.setAllowCredentials(true);
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", cfg);
        return source;
    }
}
