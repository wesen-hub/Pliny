package com.dcp.module.auth;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.dcp.common.ApiResponse;
import lombok.Data;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private final AuthService service;

    public AuthController(AuthService service) {
        this.service = service;
    }

    @Data
    public static class LoginRequest {
        private String username;
        private String password;
    }

    @SentinelResource(value = "api-auth-login", blockHandler = "loginBlocked")
    @PostMapping("/login")
    public ApiResponse<Map<String, Object>> login(@RequestBody LoginRequest req) {
        return ApiResponse.ok(service.login(req.getUsername(), req.getPassword()));
    }

    /** M4: 登录限流兜底(与网关限流双保险), 超限返回 429 */
    public ApiResponse<Map<String, Object>> loginBlocked(LoginRequest req, BlockException ex) {
        return ApiResponse.error(429, "登录尝试过于频繁, 请稍后重试");
    }

    @GetMapping("/me")
    public ApiResponse<Map<String, Object>> me() {
        return ApiResponse.ok(service.me());
    }
}
