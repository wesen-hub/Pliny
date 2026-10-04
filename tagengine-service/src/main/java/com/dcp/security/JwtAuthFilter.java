package com.dcp.security;

import io.jsonwebtoken.Claims;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import com.dcp.module.system.entity.SysRole;
import com.dcp.module.system.mapper.SysUserMapper;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;

// 低危-2: 不再标注 @Component —— 否则 Boot 会把它自动注册进过滤器链, 与 SecurityConfig 的
// addFilterBefore 显式注册重复(同一请求会被过滤两次); 现在由 SecurityConfig 注入并注册。
public class JwtAuthFilter extends OncePerRequestFilter {
    private final JwtUtil jwtUtil;
    private final SysUserMapper userMapper;

    public JwtAuthFilter(JwtUtil jwtUtil, SysUserMapper userMapper) {
        this.jwtUtil = jwtUtil;
        this.userMapper = userMapper;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        String auth = request.getHeader("Authorization");
        if (auth != null && auth.startsWith("Bearer ")) {
            Claims claims = jwtUtil.parse(auth.substring(7));
            if (claims != null) {
                Long userId = Long.valueOf(claims.getSubject());
                String username = claims.get("username", String.class);
                // H3: 角色不再信任 token claim, 每请求从 DB 回查(用户已删/角色已回收 → 不设认证)
                List<String> roles = userMapper.selectRolesByUserId(userId)
                        .stream().map(SysRole::getRoleCode).toList();
                if (!roles.isEmpty()) {
                    LoginUser user = new LoginUser(userId, username, "", roles);
                    UsernamePasswordAuthenticationToken token =
                            new UsernamePasswordAuthenticationToken(user, null, user.getAuthorities());
                    token.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));
                    SecurityContextHolder.getContext().setAuthentication(token);
                }
            }
        }
        chain.doFilter(request, response);
    }
}
