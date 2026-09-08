package com.dcp.module.auth;

import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.module.system.entity.SysRole;
import com.dcp.module.system.entity.SysUser;
import com.dcp.module.system.mapper.SysUserMapper;
import com.dcp.module.system.mapper.SysUserRoleMapper;
import com.dcp.module.system.entity.SysUserRole;
import com.dcp.module.system.service.AuditService;
import com.dcp.security.JwtUtil;
import com.dcp.security.LoginUser;
import com.dcp.security.SecurityUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
public class AuthService {

    private final SysUserMapper userMapper;
    private final SysUserRoleMapper userRoleMapper;
    private final PasswordEncoder passwordEncoder;
    private final JwtUtil jwtUtil;
    private final AuditService auditService;

    /** 登录失败锁定: username -> [失败次数, 首次失败时间] */
    private final Map<String, long[]> failCache = new ConcurrentHashMap<>();
    private static final int MAX_FAIL = 5;
    private static final long LOCK_MINUTES = 15;

    public Map<String, Object> login(String username, String password) {
        long[] fails = failCache.get(username);
        if (fails != null && fails[0] >= MAX_FAIL) {
            long remain = LOCK_MINUTES * 60_000 - (System.currentTimeMillis() - fails[1]);
            if (remain > 0) {
                throw new BizException(ErrorCode.PARAM_ERROR, "登录失败次数过多, 请 " + (remain / 1000 / 60 + 1) + " 分钟后重试");
            }
            failCache.remove(username);
        }
        SysUser user = userMapper.selectOne(new com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper<SysUser>()
                .eq(SysUser::getUsername, username));
        if (user == null || !passwordEncoder.matches(password, user.getPasswordHash()) || user.getStatus() != 1) {
            long[] f = failCache.computeIfAbsent(username, k -> new long[]{0, System.currentTimeMillis()});
            f[0]++;
            throw new BizException(ErrorCode.PARAM_ERROR, "用户名或密码错误(剩余 " + (MAX_FAIL - f[0]) + " 次机会)");
        }
        failCache.remove(username);
        List<String> roles = userMapper.selectRolesByUserId(user.getId())
                .stream().map(SysRole::getRoleCode).toList();
        String token = jwtUtil.generate(user.getId(), user.getUsername(), roles);
        auditService.record("system", "LOGIN", "user", user.getId(), "用户登录: " + username);
        return Map.of(
                "token", token,
                "user", Map.of("id", user.getId(), "username", user.getUsername(),
                        "nickname", user.getNickname(), "roles", roles));
    }

    public Map<String, Object> me() {
        LoginUser lu = SecurityUtils.currentUser();
        SysUser user = userMapper.selectById(lu.getUserId());
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("id", user.getId());
        out.put("username", user.getUsername());
        out.put("nickname", user.getNickname());
        out.put("roles", lu.getRoles());
        // P1 RBAC: 权限点全集(ADMIN=null → 前端按全量处理)
        java.util.Set<String> perms = permService.permsOfCurrent();
        out.put("perms", perms);
        return out;
    }

    @org.springframework.beans.factory.annotation.Autowired
    private com.dcp.security.PermissionService permService;
}
