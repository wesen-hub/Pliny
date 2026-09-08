package com.dcp.security;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.dcp.module.system.entity.SysRolePermission;
import com.dcp.module.system.mapper.SysRoleMapper;
import com.dcp.module.system.mapper.SysRolePermissionMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 方法级权限判断: @PreAuthorize("@perm.canWrite()") 兼容旧注解;
 * RBAC(P1): @PreAuthorize("@perm.has('code')") —— 当前用户角色 → 权限点(sys_role_permission), ADMIN=全部。
 * 缓存: 角色权限变更后调 refresh()。
 */
@Component("perm")
@RequiredArgsConstructor
public class PermissionService {

    private final SysRoleMapper roleMapper;
    private final SysRolePermissionMapper rolePermMapper;

    private static final Map<String, Set<String>> ROLE_PERMS = new ConcurrentHashMap<>();
    private static volatile boolean loaded = false;

    @PostConstruct
    public void init() {
        refresh();
    }

    /** 从 DB 重载 角色→权限点 缓存(角色授权/权限变更后调用) */
    public synchronized void refresh() {
        ROLE_PERMS.clear();
        var roles = roleMapper.selectList(null);
        for (var r : roles) {
            var rows = rolePermMapper.selectList(new LambdaQueryWrapper<SysRolePermission>()
                    .eq(SysRolePermission::getRoleId, r.getId()));
            Set<String> codes = new HashSet<>();
            rows.forEach(x -> codes.add(x.getPermCode()));
            ROLE_PERMS.put(r.getRoleCode(), codes);
        }
        loaded = true;
    }

    private static void ensureLoaded() {
        if (!loaded) {
            // 正常情况下 init() 已加载; 极端时序下由实例触发(静态无法注入, 通过线程内已有缓存容忍)
        }
    }

    /** 可写操作: 排除 VIEWER / AUDITOR(兼容旧注解 canWrite) */
    public boolean canWrite() {
        return !SecurityUtils.hasRole("VIEWER") && !SecurityUtils.hasRole("AUDITOR");
    }

    public boolean isAdmin() {
        return SecurityUtils.hasRole("ADMIN");
    }

    private void lazyLoad() {
        if (!loaded || ROLE_PERMS.isEmpty()) {
            synchronized (ROLE_PERMS) {
                if (!loaded || ROLE_PERMS.isEmpty()) refresh();
            }
        }
    }

    /** 当前登录用户是否拥有权限点; ADMIN 恒真 */
    public boolean has(String code) {
        lazyLoad();
        List<String> roles = SecurityUtils.roles();
        if (roles == null || roles.isEmpty()) return false;
        if (roles.contains("ADMIN")) return true;
        for (String role : roles) {
            Set<String> codes = ROLE_PERMS.get(role);
            if (codes != null && codes.contains(code)) return true;
        }
        return false;
    }

    /** 当前用户权限点全集(null=ADMIN 全量; 前端下发) */
    public Set<String> permsOfCurrent() {
        lazyLoad();
        List<String> roles = SecurityUtils.roles();
        if (roles == null || roles.isEmpty()) return new HashSet<>();
        if (roles.contains("ADMIN")) return null;
        Set<String> out = new HashSet<>();
        for (String role : roles) {
            Set<String> codes = ROLE_PERMS.get(role);
            if (codes != null) out.addAll(codes);
        }
        return out;
    }

    /** 角色拥有的权限码(管理页读取) */
    public Set<String> permsOfRole(Long roleId) {
        var rows = rolePermMapper.selectList(new LambdaQueryWrapper<SysRolePermission>()
                .eq(SysRolePermission::getRoleId, roleId));
        Set<String> out = new HashSet<>();
        rows.forEach(x -> out.add(x.getPermCode()));
        return out;
    }
}
