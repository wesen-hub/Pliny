package com.dcp.security;

import org.springframework.stereotype.Component;

/** 方法级权限判断 @PreAuthorize("@perm.canWrite()") */
@Component("perm")
public class PermissionService {

    /** 可写操作: 排除 VIEWER / AUDITOR */
    public boolean canWrite() {
        return !SecurityUtils.hasRole("VIEWER") && !SecurityUtils.hasRole("AUDITOR");
    }

    public boolean isAdmin() {
        return SecurityUtils.hasRole("ADMIN");
    }
}
