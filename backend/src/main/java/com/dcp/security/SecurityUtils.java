package com.dcp.security;

import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/** 当前登录用户工具 */
public final class SecurityUtils {

    private SecurityUtils() {
    }

    public static LoginUser currentUser() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof LoginUser lu) {
            return lu;
        }
        throw new BizException(ErrorCode.UNAUTHORIZED);
    }

    public static Long currentUserId() {
        return currentUser().getUserId();
    }

    public static String currentUsername() {
        return currentUser().getUsername();
    }

    public static boolean hasRole(String role) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof LoginUser lu) {
            return lu.getRoles().contains(role);
        }
        return false;
    }
}
