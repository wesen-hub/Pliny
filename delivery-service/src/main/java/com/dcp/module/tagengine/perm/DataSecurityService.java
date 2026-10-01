package com.dcp.module.tagengine.perm;

/**
 * 主体侧数据权限判定(L0-L3)的精简接口 —— delivery-service 无用户认证上下文,
 * 契约 ∩ 主体的收窄由契约侧(OutletPermissionEngine)完成, 主体侧恒按 FULL 放行;
 * 本类仅满足 OutletPermissionEngine 的编译依赖, **不注册 bean**(骨架退化语义: 不存在 = 仅契约)。
 */
public class DataSecurityService {

    public static final int HIDDEN = 0, META = 1, MASKED = 2, FULL = 3;

    public int columnLevel(String dataset, String column) {
        return FULL;
    }

    public int tableLevel(String dataset) {
        return FULL;
    }
}
