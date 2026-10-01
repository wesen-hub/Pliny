package com.dcp.outlet.rt;

import com.dcp.outlet.rt.Masker;

/**
 * 脱敏执行器:与出口权限引擎的 `applyMask` **同源同语义**
 * (`MASK_PHONE` / `MASK_FULL` / 其它 → 保留首尾 2 位)。
 *
 * <p>计算面独立部署时无法调用平台进程内的引擎,因此规则名由「契约视图」下发、实现在此;
 * **规则集扩展时两处需同步**(已在进度文档登记该约束)。
 */
public class PlatformMasker implements Masker {

    @Override
    public Object mask(String column, Object value, String rule, int maxLevel) {
        if (value == null) return null;
        String s = String.valueOf(value);
        if (rule == null) return value;
        return switch (rule) {
            case "MASK_PHONE" -> s.length() >= 7 ? s.substring(0, 3) + "****" + s.substring(s.length() - 2) : "***";
            case "MASK_FULL" -> "***";
            default -> s.length() > 4 ? s.substring(0, 2) + "****" + s.substring(s.length() - 2) : "***";
        };
    }
}
