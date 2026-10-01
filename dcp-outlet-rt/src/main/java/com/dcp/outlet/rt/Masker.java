package com.dcp.outlet.rt;

/** 脱敏执行器(契约 strip 阶段的最后一步)。 */
public interface Masker {

    Object mask(String column, Object value, String rule, int maxLevel);
}
