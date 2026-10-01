package com.dcp.outlet.rt;

import java.util.Map;
import java.util.Set;

/**
 * 契约视图(裁剪依据)。生产实现由现有 `Contract` + `OutletPermissionEngine` 适配而来 ——
 * **只能收窄,不能放大**:实际授权 = 契约 ∩ 主体权限。
 */
public interface ContractView {

    /** 该流允许出现的列;{@code null} 表示不限(仅测试用)。 */
    Set<String> columnWhitelist(String stream);

    /** 密级上限;超过则按 {@link #maskRules()} 脱敏。 */
    int maxLevel();

    Map<String, String> maskRules();

    /** 行条件(与主体数据范围 AND);为空表示不限制。 */
    String rowCondition();
}
