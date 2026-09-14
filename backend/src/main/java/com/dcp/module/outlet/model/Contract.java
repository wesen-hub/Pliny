package com.dcp.module.outlet.model;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;

/**
 * 契约(授权实例,设计 §7.2)。表结构见 DDL 里的 contract 表。
 *
 * <p>铁律:契约**只能收窄**不能放大 —— 实际授权 = 契约 ∩ 主体自身权限(见 OutletPermissionEngine)。
 * 判定发生在**每次读**的时候,因此收窄/吊销即时生效。
 */
@Data
public class Contract {

    public static final String STATUS_ACTIVE = "active";

    private String contractId;
    private String consumerId;
    /** 覆盖的流(逻辑表) */
    private Set<String> streams;
    /** 列白名单:{流: 列集合};不在白名单 → 连 schema 都不返回 */
    private Map<String, Set<String>> columnWhitelist;
    /** 行条件(与主体数据范围做 AND) */
    private String rowCondition;
    /** 密级上限(超限剔除或按 maskRules 脱敏) */
    private int maxLevel;
    /** 脱敏规则:列 → 掩码方式(快照段与增量段必须同一套) */
    private Map<String, String> maskRules;
    private String channel = "api";
    /** snapshot | latest | position */
    private String startPolicy = "snapshot";
    private String status = STATUS_ACTIVE;
    private LocalDateTime expireAt;

    public boolean active() {
        boolean notExpired = expireAt == null || expireAt.isAfter(LocalDateTime.now());
        return STATUS_ACTIVE.equals(status) && notExpired;
    }

    public boolean covers(String streamId) {
        return streams != null && streams.contains(streamId);
    }
}
