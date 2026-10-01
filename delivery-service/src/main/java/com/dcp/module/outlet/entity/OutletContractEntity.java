package com.dcp.module.outlet.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * 契约(表:contract)。JSON 列(streams / column_whitelist / mask_rules)在注册表里解析。
 */
@Data
@TableName("contract")
public class OutletContractEntity {

    @TableId
    private String contractId;
    private String consumerId;
    /** JSON 数组,如 ["t_order"] */
    private String streams;
    /** JSON 对象,如 {"t_order":["id","name"]} */
    private String columnWhitelist;
    private String rowCondition;
    private Integer maxLevel;
    /** JSON 对象,如 {"phone":"MASK_PHONE"} */
    private String maskRules;
    private String channel;
    private String startPolicy;
    private String tokenHash;
    private LocalDateTime tokenExpireAt;
    private String status;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
