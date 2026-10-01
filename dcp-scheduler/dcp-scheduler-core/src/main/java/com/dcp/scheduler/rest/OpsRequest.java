package com.dcp.scheduler.rest;

import com.dcp.scheduler.model.ScaleRules;

/**
 * 运维指令提交请求(调度运维后台 §2):ns + 指令类型 + 目标参数。
 * 校验规则:分区类(PIN/UNPIN/MIGRATE_PIN)需 taskId + partId;
 * 实例类(DRAIN/UNDRAIN/PAUSE/UNPAUSE/SET_WEIGHT)需 target;SET_WEIGHT 需 weight;
 * 规则类(SET_RULES)需 rules。
 */
public class OpsRequest {

    public String ns;
    public String type;
    public String taskId;
    public Integer partId;
    public String target;
    public Double weight;
    public Long ttlSec;      // PIN 锁时效(秒);null = 永久锁
    public ScaleRules rules; // SET_RULES 目标默认规则
    public String reason;
}
