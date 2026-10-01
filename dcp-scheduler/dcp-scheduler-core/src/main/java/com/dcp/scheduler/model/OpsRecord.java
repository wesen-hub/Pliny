package com.dcp.scheduler.model;

/**
 * 运维指令(/dcp/{ns}/ops/{opId},子文档⑧ §六):后台写 PENDING,主消费执行,回写 DONE/FAILED。
 * 归属键唯一写者是主,后台不直接写 parts —— 钉/解/定向迁移全部走 ops 队列。
 */
public class OpsRecord {

    /** ops 类型(锁机制三类 + 实例运维五类;扩缩人工触发等后续扩展)。 */
    public static final class Type {
        public static final String PIN = "PIN";                 // 钉住当前 owner(可选 ttl)
        public static final String UNPIN = "UNPIN";             // 解除锁
        public static final String MIGRATE_PIN = "MIGRATE_PIN"; // 定向迁移到 target 并钉住
        public static final String DRAIN_INSTANCE = "DRAIN_INSTANCE";     // 实 例下架(不再接单,迁空后可关机)
        public static final String UNDRAIN_INSTANCE = "UNDRAIN_INSTANCE"; // 取 消下架(已迁出分区不回流)
        public static final String PAUSE_INSTANCE = "PAUSE_INSTANCE";     // 暂 停接单(已跑分区不动)
        public static final String UNPAUSE_INSTANCE = "UNPAUSE_INSTANCE"; // 恢 复接单
        public static final String SET_WEIGHT = "SET_WEIGHT";             // 调 权重(0.5~2.0,影响目标打分)
        public static final String SET_RULES = "SET_RULES";               // 改 ns 默认扩缩规则(含缩容总开关,主消费写 rules 键)
        private Type() {}
    }

    public static final class OpState {
        public static final String PENDING = "PENDING";
        public static final String DONE = "DONE";
        public static final String FAILED = "FAILED";
        private OpState() {}
    }

    public String opId;
    public String type;
    public String taskId;
    public Integer partId;
    public String target;           // MIGRATE_PIN 的目标实例 id;实例类 op 的 目标实例 id
    public Double weight;           // SET_WEIGHT 目标权重(0.5~2.0)
    public Long ttlSec;             // PIN 锁时效(秒);null = 永久锁
    public ScaleRules rules;         // SET_RULES 目标规则(ns 默认扩缩规则整包覆盖)
    public String operator;         // 操作人(审计)
    public String reason;           // 备注
    public String state = OpState.PENDING;
    public String detail;           // FAILED 时的原因

    public static OpsRecord pin(String opId, String taskId, int partId, Long ttlSec,
                                String operator, String reason) {
        OpsRecord o = new OpsRecord();
        o.opId = opId; o.type = Type.PIN; o.taskId = taskId; o.partId = partId;
        o.ttlSec = ttlSec; o.operator = operator; o.reason = reason;
        return o;
    }

    public static OpsRecord unpin(String opId, String taskId, int partId,
                                  String operator, String reason) {
        OpsRecord o = new OpsRecord();
        o.opId = opId; o.type = Type.UNPIN; o.taskId = taskId; o.partId = partId;
        o.operator = operator; o.reason = reason;
        return o;
    }

    public static OpsRecord migratePin(String opId, String taskId, int partId, String target,
                                       String operator, String reason) {
        OpsRecord o = new OpsRecord();
        o.opId = opId; o.type = Type.MIGRATE_PIN; o.taskId = taskId; o.partId = partId;
        o.target = target; o.operator = operator; o.reason = reason;
        return o;
    }

    public static OpsRecord drainInstance(String opId, String instId,
                                          String operator, String reason) {
        OpsRecord o = new OpsRecord();
        o.opId = opId; o.type = Type.DRAIN_INSTANCE; o.target = instId;
        o.operator = operator; o.reason = reason;
        return o;
    }

    public static OpsRecord undrainInstance(String opId, String instId,
                                            String operator, String reason) {
        OpsRecord o = new OpsRecord();
        o.opId = opId; o.type = Type.UNDRAIN_INSTANCE; o.target = instId;
        o.operator = operator; o.reason = reason;
        return o;
    }

    public static OpsRecord pauseInstance(String opId, String instId,
                                           String operator, String reason) {
        OpsRecord o = new OpsRecord();
        o.opId = opId; o.type = Type.PAUSE_INSTANCE; o.target = instId;
        o.operator = operator; o.reason = reason;
        return o;
    }

    public static OpsRecord unpauseInstance(String opId, String instId,
                                             String operator, String reason) {
        OpsRecord o = new OpsRecord();
        o.opId = opId; o.type = Type.UNPAUSE_INSTANCE; o.target = instId;
        o.operator = operator; o.reason = reason;
        return o;
    }

    public static OpsRecord setWeight(String opId, String instId, double weight,
                                       String operator, String reason) {
        OpsRecord o = new OpsRecord();
        o.opId = opId; o.type = Type.SET_WEIGHT; o.target = instId; o.weight = weight;
        o.operator = operator; o.reason = reason;
        return o;
    }

    public static OpsRecord setRules(String opId, ScaleRules rules,
                                      String operator, String reason) {
        OpsRecord o = new OpsRecord();
        o.opId = opId; o.type = Type.SET_RULES; o.rules = rules;
        o.operator = operator; o.reason = reason;
        return o;
    }
}
