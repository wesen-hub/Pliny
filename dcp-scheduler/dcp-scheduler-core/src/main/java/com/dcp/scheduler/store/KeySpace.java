package com.dcp.scheduler.store;

import io.etcd.jetcd.ByteSequence;

import java.nio.charset.StandardCharsets;

/** etcd key 空间(总纲 §3.5)。所有 key 常量集中于此,避免散落魔法字符串。 */
public final class KeySpace {

    private KeySpace() {}

    private static ByteSequence bs(String s) {
        return ByteSequence.from(s, StandardCharsets.UTF_8);
    }

    // ── 全局 ──
    public static ByteSequence instancesPrefix() {
        return bs("/dcp/instances/");
    }

    public static ByteSequence instance(String instId) {
        return bs("/dcp/instances/" + instId);
    }

    // ── 每 ns ──
    public static ByteSequence leader(String ns) {
        return bs("/dcp/" + ns + "/leader");
    }

    public static ByteSequence nsInstancesPrefix(String ns) {
        return bs("/dcp/" + ns + "/instances/");
    }

    public static ByteSequence nsInstance(String ns, String instId) {
        return bs("/dcp/" + ns + "/instances/" + instId);
    }

    /** 实例下架意图(主写,持久):存在 = 不接新单,已有分区逐步迁出;删除 = 取消 下架。 */
    public static ByteSequence instDrainIntent(String ns, String instId) {
        return bs("/dcp/" + ns + "/instances/" + instId + "/drain");
    }
    
    /** 暂停接单意图(主写,持久):存在 = 不接新单,已跑分区不动;删除 = 恢复接单。 */
    public static ByteSequence instPauseIntent(String ns, String instId) {
        return bs("/dcp/" + ns + "/instances/" + instId + "/paused");
    }
    
    /** 调权意图(主写,持久):覆盖注册键 weight(0.5~2.0),影响目标打分。 */
    public static ByteSequence instWeightIntent(String ns, String instId) {
        return bs("/dcp/" + ns + "/instances/" + instId + "/weight");
    }

    public static ByteSequence rules(String ns) {
        return bs("/dcp/" + ns + "/rules");
    }

    /** 再平衡规则(运维后台写,缺省用 RebalanceRules 内置默认)。 */
    public static ByteSequence rebalanceRules(String ns) {
        return bs("/dcp/" + ns + "/rebalance-rules");
    }

    /** 打分系数配置(运维后台写,可选;缺省 0.4/0.3/0.2/0.1)。 */
    public static ByteSequence scoringConfig(String ns) {
        return bs("/dcp/" + ns + "/scoring");
    }

    /** Kafka 连接配置(运维后台写,可选;缺失时追平判定回退 rps 近似)。 */
    public static ByteSequence kafkaConfig(String ns) {
        return bs("/dcp/" + ns + "/kafka");
    }

    /** 告警事件流前缀(主写,后台页面读;滚动保留)。 */
    public static ByteSequence alertsPrefix(String ns) {
        return bs("/dcp/" + ns + "/alerts/");
    }

    /** 单条告警事件(恢复时覆盖同一键)。 */
    public static ByteSequence alert(String ns, String alertId) {
        return bs("/dcp/" + ns + "/alerts/" + alertId);
    }

    public static ByteSequence tasksPrefix(String ns) {
        return bs("/dcp/" + ns + "/tasks/");
    }

    public static ByteSequence spec(String ns, String taskId) {
        return bs("/dcp/" + ns + "/tasks/" + taskId + "/spec");
    }

    public static ByteSequence status(String ns, String taskId) {
        return bs("/dcp/" + ns + "/tasks/" + taskId + "/status");
    }

    /** 任务事件(滚动覆盖,供业务外壳观察:扩容建议/缩容完成)。 */
    public static ByteSequence taskEvent(String ns, String taskId, String name) {
        return bs("/dcp/" + ns + "/tasks/" + taskId + "/events/" + name);
    }

    public static ByteSequence partsPrefix(String ns, String taskId) {
        return bs("/dcp/" + ns + "/tasks/" + taskId + "/parts/");
    }

    public static ByteSequence part(String ns, String taskId, int partId) {
        return bs("/dcp/" + ns + "/tasks/" + taskId + "/parts/" + partId);
    }

    public static ByteSequence snap(String ns, String taskId, int partId) {
        return bs("/dcp/" + ns + "/tasks/" + taskId + "/parts/" + partId + "/snap");
    }

    // ── 监控聚合键(主侧每决策轮派生写入;后台页面直读, 不经计算) ──

    public static ByteSequence metricsPrefix(String ns) {
        return bs("/dcp/" + ns + "/metrics/");
    }

    /** 任务级聚合(Σ分区派生指标 + 容量水位)。 */
    public static ByteSequence taskMetrics(String ns, String taskId) {
        return bs("/dcp/" + ns + "/metrics/tasks/" + taskId);
    }

    /** 实例级聚合(Σ该实例分区 + 机器指标)。 */
    public static ByteSequence instMetrics(String ns, String instId) {
        return bs("/dcp/" + ns + "/metrics/instances/" + instId);
    }

    /** ns 级大盘(计数 + 总 rps/lag)。 */
    public static ByteSequence nsMetrics(String ns) {
        return bs("/dcp/" + ns + "/metrics/ns");
    }

    /** 分区级派生(四级下钻最底层;metricsPrefix 清理失效键时一并覆盖)。 */
    public static ByteSequence partMetrics(String ns, String taskId, int partId) {
        return bs("/dcp/" + ns + "/metrics/tasks/" + taskId + "/parts/" + partId);
    }

    public static ByteSequence opsPrefix(String ns) {
        return bs("/dcp/" + ns + "/ops/");
    }

    public static ByteSequence op(String ns, String opId) {
        return bs("/dcp/" + ns + "/ops/" + opId);
    }

    // ── 解析辅助 ──
    /** 从 part key 反解 partId(形如 .../parts/5)。 */
    public static int partIdOf(String key) {
        int i = key.lastIndexOf('/');
        return Integer.parseInt(key.substring(i + 1));
    }

    /** 从 spec key 反解 taskId(形如 .../tasks/{taskId}/spec)。 */
    public static String taskIdOfSpecKey(String key) {
        int start = key.indexOf("/tasks/") + "/tasks/".length();
        int end = key.indexOf("/spec");
        return key.substring(start, end);
    }

    /** 从 status key 反解 taskId(形如 .../tasks/{taskId}/status)。 */
    public static String taskIdOfStatusKey(String key) {
        int start = key.indexOf("/tasks/") + "/tasks/".length();
        int end = key.indexOf("/status");
        return key.substring(start, end);
    }

    /** 从归属 key 反解 taskId(形如 .../tasks/{taskId}/parts/N)。 */
    public static String taskIdOfPartKey(String key) {
        int start = key.indexOf("/tasks/") + "/tasks/".length();
        int end = key.indexOf("/parts/");
        return key.substring(start, end);
    }

    /** 从 ns 实例 key 反解实例 id(形如 /dcp/{ns}/instances/{id})。 */
    public static String instIdOfNsKey(String key) {
        return key.substring(key.lastIndexOf('/') + 1);
    }

    /** 从 op key 反解 opId(形如 /dcp/{ns}/ops/{opId})。 */
    public static String opIdOf(String key) {
        return key.substring(key.lastIndexOf('/') + 1);
    }

    // ── 全局审计(不随 ns 删除而消失) ──

    /** 运维审计前缀(后台写,不可变;创建/删除 ns、改规则等配置动作落这里)。 */
    public static ByteSequence auditPrefix() {
        return bs("/dcp/audit/");
    }

    public static ByteSequence audit(String opId) {
        return bs("/dcp/audit/" + opId);
    }
}
