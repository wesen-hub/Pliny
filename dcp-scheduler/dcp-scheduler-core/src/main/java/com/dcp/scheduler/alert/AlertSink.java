package com.dcp.scheduler.alert;

import com.dcp.scheduler.model.AlertEvent;
import com.dcp.scheduler.store.EtcdStore;
import com.dcp.scheduler.store.Json;
import com.dcp.scheduler.store.KeySpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 告警事件流通道(监控子文档 §六):主侧判定 → 写 /dcp/{ns}/alerts/{alertId}(后台页面读)。
 *   - 去重:同对象(实例/任务/分区组合)同类型 10min 只发一次;
 *   - 恢复:状态翻转时 resolve(),同一 alertId 覆盖写 resolvedAt(补发恢复通知语义);
 *   - 滚动保留:最多 MAX_ALERTS 条,超出删最旧(主侧单写,无并发)。
 */
public class AlertSink {

    private static final Logger log = LoggerFactory.getLogger(AlertSink.class);

    /** 事件流滚动保留条数。 */
    public static final int MAX_ALERTS = 200;
    /** 同对象同类型去重窗口。 */
    public static final long DEDUP_MS = 10 * 60_000;

    private final String ns;
    private final EtcdStore store;

    /** 去重键 → 上次触发时间。 */
    private final Map<String, Long> lastFiredAt = new HashMap<>();
    /** 去重键 → 在途告警 alertId(恢复时覆盖同一键)。 */
    private final Map<String, String> openAlerts = new HashMap<>();

    public AlertSink(String ns, EtcdStore store) {
        this.ns = ns;
        this.store = store;
    }

    private static String dedupKey(String type, String instanceId, String taskId, Integer partId) {
        return type + "|" + (instanceId == null ? "" : instanceId)
                + "|" + (taskId == null ? "" : taskId) + "|" + (partId == null ? "" : partId);
    }

    /** 触发告警(10min 去重;在途告警复用同一 alertId 保持 since)。 */
    public void fire(String level, String type, String instanceId, String taskId,
                     Integer partId, String message) {
        long now = System.currentTimeMillis();
        String key = dedupKey(type, instanceId, taskId, partId);
        Long last = lastFiredAt.get(key);
        if (last != null && now - last < DEDUP_MS) return; // 去重窗口内不再发
        lastFiredAt.put(key, now);

        String alertId = openAlerts.get(key);
        AlertEvent ev = new AlertEvent();
        ev.alertId = alertId != null ? alertId : UUID.randomUUID().toString();
        ev.ns = ns;
        ev.level = level;
        ev.type = type;
        ev.instanceId = instanceId;
        ev.taskId = taskId;
        ev.partId = partId;
        ev.message = message;
        ev.since = alertId != null
                ? store.getObj(KeySpace.alert(ns, ev.alertId), AlertEvent.class)
                        .map(a -> a.since).orElse(now)
                : now;
        ev.resolvedAt = 0;
        store.put(KeySpace.alert(ns, ev.alertId), ev);
        openAlerts.put(key, ev.alertId);
        trim();
        log.warn("[{}] 告警 {}({}): {}{}{} — {}", ns, level, type,
                instanceId == null ? "" : " 实例=" + instanceId,
                taskId == null ? "" : " 任务=" + taskId,
                partId == null ? "" : " 分区=" + partId, message);
    }

    /** 恢复:同一 alertId 覆盖写 resolvedAt;未在途则忽略。 */
    public void resolve(String type, String instanceId, String taskId, Integer partId, String message) {
        String key = dedupKey(type, instanceId, taskId, partId);
        String alertId = openAlerts.remove(key);
        if (alertId == null) return;
        AlertEvent ev = store.getObj(KeySpace.alert(ns, alertId), AlertEvent.class).orElse(null);
        if (ev == null) return;
        ev.resolvedAt = System.currentTimeMillis();
        ev.message = message;
        store.put(KeySpace.alert(ns, ev.alertId), ev);
        log.info("[{}] 告警恢复 {}({}): {}", ns, type, alertId, message);
    }

    /** 滚动保留:超出 MAX_ALERTS 删最旧。 */
    private void trim() {
        List<AlertEvent> all = new ArrayList<>();
        for (byte[] v : store.getPrefix(KeySpace.alertsPrefix(ns)).values()) {
            AlertEvent a = Json.fromBytes(v, AlertEvent.class);
            if (a != null) all.add(a);
        }
        if (all.size() <= MAX_ALERTS) return;
        all.sort(Comparator.comparingLong(a -> a.since));
        int overflow = all.size() - MAX_ALERTS;
        for (int i = 0; i < overflow; i++) {
            store.delete(KeySpace.alert(ns, all.get(i).alertId));
        }
    }
}
