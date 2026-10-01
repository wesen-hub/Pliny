package com.dcp.scheduler.rest;

import com.dcp.scheduler.model.OpsRecord;
import com.dcp.scheduler.model.ScaleRules;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 统一调度框架查询 + 运维指令通道(迁自 backend SchedulerController,方案 N2):
 * 页面展示调度主持久化的运维事件与四级监控,
 * 并通过 ops 队列下发运维指令(调度运维后台 §2:钉住/解锁/定向迁移/实例下架/暂停/调权)。
 * 宿主 = dcp-worker,前端经 Gateway 路由 /api/scheduler → dcp-worker(方案 M2)。
 * 操作人取请求头 X-Operator(网关统一鉴权注入),缺省 anonymous。
 */
@RestController
@RequestMapping("/api/scheduler")
public class SchedulerRestController {

    private final EtcdAdminService service;

    public SchedulerRestController(EtcdAdminService service) {
        this.service = service;
    }

    /** 告警事件流:ns 必填,按触发时间倒序(未恢复在前端置顶)。 */
    @GetMapping("/alerts")
    public RestResp<List<SchedulerAlert>> alerts(@RequestParam String ns) {
        return RestResp.ok(service.listAlerts(ns));
    }

    /**
     * 监控四级聚合(监控子文档 §二):分区/任务/实例/ns 一次拉全,页面下钻直读不经计算。
     * taskId 可选:非空时 parts 只返回该任务分区(下钻细化);为空返回全量分区。
     */
    @GetMapping("/metrics")
    public RestResp<Map<String, Object>> metrics(@RequestParam String ns,
                                                 @RequestParam(required = false) String taskId) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ns", service.readNsMetrics(ns));
        out.put("instances", service.listInstanceMetrics(ns));
        out.put("tasks", service.listTaskMetrics(ns));
        out.put("parts", service.listPartMetrics(ns, taskId));
        return RestResp.ok(out);
    }

    // ── 运维指令通道(调度运维后台 §2:后台写 ops,PENDING → 主消费,DONE 删除 / FAILED 保留) ──

    /** 下发运维指令:白名单 + 必填校验后写 /dcp/{ns}/ops/{opId},操作人取请求头 X-Operator。 */
    @PostMapping("/ops")
    public RestResp<Map<String, Object>> submitOp(@RequestBody OpsRequest req,
                                                  @RequestHeader(value = "X-Operator",
                                                          required = false,
                                                          defaultValue = "anonymous") String operator) {
        String ns = req.ns == null ? "" : req.ns.trim();
        String type = req.type == null ? "" : req.type.trim();
        Set<String> types = Set.of(
                OpsRecord.Type.PIN, OpsRecord.Type.UNPIN, OpsRecord.Type.MIGRATE_PIN,
                OpsRecord.Type.DRAIN_INSTANCE, OpsRecord.Type.UNDRAIN_INSTANCE,
                OpsRecord.Type.PAUSE_INSTANCE, OpsRecord.Type.UNPAUSE_INSTANCE,
                OpsRecord.Type.SET_WEIGHT, OpsRecord.Type.SET_RULES);
        if (ns.isEmpty() || !types.contains(type)) {
            throw new IllegalArgumentException("ns 或指令类型非法");
        }
        boolean instOp = OpsRecord.Type.DRAIN_INSTANCE.equals(type)
                || OpsRecord.Type.UNDRAIN_INSTANCE.equals(type)
                || OpsRecord.Type.PAUSE_INSTANCE.equals(type)
                || OpsRecord.Type.UNPAUSE_INSTANCE.equals(type)
                || OpsRecord.Type.SET_WEIGHT.equals(type);
        boolean rulesOp = OpsRecord.Type.SET_RULES.equals(type);
        if (!instOp && !rulesOp && (req.taskId == null || req.taskId.isBlank() || req.partId == null)) {
            throw new IllegalArgumentException("分区类指令需 taskId 与 partId");
        }
        if (instOp && (req.target == null || req.target.isBlank())) {
            throw new IllegalArgumentException("实例类指令需 target 实例 id");
        }
        if (OpsRecord.Type.SET_WEIGHT.equals(type) && req.weight == null) {
            throw new IllegalArgumentException("SET_WEIGHT 需 weight(0.5~2.0)");
        }
        if (rulesOp && req.rules == null) {
            throw new IllegalArgumentException("SET_RULES 需 rules(默认扩缩规则)");
        }
        String opId = service.submitOp(ns, type, req.taskId, req.partId, req.target, req.weight,
                req.ttlSec, req.rules, operator, req.reason);
        return RestResp.ok(Map.of("opId", opId));
    }

    /** ops 队列(PENDING 待消费 + FAILED 保留展示;DONE 由主删除)。 */
    @GetMapping("/ops")
    public RestResp<List<OpsRecord>> ops(@RequestParam String ns) {
        return RestResp.ok(service.listOps(ns));
    }

    /** 实例运维视图(注册键 + 主写意图键合并,意图优先)。 */
    @GetMapping("/instances")
    public RestResp<List<SchedulerInstanceView>> instances(@RequestParam String ns) {
        return RestResp.ok(service.listInstances(ns));
    }

    // ── namespace 管理(运维后台设计 §2.1:创建/删除 ns、默认规则走 ops SET_RULES) ──

    /** ns 列表:名字 + 任务/实例/分区统计 + 默认规则。 */
    @GetMapping("/namespaces")
    public RestResp<List<NamespaceView>> namespaces() {
        return RestResp.ok(service.listNamespaces());
    }

    /** 创建 ns = 初始化默认规则键;已存在返回冲突提示。 */
    @PostMapping("/namespaces")
    public RestResp<Map<String, Object>> createNamespace(@RequestBody Map<String, Object> body,
                                                         @RequestHeader(value = "X-Operator",
                                                                 required = false,
                                                                 defaultValue = "anonymous") String operator) {
        String ns = body == null || body.get("ns") == null ? "" : String.valueOf(body.get("ns")).trim();
        if (ns.isEmpty()) throw new IllegalArgumentException("缺 ns 名");
        ScaleRules rules = null;
        if (body.get("rules") != null) {
            rules = service.parseRules(body.get("rules"));
        }
        boolean created = service.createNamespace(ns, rules, operator, strOf(body, "reason"));
        if (!created) throw new IllegalArgumentException("namespace 已存在: " + ns);
        return RestResp.ok(Map.of("ns", ns));
    }

    /** 删除 ns:前置校验无任务且无在线实例,否则 409 语义拒绝并提示残留。 */
    @DeleteMapping("/namespaces/{ns}")
    public RestResp<Map<String, Object>> deleteNamespace(@PathVariable String ns,
                                                         @RequestHeader(value = "X-Operator",
                                                                 required = false,
                                                                 defaultValue = "anonymous") String operator) {
        String residual = service.deleteNamespace(ns, operator, null);
        if (residual != null) throw new IllegalArgumentException(residual);
        return RestResp.ok(Map.of("ns", ns));
    }

    /** 全局审计(创建/删除 ns、改规则等配置动作;不随 ns 删除消失)。 */
    @GetMapping("/audits")
    public RestResp<List<Map<String, Object>>> audits() {
        return RestResp.ok(service.listAudits());
    }

    private static String strOf(Map<String, Object> body, String key) {
        Object v = body == null ? null : body.get(key);
        return v == null ? null : String.valueOf(v);
    }
}
