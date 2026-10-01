package com.dcp.outlet.control;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 任务内核服务:接收(任意来源的)任务定义 → **五项校验** → 落库(纯业务登记)→ 交给调度器
 * (调度状态/防双消费/规划代/并发全部由统一调度框架承担,元库不再存调度状态)。
 *
 * <p>业务参数的校验**不在这里**(由任务类型的垂直模块负责,ADR-9)—— 内核只保证通用参数正确。
 */
public class TaskKernelService {

    private final StreamRegistry streams;
    private final LogicCatalog logicCatalog;
    private final ContractLookup contracts;
    private final TaskStore tasks;
    private final TaskScheduler scheduler;

    public TaskKernelService(StreamRegistry streams, LogicCatalog logicCatalog, ContractLookup contracts,
                             TaskStore tasks, TaskScheduler scheduler) {
        this.streams = streams; this.logicCatalog = logicCatalog; this.contracts = contracts;
        this.tasks = tasks; this.scheduler = scheduler;
    }

    /** 创建任务(幂等由调用方用 {@link #exists} 或唯一 taskId 保证)。 */
    public TaskRecord create(TaskRecord draft) {
        List<String> errs = new ArrayList<>();

        // ① taskId 必填且未占用
        if (isBlank(draft.taskId())) {
            errs.add("taskId 必填");
        } else if (tasks.exists(draft.taskId())) {
            errs.add("任务已存在:" + draft.taskId());
        }

        // ② streams 非空且**全部已登记**(逻辑表名 → topic 的唯一入口)
        Map<String, StreamInfo> resolved = new LinkedHashMap<>();
        if (draft.streams().isEmpty()) {
            errs.add("streams 为空");
        } else {
            for (String s : draft.streams()) {
                Optional<StreamInfo> si = streams.get(s);
                if (si.isEmpty()) errs.add("流未登记:" + s);
                else resolved.put(s, si.get());
            }
        }

        // ③ 逻辑已注册且 status=active(生产化:disabled/archived 拦新建任务,不影响运行中单元)
        if (isBlank(draft.logicId()) || isBlank(draft.logicVersion())) {
            errs.add("logic 必填(logicId + version)");
        } else {
            Optional<LogicInfo> li = logicCatalog.find(draft.logicId(), draft.logicVersion());
            if (li.isEmpty()) {
                errs.add("逻辑未注册:" + draft.logicId() + "@" + draft.logicVersion());
            } else if (!li.get().active()) {
                errs.add("逻辑已禁用或废弃(LOGIC_DISABLED):" + draft.logicId() + "@"
                        + draft.logicVersion() + " status=" + li.get().status());
            }
        }

        // ④ 契约存在、active,且覆盖全部 stream
        if (isBlank(draft.contractRef())) {
            errs.add("contractRef 必填");
        } else {
            Optional<ContractInfo> c = contracts.find(draft.contractRef());
            if (c.isEmpty()) errs.add("契约不存在:" + draft.contractRef());
            else if (!c.get().active()) errs.add("契约未生效:" + draft.contractRef());
            else {
                for (String s : resolved.keySet()) {
                    if (!c.get().streams().contains(s)) errs.add("契约未覆盖流:" + s);
                }
            }
        }

        // ⑤ 消费组唯一(一任务一组)
        String group = "task-" + draft.taskId();
        if (tasks.groupExists(group)) errs.add("消费组已存在:" + group);

        if (!errs.isEmpty()) throw new TaskValidationException(errs);

        draft.consumerGroup(group);
        tasks.insert(draft);

        // desiredState=stopped = 只登记不调度(P3 底座任务先登记后激活: 执行栈归属/解码器
        // 就绪前不拉起消费; 后续 resume 经 scheduler.onCreated 复活, TasksController.action 已支持)
        if (!"stopped".equalsIgnoreCase(draft.desiredState())) {
            scheduler.onCreated(draft);
        }
        return draft;
    }

    public boolean exists(String taskId) { return tasks.exists(taskId); }

    private static boolean isBlank(String s) { return s == null || s.isBlank(); }
}
