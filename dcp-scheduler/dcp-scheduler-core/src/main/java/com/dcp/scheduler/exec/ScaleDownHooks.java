package com.dcp.scheduler.exec;

import java.util.List;

/**
 * 缩容钩子契约(扩缩容子文档 §五):业务侧实现,按名注册,TaskSpec.scaleDownHooks 声明引用。
 * 属于任务维度(不是 ns):多任务可共享同名实现,但注册与声明都以任务为单位。
 * 约定:
 *   - 必须幂等(框架可能因超时重调);
 *   - 钩子执行超时(缺省 30s)视为失败,缩容中止;
 *   - hooks=null 的任务视为"只扩不缩"。
 */
public interface ScaleDownHooks {

    /**
     * 切路由:生产端停止向指定分区写。
     *
     * @return true=已切(缩容继续);false/异常=缩容中止,冷却期重置
     */
    boolean routeCut(String taskId, List<Integer> partIds);

    /** 可选:追平期观察(业务侧确认无在途数据)。返回 true=可停线程。缺省直接放行。 */
    default boolean drainReady(String taskId, List<Integer> partIds) {
        return true;
    }
}
