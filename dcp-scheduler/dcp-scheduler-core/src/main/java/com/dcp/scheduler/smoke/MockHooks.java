package com.dcp.scheduler.smoke;

import com.dcp.scheduler.exec.ScaleDownHooks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 冒烟用缩容钩子:routeCut 恒成功,并把被切分区的 RATE 清零(模拟生产路由切走 → 流量停)。
 * 幂等:重复调用只是重复清零 + 计数。
 */
public class MockHooks implements ScaleDownHooks {

    private static final Logger log = LoggerFactory.getLogger(MockHooks.class);

    /** routeCut 调用次数(验证缩容编排确实回调了钩子)。 */
    public static final AtomicInteger CUTS = new AtomicInteger();

    @Override
    public boolean routeCut(String taskId, List<Integer> partIds) {
        for (int p : partIds) {
            MockExecutor.RATE.put(taskId + "/" + p, 0); // 切路由 = 该分区停止进新数据
        }
        int n = CUTS.incrementAndGet();
        log.info("[smoke] 钩子 routeCut #{}(已切生产路由,流量停): task={} parts={}", n, taskId, partIds);
        return true;
    }
}
