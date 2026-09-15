package com.dcp.outlet.sdk;

import java.util.List;
import java.util.Map;

/**
 * 逻辑初始化上下文(D26):单元启动时注入一次。
 *
 * <p>用它来建连接池 / 加载规则缓存 / 编译表达式 —— <b>不要</b>在 {@code process()} 里做这些。
 */
public interface LogicInitContext {

    /** 任务级业务参数(平台内核原样透传;形状由调用方保证,见 D28)。 */
    Map<String, Object> params();

    String taskId();

    String logicVersion();

    /** 物理 topic(只读;逻辑通常只用 {@code Batch.stream()} 的逻辑表名)。 */
    String topic();

    /** 该单元持有的分区组(默认 1 个;{@code instances < 分区数} 时会多于 1 个)。 */
    List<Integer> partitions();

    SdkLogger log();
}
