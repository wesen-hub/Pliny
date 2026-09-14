package com.dcp.module.outlet.batch;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * Spark 作业规格(设计 §6.3):由 {@link SparkJobSpecBuilder} 生成,交给 {@link SparkLauncher} 执行。
 *
 * <p>{@code jobJson} 是交给 Spark 端的作业描述(边界/区间/读入口/转换/目标/staging);
 * {@code args} 是 spark-submit 的完整命令行参数。**两者都可由单测断言**,因此不依赖 Spark 运行时也能验语义。
 */
@Data
@AllArgsConstructor
public class SparkJobSpec {

    private String jobName;
    /** bootstrap | incr_batch */
    private String mode;
    /** 不可变 staging 路径(按 boundary/区间版本化) */
    private String stagingPath;
    /** 作业描述 JSON(Spark 端解释) */
    private String jobJson;
    /** spark-submit 参数 */
    private List<String> args;
}
