package com.dcp.module.outlet.batch;

import lombok.AllArgsConstructor;
import lombok.Data;

/** 一次批执行的结果(由执行器返回,供 manifest 登记与发布)。 */
@Data
@AllArgsConstructor
public class ExecutionResult {

    /** 不可变 staging 路径/表(按 boundary 版本化;**绝不原地覆盖**) */
    private String stagingPath;
    private long rowsWritten;
    /** 行数/主键校验和(用于对账) */
    private String checksum;
}
