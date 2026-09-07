package com.dcp.engine;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EngineResult {
    private long sourceCount;
    private long targetCount;
    /** 增量模式探测到的窗口上界 m（成功后才由调度器推进位点） */
    private String windowMax;
    /** 无新数据（增量模式下 m 为 null） */
    private boolean noData;
}
