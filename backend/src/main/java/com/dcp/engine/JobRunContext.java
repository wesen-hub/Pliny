package com.dcp.engine;

import com.dcp.module.job.entity.ExtractConfig;
import com.dcp.module.job.entity.FieldMapping;
import com.dcp.module.job.entity.JobDefinition;
import com.dcp.module.job.entity.SyncState;
import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** 一次任务执行的完整上下文（由配置解析引擎装配，设计文档 §4.1） */
@Data
@Builder
public class JobRunContext {
    private JobDefinition job;
    private ExtractConfig extract;
    private List<FieldMapping> mappings;
    /** type/url/username/password */
    private Map<String, String> sourceConn;
    private Map<String, String> targetConn;
    private SyncState syncState;
    /** 字典名 -> (源值 -> 目标值) */
    private Map<String, Map<String, String>> dictMaps;
    private AtomicBoolean stopFlag;
    private Consumer<String> infoLog;
    private Consumer<String> errorLog;
    private long timeoutMillis;
    private Long executionId;
    /** 重试尝试序号（1 起）, 用于生成唯一 SeaTunnel jobId, 避免重试提交冲突 */
    private int attempt;
}
