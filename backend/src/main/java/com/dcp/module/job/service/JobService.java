package com.dcp.module.job.service;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.engine.ExecutionRunner;
import com.dcp.engine.ExpressionUtil;
import com.dcp.module.datasource.DatasourceService;
import com.dcp.module.datasource.entity.DatasourceConfig;
import com.dcp.module.dict.DictService;
import com.dcp.module.execution.entity.JobExecutionHistory;
import com.dcp.module.execution.mapper.JobExecutionHistoryMapper;
import com.dcp.module.job.entity.ExtractConfig;
import com.dcp.module.job.entity.FieldMapping;
import com.dcp.module.job.entity.JobDefinition;
import com.dcp.module.job.entity.SyncState;
import com.dcp.module.job.mapper.ExtractConfigMapper;
import com.dcp.module.job.mapper.FieldMappingMapper;
import com.dcp.module.job.mapper.JobDefinitionMapper;
import com.dcp.module.job.mapper.SyncStateMapper;
import com.dcp.module.system.service.AuditService;
import com.dcp.security.SecurityUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class JobService {

    private final JobDefinitionMapper jobMapper;
    private final ExtractConfigMapper extractMapper;
    private final FieldMappingMapper mappingMapper;
    private final SyncStateMapper syncStateMapper;
    private final JobExecutionHistoryMapper historyMapper;
    private final DatasourceService datasourceService;
    private final DictService dictService;
    private final ExecutionRunner executionRunner;
    private final AuditService auditService;

    // ---------- CRUD ----------

    public Page<JobDefinition> page(int page, int pageSize, String keyword, String status, String syncMode) {
        LambdaQueryWrapper<JobDefinition> qw = new LambdaQueryWrapper<>();
        qw.like(keyword != null && !keyword.isBlank(), JobDefinition::getJobName, keyword)
          .eq(status != null && !status.isBlank(), JobDefinition::getStatus, status)
          .eq(syncMode != null && !syncMode.isBlank(), JobDefinition::getSyncMode, syncMode)
          .orderByDesc(JobDefinition::getId);
        return jobMapper.selectPage(Page.of(page, pageSize), qw);
    }

    public JobDefinition get(Long id) {
        JobDefinition job = jobMapper.selectById(id);
        if (job == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "任务不存在: id=" + id);
        }
        return job;
    }

    /** 详情（含抽取配置 + 字段映射） */
    public Map<String, Object> detail(Long id) {
        JobDefinition job = get(id);
        ExtractConfig extract = extractMapper.selectOne(
                new LambdaQueryWrapper<ExtractConfig>().eq(ExtractConfig::getJobId, id));
        List<FieldMapping> mappings = mappingMapper.selectList(
                new LambdaQueryWrapper<FieldMapping>().eq(FieldMapping::getJobId, id)
                        .orderByAsc(FieldMapping::getSortNo));
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("job", job);
        result.put("extract", extract);
        result.put("mappings", mappings);
        return result;
    }

    /** 创建任务（含抽取+映射，事务） */
    @Transactional
    public Long create(JobDefinition job, ExtractConfig extract, List<FieldMapping> mappings) {
        validate(job, extract, mappings, null);
        job.setId(null);
        job.setStatus("READY");
        job.setEnabled(job.getEnabled() == null ? 1 : job.getEnabled());
        job.setVersion(0);
        job.setCreateBy(SecurityUtils.currentUsername());
        jobMapper.insert(job);
        extract.setId(null);
        extract.setJobId(job.getId());
        extractMapper.insert(extract);
        int sort = 0;
        for (FieldMapping m : mappings) {
            m.setId(null);
            m.setJobId(job.getId());
            m.setSortNo(++sort);
            mappingMapper.insert(m);
        }
        auditService.record("job", "CREATE", "job", job.getId(), "创建任务: " + job.getJobName());
        return job.getId();
    }

    /** 更新任务（RUNNING 时拒绝，F-203） */
    @Transactional
    public void update(Long id, JobDefinition req, ExtractConfig extract, List<FieldMapping> mappings) {
        JobDefinition job = get(id);
        if ("RUNNING".equals(job.getStatus())) {
            throw new BizException(ErrorCode.JOB_RUNNING, "任务正在运行, 禁止编辑");
        }
        validate(req, extract, mappings, id);
        req.setId(id);
        req.setCreateBy(null);
        req.setCreateTime(null);
        req.setUpdateBy(SecurityUtils.currentUsername());
        jobMapper.updateById(req);
        extractMapper.delete(new LambdaQueryWrapper<ExtractConfig>().eq(ExtractConfig::getJobId, id));
        mappingMapper.delete(new LambdaQueryWrapper<FieldMapping>().eq(FieldMapping::getJobId, id));
        extract.setId(null);
        extract.setJobId(id);
        extractMapper.insert(extract);
        int sort = 0;
        for (FieldMapping m : mappings) {
            m.setId(null);
            m.setJobId(id);
            m.setSortNo(++sort);
            mappingMapper.insert(m);
        }
        auditService.record("job", "UPDATE", "job", id, "更新任务: " + req.getJobName());
    }

    public void delete(Long id) {
        JobDefinition job = get(id);
        if ("RUNNING".equals(job.getStatus())) {
            throw new BizException(ErrorCode.JOB_RUNNING, "任务正在运行, 请先停止");
        }
        jobMapper.deleteById(id);
        extractMapper.delete(new LambdaQueryWrapper<ExtractConfig>().eq(ExtractConfig::getJobId, id));
        mappingMapper.delete(new LambdaQueryWrapper<FieldMapping>().eq(FieldMapping::getJobId, id));
        syncStateMapper.deleteById(id);
        auditService.record("job", "DELETE", "job", id, "删除任务: " + job.getJobName());
    }

    public void setEnabled(Long id, boolean enabled) {
        JobDefinition job = get(id);
        job.setEnabled(enabled ? 1 : 0);
        jobMapper.updateById(job);
        auditService.record("job", enabled ? "ENABLE" : "DISABLE", "job", id, job.getJobName());
    }

    /** 复制任务（深拷贝，不复制执行历史与位点） */
    @Transactional
    public Long copy(Long id) {
        Map<String, Object> detail = detail(id);
        JobDefinition src = (JobDefinition) detail.get("job");
        ExtractConfig extract = (ExtractConfig) detail.get("extract");
        @SuppressWarnings("unchecked")
        List<FieldMapping> mappings = (List<FieldMapping>) detail.get("mappings");

        JobDefinition copy = new JobDefinition();
        copy.setJobName(src.getJobName() + "-副本");
        copy.setSourceDatasourceId(src.getSourceDatasourceId());
        copy.setTargetDatasourceId(src.getTargetDatasourceId());
        copy.setTargetTableName(src.getTargetTableName());
        copy.setSyncMode(src.getSyncMode());
        copy.setIncrementalField(src.getIncrementalField());
        copy.setCronExpr(src.getCronExpr());
        copy.setMisfirePolicy(src.getMisfirePolicy());
        copy.setEnabled(0);
        copy.setStatus("READY");
        copy.setTimeoutMinutes(src.getTimeoutMinutes());
        copy.setParallelism(src.getParallelism());
        copy.setRetryTimes(src.getRetryTimes());
        copy.setRetryIntervalSeconds(src.getRetryIntervalSeconds());
        copy.setAlarmEnabled(src.getAlarmEnabled());
        copy.setDescription(src.getDescription());
        copy.setCreateBy(SecurityUtils.currentUsername());
        jobMapper.insert(copy);

        ExtractConfig ec = new ExtractConfig();
        ec.setJobId(copy.getId());
        ec.setSourceType(extract.getSourceType());
        ec.setContent(extract.getContent());
        ec.setPreviewLimit(extract.getPreviewLimit());
        extractMapper.insert(ec);

        int sort = 0;
        for (FieldMapping m : mappings) {
            FieldMapping nm = new FieldMapping();
            nm.setJobId(copy.getId());
            nm.setSourceField(m.getSourceField());
            nm.setTargetField(m.getTargetField());
            nm.setTransformType(m.getTransformType());
            nm.setTransformRule(m.getTransformRule());
            nm.setSortNo(++sort);
            mappingMapper.insert(nm);
        }
        auditService.record("job", "COPY", "job", copy.getId(), "复制任务: " + copy.getJobName());
        return copy.getId();
    }

    // ---------- 校验 ----------

    private void validate(JobDefinition job, ExtractConfig extract, List<FieldMapping> mappings, Long selfId) {
        if (job.getJobName() == null || job.getJobName().isBlank()) {
            throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "任务名称不能为空");
        }
        if (job.getSourceDatasourceId() == null || job.getTargetDatasourceId() == null) {
            throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "源/目标数据源必选");
        }
        DatasourceConfig src = datasourceService.get(job.getSourceDatasourceId());
        DatasourceConfig tgt = datasourceService.get(job.getTargetDatasourceId());
        if (src.getEnabled() != 1) {
            throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "源数据源未启用: " + src.getName());
        }
        if (tgt.getEnabled() != 1) {
            throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "目标数据源未启用: " + tgt.getName());
        }
        if (job.getTargetTableName() == null || job.getTargetTableName().isBlank()) {
            throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "目标表名不能为空");
        }
        if (job.getSyncMode() == null) {
            job.setSyncMode("full");
        }
        if ("incremental".equals(job.getSyncMode())
                && (job.getIncrementalField() == null || job.getIncrementalField().isBlank())) {
            throw new BizException(ErrorCode.JOB_INC_FIELD_MISSING, "增量模式必须配置增量字段");
        }
        if (job.getCronExpr() != null && !job.getCronExpr().isBlank()) {
            try {
                CronExpression.parse(normalizeCron(job.getCronExpr()));
            } catch (Exception e) {
                throw new BizException(ErrorCode.JOB_CRON_INVALID, "Cron 表达式无效: " + e.getMessage());
            }
        }
        if (extract == null || extract.getContent() == null || extract.getContent().isBlank()) {
            throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "抽取配置不能为空");
        }
        if ("sql".equalsIgnoreCase(extract.getSourceType())
                && !extract.getContent().trim().toLowerCase().startsWith("select")) {
            throw new BizException(ErrorCode.SQL_VALIDATE_FAIL, "抽取 SQL 必须以 SELECT 开头");
        }
        if (mappings == null || mappings.isEmpty()) {
            throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "字段映射不能为空");
        }
        Set<String> targets = new HashSet<>();
        // 表达式可用字段: 源字段 + 目标字段（前置转换输出）
        Set<String> available = mappings.stream()
                .map(m -> m.getSourceField() == null ? "" : m.getSourceField().toLowerCase())
                .collect(Collectors.toSet());
        for (FieldMapping m : mappings) {
            if (m.getSourceField() == null || m.getSourceField().isBlank()) {
                throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "映射存在空源字段");
            }
            if (m.getTargetField() == null || m.getTargetField().isBlank()) {
                throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "映射存在空目标字段");
            }
            if (!targets.add(m.getTargetField())) {
                throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "目标字段重复: " + m.getTargetField());
            }
            if ("dictionary".equals(m.getTransformType())) {
                if (m.getTransformRule() == null || m.getTransformRule().isBlank()) {
                    throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "字典转换必须选择字典: " + m.getTargetField());
                }
                if (dictService.all().stream().noneMatch(d -> d.getDictName().equals(m.getTransformRule()))) {
                    throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "字典不存在: " + m.getTransformRule());
                }
            }
            if ("expression".equals(m.getTransformType())) {
                String err = ExpressionUtil.validate(m.getTransformRule(), available);
                if (err != null) {
                    throw new BizException(ErrorCode.EXPR_COMPILE_FAIL, "表达式编译失败 [" + m.getTargetField() + "]: " + err);
                }
            }
            available.add(m.getTargetField().toLowerCase());
        }
    }

    // ---------- 运行 / 停止 ----------

    /** 手动触发（绕过 cron，与调度触发互斥，K5 乐观锁防重） */
    public Long run(Long jobId, String triggerType) {
        JobDefinition job = get(jobId);
        // 乐观锁: RUNNING 状态防重
        int updated = jobMapper.update(null, new com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper<JobDefinition>()
                .eq(JobDefinition::getId, jobId)
                .eq(JobDefinition::getVersion, job.getVersion())
                .ne(JobDefinition::getStatus, "RUNNING")
                .set(JobDefinition::getVersion, job.getVersion() + 1)
                .set(JobDefinition::getStatus, "RUNNING")
                .set(JobDefinition::getLastExecutionTime, LocalDateTime.now()));
        if (updated == 0) {
            throw new BizException(ErrorCode.JOB_RUNNING, "任务正在运行或已被其他实例触发");
        }
        ExtractConfig extract = extractMapper.selectOne(
                new LambdaQueryWrapper<ExtractConfig>().eq(ExtractConfig::getJobId, jobId));
        List<FieldMapping> mappings = mappingMapper.selectList(
                new LambdaQueryWrapper<FieldMapping>().eq(FieldMapping::getJobId, jobId)
                        .orderByAsc(FieldMapping::getSortNo));
        if (extract == null || mappings.isEmpty()) {
            job.setStatus("FAILED");
            job.setLastExecutionStatus("FAILED");
            jobMapper.updateById(job);
            throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "任务缺少抽取配置或字段映射");
        }
        JobExecutionHistory history = new JobExecutionHistory();
        history.setJobId(jobId);
        history.setTriggerType(triggerType);
        history.setStatus("RUNNING");
        history.setStartTime(LocalDateTime.now());
        history.setSeaTunnelJobId("exec-" + System.currentTimeMillis());
        historyMapper.insert(history);

        auditService.record("job", "RUN", "job", jobId, "触发执行: " + job.getJobName());
        executionRunner.info(history.getId(), "任务触发(" + triggerType + "), 提交执行引擎");
        executionRunner.startAsync(history.getId(), job, extract, mappings);
        return history.getId();
    }

    public void stop(Long jobId) {
        JobDefinition job = get(jobId);
        JobExecutionHistory running = historyMapper.selectOne(
                new LambdaQueryWrapper<JobExecutionHistory>()
                        .eq(JobExecutionHistory::getJobId, jobId)
                        .eq(JobExecutionHistory::getStatus, "RUNNING")
                        .orderByDesc(JobExecutionHistory::getId)
                        .last("LIMIT 1"));
        if (running == null) {
            throw new BizException(ErrorCode.ENGINE_STOP_FAIL, "任务没有运行中的执行");
        }
        executionRunner.stopExecution(running.getId());
        auditService.record("job", "STOP", "job", jobId, "停止任务: " + job.getJobName());
    }

    // ---------- 位点 ----------

    public SyncState syncState(Long jobId) {
        return syncStateMapper.selectById(jobId);
    }

    public void resetSyncState(Long jobId, String value, String valueType) {
        SyncState exist = syncStateMapper.selectById(jobId);
        if (value == null || value.isBlank()) {
            if (exist != null) {
                syncStateMapper.deleteById(jobId);
            }
        } else {
            if (exist == null) {
                exist = new SyncState();
                exist.setJobId(jobId);
            }
            exist.setLastSyncValue(value);
            exist.setValueType(valueType == null ? "LONG" : valueType);
            if (exist.getJobId() == null) {
                syncStateMapper.insert(exist);
            } else {
                syncStateMapper.updateById(exist);
            }
        }
        auditService.record("job", "RESET_OFFSET", "job", jobId, "重置位点: " + value);
    }

    // ---------- 工具接口 ----------

    /** 兼容 Quartz 5 段 cron -> Spring 6 段（秒+分+时+日+月+周） */
    public static String normalizeCron(String expr) {
        if (expr == null || expr.isBlank()) {
            return expr;
        }
        String e = expr.trim();
        if (e.split("\\s+").length == 5) {
            return "0 " + e;
        }
        return e;
    }

    /** 表达式编译校验（F-405） */
    public Map<String, Object> validateExpression(String expression, List<String> availableFields) {
        Set<String> fields = availableFields == null ? Set.of()
                : availableFields.stream().map(String::toLowerCase).collect(Collectors.toSet());
        String err = ExpressionUtil.validate(expression, fields);
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("valid", err == null);
        if (err != null) {
            r.put("message", err);
        }
        return r;
    }

    /** Cron 最近 5 次触发时间预览 */
    public List<String> cronPreview(String expr) {
        try {
            CronExpression cron = CronExpression.parse(normalizeCron(expr));
            LocalDateTime t = LocalDateTime.now();
            List<String> result = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                t = cron.next(t);
                if (t == null) break;
                result.add(t.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            }
            return result;
        } catch (Exception e) {
            throw new BizException(ErrorCode.JOB_CRON_INVALID, "Cron 表达式无效: " + e.getMessage());
        }
    }

    /** 自动映射（按名称归一化匹配，§5.4） */
    public List<Map<String, Object>> autoMapping(List<String> sourceColumns, List<String> targetColumns) {
        Map<String, String> normTarget = new HashMap<>();
        if (targetColumns != null) {
            for (String t : targetColumns) {
                normTarget.put(normalize(t), t);
            }
        }
        List<Map<String, Object>> result = new ArrayList<>();
        if (sourceColumns != null) {
            for (String s : sourceColumns) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("sourceField", s);
                String matched = normTarget.get(normalize(s));
                m.put("targetField", matched == null ? s : matched);
                m.put("transformType", "direct");
                m.put("transformRule", null);
                result.add(m);
            }
        }
        return result;
    }

    private String normalize(String s) {
        return s.toLowerCase().replace("_", "").replace("-", "").replace(".", "");
    }
}
