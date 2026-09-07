package com.dcp.module.alarm;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.module.alarm.entity.AlarmConfig;
import com.dcp.module.alarm.mapper.AlarmConfigMapper;
import com.dcp.module.job.entity.JobDefinition;
import com.dcp.module.system.service.AuditService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 告警通知（设计文档 §4.5 / §5.7）
 * 渠道: DINGTALK / WECHAT webhook + EMAIL；同一执行同一事件只发一次（去重）
 */
@Slf4j
@Service
public class AlarmService {

    private final AlarmConfigMapper alarmMapper;
    private final AuditService auditService;
    private final RestTemplate restTemplate = new RestTemplate();
    private final JavaMailSender mailSender;

    @Value("${dcp.alarm.default-webhook:}")
    private String defaultWebhook;

    @Value("${spring.mail.host:}")
    private String mailHost;

    public AlarmService(AlarmConfigMapper alarmMapper, AuditService auditService,
                        ObjectProvider<JavaMailSender> mailSenderProvider) {
        this.alarmMapper = alarmMapper;
        this.auditService = auditService;
        this.mailSender = mailSenderProvider.getIfAvailable();
    }

    /** executionId:event -> 已发送去重 */
    private final Set<String> dedupe = ConcurrentHashMap.newKeySet();

    public List<AlarmConfig> list(Long jobId) {
        return alarmMapper.selectList(new LambdaQueryWrapper<AlarmConfig>()
                .eq(jobId != null, AlarmConfig::getJobId, jobId)
                .orderByDesc(AlarmConfig::getId));
    }

    public Long create(AlarmConfig cfg) {
        cfg.setId(null);
        alarmMapper.insert(cfg);
        auditService.record("alarm", "CREATE", "alarm", cfg.getId(), "创建告警配置");
        return cfg.getId();
    }

    public void update(Long id, AlarmConfig cfg) {
        cfg.setId(id);
        alarmMapper.updateById(cfg);
        auditService.record("alarm", "UPDATE", "alarm", id, "更新告警配置");
    }

    public void delete(Long id) {
        alarmMapper.deleteById(id);
        auditService.record("alarm", "DELETE", "alarm", id, "删除告警配置");
    }

    public void notify(JobDefinition job, String event, Long executionId, String message) {
        List<AlarmConfig> configs = alarmMapper.selectList(new LambdaQueryWrapper<AlarmConfig>()
                .eq(AlarmConfig::getJobId, job.getId())
                .eq(AlarmConfig::getTriggerEvent, event)
                .eq(AlarmConfig::getEnabled, 1));
        if (configs.isEmpty() && (defaultWebhook == null || defaultWebhook.isBlank())) {
            return;
        }
        String dedupeKey = executionId + ":" + event;
        if (!dedupe.add(dedupeKey)) {
            return;
        }
        String title = "【数据采集平台】任务 " + job.getJobName() + " " + event;
        String content = title + "\n执行ID: " + executionId + "\n" + message
                + "\n时间: " + java.time.LocalDateTime.now();
        for (AlarmConfig cfg : configs) {
            try {
                switch (cfg.getAlarmType()) {
                    case "DINGTALK" -> sendWebhook(cfg.getReceiver(), Map.of(
                            "msgtype", "text", "text", Map.of("content", content)));
                    case "WECHAT" -> sendWebhook(cfg.getReceiver(), Map.of(
                            "msgtype", "text", "text", Map.of("content", content)));
                    case "EMAIL" -> sendEmail(cfg.getReceiver(), title, content);
                    default -> {
                    }
                }
            } catch (Exception e) {
                log.warn("告警发送失败 {}: {}", cfg.getAlarmType(), e.getMessage());
            }
        }
        // 默认 webhook 兜底
        if (defaultWebhook != null && !defaultWebhook.isBlank()) {
            try {
                sendWebhook(defaultWebhook, Map.of("msgtype", "text", "text", Map.of("content", content)));
            } catch (Exception e) {
                log.warn("默认 webhook 发送失败: {}", e.getMessage());
            }
        }
    }

    private void sendWebhook(String url, Map<String, Object> body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> resp = restTemplate.postForEntity(url,
                new HttpEntity<>(body, headers), String.class);
        if (!resp.getStatusCode().is2xxSuccessful()) {
            throw new BizException(ErrorCode.INTERNAL_ERROR, "webhook 返回 " + resp.getStatusCode());
        }
    }

    private void sendEmail(String receivers, String title, String content) {
        if (mailSender == null || !org.springframework.util.StringUtils.hasText(mailHost)) {
            log.info("[EMAIL告警-未配置SMTP] {} {}", title, content);
            return;
        }
        try {
            SimpleMailMessage msg = new SimpleMailMessage();
            msg.setTo(receivers.split("[,; ]+"));
            msg.setSubject(title);
            msg.setText(content);
            mailSender.send(msg);
        } catch (Exception e) {
            log.warn("邮件告警发送失败: {}", e.getMessage());
        }
    }
}
