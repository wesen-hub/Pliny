package com.dcp.module.outlet;

import com.dcp.module.system.service.AuditService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 出口审计(设计 §7.2 审计行 / §7.6)。
 *
 * <p>复用 {@code sys_operation_log},module 固定为 {@code chg-outlet};每条记录要能回答
 * **"为什么少了几列/几行"**:记位点区间、行数、裁剪项、脱敏项。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutletAuditService {

    public static final String MODULE = "chg-outlet";

    private final AuditService auditService;

    public void recordRead(String consumerId, String contractId, String streamId,
                           Long loTso, Long hiTso, long rows,
                           java.util.List<String> trimmed, java.util.List<String> masked) {
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("consumerId", consumerId);
        detail.put("contractId", contractId);
        detail.put("streamId", streamId);
        detail.put("positionFrom", loTso);
        detail.put("positionTo", hiTso);
        detail.put("rows", rows);
        detail.put("trimmedColumns", trimmed);
        detail.put("maskedColumns", masked);
        String json = detail.toString();
        try {
            auditService.record(MODULE, "read", "stream", null, json);
        } catch (Exception e) {
            // 审计失败不影响数据供给,但必须留痕告警
            log.warn("[outlet] 审计写入失败 consumer={} stream={} detail={} err={}",
                    consumerId, streamId, json, e.getMessage());
        }
    }

    public void recordReject(String consumerId, String streamId, int code, String message) {
        try {
            auditService.record(MODULE, "reject", "stream", null,
                    "consumer=" + consumerId + ", stream=" + streamId + ", code=" + code + ", msg=" + message);
        } catch (Exception e) {
            log.warn("[outlet] 拒绝审计写入失败:{}", e.getMessage());
        }
    }
}
