package com.dcp.outlet.service.web;

import com.dcp.outlet.control.LogicInfo;
import com.dcp.outlet.control.LogicPublishException;
import com.dcp.outlet.service.logic.LogicService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 逻辑管理视图(前端"逻辑管理"页对接的全部接口),风格对齐 {@link TasksController}(同一响应形态)。
 *
 * <pre>
 * POST   /api/outlet/logics                              上传发布(multipart: jar + logicId + version + description)
 * GET    /api/outlet/logics                              逻辑列表(logicId 聚合 + 最新版本 + 版本数 + 绑定任务数 + 状态)
 * GET    /api/outlet/logics/{logicId}/versions           版本列表(状态/哈希/大小/发布人/时间/预检/绑定任务)
 * POST   /api/outlet/logics/{logicId}/versions/{v}/disable|enable   版本级启停
 * POST   /api/outlet/logics/{logicId}/disable|enable                 逻辑级启停
 * DELETE /api/outlet/logics/{logicId}/versions/{v}       删除(= 标记 archived;被引用拒绝)
 * GET    /api/outlet/logics/{logicId}/versions/{v}/jar   下载(带审计)
 * </pre>
 */
@RestController
@RequestMapping("/api/outlet/logics")
public class LogicsController {

    private final LogicService logics;

    public LogicsController(LogicService logics) { this.logics = logics; }

    /** 上传发布(一次动作七步)。 */
    @PostMapping
    public Map<String, Object> publish(@RequestParam("jar") MultipartFile jar,
                                       @RequestParam("logicId") String logicId,
                                       @RequestParam("version") String version,
                                       @RequestParam(value = "description", required = false) String description,
                                       @RequestHeader(value = "X-Operator", required = false) String operator) {
        try {
            LogicService.PublishResult r = logics.publish(jar.getBytes(), logicId, version, description, op(operator));
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ok", true);
            m.put("logicId", r.logicId());
            m.put("version", r.version());
            m.put("artifactHash", r.artifactHash());
            m.put("jarSize", r.jarSize());
            m.put("status", r.status());
            m.put("preflight", r.preflight());
            return m;
        } catch (LogicPublishException e) {
            return Map.of("ok", false, "error", e.getMessage());
        } catch (java.io.IOException e) {
            return Map.of("ok", false, "error", "读取上传文件失败: " + e.getMessage());
        }
    }

    @GetMapping
    public List<Map<String, Object>> list() { return logics.list(); }

    @GetMapping("/{logicId}/versions")
    public List<Map<String, Object>> versions(@PathVariable String logicId) {
        return logics.versionsOf(logicId);
    }

    @PostMapping("/{logicId}/versions/{version}/disable")
    public Map<String, Object> disableVersion(@PathVariable String logicId, @PathVariable String version,
                                              @RequestHeader(value = "X-Operator", required = false) String operator) {
        return status(() -> logics.setVersionStatus(logicId, version, LogicInfo.STATUS_DISABLED, op(operator)),
                logicId, version);
    }

    @PostMapping("/{logicId}/versions/{version}/enable")
    public Map<String, Object> enableVersion(@PathVariable String logicId, @PathVariable String version,
                                             @RequestHeader(value = "X-Operator", required = false) String operator) {
        return status(() -> logics.setVersionStatus(logicId, version, LogicInfo.STATUS_ACTIVE, op(operator)),
                logicId, version);
    }

    @PostMapping("/{logicId}/disable")
    public Map<String, Object> disableLogic(@PathVariable String logicId,
                                            @RequestHeader(value = "X-Operator", required = false) String operator) {
        return status(() -> logics.setLogicStatus(logicId, LogicInfo.STATUS_DISABLED, op(operator)), logicId, null);
    }

    @PostMapping("/{logicId}/enable")
    public Map<String, Object> enableLogic(@PathVariable String logicId,
                                           @RequestHeader(value = "X-Operator", required = false) String operator) {
        return status(() -> logics.setLogicStatus(logicId, LogicInfo.STATUS_ACTIVE, op(operator)), logicId, null);
    }

    @DeleteMapping("/{logicId}/versions/{version}")
    public Map<String, Object> delete(@PathVariable String logicId, @PathVariable String version,
                                      @RequestHeader(value = "X-Operator", required = false) String operator) {
        return logics.deleteVersion(logicId, version, op(operator));
    }

    @GetMapping("/{logicId}/versions/{version}/jar")
    public ResponseEntity<?> download(@PathVariable String logicId, @PathVariable String version,
                                      @RequestHeader(value = "X-Operator", required = false) String operator) {
        try {
            LogicService.Download d = logics.download(logicId, version, op(operator));
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + d.fileName() + "\"")
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(d.bytes());
        } catch (LogicPublishException e) {
            return ResponseEntity.badRequest().body(Map.of("ok", false, "error", e.getMessage()));
        }
    }

    private Map<String, Object> status(Runnable action, String logicId, String version) {
        try {
            action.run();
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ok", true);
            m.put("logicId", logicId);
            if (version != null) m.put("version", version);
            return m;
        } catch (LogicPublishException e) {
            return Map.of("ok", false, "error", e.getMessage());
        }
    }

    private static String op(String operator) {
        return (operator == null || operator.isBlank()) ? "system" : operator;
    }
}
