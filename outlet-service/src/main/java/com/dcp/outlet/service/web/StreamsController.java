package com.dcp.outlet.service.web;

import com.dcp.outlet.control.StreamInfo;
import com.dcp.outlet.control.StreamRegistry;
import com.dcp.outlet.service.schedulerbridge.IndexerSchedulerRuntime;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 可消费流(新表感知的产物)。
 *
 * <ul>
 *   <li>{@code GET /api/outlet/streams} —— 前端“可消费流”列表</li>
 *   <li>{@code POST /api/outlet/streams/scan} —— 手动触发一轮扫描(排障/等待期);
 *        框架托管时随后把登记流提交为 TaskSpec(接入设计 §五)</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/outlet/streams")
public class StreamsController {

    private final StreamRegistry registry;
    private final IndexerSchedulerRuntime runtime;

    public StreamsController(StreamRegistry registry, IndexerSchedulerRuntime runtime) {
        this.registry = registry;
        this.runtime = runtime;
    }

    @GetMapping
    public List<Map<String, Object>> list() {
        // 领域类方法风格访问器不可 Jackson 序列化,Web 层转 Map 视图(内核保持零依赖)
        return registry.list().stream().map(s -> Map.<String, Object>of(
                "stream", s.stream(), "topic", s.topic(), "partitions", s.partitions())).toList();
    }

    @PostMapping("/scan")
    public Map<String, Object> scan() {
        int n = runtime.scanAndSubmit();
        return Map.of("scanned", n, "streams", list());
    }
}
