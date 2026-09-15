package com.dcp.outlet.service.web;

import com.dcp.outlet.adapters.KafkaAdminStreamScanner;
import com.dcp.outlet.control.StreamInfo;
import com.dcp.outlet.control.StreamRegistry;
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
 *   <li>{@code GET /api/outlet/streams} —— 前端"可消费流"列表</li>
 *   <li>{@code POST /api/outlet/streams/scan} —— 手动触发一轮扫描(排障/等待期)</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/outlet/streams")
public class StreamsController {

    private final StreamRegistry registry;
    private final KafkaAdminStreamScanner scanner;

    public StreamsController(StreamRegistry registry, KafkaAdminStreamScanner scanner) {
        this.registry = registry;
        this.scanner = scanner;
    }

    @GetMapping
    public List<StreamInfo> list() { return registry.list(); }

    @PostMapping("/scan")
    public Map<String, Object> scan() {
        int n = scanner.scanOnce();
        return Map.of("scanned", n, "streams", registry.list());
    }
}
