package com.dcp.outlet.service.web;

import com.dcp.outlet.control.StartOffset;
import com.dcp.outlet.control.StartOffsetService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 起点查询(排障/交接核对用):给一个 TSO,返回**每分区**应从哪个 offset 开始。
 *
 * <p>{@code via} 字段说明它是走锚点快路径还是退化路径,前端可直接展示。
 */
@RestController
@RequestMapping("/api/outlet/anchor")
public class AnchorsController {

    private final StartOffsetService startOffsets;

    public AnchorsController(StartOffsetService startOffsets) { this.startOffsets = startOffsets; }

    @GetMapping
    public Map<String, Object> anchor(@RequestParam String topic,
                                      @RequestParam long tso,
                                      @RequestParam(defaultValue = "1") int epoch,
                                      @RequestParam(defaultValue = "3") int partitions) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("topic", topic);
        out.put("tso", tso);
        out.put("epoch", epoch);
        Map<Integer, Map<String, Object>> per = new LinkedHashMap<>();
        for (int p = 0; p < partitions; p++) {
            StartOffset so = startOffsets.startOffset(topic, p, epoch, tso);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("offset", so.offset());
            item.put("via", so.source().name());
            item.put("scannedRows", so.scannedRows());
            per.put(p, item);
        }
        out.put("partitions", per);
        return out;
    }
}
