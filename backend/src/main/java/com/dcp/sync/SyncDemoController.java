package com.dcp.sync;

import com.dcp.common.ApiResponse;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * P0 演示入口: 手动触发一批 CDC 数据走通 源 → 打 seq → Kafka → 消费 → 分表幂等写 全链路。
 *
 * <pre>
 *   POST /api/sync/demo/send           触发一批演示数据(订单)
 * </pre>
 */
@Slf4j
@RestController
@RequestMapping("/api/sync/demo")
public class SyncDemoController {

    private final SyncProducer producer;

    @Value("${dcp.sync.demo.topic:dcp.order.demo}")
    private String topic;

    public SyncDemoController(SyncProducer producer) {
        this.producer = producer;
    }

    /** 演示: 同一订单多次更新(含乱序模拟) + 一批不同订单, 验证 seq 裁判 */
    @PostMapping("/send")
    public ApiResponse<Map<String, Object>> send(@RequestParam(defaultValue = "20") int rows,
                                    @RequestParam(defaultValue = "100") long orderIdStart) {
        List<Map<String, Object>> batch = new ArrayList<>();
        for (long i = 0; i < rows; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("order_id", orderIdStart + i);
            row.put("amount", 100 + i * 10);
            row.put("status", "PAID");
            batch.add(row);
        }
        long[] range = producer.sendBatch("t_order", topic, 4096, 8,
                "order_id", List.of("order_id"), "INSERT", batch);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("topic", topic);
        out.put("sent", batch.size());
        out.put("seqRange", List.of(range[0], range[1]));
        return ApiResponse.ok(out);
    }

    /** 演示: 对同一订单再发一次更新(新 seq 更大, 应覆盖) */
    @PostMapping("/update")
    public ApiResponse<Map<String, Object>> update(@RequestParam long orderId, @RequestParam int amount) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", orderId);
        row.put("amount", amount);
        row.put("status", "UPDATED");
        long[] range = producer.sendBatch("t_order", topic, 4096, 8,
                "order_id", List.of("order_id"), "UPDATE", List.of(row));
        return ApiResponse.ok(Map.of("seq", range[0]));
    }

    /** 演示: 删除一笔订单 */
    @PostMapping("/delete")
    public ApiResponse<Map<String, Object>> delete(@RequestParam long orderId) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", orderId);
        long[] range = producer.sendBatch("t_order", topic, 4096, 8,
                "order_id", List.of("order_id"), "DELETE", List.of(row));
        return ApiResponse.ok(Map.of("seq", range[0]));
    }

    /** 演示: 主键修改(旧单号 → 新单号), 应拆成 DELETE(旧) + INSERT(新) 同批原子 */
    @PostMapping("/rekey")
    public ApiResponse<Map<String, Object>> rekey(@RequestParam long oldOrderId, @RequestParam long newOrderId) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("order_id", newOrderId);
        row.put("amount", 777);
        row.put("status", "REKEYED");
        CdcRecord rec = CdcRecord.of("t_order", String.valueOf(newOrderId),
                List.of("order_id"), "UPDATE", 0, row);
        rec.setBeforeKey(String.valueOf(oldOrderId));
        long[] range = producer.sendRaw("t_order", topic, 4096, 8, rec);
        return ApiResponse.ok(Map.of("beforeKey", oldOrderId, "afterKey", newOrderId, "seq", range[0]));
    }
}
