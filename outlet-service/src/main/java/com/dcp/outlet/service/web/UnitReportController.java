package com.dcp.outlet.service.web;

import com.dcp.outlet.control.HeartbeatStore;
import com.dcp.outlet.control.LeaseManager;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 计算面 → 控制面的上报入口(心跳 / 续约)。
 *
 * <p>心跳落库后,进度页与巡检器就能看到"单元还活着 + 消费到哪了"。
 * 续约失败由调用方(计算面)决定自停。
 */
@RestController
@RequestMapping("/api/units")
public class UnitReportController {

    private final HeartbeatStore heartbeats;
    private final LeaseManager leases;

    public UnitReportController(HeartbeatStore heartbeats, LeaseManager leases) {
        this.heartbeats = heartbeats; this.leases = leases;
    }

    @PostMapping("/{unitId}/heartbeat")
    public Map<String, Object> heartbeat(@PathVariable String unitId,
                                         @RequestParam int partition,
                                         @RequestParam long offset,
                                         @RequestParam long tso) {
        heartbeats.record(unitId, partition, offset, tso, System.currentTimeMillis());
        return Map.of("ok", true);
    }

    @PostMapping("/{unitId}/lease/renew")
    public Map<String, Object> renew(@PathVariable String unitId,
                                     @RequestParam(defaultValue = "30000") long ttlMillis) {
        boolean renewed = leases.renew(unitId, ttlMillis, System.currentTimeMillis());
        return Map.of("renewed", renewed);
    }
}
