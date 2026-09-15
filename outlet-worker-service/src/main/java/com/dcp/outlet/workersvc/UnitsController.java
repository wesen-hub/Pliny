package com.dcp.outlet.workersvc;

import com.dcp.outlet.adapters.UnitCommandCodec;
import com.dcp.outlet.worker.CreateUnitCmd;
import com.dcp.outlet.worker.UnitRunner;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 计算面的单元入口(控制面下发的落点)。
 *
 * <p>指令格式见 {@link UnitCommandCodec}(扁平键值)。
 * **幂等**:同 unitId 重复下发由 {@code UnitRunner} 保证只启动一个单元。
 */
@RestController
@RequestMapping("/units")
public class UnitsController {

    private final UnitRunner runner;

    public UnitsController(UnitRunner runner) { this.runner = runner; }

    @PostMapping
    public Map<String, Object> create(@RequestParam Map<String, String> params) {
        CreateUnitCmd cmd = UnitCommandCodec.decode(params);
        runner.start(cmd);
        return Map.of("accepted", true, "unitId", cmd.unitId(),
                "partitions", cmd.partitions());
    }

    @DeleteMapping("/{unitId}")
    public Map<String, Object> stop(@PathVariable String unitId) {
        runner.stop(unitId);
        return Map.of("stopped", unitId);
    }

    @GetMapping("/{unitId}")
    public Map<String, Object> state(@PathVariable String unitId) {
        return Map.of("unitId", unitId,
                "state", String.valueOf(runner.state(unitId)),
                "lastCommittedOffset", runner.lastCommittedOffset(unitId),
                "lastTso", runner.lastTso(unitId));
    }

    @GetMapping("/health")
    public Map<String, Object> health() { return Map.of("ok", true); }
}
