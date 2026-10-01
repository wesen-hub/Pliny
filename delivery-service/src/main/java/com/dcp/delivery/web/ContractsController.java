package com.dcp.delivery.web;

import com.dcp.outlet.adapters.ContractViewCodec;
import com.dcp.outlet.adapters.JdbcContractViewSource;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 契约视图下发(计算面拉取)。
 *
 * <p>返回**扁平键值**而非 JSON,与指令协议同风格(可 curl 联调)。
 * <b>安全语义</b>:视图里的 allowedColumns 已是「契约 ∩ 主体」;契约不存在 → 返回**空视图**(拒绝)。
 *
 * <p>{@code consumerId} 参数**不参与授权判定**(授权以契约行 + 主体策略为准),只作审计标识;
 * 因此它是可选的 —— 不存在"调用方自报身份就能放大权限"。
 */
@RestController
@RequestMapping("/api/outlet/contracts")
public class ContractsController {

    private final JdbcContractViewSource source;

    public ContractsController(JdbcContractViewSource source) { this.source = source; }

    @GetMapping(value = "/{contractRef}", produces = "text/plain;charset=UTF-8")
    public String view(@PathVariable String contractRef,
                       @RequestParam(required = false, defaultValue = "") String consumerId,
                       @RequestParam(defaultValue = "rt_sync_c") String stream) {
        return source.viewOf(contractRef, stream).map(v -> {
            if (!v.active || (v.allowedColumns != null && v.allowedColumns.isEmpty())) {
                // 契约无效 / 未覆盖该流 / 主体可见列为空 → 下发放空视图(调用方 fail-closed)
                return ContractViewCodec.toQuery(ContractViewCodec.encode(
                        v.contractId, v.consumerId, stream, java.util.Set.of(), Map.of(), -1, null));
            }
            // allowedColumns = null → encode 为 "*"(全放行, 内部平台任务形态)
            return ContractViewCodec.toQuery(ContractViewCodec.encode(
                    v.contractId, v.consumerId, stream, v.allowedColumns, v.maskRules, v.maxLevel, v.rowCondition));
        }).orElseGet(() -> "error=contract_not_found&contractId=" + contractRef + "&stream=" + stream);
    }
}
