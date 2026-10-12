package com.perm.demo;

import com.perm.pep.BoundSql;
import com.perm.pep.BoundSqlRewriter;
import com.perm.pep.MaskApplier;
import com.perm.pep.PepContext;
import com.perm.pep.PepDeniedException;
import com.perm.pep.PermClient;
import com.perm.pep.VerifiedArtifact;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 业务侧演示接口：GET /demo/orders（PEP 接入路径全展示）
 * 上下文提取（演示级从请求头）：X-User-Id / X-Roles（逗号分隔）/ X-Attr-<name>（属性）
 * 流程：PepContext → PermClient.authorize（L1+验签）→ BoundSqlRewriter.rewrite → 数据过滤 + 掩码 → 返回展示 JSON
 */
@RestController
@RequestMapping("/demo")
public class DemoController {

    private final PermClient permClient;

    public DemoController(PermClient permClient) {
        this.permClient = permClient;
    }

    /** 内存演示数据（模拟 sdm.sales_order 表；region 用英文值避免 HTTP 头中文编码干扰演示） */
    private static final List<Map<String, Object>> ORDERS = List.of(
            row(1L, "SO-1001", 101, "east", "12888.00", "PAID", "13800000001"),
            row(2L, "SO-1002", 102, "east", "9600.00", "SHIPPED", "13800000002"),
            row(3L, "SO-1003", 103, "south", "5200.00", "PAID", "13800000003"),
            row(4L, "SO-1004", 104, "north", "7600.00", "CANCELLED", "13800000004"));

    private static Map<String, Object> row(Long id, String orderNo, int customerId,
                                           String region, String amount, String status, String phone) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("order_no", orderNo);
        m.put("customer_id", customerId);
        m.put("region", region);
        m.put("amount", amount);
        m.put("status", status);
        m.put("phone", phone);
        return m;
    }

    @GetMapping("/orders")
    public ResponseEntity<?> orders(
            @RequestHeader(value = "X-User-Id", required = false) String userId,
            @RequestHeader(value = "X-Roles", required = false) String roles,
            @RequestHeader(value = "X-Attr-region", required = false) String region,
            @RequestHeader(value = "X-Attr-allowedStatus", required = false) String allowedStatus) {

        if (userId == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "缺少 X-User-Id 请求头"));
        }

        // 1. 业务侧提取上下文（演示级：请求头 → 属性）
        Map<String, Object> attributes = new LinkedHashMap<>();
        if (region != null) {
            attributes.put("region", region);
        }
        if (allowedStatus != null) {
            attributes.put("allowedStatus", List.of(allowedStatus.split(",")));
        }
        PepContext ctx = PepContext.builder()
                .userId(userId)
                .roles(roles == null ? List.of() : List.of(roles.split(",")))
                .attributes(attributes)
                .resourceId("sdm.sales_order")
                .action("select")
                .dialect("mysql")
                .build();

        // 2. SDK 判定（L1 缓存 + Ed25519 验签；任何异常 fail-closed 拒绝）
        VerifiedArtifact artifact;
        try {
            artifact = permClient.authorize(ctx);
        } catch (PepDeniedException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("decision", "DENY", "reason", e.getMessage()));
        }

        // 3. 业务 SQL + 改写（绑定 → 谓词注入 → 列裁剪/掩码元信息）
        String businessSql = "SELECT id, order_no, region, amount FROM sales_order";
        BoundSql bound;
        try {
            bound = BoundSqlRewriter.rewrite(artifact, ctx, businessSql);
        } catch (PepDeniedException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("decision", "DENY", "reason", e.getMessage()));
        }

        // 4. 演示级"执行"：按绑定值过滤（region）+ 掩码（amount HASH）
        List<Map<String, Object>> filtered = new ArrayList<>();
        for (Map<String, Object> order : ORDERS) {
            if (region != null && !region.equals(order.get("region"))) {
                continue; // 模拟行谓词过滤（绑定值 region）
            }
            filtered.add(MaskApplier.applyRow(order, artifact.masks()));
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("decision", artifact.decision());
        result.put("matchedPolicies", artifact.matchedPolicies());
        result.put("planVersion", artifact.planVersion());
        result.put("businessSql", businessSql);
        result.put("rewrittenSql", bound.sql());
        result.put("bindValues", bound.bindValues());
        result.put("visibleColumns", bound.visibleColumns());
        result.put("masks", artifact.masks());
        result.put("rows", filtered);
        return ResponseEntity.ok(result);
    }
}
