package com.perm.pep;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;

/**
 * 结果值掩码（PEP 后处理阶段，R-D9 最严者胜——合并阶段已按强度排序）
 * 演示级支持 HASH/NULL；生产形态由业务适配器（ORM 结果集钩子）在投影后逐列应用。
 */
public final class MaskApplier {

    private MaskApplier() {
    }

    /** 对一行结果应用掩码（不修改原 map，返回新 map） */
    public static Map<String, Object> applyRow(Map<String, Object> row, List<VerifiedArtifact.Mask> masks) {
        if (masks == null || masks.isEmpty() || row == null) {
            return row;
        }
        Map<String, Object> masked = new java.util.LinkedHashMap<>(row);
        for (VerifiedArtifact.Mask mask : masks) {
            if (!masked.containsKey(mask.column())) {
                continue;
            }
            masked.put(mask.column(), applyValue(masked.get(mask.column()), mask.type()));
        }
        return masked;
    }

    /** 单值掩码：HASH → SHA-256 截断；NULL → null；未知类型 → 原值（fail-open 于展示层，判定层已 fail-closed） */
    public static Object applyValue(Object value, String maskType) {
        if (value == null) {
            return null;
        }
        return switch (maskType) {
            case "HASH" -> sha256Hex(String.valueOf(value));
            case "NULL" -> null;
            default -> value;
        };
    }

    private static String sha256Hex(String input) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8; i++) {
                sb.append(String.format("%02x", digest[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("掩码哈希失败", e);
        }
    }
}
