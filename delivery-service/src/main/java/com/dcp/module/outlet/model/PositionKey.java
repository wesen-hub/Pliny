package com.dcp.module.outlet.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * 位点(设计 §6.8):对外是不透明串 {@code v1.{epoch}.{tso}} 的 base64url 编码,
 * 消费方**只存/只回传,不解析**;平台内部才解释为 (epoch, tso) 二元组。
 *
 * <p>epoch:流代次。重建 changefeed / 换 sink / topic 重建时 +1,
 * 用于拦截"老位点巧合有效但指向错误数据"。
 */
@Data
@AllArgsConstructor
public class PositionKey {

    private static final String VERSION = "v1";

    /** 流代次 */
    private int epoch;
    /** TSO(PD 全局单调) */
    private long tso;

    /** 编码为不透明位点串 */
    public String encode() {
        String raw = VERSION + "." + epoch + "." + tso;
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /** 解码;格式非法抛 IllegalArgumentException(调用方转 PARAM_ERROR) */
    public static PositionKey decode(String opaque) {
        if (opaque == null || opaque.isBlank()) {
            return null;
        }
        try {
            String raw = new String(Base64.getUrlDecoder().decode(opaque), StandardCharsets.UTF_8);
            String[] parts = raw.split("\\.");
            if (parts.length != 3 || !VERSION.equals(parts[0])) {
                throw new IllegalArgumentException("位点格式非法");
            }
            return new PositionKey(Integer.parseInt(parts[1]), Long.parseLong(parts[2]));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("位点格式非法: " + opaque, e);
        }
    }

    public static String of(int epoch, long tso) {
        return new PositionKey(epoch, tso).encode();
    }
}
