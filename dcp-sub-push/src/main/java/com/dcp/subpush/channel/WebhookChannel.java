package com.dcp.subpush.channel;

import com.dcp.subpush.PushParams;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * webhook 推送通道(与切流前 DataSubService.deliver 的 webhook 分支同款):
 * HMAC-SHA256(secret, ts+body) 验签 + X-Dcp-Contract/X-Dcp-Timestamp/X-Dcp-Sign 头,
 * 非 2xx 抛异常(订阅方失败可见交付记录语义由平台重试/死信承接)。
 */
public final class WebhookChannel implements PushChannel {

    private final PushParams cfg;
    private final HttpClient client;

    public WebhookChannel(PushParams cfg) {
        this.cfg = cfg;
        this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @Override
    public void send(String body) throws Exception {
        if (cfg.targetUrl == null || cfg.targetUrl.isBlank()) {
            throw new IllegalStateException("[sub-push] webhook 契约未配置目标 URL");
        }
        long ts = System.currentTimeMillis();
        String sign = hmac(cfg.webhookSecret, ts, body);
        HttpRequest req = HttpRequest.newBuilder(URI.create(cfg.targetUrl))
                .header("Content-Type", "application/json")
                .header("X-Dcp-Contract", String.valueOf(cfg.contractId))
                .header("X-Dcp-Timestamp", String.valueOf(ts))
                .header("X-Dcp-Sign", sign)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(5))
                .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            throw new IllegalStateException("webhook HTTP " + resp.statusCode() + " " + resp.body());
        }
    }

    /** HMAC-SHA256(secret, ts+body) 十六进制小写(与 DataSubService.hmac 同款)。 */
    static String hmac(String secret, long ts, String body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec((secret == null ? "" : secret).getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return java.util.HexFormat.of().formatHex(mac.doFinal((ts + body).getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            return "";
        }
    }
}
