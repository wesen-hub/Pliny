package com.dcp.module.outlet.sdk;

import com.dcp.common.ApiResponse;
import com.dcp.module.outlet.model.ChangeBatch;
import com.dcp.module.outlet.model.SnapshotPage;
import com.dcp.module.outlet.model.StreamStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Map;

/**
 * L3 接口的 HTTP 实现(设计 §6.9)。鉴权走契约 token 头(生产:`X-Dcp-Token`)。
 *
 * <p>职责边界:本类**只做协议转换**(URL/参数/错误码解包),不做重试、不做位点管理 ——
 * 那些属于 {@link OutletConsumer} 与 {@link RetryPolicy}。
 */
@Slf4j
public class HttpOutletApi implements OutletApi {

    private final String baseUrl;
    private final String token;
    private final RestTemplate rest;
    private final ObjectMapper mapper = new ObjectMapper();

    public HttpOutletApi(String baseUrl, String token, RestTemplate restTemplate) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.token = token;
        this.rest = restTemplate;
    }

    /* ------------------------------------------------------------------ 接口实现 */

    @Override
    public StreamStatus streamStatus(String streamId) {
        return get(path(streamId, "stream-status"), StreamStatus.class);
    }

    @Override
    public SnapshotPage snapshot(String streamId, Long boundaryTso, String pageToken, int limit) {
        UriComponentsBuilder b = UriComponentsBuilder.fromHttpUrl(path(streamId, "snapshot"))
                .queryParam("limit", limit);
        if (boundaryTso != null) {
            b.queryParam("boundaryTso", boundaryTso);
        }
        if (pageToken != null && !pageToken.isBlank()) {
            b.queryParam("pageToken", pageToken);
        }
        return get(b.build().toUriString(), SnapshotPage.class);
    }

    @Override
    public ChangeBatch changes(String streamId, String afterPosition, int limit) {
        UriComponentsBuilder b = UriComponentsBuilder.fromHttpUrl(path(streamId, "changes"))
                .queryParam("limit", limit);
        if (afterPosition != null && !afterPosition.isBlank()) {
            b.queryParam("after", afterPosition);
        }
        return get(b.build().toUriString(), ChangeBatch.class);
    }

    @Override
    public Map<String, Object> keyLookup(String streamId, String bizKey, Long asOfTso) {
        UriComponentsBuilder b = UriComponentsBuilder.fromHttpUrl(path(streamId, "key/" + bizKey));
        if (asOfTso != null) {
            b.queryParam("asOf", asOfTso);
        }
        return get(b.build().toUriString(), Map.class);
    }

    @Override
    public Map<String, Object> positionAt(String streamId, long timeMs) {
        String url = UriComponentsBuilder.fromHttpUrl(path(streamId, "position-at"))
                .queryParam("time", timeMs).build().toUriString();
        return get(url, Map.class);
    }

    @Override
    public Map<String, Object> commit(String streamId, String position) {
        String url = UriComponentsBuilder.fromHttpUrl(path(streamId, "commit"))
                .queryParam("position", position).build().toUriString();
        return post(url, Map.class);
    }

    /* ------------------------------------------------------------------ 内部 */

    private String path(String streamId, String suffix) {
        return baseUrl + "/api/chg/" + streamId + "/" + suffix;
    }

    private <T> T get(String url, Class<T> type) {
        ApiResponse<?> resp = rest.getForObject(withToken(url), ApiResponse.class);
        return unwrap(resp, type);
    }

    private <T> T post(String url, Class<T> type) {
        ApiResponse<?> resp = rest.postForObject(withToken(url), null, ApiResponse.class);
        return unwrap(resp, type);
    }

    /** 契约 token:生产由调用方从安全存储取,仅显示一次下发(设计 §7.5) */
    private String withToken(String url) {
        if (token == null || token.isBlank()) {
            return url;
        }
        return url + (url.contains("?") ? "&" : "?") + "consumerId=" + token;
    }

    private <T> T unwrap(ApiResponse<?> resp, Class<T> type) {
        if (resp == null) {
            throw new OutletApiException(com.dcp.module.outlet.OutletErrorCode.OUTLET_INTERNAL, "空响应");
        }
        if (resp.getCode() != 0) {
            throw new OutletApiException(resp.getCode(), resp.getMessage());
        }
        return mapper.convertValue(resp.getData(), type);
    }
}
