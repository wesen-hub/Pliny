package com.dcp.outlet.adapters;

import com.dcp.outlet.control.Slice;
import com.dcp.outlet.control.SnapshotSource;
import com.dcp.outlet.sdk.ChangeRecord;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * 真实快照源:走**平台既有快照读路径**(它已精确支持 {@code boundaryTso} 与分页),
 * 而不是自己拼 `AS OF TIMESTAMP` —— 后者靠墙钟换算会引入毫秒级偏差,可能造成批/流之间的空洞。
 *
 * <p>接口:`GET {base}/api/outlet/{stream}/snapshot?boundaryTso=&pageToken=&limit=`
 */
public class HttpSnapshotSource implements SnapshotSource {

    private static final Logger LOG = Logger.getLogger("outlet-snapshot");

    private final String baseUrl;
    private final String consumerId;
    private final HttpClient http;
    private final int pageSize;
    private final Duration timeout;

    public HttpSnapshotSource(String baseUrl, String consumerId, int pageSize, Duration timeout) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.consumerId = consumerId;
        this.pageSize = pageSize;
        this.timeout = timeout;
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    public HttpSnapshotSource(String baseUrl, String consumerId) {
        this(baseUrl, consumerId, 1000, Duration.ofSeconds(30));
    }

    /**
     * 行数估算:平台快照接口暂不提供计数,返回 -1 → 切片策略退化为"按分区"(安全且可用)。
     * 待平台补 count 接口后即可启用"区间×分区"。
     */
    @Override
    public long estimateRows(String stream, long tso) {
        return -1L;
    }

    @Override
    public List<ChangeRecord> readSlice(String stream, long tso, Slice slice) {
        List<ChangeRecord> out = new ArrayList<>();
        String pageToken = null;
        int pages = 0;
        do {
            String url = baseUrl + "/api/outlet/" + stream + "/snapshot?consumerId=" + consumerId
                    + "&boundaryTso=" + tso + "&limit=" + pageSize
                    + (pageToken == null ? "" : "&pageToken=" + pageToken);
            String body = get(url);
            List<Map<String, String>> rows = FlatJson.rows(body);
            for (Map<String, String> r : rows) {
                Map<String, Object> row = new LinkedHashMap<>(r);
                out.add(new SnapshotRecord(row, tso, stream));
            }
            pageToken = FlatJson.string(body, "nextPageToken");
            if (pageToken != null && (pageToken.isBlank() || "null".equals(pageToken))) pageToken = null;
            pages++;
            if (pages > 100_000) {                      // 防御:分页 token 异常时不死循环
                throw new IllegalStateException("快照分页异常:页数超过上限");
            }
        } while (pageToken != null);
        int pagesDone = pages;
        int rowCount = out.size();
        LOG.info(() -> "snapshot slice " + slice + " rows=" + rowCount + " pages=" + pagesDone);
        return out;
    }

    private String get(String url) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url)).timeout(timeout).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("HTTP " + resp.statusCode() + " " + resp.body());
            }
            return resp.body();
        } catch (Exception e) {
            throw new IllegalStateException("快照读取失败:" + url, e);
        }
    }
}
