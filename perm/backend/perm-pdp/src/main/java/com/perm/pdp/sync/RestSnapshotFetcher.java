package com.perm.pdp.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClient;

/**
 * 生产实现：RestClient 拉取管理面快照接口
 */
public class RestSnapshotFetcher implements SnapshotFetcher {

    private static final Logger log = LoggerFactory.getLogger(RestSnapshotFetcher.class);

    private final RestClient restClient;
    private final String adminBaseUrl;

    public RestSnapshotFetcher(RestClient restClient, String adminBaseUrl) {
        this.restClient = restClient;
        this.adminBaseUrl = adminBaseUrl;
    }

    @Override
    public AdminSnapshotDto fetch() {
        AdminSnapshotDto snapshot = restClient.get()
                .uri(adminBaseUrl + "/api/v1/snapshot")
                .retrieve()
                .body(AdminSnapshotDto.class);
        log.debug("[同步] 拉取管理面快照 v{}，策略 {} 条",
                snapshot == null ? -1 : snapshot.getVersion(),
                snapshot == null ? 0 : snapshot.getPolicyCount());
        return snapshot;
    }
}
