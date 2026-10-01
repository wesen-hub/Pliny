package com.dcp.module.outlet;

import com.dcp.module.outlet.batch.BatchExecutor;
import com.dcp.module.outlet.batch.BatchJobService;
import com.dcp.module.outlet.batch.LocalBatchExecutor;
import com.dcp.module.outlet.batch.ManifestService;
import com.dcp.module.outlet.metrics.OutletMetrics;
import com.dcp.module.outlet.sdk.ConsumerOffsetStore;
import com.dcp.module.outlet.sdk.HttpOutletApi;
import com.dcp.module.outlet.sdk.InProcessOutletApi;
import com.dcp.module.outlet.sdk.JdbcConsumerOffsetStore;
import com.dcp.module.outlet.sdk.OutletApi;
import com.dcp.module.outlet.sdk.OutletApiFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.client.RestTemplate;

/**
 * 出口模块装配(设计 §6.1 消费方 SDK 的运行时接线)。
 *
 * <p>装配内容:
 * <ul>
 *   <li>{@link OutletApi}:进程内实现(默认,内部消费方)或 HTTP 实现({@code dcp.outlet.api.mode=http});</li>
 *   <li>{@link ConsumerOffsetStore}:位点自持(演示/内部消费方用元库;生产用消费方自己的库);</li>
 *   <li>{@link BatchExecutor} + {@link BatchJobService}:执行器 B 的批通道入口。</li>
 * </ul>
 * 说明:出口服务本身(ChangeOutletService / 权限引擎 / 配额 / 审计 / snapshot_job / manifest)已是 @Service/@Component,
 * 数据源按 {@code dcp.outlet.source}(mock|kafka)与 {@code dcp.outlet.registry}(memory|mapper)自动切换。
 */
@Slf4j
@Configuration
public class OutletConfig {

    @Bean
    @ConditionalOnMissingBean
    public OutletApiFactory outletApiFactory(ChangeOutletService service, OutletProperties props,
                                             ObjectProvider<RestTemplate> restTemplateProvider) {
        if ("http".equalsIgnoreCase(props.getApi().getMode())) {
            log.info("[outlet] OutletApi 模式=http baseUrl={}", props.getApi().getBaseUrl());
            RestTemplate rest = restTemplateProvider.getIfAvailable(RestTemplate::new);
            return consumerId -> new HttpOutletApi(props.getApi().getBaseUrl(), consumerId, rest);
        }
        log.info("[outlet] OutletApi 模式=inprocess(内部消费方直调出口服务)");
        return consumerId -> new InProcessOutletApi(service, consumerId);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutletApi outletApi(OutletApiFactory factory, OutletProperties props) {
        return factory.forConsumer(props.getApi().getConsumerId());
    }

    @Bean
    @ConditionalOnMissingBean
    public ConsumerOffsetStore consumerOffsetStore(JdbcTemplate metaJdbc) {
        // 演示/内部消费方:位点表落在元库(生产:消费方自己的库,与业务表同库同事务)
        return new JdbcConsumerOffsetStore(metaJdbc);
    }

    @Bean
    @ConditionalOnMissingBean
    public BatchExecutor batchExecutor(OutletApi api) {
        return new LocalBatchExecutor(api);
    }

    @Bean
    @ConditionalOnMissingBean
    public BatchJobService batchJobService(OutletApi api, BatchExecutor executor,
                                           ManifestService manifests, ConsumerOffsetStore offsets,
                                           OutletMetrics metrics) {
        return new BatchJobService(api, executor, manifests, offsets, metrics);
    }
}
