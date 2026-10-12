package com.perm.pdp.audit;

import com.perm.common.event.EventTopics;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Kafka 审计记录器测试（mock KafkaTemplate，不依赖真实 broker）
 * 覆盖：sync 直写+重试 / sync 全失败不阻断判定 / async 积压队列后台冲刷
 */
class KafkaAuditRecorderTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);

    private KafkaAuditRecorder recorder() {
        return new KafkaAuditRecorder(kafka);
    }

    private CompletableFuture<SendResult<String, String>> okFuture() {
        CompletableFuture<SendResult<String, String>> f = new CompletableFuture<>();
        f.complete(null);
        return f;
    }

    @Test
    @DisplayName("sync 义务：同步投递确认；首次失败重试一次后成功")
    void syncSendWithRetry() {
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker 抖动")))
                .thenReturn(okFuture());

        KafkaAuditRecorder r = recorder();
        r.record("lisi", "sdm.sales_order", "select", "PERMIT", "sync", List.of("pol_1"));

        verify(kafka, times(2)).send(eq(EventTopics.AUDIT), eq("lisi"), anyString());
        assertEquals(1, r.size());
    }

    @Test
    @DisplayName("sync 义务：两次均失败不抛出（审计不可用不阻断判定），本地视图兜底")
    void syncAllFailDoesNotThrow() {
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker 不可用")));

        KafkaAuditRecorder r = recorder();
        assertDoesNotThrow(() ->
                r.record("lisi", "sdm.sales_order", "select", "PERMIT", "sync", List.of("pol_1")));

        verify(kafka, times(2)).send(anyString(), anyString(), anyString());
        assertEquals(1, r.size());
    }

    @Test
    @DisplayName("async 义务：入积压队列，后台冲刷线程批量投递")
    void asyncBacklogFlushedByBackgroundThread() throws Exception {
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(okFuture());

        KafkaAuditRecorder r = recorder();
        r.record("lisi", "sdm.sales_order", "select", "PERMIT", "async", List.of("pol_1"));
        r.record("wangwu", "sdm.sales_order", "select", "PERMIT", "async", List.of("pol_2"));

        // 冲刷线程周期 1s，等待两轮确保投递完成
        Thread.sleep(2500);

        verify(kafka, atLeast(2)).send(eq(EventTopics.AUDIT), anyString(), anyString());
        assertEquals(2, r.size());
    }
}
