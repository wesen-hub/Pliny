package com.dcp.agent;

import brave.Tracing;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Sender;
import zipkin2.reporter.brave.ZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

/**
 * M6: 链路追踪(Brave → Zipkin, 与 Spring 服务同 b3 协议)。
 *
 * <p>懒加载单例; 上报地址 ZIPKIN_ENDPOINT(缺省本机 9411); 上报走 AsyncReporter
 * 异步缓冲, 平台不可达不阻塞采集主流程; JVM 退出时 flush 未上报 span。
 */
public final class AgentTracing {

    private static volatile Tracing tracing;
    private static volatile AsyncReporter<zipkin2.Span> reporter;

    private AgentTracing() {
    }

    public static synchronized Tracing get() {
        if (tracing == null) {
            String endpoint = System.getenv("ZIPKIN_ENDPOINT");
            if (endpoint == null || endpoint.isBlank()) {
                endpoint = "http://127.0.0.1:9411";
            }
            Sender sender = URLConnectionSender.create(endpoint + "/api/v2/spans");
            reporter = AsyncReporter.builder(sender).build();
            tracing = Tracing.newBuilder()
                    .localServiceName("dcp-agent")
                    .addSpanHandler(ZipkinSpanHandler.create(reporter))
                    .build();
            Runtime.getRuntime().addShutdownHook(new Thread(AgentTracing::close));
        }
        return tracing;
    }

    public static brave.Tracer tracer() {
        return get().tracer();
    }

    public static synchronized void close() {
        if (reporter != null) {
            reporter.flush();
            reporter.close();
        }
    }
}
