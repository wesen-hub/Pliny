package com.dcp.sync;

import com.dcp.engine.SeaTunnelClient;
import com.dcp.engine.SeaTunnelConfigParser;
import com.dcp.module.datasource.DatasourceService;
import com.dcp.module.datasource.entity.DatasourceConfig;
import com.dcp.module.job.entity.ExtractConfig;
import com.dcp.module.job.entity.FieldMapping;
import com.dcp.module.job.entity.JobDefinition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SeaTunnel CDC 采集(实现 3): 复用现有 seatunnel 环境的
 * connector-cdc-mysql + connector-jdbc, 零新增下载。
 *
 * <p>链路: MySQL binlog(CDC source) → Jdbc sink 直写目标表(单表, generate_sink_sql 自动建表)。
 * 与 CdcStService(Kafka 缓冲链路)并存: 本服务是 SeaTunnel 直连路线。
 *
 * <p>注意: 当前 seatunnel 环境无 connector-kafka, 故走 CDC→JDBC 直连;
 * 生产如需 CDC→Kafka, 补充 kafka sink connector 后改 sink 即可。
 */
@Slf4j
@Service
public class SeaTunnelCdcService {

    private final DatasourceService datasourceService;
    private final SeaTunnelClient seaTunnelClient;
    private final String encKey;

    public SeaTunnelCdcService(DatasourceService datasourceService, SeaTunnelClient seaTunnelClient,
                               @org.springframework.beans.factory.annotation.Value("${dcp.enc-key}") String encKey) {
        this.datasourceService = datasourceService;
        this.seaTunnelClient = seaTunnelClient;
        this.encKey = encKey;
    }

    /**
     * 启动 SeaTunnel CDC 任务: 源表 → 目标表(直连, 不经 Kafka)。
     *
     * @param sourceDsId   源数据源(MySQL)
     * @param sourceDb     源库
     * @param sourceTable  源表
     * @param targetDsId   目标数据源
     * @param targetTable  目标表(单表, 自动建表)
     * @param primaryKey   目标主键列(幂等写依据)
     */
    public Map<String, Object> startCdc(Long sourceDsId, String sourceDb, String sourceTable,
                                        Long targetDsId, String targetTable, String primaryKey) {
        DatasourceConfig srcDs = datasourceService.get(sourceDsId);
        DatasourceConfig tgtDs = datasourceService.get(targetDsId);

        JobDefinition job = new JobDefinition();
        job.setJobName("cdc-" + sourceTable + "-to-" + targetTable);
        job.setSourceDatasourceId(sourceDsId);
        job.setTargetDatasourceId(targetDsId);
        job.setTargetTableName(targetTable);
        job.setSyncMode("INCREMENTAL");
        job.setParallelism(1);

        ExtractConfig extract = new ExtractConfig();
        extract.setSourceType(srcDs.getType());
        extract.setContent(sourceTable);

        FieldMapping pk = new FieldMapping();
        pk.setSortNo(1);
        pk.setSourceField(primaryKey);
        pk.setTargetField(primaryKey);

        // 手动构造 JDBC URL(与 DsType.buildJdbcUrl 一致)
        String srcUrl = switch (srcDs.getType()) {
            case "mysql" -> "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai".formatted(
                    srcDs.getHost(), srcDs.getPort(), sourceDb);
            default -> throw new IllegalArgumentException("SeaTunnel CDC 仅支持 MySQL 源, 当前: " + srcDs.getType());
        };
        Map<String, String> srcConn = Map.of("type", srcDs.getType(), "url", srcUrl,
                "username", srcDs.getUsername(),
                "password", com.dcp.common.CryptoUtil.decrypt(srcDs.getPasswordCiphertext(), encKey));

        Map<String, String> tgtConn = Map.of(
                "type", tgtDs.getType(),
                "url", "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai".formatted(
                        tgtDs.getHost(), tgtDs.getPort(), tgtDs.getDatabaseName()),
                "username", tgtDs.getUsername(),
                "password", com.dcp.common.CryptoUtil.decrypt(tgtDs.getPasswordCiphertext(), encKey));

        Map<String, Object> config = SeaTunnelConfigParser.buildCdcConfig(job, extract,
                List.of(pk), srcConn, tgtConn);
        String jobId = String.valueOf(System.currentTimeMillis());
        String seaTunnelJobId = seaTunnelClient.submitJob(jobId, config);
        log.info("SeaTunnel CDC 已提交 jobId={} seatunnelJobId={} ({} → {})", jobId, seaTunnelJobId,
                sourceDb + "." + sourceTable, targetTable);
        return Map.of("jobId", jobId, "seaTunnelJobId", seaTunnelJobId,
                "source", sourceDb + "." + sourceTable, "target", targetTable);
    }
}
