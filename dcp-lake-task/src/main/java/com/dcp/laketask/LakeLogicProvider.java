package com.dcp.laketask;

import com.dcp.outlet.sdk.ConsumerLogicProvider;

import java.util.List;

/**
 * SPI 声明(D14): 逻辑仓库 jar 经 {@code META-INF/services/} 暴露本提供者,
 * dcp-outlet-rt 的 JarLogicLoader/SpiLogicLoader 据此发现 lake-sync@1.0.0。
 */
public class LakeLogicProvider implements ConsumerLogicProvider {

    @Override
    public List<ProviderEntry> logics() {
        return List.of(ProviderEntry.of("lake-sync", "1.0.0", new LakeLogic()));
    }
}
