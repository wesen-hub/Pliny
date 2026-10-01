package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.LogicRef;

import java.util.Optional;

/**
 * 逻辑仓库端口:按 {@code logicId@version} 定位制品(jar)。
 *
 * <p>生产实现可指向对象存储 / HTTP 仓库;骨架提供 {@link FileLogicRepository}(本地目录),
 * 与"发布流程注册"对接时只需换实现。
 */
public interface LogicRepository {

    Optional<LogicArtifact> locate(LogicRef ref);
}
