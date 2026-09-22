package com.dcp.outlet.control;

import java.util.List;
import java.util.Optional;

/**
 * 逻辑目录端口:控制面据此在**建任务时**拦住"逻辑没注册 / 已禁用"的情况
 * (错误码 {@code LOGIC_NOT_AVAILABLE} / {@code LOGIC_DISABLED});实际加载失败由计算面另行上报。
 *
 * <p>生产化扩展:除"是否注册过"外,补注册 / 列表 / 版本查询 / 状态查询 / 启停 / 审计,
 * 支撑"上传发布一次动作"的元库登记与"版本级 / 逻辑级启停"的管理面语义。
 * 接口与值对象零框架依赖(内核不碰 Spring / 数据库 / JSON)。
 */
public interface LogicCatalog {

    /** 该 {@code logicId@version} 是否**存在**(不判状态;状态判定用 {@link #find})。 */
    boolean registered(String logicId, String version);

    /** 某逻辑的全部版本(含各版本状态/哈希/大小/发布人/预检结果),无则空列表。 */
    List<LogicInfo> versionsOf(String logicId);

    /** 全部已登记逻辑(多版本各自成行)。 */
    List<LogicInfo> all();

    /** 精确查找某版本,不存在返回 {@link Optional#empty()}。 */
    Optional<LogicInfo> find(String logicId, String version);

    /**
     * 登记一个版本(发布动作的一环)。
     * 同 {@code logicId@version} 已存在时抛 {@link LogicPublishException}(**禁止覆盖**,发错内容就发新版本)。
     */
    void register(LogicInfo info);

    /** 版本级状态迁移(active / disabled / archived)。 */
    void setStatus(String logicId, String version, String status);

    /** 写一条审计(action 语义见 {@code logic_publish_audit.action} 列)。 */
    void audit(String logicId, String version, String action, String operator, String detail);
}
