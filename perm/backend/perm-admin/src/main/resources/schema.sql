-- 权限管理面持久化表（演示级 spring.sql.init 执行；正式形态升级 Flyway 版本化迁移）
-- 仅在 jdbc profile（perm.storage=jdbc）下初始化，内存模式不执行。

-- 策略表：DSL JSON 存整条策略语义 + 生命周期 + 元信息（读回时编译重建 Policy）
CREATE TABLE IF NOT EXISTS perm_policy (
    policy_id         VARCHAR(128)  NOT NULL COMMENT '策略 ID（主键）',
    dsl_json          TEXT          NOT NULL COMMENT '策略 DSL JSON（PolicyDsl）',
    status            VARCHAR(20)   NOT NULL COMMENT '生命周期状态（R-M1）',
    created_by        VARCHAR(64)   NOT NULL COMMENT '创建人',
    created_at        DATETIME(3)   NOT NULL COMMENT '创建时间',
    updated_at        DATETIME(3)   NOT NULL COMMENT '最近更新时间',
    published_version INT           NOT NULL DEFAULT 0 COMMENT '已发布版本号（未发布=0）',
    PRIMARY KEY (policy_id),
    KEY idx_status (status)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '权限策略';

-- 元数据表：planVersion 原子递增（R-M4，UPDATE 行锁串行化多实例安全）
CREATE TABLE IF NOT EXISTS perm_meta (
    k VARCHAR(64) NOT NULL COMMENT '键',
    v INT         NOT NULL COMMENT '值',
    PRIMARY KEY (k)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '权限系统元数据';

-- 发布流水表（R-M3 第六步确认存档）
CREATE TABLE IF NOT EXISTS perm_publication (
    version       INT          NOT NULL COMMENT '发布后 planVersion（主键）',
    change_type   VARCHAR(20)  NOT NULL COMMENT 'PUBLISH / REVISE / DEPRECATE / RETIRE',
    policy_ids    TEXT         NOT NULL COMMENT '涉及策略 ID（JSON 数组）',
    operator      VARCHAR(64)  NOT NULL COMMENT '操作人',
    ts            DATETIME(3)  NOT NULL COMMENT '发布时间',
    conflicts     TEXT         NULL COMMENT '冲突报告 JSON（WARNING/INFO 存档）',
    PRIMARY KEY (version)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '策略发布流水';

-- 种子：planVersion 起始值
INSERT IGNORE INTO perm_meta (k, v) VALUES ('plan_version', 1);

-- 元数据目录表（冲突检测 C8 属性目录输入；正式形态由元数据服务维护）
CREATE TABLE IF NOT EXISTS perm_attr_meta (
    attr_ref    VARCHAR(128)  NOT NULL COMMENT '属性引用（subject.xxx）',
    channel     VARCHAR(20)   NOT NULL COMMENT '属性渠道（身份/属性/标签三渠道）',
    description VARCHAR(255)  NULL COMMENT '说明',
    PRIMARY KEY (attr_ref)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '主体属性目录';

-- 元数据目录表（冲突检测 C7 标签路径目录输入）
CREATE TABLE IF NOT EXISTS perm_tag_meta (
    tag_path    VARCHAR(255)  NOT NULL COMMENT '标签路径（如 业务域/客户价值/高价值）',
    channel     VARCHAR(20)   NOT NULL COMMENT '标签渠道',
    description VARCHAR(255)  NULL COMMENT '说明',
    PRIMARY KEY (tag_path)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '标签路径目录';

-- 目录种子（与内存模式默认目录一致；INSERT IGNORE 幂等）
INSERT IGNORE INTO perm_attr_meta (attr_ref, channel, description) VALUES
    ('subject.region', 'builtin', '区域属性（行谓词区域过滤）'),
    ('subject.allowedStatus', 'builtin', '允许状态列表属性'),
    ('subject.depts', 'builtin', '部门列表属性'),
    ('subject.groups', 'builtin', '组列表属性');
INSERT IGNORE INTO perm_tag_meta (tag_path, channel, description) VALUES
    ('业务域/客户价值/高价值', 'builtin', '高价值客户标签'),
    ('业务域/客户价值/中价值', 'builtin', '中价值客户标签'),
    ('业务域/区域/华东', 'builtin', '华东区域标签');

-- 审计表（PDP 审计义务经 Kafka 投递，admin 订阅落库；正式查询入口在 admin 审计接口）
CREATE TABLE IF NOT EXISTS perm_audit (
    id               BIGINT AUTO_INCREMENT COMMENT '自增主键',
    ts               DATETIME(3)   NOT NULL COMMENT '判定时间',
    user_id          VARCHAR(64)   NOT NULL COMMENT '主体 ID',
    resource_id      VARCHAR(128)  NOT NULL COMMENT '资源标识',
    action           VARCHAR(32)   NOT NULL COMMENT '动作',
    decision         VARCHAR(16)   NOT NULL COMMENT 'PERMIT / DENY',
    mode             VARCHAR(16)   NOT NULL COMMENT '义务模式（sync/async）',
    matched_policies TEXT          NULL COMMENT '命中策略 ID（JSON 数组）',
    PRIMARY KEY (id),
    KEY idx_user_ts (user_id, ts),
    KEY idx_resource_ts (resource_id, ts)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COMMENT = '权限判定审计流水';
