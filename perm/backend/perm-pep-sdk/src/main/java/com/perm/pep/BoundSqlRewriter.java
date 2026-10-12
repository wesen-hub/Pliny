package com.perm.pep;

import com.perm.common.bind.ParamBinder;
import com.perm.common.inject.PredicateInjector;
import com.perm.common.model.AuthorizationContext;
import com.perm.common.render.PredicateTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PEP 改写阶段（执行管线）：绑定 → 谓词渲染 → WHERE 注入 → 列裁剪校验
 * 复用 perm-common 的 ParamBinder / PredicateInjector（与 PDP 渲染产物配套）。
 * 铁律 C1：产物模板无值，值在本阶段从上下文绑定注入。
 */
public final class BoundSqlRewriter {

    private BoundSqlRewriter() {
    }

    /**
     * 产物 + 上下文 + 业务 SQL → 改写后的 BoundSql
     * @throws PepDeniedException 判定为 DENY 时（业务侧应拒绝执行而非继续查询）
     */
    public static BoundSql rewrite(VerifiedArtifact artifact, PepContext ctx, String businessSql) {
        if (!artifact.permitted()) {
            throw new PepDeniedException("判定拒绝："
                    + (artifact.denyReason() == null ? "无权限" : artifact.denyReason()));
        }

        // 1. 占位符 DTO → 渲染占位符（kind 字符串 → Kind 枚举）
        List<PredicateTemplate.Placeholder> placeholders = artifact.placeholders().stream()
                .map(p -> new PredicateTemplate.Placeholder(
                        p.name(),
                        p.ref(),
                        "LIST".equals(p.kind()) ? PredicateTemplate.Kind.LIST : PredicateTemplate.Kind.SCALAR,
                        p.value()))
                .toList();

        // 2. 上下文转换 + 绑定（按 bindOrder 产出绑定值）
        AuthorizationContext authCtx = toAuthContext(ctx);
        List<ParamBinder.Binding> bindings = ParamBinder.bind(placeholders, authCtx);
        List<Object> bindValues = bindings.stream().map(ParamBinder.Binding::value).toList();

        // 3. 行谓词渲染 + WHERE 注入（权限 ? 追加在原业务 ? 之后，bindOrder 业务参数在前）
        String permPredicate = null;
        if (artifact.rowPredicateTemplate() != null) {
            permPredicate = PredicateInjector.renderBoundPredicate(artifact.rowPredicateTemplate(), bindings);
        }
        String rewrittenSql = PredicateInjector.injectWhere(businessSql, permPredicate);

        return new BoundSql(rewrittenSql, bindValues, artifact.visibleColumns(), artifact.masks(),
                artifact.tagScope() == null ? null : artifact.tagScope().expandMode());
    }

    private static AuthorizationContext toAuthContext(PepContext ctx) {
        AuthorizationContext.Builder builder = AuthorizationContext.builder().userId(ctx.userId());
        ctx.roles().forEach(builder::role);
        for (Map.Entry<String, Object> e : ctx.attributes().entrySet()) {
            builder.attribute(e.getKey(), e.getValue());
        }
        for (Map.Entry<String, Integer> e : ctx.tagLevels().entrySet()) {
            builder.tagLevel(e.getKey(), e.getValue());
        }
        return builder.build();
    }
}
