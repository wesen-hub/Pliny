package com.perm.pdp.render;

import com.perm.common.model.Artifact;
import com.perm.common.model.Policy;
import com.perm.common.model.PolicyConstraints;
import com.perm.common.render.DialectRenderer;
import com.perm.common.render.MySqlRenderer;
import com.perm.common.render.PredicateTemplate;
import com.perm.pdp.dto.ArtifactResponseDto;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 判定产物渲染器（PDP 翻译阶段 ㉒ 的服务层封装）
 * 按 dialect 选择方言渲染器 → 行谓词 AST 渲染为模板 + 占位符（铁律 C1：模板无值）。
 * 官方内置 mysql；新引擎接入 = 注册新 DialectRenderer（SPI）。
 */
@Component
public class ArtifactRenderer {

    private final Map<String, DialectRenderer> renderers = new HashMap<>();

    public ArtifactRenderer() {
        register(new MySqlRenderer());
    }

    public void register(DialectRenderer renderer) {
        renderers.put(renderer.dialect(), renderer);
    }

    public DialectRenderer rendererOf(String dialect) {
        DialectRenderer renderer = renderers.get(dialect == null ? "mysql" : dialect);
        if (renderer == null) {
            throw new IllegalArgumentException("未注册的方言渲染器: " + dialect + "（fail-closed）");
        }
        return renderer;
    }

    /** Artifact → 响应 DTO（渲染行谓词模板 + 映射约束字段） */
    public ArtifactResponseDto toDto(Artifact artifact, String dialect) {
        ArtifactResponseDto dto = new ArtifactResponseDto();
        dto.setDecision(artifact.decision().name());
        dto.setDenyReason(artifact.denyReason());
        dto.setPlanVersion(artifact.planVersion());
        dto.setVisibleColumns(artifact.visibleColumns());
        dto.setMatchedPolicies(artifact.matchedPolicies());
        dto.setObligations(artifact.obligations().stream()
                .map(o -> new ArtifactResponseDto.ObligationDto(o.type(), o.mode()))
                .toList());
        dto.setMasks(artifact.masks().stream()
                .map(m -> new ArtifactResponseDto.MaskDto(m.column(), m.type().name()))
                .toList());
        PolicyConstraints.TagScope ts = artifact.tagScope();
        if (ts != null) {
            dto.setTagScope(new ArtifactResponseDto.TagScopeDto(
                    ts.tagPath(), ts.expandMode().name(), ts.snapshotVersion(),
                    ts.targetKey(), ts.requiredLevel(), ts.entities()));
        }
        // 行谓词渲染（DENY 无行谓词）
        if (artifact.rowPredicate() != null) {
            PredicateTemplate template = rendererOf(dialect).renderPredicate(artifact.rowPredicate());
            dto.setRowPredicateTemplate(template.template());
            dto.setPlaceholders(template.placeholders().stream()
                    .map(p -> new ArtifactResponseDto.PlaceholderDto(
                            p.name(), p.ref(), p.kind().name(), p.value()))
                    .toList());
        }
        return dto;
    }

    /** 从内置快照策略重建 Policy 列表（管理面热更新接口复用） */
    public static List<Policy> clonePolicies(List<Policy> policies) {
        return List.copyOf(policies);
    }
}
