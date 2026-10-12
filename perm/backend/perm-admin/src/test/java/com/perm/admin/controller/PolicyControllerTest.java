package com.perm.admin.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 管理面 REST 端到端测试（Spring Boot 全栈 + MockMvc）
 * 覆盖：创建→提交→审批发布全链路 / 冲突阻断 409 / 发布历史
 */
@SpringBootTest
@AutoConfigureMockMvc
class PolicyControllerTest {

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper objectMapper;

    private String policyBody(String id, String name, String effect, List<String> subjectIds) throws Exception {
        return objectMapper.writeValueAsString(java.util.Map.of(
                "policyId", id,
                "policyName", name,
                "effect", effect,
                "subjectType", "role",
                "subjectIds", subjectIds,
                "resourceId", "sdm.sales_order",
                "actions", List.of("select"),
                "constraints", java.util.Map.of(
                        "visible", "CONDITIONAL",
                        "columns", List.of("id", "region"))));
    }

    @Test
    @DisplayName("端到端：创建(DRAFT)→提交(IN_REVIEW)→审批(ACTIVE)→发布历史可见")
    void lifecycleEndToEnd() throws Exception {
        String id = "pol_e2e_001";
        mockMvc.perform(post("/api/v1/policies")
                        .header("X-Operator", "e2e-tester")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(policyBody(id, "端到端策略", "GRANT", List.of("regional_manager"))))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("DRAFT"));

        mockMvc.perform(post("/api/v1/policies/" + id + "/submit"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("IN_REVIEW"));

        mockMvc.perform(post("/api/v1/policies/" + id + "/approve")
                        .header("X-Operator", "e2e-tester"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.published").value(true))
                .andExpect(jsonPath("$.version").isNumber());

        mockMvc.perform(get("/api/v1/policies/" + id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ACTIVE"))
                .andExpect(jsonPath("$.publishedVersion").value(
                        org.hamcrest.Matchers.greaterThan(0)));

        mockMvc.perform(get("/api/v1/publications"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.publications[0].changeType").value("PUBLISH"));
    }

    @Test
    @DisplayName("冲突阻断：行谓词互斥策略发布返回 409 + conflict_blocked")
    void conflictBlockedEndToEnd() throws Exception {
        // 策略 A：region=华北 发布成功
        String aBody = """
                {
                  "policyId": "pol_e2e_a",
                  "policyName": "华北策略",
                  "effect": "GRANT",
                  "subjectType": "role",
                  "subjectIds": ["regional_manager"],
                  "resourceId": "sdm.sales_order",
                  "actions": ["select"],
                  "constraints": {
                    "visible": "CONDITIONAL",
                    "columns": ["id", "region"],
                    "rowRules": [{"column": "region", "op": "EQ", "value": "华北"}]
                  }
                }
                """;
        mockMvc.perform(post("/api/v1/policies")
                        .contentType(MediaType.APPLICATION_JSON).content(aBody))
                .andExpect(status().isCreated());
        mockMvc.perform(post("/api/v1/policies/pol_e2e_a/submit")).andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/policies/pol_e2e_a/approve")).andExpect(status().isOk());

        // 策略 B：region=华东（同域）→ 冲突 ERROR 阻断
        String bBody = aBody.replace("pol_e2e_a", "pol_e2e_b")
                .replace("华北策略", "华东策略")
                .replace("\"华北\"", "\"华东\"");
        mockMvc.perform(post("/api/v1/policies")
                        .contentType(MediaType.APPLICATION_JSON).content(bBody))
                .andExpect(status().isCreated());
        mockMvc.perform(post("/api/v1/policies/pol_e2e_b/submit")).andExpect(status().isOk());
        mockMvc.perform(post("/api/v1/policies/pol_e2e_b/approve"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("conflict_blocked"))
                .andExpect(jsonPath("$.conflicts").isArray());
    }

    @Test
    @DisplayName("非法状态转换：DRAFT 直接 deprecate 返回 409")
    void illegalTransitionEndToEnd() throws Exception {
        String id = "pol_e2e_003";
        mockMvc.perform(post("/api/v1/policies")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(policyBody(id, "非法转换", "GRANT", List.of("regional_manager"))))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/api/v1/policies/" + id + "/deprecate"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("illegal_state"));
    }

    @Test
    @DisplayName("变量引用失效：validate 预览返回 ERROR 报告")
    void validatePreviewEndToEnd() throws Exception {
        String body = """
                {
                  "policyId": "pol_e2e_004",
                  "policyName": "变量失效",
                  "effect": "GRANT",
                  "subjectType": "role",
                  "subjectIds": ["regional_manager"],
                  "resourceId": "sdm.sales_order",
                  "actions": ["select"],
                  "constraints": {
                    "visible": "CONDITIONAL",
                    "rowRules": [{"column": "region", "op": "EQ", "ref": "subject.unknownAttr"}]
                  }
                }
                """;
        mockMvc.perform(post("/api/v1/policies")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated());

        mockMvc.perform(post("/api/v1/policies/pol_e2e_004/validate"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.blocked").value(true))
                .andExpect(jsonPath("$.conflicts[0].type").value("VAR_REF_INVALID"));
    }

    @Test
    @DisplayName("DRAFT 删除：DELETE 返回 204")
    void deleteDraftEndToEnd() throws Exception {
        String id = "pol_e2e_005";
        mockMvc.perform(post("/api/v1/policies")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(policyBody(id, "待删", "GRANT", List.of("regional_manager"))))
                .andExpect(status().isCreated());
        mockMvc.perform(delete("/api/v1/policies/" + id))
                .andExpect(status().isNoContent());
        mockMvc.perform(get("/api/v1/policies/" + id))
                .andExpect(status().isNotFound());
    }
}
