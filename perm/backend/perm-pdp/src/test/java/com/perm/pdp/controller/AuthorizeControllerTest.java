package com.perm.pdp.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.perm.pdp.service.PdpService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.List;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端测试（Spring Boot 全栈 + MockMvc）
 * 覆盖：判定 REST 全链路 / 缺参 fail-closed / 管理面快照与审计
 */
@SpringBootTest
@AutoConfigureMockMvc
class AuthorizeControllerTest {

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private PdpService pdpService;

    private String regionManagerBody() {
        return """
                {
                  "userId": "lisi",
                  "roles": ["regional_manager"],
                  "attributes": {"region": "华东", "allowedStatus": ["PAID", "SHIPPED"]},
                  "resourceId": "sdm.sales_order",
                  "action": "select",
                  "dialect": "mysql"
                }
                """;
    }

    @Test
    @DisplayName("端到端：PERMIT 判定 → 模板无值 + 签名 + 管理面审计可见")
    void authorizePermitEndToEnd() throws Exception {
        MvcResult result = mockMvc.perform(post("/api/v1/authorize")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(regionManagerBody()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.decision").value("PERMIT"))
                .andExpect(jsonPath("$.rowPredicateTemplate").isNotEmpty())
                .andExpect(jsonPath("$.signature").isNotEmpty())
                .andExpect(jsonPath("$.ts").isNumber())
                .andExpect(jsonPath("$.placeholders").isArray())
                .andExpect(jsonPath("$.masks[0].column").value("amount"))
                .andReturn();

        // 响应可反序列化且验签通过（PEP 视角）
        var dto = objectMapper.readValue(result.getResponse().getContentAsString(),
                com.perm.pdp.dto.ArtifactResponseDto.class);
        org.junit.jupiter.api.Assertions.assertTrue(pdpService.verify(dto), "端到端响应验签通过");

        // 审计义务已落
        mockMvc.perform(get("/api/v1/admin/audit"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(org.hamcrest.Matchers.greaterThanOrEqualTo(1)));
    }

    @Test
    @DisplayName("端到端：DENY（黑名单）→ denyReason 非空、无行谓词")
    void authorizeDenyEndToEnd() throws Exception {
        String body = regionManagerBody().replace("\"lisi\"", "\"zhangsan\"");
        mockMvc.perform(post("/api/v1/authorize")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.decision").value("DENY"))
                .andExpect(jsonPath("$.denyReason").isNotEmpty())
                .andExpect(jsonPath("$.rowPredicateTemplate").doesNotExist());
    }

    @Test
    @DisplayName("fail-closed：缺 resourceId 返回 400")
    void missingResourceIdRejected() throws Exception {
        Map<String, Object> body = Map.of(
                "userId", "lisi",
                "action", "select");
        mockMvc.perform(post("/api/v1/authorize")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("管理面：快照版本与策略清单")
    void snapshotEndpoint() throws Exception {
        mockMvc.perform(get("/api/v1/admin/snapshot"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.version").value(1))
                .andExpect(jsonPath("$.policyCount").value(4))
                .andExpect(jsonPath("$.policyIds").isArray());
    }

    @Test
    @DisplayName("标签模式 A：tagScope 列表下推经 REST 完整返回")
    void tagScopeEndToEnd() throws Exception {
        String body = """
                {
                  "userId": "zhaoliu",
                  "roles": ["marketing_analyst"],
                  "tagLevels": {"业务域/客户价值/高价值": 3},
                  "resourceId": "sdm.sales_order",
                  "action": "select",
                  "dialect": "mysql"
                }
                """;
        mockMvc.perform(post("/api/v1/authorize")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.decision").value("PERMIT"))
                .andExpect(jsonPath("$.tagScope.expandMode").value("LIST"))
                .andExpect(jsonPath("$.tagScope.targetKey").value("customer_id"))
                .andExpect(jsonPath("$.tagScope.entities").isArray())
                .andExpect(jsonPath("$.tagScope.entities.length()").value(5));
    }
}
