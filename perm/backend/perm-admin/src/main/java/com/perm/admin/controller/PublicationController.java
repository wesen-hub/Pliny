package com.perm.admin.controller;

import com.perm.admin.publisher.Publication;
import com.perm.admin.service.PolicyService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 发布历史接口
 */
@RestController
@RequestMapping("/api/v1/publications")
public class PublicationController {

    private final PolicyService policyService;

    public PublicationController(PolicyService policyService) {
        this.policyService = policyService;
    }

    @GetMapping
    public Map<String, Object> history() {
        List<Publication> publications = policyService.publications();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("total", publications.size());
        body.put("publications", publications);
        return body;
    }
}
