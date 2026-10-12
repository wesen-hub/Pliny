package com.perm.pdp.controller;

import com.perm.pdp.dto.ArtifactResponseDto;
import com.perm.pdp.dto.AuthorizeRequestDto;
import com.perm.pdp.service.PdpService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * 判定接口（PEP → PDP）
 * POST /api/v1/authorize：返回渲染后模板无值产物（铁律 C1）+ HMAC 签名（R-Sec1）
 */
@RestController
@RequestMapping("/api/v1")
public class AuthorizeController {

    private final PdpService pdpService;

    public AuthorizeController(PdpService pdpService) {
        this.pdpService = pdpService;
    }

    @PostMapping("/authorize")
    public ArtifactResponseDto authorize(@RequestBody AuthorizeRequestDto request) {
        if (request.getResourceId() == null || request.getAction() == null || request.getUserId() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "resourceId/action/userId 必填（fail-closed：缺参拒绝）");
        }
        return pdpService.authorize(request);
    }
}
