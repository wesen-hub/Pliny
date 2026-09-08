package com.dcp.module.dict;

import com.dcp.common.ApiResponse;
import com.dcp.module.dict.entity.DictConfig;
import com.dcp.module.dict.entity.DictEntry;
import lombok.Data;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/dicts")
public class DictController {

    private final DictService service;

    public DictController(DictService service) {
        this.service = service;
    }

    @GetMapping
    public ApiResponse<Map<String, Object>> page(@RequestParam(defaultValue = "1") int page,
                                                 @RequestParam(defaultValue = "10") int pageSize,
                                                 @RequestParam(required = false) String keyword) {
        var p = service.page(page, pageSize, keyword);
        return ApiResponse.ok(Map.of("total", p.getTotal(), "list", p.getRecords()));
    }

    @GetMapping("/all")
    public ApiResponse<List<DictConfig>> all() {
        return ApiResponse.ok(service.all());
    }

    @PostMapping
    @PreAuthorize("@perm.has('dict:write')")
    public ApiResponse<Long> create(@RequestBody DictConfig cfg) {
        return ApiResponse.ok(service.create(cfg));
    }

    @PutMapping("/{id}")
    @PreAuthorize("@perm.has('dict:write')")
    public ApiResponse<Void> update(@PathVariable Long id, @RequestBody DictConfig cfg) {
        service.update(id, cfg);
        return ApiResponse.ok();
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("@perm.has('dict:write')")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        service.delete(id);
        return ApiResponse.ok();
    }

    // ---------- 条目 ----------

    @GetMapping("/{id}/entries")
    public ApiResponse<List<DictEntry>> entries(@PathVariable Long id) {
        return ApiResponse.ok(service.entries(id));
    }

    @PostMapping("/{id}/entries")
    @PreAuthorize("@perm.has('dict:write')")
    public ApiResponse<Long> createEntry(@PathVariable Long id, @RequestBody DictEntry entry) {
        return ApiResponse.ok(service.createEntry(id, entry));
    }

    @PutMapping("/entries/{entryId}")
    @PreAuthorize("@perm.has('dict:write')")
    public ApiResponse<Void> updateEntry(@PathVariable Long entryId, @RequestBody DictEntry entry) {
        service.updateEntry(entryId, entry);
        return ApiResponse.ok();
    }

    @DeleteMapping("/entries/{entryId}")
    @PreAuthorize("@perm.has('dict:write')")
    public ApiResponse<Void> deleteEntry(@PathVariable Long entryId) {
        service.deleteEntry(entryId);
        return ApiResponse.ok();
    }

    // ---------- 导入导出 ----------

    @Data
    public static class ImportRequest {
        private boolean overwrite;
    }

    @PostMapping("/{id}/import")
    @PreAuthorize("@perm.has('dict:write')")
    public ApiResponse<Integer> importCsv(@PathVariable Long id,
                                          @RequestParam("file") MultipartFile file,
                                          @RequestParam(defaultValue = "false") boolean overwrite) {
        return ApiResponse.ok(service.importCsv(id, file, overwrite));
    }

    @GetMapping("/{id}/export")
    public ResponseEntity<byte[]> export(@PathVariable Long id) {
        String csv = service.exportCsv(id);
        String name = "dict_" + id + ".csv";
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + name + "\"")
                .contentType(new MediaType("text", "csv", StandardCharsets.UTF_8))
                .body(csv.getBytes(StandardCharsets.UTF_8));
    }

    @GetMapping("/template")
    public ResponseEntity<byte[]> template() {
        String csv = "source,target\n0,待支付\n1,已支付\n";
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"dict_template.csv\"")
                .contentType(new MediaType("text", "csv", StandardCharsets.UTF_8))
                .body(csv.getBytes(StandardCharsets.UTF_8));
    }
}
