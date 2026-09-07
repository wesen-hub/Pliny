package com.dcp.module.dict;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.module.dict.entity.DictConfig;
import com.dcp.module.dict.entity.DictEntry;
import com.dcp.module.dict.mapper.DictConfigMapper;
import com.dcp.module.dict.mapper.DictEntryMapper;
import com.dcp.module.job.entity.FieldMapping;
import com.dcp.module.job.mapper.FieldMappingMapper;
import com.dcp.module.system.service.AuditService;
import com.dcp.security.SecurityUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class DictService {

    private final DictConfigMapper dictMapper;
    private final DictEntryMapper entryMapper;
    private final FieldMappingMapper fieldMappingMapper;
    private final AuditService auditService;

    // ---------- 字典定义 ----------

    public Page<DictConfig> page(int page, int pageSize, String keyword) {
        LambdaQueryWrapper<DictConfig> qw = new LambdaQueryWrapper<>();
        qw.like(keyword != null && !keyword.isBlank(), DictConfig::getDictName, keyword)
          .orderByDesc(DictConfig::getId);
        return dictMapper.selectPage(Page.of(page, pageSize), qw);
    }

    public List<DictConfig> all() {
        return dictMapper.selectList(new LambdaQueryWrapper<DictConfig>().orderByDesc(DictConfig::getId));
    }

    public Long create(DictConfig cfg) {
        if (dictMapper.selectCount(new LambdaQueryWrapper<DictConfig>()
                .eq(DictConfig::getDictName, cfg.getDictName())) > 0) {
            throw new BizException(ErrorCode.PARAM_ERROR, "字典名称已存在: " + cfg.getDictName());
        }
        cfg.setId(null);
        cfg.setCreateBy(SecurityUtils.currentUsername());
        dictMapper.insert(cfg);
        auditService.record("dict", "CREATE", "dict", cfg.getId(), "创建字典: " + cfg.getDictName());
        return cfg.getId();
    }

    public void update(Long id, DictConfig req) {
        DictConfig cfg = dictMapper.selectById(id);
        if (cfg == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "字典不存在");
        }
        req.setId(id);
        req.setCreateBy(null);
        req.setCreateTime(null);
        req.setUpdateBy(SecurityUtils.currentUsername());
        dictMapper.updateById(req);
        auditService.record("dict", "UPDATE", "dict", id, "更新字典: " + req.getDictName());
    }

    public void delete(Long id) {
        DictConfig cfg = dictMapper.selectById(id);
        if (cfg == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "字典不存在");
        }
        long refs = fieldMappingMapper.selectCount(new LambdaQueryWrapper<FieldMapping>()
                .eq(FieldMapping::getTransformType, "dictionary")
                .eq(FieldMapping::getTransformRule, cfg.getDictName()));
        if (refs > 0) {
            throw new BizException(ErrorCode.DICT_REFERENCED, "字典被 " + refs + " 个字段映射引用, 无法删除");
        }
        dictMapper.deleteById(id);
        entryMapper.delete(new LambdaQueryWrapper<DictEntry>().eq(DictEntry::getDictId, id));
        auditService.record("dict", "DELETE", "dict", id, "删除字典: " + cfg.getDictName());
    }

    // ---------- 条目 ----------

    public List<DictEntry> entries(Long dictId) {
        return entryMapper.selectList(new LambdaQueryWrapper<DictEntry>()
                .eq(DictEntry::getDictId, dictId).orderByAsc(DictEntry::getSortNo));
    }

    public Long createEntry(Long dictId, DictEntry entry) {
        ensureDict(dictId);
        if (entryMapper.selectCount(new LambdaQueryWrapper<DictEntry>()
                .eq(DictEntry::getDictId, dictId).eq(DictEntry::getSourceValue, entry.getSourceValue())) > 0) {
            throw new BizException(ErrorCode.PARAM_ERROR, "源值已存在: " + entry.getSourceValue());
        }
        entry.setId(null);
        entry.setDictId(dictId);
        if (entry.getSortNo() == null) {
            entry.setSortNo(0);
        }
        entryMapper.insert(entry);
        auditService.record("dict", "CREATE", "dict_entry", entry.getId(), "新增条目: " + entry.getSourceValue());
        return entry.getId();
    }

    public void updateEntry(Long id, DictEntry entry) {
        DictEntry old = entryMapper.selectById(id);
        if (old == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "条目不存在");
        }
        entry.setId(id);
        entry.setDictId(old.getDictId());
        entryMapper.updateById(entry);
        auditService.record("dict", "UPDATE", "dict_entry", id, "更新条目: " + entry.getSourceValue());
    }

    public void deleteEntry(Long id) {
        entryMapper.deleteById(id);
        auditService.record("dict", "DELETE", "dict_entry", id, "删除条目");
    }

    private void ensureDict(Long dictId) {
        if (dictMapper.selectById(dictId) == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "字典不存在");
        }
    }

    // ---------- CSV 导入导出 ----------

    public int importCsv(Long dictId, MultipartFile file, boolean overwrite) {
        ensureDict(dictId);
        List<DictEntry> parsed = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            boolean first = true;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                if (first && line.toLowerCase().startsWith("source")) { first = false; continue; } // 跳过表头
                first = false;
                String[] parts = line.split(",", 2);
                if (parts.length < 2) continue;
                String src = parts[0].trim();
                String tgt = parts[1].trim();
                if (src.isEmpty()) continue;
                DictEntry e = new DictEntry();
                e.setDictId(dictId);
                e.setSourceValue(src);
                e.setTargetValue(tgt);
                e.setSortNo(0);
                parsed.add(e);
            }
        } catch (Exception e) {
            throw new BizException(ErrorCode.PARAM_ERROR, "CSV 解析失败: " + e.getMessage());
        }
        if (overwrite) {
            entryMapper.delete(new LambdaQueryWrapper<DictEntry>().eq(DictEntry::getDictId, dictId));
        }
        int inserted = 0;
        for (DictEntry e : parsed) {
            if (entryMapper.selectCount(new LambdaQueryWrapper<DictEntry>()
                    .eq(DictEntry::getDictId, dictId).eq(DictEntry::getSourceValue, e.getSourceValue())) > 0) {
                continue;
            }
            entryMapper.insert(e);
            inserted++;
        }
        auditService.record("dict", "IMPORT", "dict", dictId, "导入条目 " + inserted + " 条");
        return inserted;
    }

    public String exportCsv(Long dictId) {
        DictConfig cfg = dictMapper.selectById(dictId);
        if (cfg == null) {
            throw new BizException(ErrorCode.NOT_FOUND, "字典不存在");
        }
        StringBuilder sb = new StringBuilder("source,target\n");
        entries(dictId).forEach(e -> sb.append(e.getSourceValue()).append(',').append(e.getTargetValue()).append('\n'));
        return sb.toString();
    }

    /** 字典全量 map（供转换引擎使用） */
    public Map<String, Map<String, String>> loadDictMaps(List<String> dictNames) {
        Map<String, Map<String, String>> result = new java.util.LinkedHashMap<>();
        if (dictNames == null || dictNames.isEmpty()) {
            return result;
        }
        List<DictConfig> configs = dictMapper.selectList(new LambdaQueryWrapper<DictConfig>()
                .in(DictConfig::getDictName, dictNames));
        for (DictConfig cfg : configs) {
            Map<String, String> m = new java.util.LinkedHashMap<>();
            entries(cfg.getId()).forEach(e -> m.put(e.getSourceValue(), e.getTargetValue()));
            result.put(cfg.getDictName(), m);
        }
        return result;
    }
}
