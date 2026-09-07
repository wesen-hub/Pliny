package com.dcp.config;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.dcp.module.datasource.entity.DatasourceConfig;
import com.dcp.module.datasource.mapper.DatasourceConfigMapper;
import com.dcp.module.dict.entity.DictConfig;
import com.dcp.module.dict.entity.DictEntry;
import com.dcp.module.dict.mapper.DictConfigMapper;
import com.dcp.module.dict.mapper.DictEntryMapper;
import com.dcp.module.job.entity.ExtractConfig;
import com.dcp.module.job.entity.FieldMapping;
import com.dcp.module.job.entity.JobDefinition;
import com.dcp.module.job.mapper.ExtractConfigMapper;
import com.dcp.module.job.mapper.FieldMappingMapper;
import com.dcp.module.job.mapper.JobDefinitionMapper;
import com.dcp.module.system.entity.SysRole;
import com.dcp.module.system.entity.SysUser;
import com.dcp.module.system.entity.SysUserRole;
import com.dcp.module.system.mapper.SysRoleMapper;
import com.dcp.module.system.mapper.SysUserMapper;
import com.dcp.module.system.mapper.SysUserRoleMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 首次启动种子数据：角色 / 用户 / 演示字典 / 演示数据源 / 演示任务
 * （幂等：已存在则跳过）
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DataInitializer implements CommandLineRunner {

    private final SysRoleMapper roleMapper;
    private final SysUserMapper userMapper;
    private final SysUserRoleMapper userRoleMapper;
    private final PasswordEncoder passwordEncoder;
    private final DictConfigMapper dictMapper;
    private final DictEntryMapper dictEntryMapper;
    private final DatasourceConfigMapper datasourceMapper;
    private final JobDefinitionMapper jobMapper;
    private final ExtractConfigMapper extractMapper;
    private final FieldMappingMapper mappingMapper;

    @Value("${dcp.enc-key}")
    private String encKey;

    @Override
    public void run(String... args) {
        if (userMapper.selectCount(new LambdaQueryWrapper<SysUser>().eq(SysUser::getUsername, "admin")) > 0) {
            log.info("种子数据已存在, 跳过初始化");
            return;
        }
        log.info("初始化种子数据...");

        // 角色
        SysRole admin = role("ADMIN", "管理员", "全部权限");
        SysRole operator = role("OPERATOR", "运维人员", "数据源/任务/字典/监控读写");
        SysRole viewer = role("VIEWER", "只读用户", "只读");
        SysRole auditor = role("AUDITOR", "审计员", "审计只读");

        // 用户
        user("admin", "admin123", "系统管理员", List.of(admin));
        user("operator", "operator123", "运维人员", List.of(operator));
        user("viewer", "viewer123", "访客", List.of(viewer));

        // 演示字典
        DictConfig orderStatus = new DictConfig();
        orderStatus.setDictName("ORDER_STATUS");
        orderStatus.setDescription("订单状态字典");
        dictMapper.insert(orderStatus);
        entry(orderStatus.getId(), "0", "待支付", 1);
        entry(orderStatus.getId(), "1", "已支付", 2);
        entry(orderStatus.getId(), "2", "已取消", 3);
        entry(orderStatus.getId(), "3", "已发货", 4);

        // 演示数据源（指向 demo-db 容器，3306 为容器内端口）
        Long srcId = datasource("演示源库-MySQL", "mysql", "demo-db", 3306, "source_db", "root", "root123", "演示");
        Long tgtId = datasource("演示目标库-MySQL", "mysql", "demo-db", 3306, "target_db", "root", "root123", "演示");

        // 演示任务：订单增量同步（含字典 + 表达式转换链）
        JobDefinition job = new JobDefinition();
        job.setJobName("订单增量同步演示");
        job.setSourceDatasourceId(srcId);
        job.setTargetDatasourceId(tgtId);
        job.setTargetTableName("dw_order");
        job.setSyncMode("incremental");
        job.setIncrementalField("id");
        job.setCronExpr(null);
        job.setMisfirePolicy("DO_NOTHING");
        job.setEnabled(1);
        job.setStatus("READY");
        job.setTimeoutMinutes(30);
        job.setParallelism(2);
        job.setRetryTimes(2);
        job.setRetryIntervalSeconds(10);
        job.setAlarmEnabled(1);
        job.setDescription("演示: 增量两段式 + 字典转换(状态) + Aviator 表达式(日期/金额)");
        job.setCreateBy("admin");
        jobMapper.insert(job);

        ExtractConfig extract = new ExtractConfig();
        extract.setJobId(job.getId());
        extract.setSourceType("table");
        extract.setContent("t_order");
        extract.setPreviewLimit(100);
        extractMapper.insert(extract);

        mapping(job.getId(), "id", "id", "direct", null, 1);
        mapping(job.getId(), "user_id", "userId", "direct", null, 2);
        mapping(job.getId(), "price", "price", "direct", null, 3);
        mapping(job.getId(), "quantity", "quantity", "direct", null, 4);
        mapping(job.getId(), "discount", "discount", "direct", null, 5);
        mapping(job.getId(), "status", "statusDesc", "dictionary", "ORDER_STATUS", 6);
        mapping(job.getId(), "created_at", "orderDate", "expression", "date.format(created_at, 'yyyy-MM-dd')", 7);
        mapping(job.getId(), "total_amount", "totalAmount", "expression", "price * quantity * (1 - discount)", 8);

        // 演示 MongoDB 数据源 + 用户同步任务（MongoDB -> MySQL, 全量; Mongo 不支持 SQL 位点）
        Long mongoId = datasource("演示源库-MongoDB", "mongodb", "mongo", 27017, "app_db", "root", "root123", "演示");
        JobDefinition mongoJob = new JobDefinition();
        mongoJob.setJobName("用户同步演示(MongoDB->MySQL)");
        mongoJob.setSourceDatasourceId(mongoId);
        mongoJob.setTargetDatasourceId(tgtId);
        mongoJob.setTargetTableName("mongo_users");
        mongoJob.setSyncMode("full");
        mongoJob.setCronExpr(null);
        mongoJob.setMisfirePolicy("DO_NOTHING");
        mongoJob.setEnabled(1);
        mongoJob.setStatus("READY");
        mongoJob.setTimeoutMinutes(30);
        mongoJob.setParallelism(2);
        mongoJob.setRetryTimes(2);
        mongoJob.setRetryIntervalSeconds(10);
        mongoJob.setAlarmEnabled(1);
        mongoJob.setDescription("演示: MongoDB users 集合 -> MySQL mongo_users 表(全量, REPLACE INTO 幂等)");
        mongoJob.setCreateBy("admin");
        jobMapper.insert(mongoJob);
        ExtractConfig mongoExtract = new ExtractConfig();
        mongoExtract.setJobId(mongoJob.getId());
        mongoExtract.setSourceType("table");
        mongoExtract.setContent("users");
        mongoExtract.setPreviewLimit(100);
        extractMapper.insert(mongoExtract);
        mapping(mongoJob.getId(), "_id", "id", "direct", null, 1);
        mapping(mongoJob.getId(), "name", "name", "direct", null, 2);
        mapping(mongoJob.getId(), "age", "age", "direct", null, 3);
        mapping(mongoJob.getId(), "email", "email", "direct", null, 4);
        mapping(mongoJob.getId(), "created_at", "created_at", "direct", null, 5);
        mapping(mongoJob.getId(), "status", "status", "direct", null, 6);

        log.info("种子数据初始化完成, 默认账号 admin/admin123");
    }

    private SysRole role(String code, String name, String desc) {
        SysRole r = new SysRole();
        r.setRoleCode(code);
        r.setRoleName(name);
        r.setDescription(desc);
        roleMapper.insert(r);
        return r;
    }

    private Long user(String username, String password, String nickname, List<SysRole> roles) {
        SysUser u = new SysUser();
        u.setUsername(username);
        u.setPasswordHash(passwordEncoder.encode(password));
        u.setNickname(nickname);
        u.setStatus(1);
        userMapper.insert(u);
        for (SysRole r : roles) {
            SysUserRole ur = new SysUserRole();
            ur.setUserId(u.getId());
            ur.setRoleId(r.getId());
            userRoleMapper.insert(ur);
        }
        return u.getId();
    }

    private void entry(Long dictId, String src, String tgt, int sort) {
        DictEntry e = new DictEntry();
        e.setDictId(dictId);
        e.setSourceValue(src);
        e.setTargetValue(tgt);
        e.setSortNo(sort);
        dictEntryMapper.insert(e);
    }

    private Long datasource(String name, String type, String host, int port, String db,
                            String user, String pass, String group) {
        DatasourceConfig ds = new DatasourceConfig();
        ds.setName(name);
        ds.setType(type);
        ds.setHost(host);
        ds.setPort(port);
        ds.setDatabaseName(db);
        ds.setUsername(user);
        ds.setPasswordCiphertext(com.dcp.common.CryptoUtil.encrypt(pass, encKey));
        ds.setKeyVersion(1);
        ds.setGroupName(group);
        ds.setEnv("demo");
        ds.setEnabled(1);
        ds.setCreateBy("system");
        datasourceMapper.insert(ds);
        return ds.getId();
    }

    private void mapping(Long jobId, String src, String tgt, String type, String rule, int sort) {
        FieldMapping m = new FieldMapping();
        m.setJobId(jobId);
        m.setSourceField(src);
        m.setTargetField(tgt);
        m.setTransformType(type);
        m.setTransformRule(rule);
        m.setSortNo(sort);
        mappingMapper.insert(m);
    }
}
