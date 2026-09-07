package com.dcp.module.execution;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/** 统计报表 SQL 聚合（F-603） */
@Mapper
public interface StatsMapper {

    @Select("SELECT COUNT(*) FROM job_execution_history WHERE start_time >= CURDATE()")
    long todayExecutions();

    @Select("SELECT COUNT(*) FROM job_execution_history WHERE status = 'SUCCESS' AND start_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR)")
    long success24h();

    @Select("SELECT COUNT(*) FROM job_execution_history WHERE start_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR)")
    long total24h();

    @Select("SELECT COUNT(*) FROM job_execution_history WHERE status = 'RUNNING'")
    long runningCount();

    @Select("SELECT COUNT(*) FROM job_definition")
    long jobCount();

    @Select("SELECT COUNT(*) FROM datasource_config")
    long datasourceCount();

    @Select("""
            SELECT DATE_FORMAT(start_time, '%Y-%m-%d') AS day,
                   COUNT(*) AS total,
                   SUM(status = 'SUCCESS') AS success
            FROM job_execution_history
            WHERE start_time >= DATE_SUB(CURDATE(), INTERVAL #{days} DAY)
            GROUP BY DATE_FORMAT(start_time, '%Y-%m-%d')
            ORDER BY day
            """)
    List<Map<String, Object>> trend(int days);

    @Select("""
            SELECT j.job_name AS jobName,
                   COUNT(*) AS total,
                   SUM(h.status = 'SUCCESS') AS success,
                   SUM(h.status = 'FAILED') AS failed,
                   COALESCE(SUM(h.target_count), 0) AS totalRows
            FROM job_execution_history h
            JOIN job_definition j ON j.id = h.job_id
            GROUP BY j.job_name
            ORDER BY total DESC
            LIMIT #{limit}
            """)
    List<Map<String, Object>> taskRank(int limit);
}
