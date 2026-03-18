-- database_changelog: DDL execution metadata table
-- 每个环境都需要此表来记录 DDL 文件的执行状态
-- Reference: 设计文档 §5.3

CREATE TABLE IF NOT EXISTS database_changelog
(
    table_name STRING COMMENT '业务表名',
    ddl_file   STRING COMMENT 'DDL SQL 文件名（如 202602141515_create.sql）',
    applied_at DATETIME COMMENT 'DDL 执行时间',
    hashcode   STRING COMMENT 'SQL 文件内容的 MD5 hash 值，用于幂等性校验',
    status     STRING COMMENT '执行状态：SUCCESS / FAILED'
)
COMMENT 'DDL execution metadata per environment'
LIFECYCLE 36500;
