CREATE TABLE IF NOT EXISTS github_test (
    `col1`  STRING          COMMENT '*',
    `col2`  STRING          COMMENT '*',
    `col3`  DECIMAL(38,18)  COMMENT '*',
    `col4`  TIMESTAMP       COMMENT '*',
    `col5`  BIGINT          COMMENT '*',
    `col6`  DECIMAL(38,18)  COMMENT '*'
)
COMMENT 'null'
PARTITIONED BY (pt STRING)
LIFECYCLE 36500;
