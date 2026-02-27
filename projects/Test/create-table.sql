CREATE TABLE IF NOT EXISTS github_test_2(
`name`                          STRING COMMENT '',
`age`                           STRING COMMENT '',
`location`                      STRING COMMENT ''
)
COMMENT 'null'
PARTITIONED BY (pt STRING) 
lifecycle 36500;
