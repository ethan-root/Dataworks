CREATE TABLE IF NOT EXISTS feature_d_test(
`name`                          STRING COMMENT '',
`age`                           STRING COMMENT '',
`location`                      STRING COMMENT ''
)
COMMENT 'null'
PARTITIONED BY (pt STRING) 
lifecycle 36500;
