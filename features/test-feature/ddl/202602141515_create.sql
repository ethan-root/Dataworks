CREATE TABLE IF NOT EXISTS feature_demo_final_01(
`name`                          STRING COMMENT '',
`age`                           STRING COMMENT '',
`location`                      STRING COMMENT ''
)
COMMENT 'null'
PARTITIONED BY (pt STRING) 
lifecycle 36500;
