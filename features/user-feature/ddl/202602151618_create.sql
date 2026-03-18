CREATE TABLE IF NOT EXISTS user_demo(
`user_id`                       STRING COMMENT '',
`user_name`                     STRING COMMENT ''
)
COMMENT 'user demo table'
PARTITIONED BY (pt STRING) 
lifecycle 36500;
