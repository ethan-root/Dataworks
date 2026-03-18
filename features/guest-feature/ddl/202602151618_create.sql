CREATE TABLE IF NOT EXISTS guest_demo(
`guest_id`                      STRING COMMENT '',
`guest_name`                    STRING COMMENT ''
)
COMMENT 'guest demo table'
PARTITIONED BY (pt STRING) 
lifecycle 36500;
