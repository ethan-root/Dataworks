CREATE TABLE IF NOT EXISTS feature_demo_final_03(
    `name`                          STRING COMMENT '',
    `age`                           STRING COMMENT '',
    `location`                      STRING COMMENT '',
    `id`                            STRING COMMENT '',
    `user_name`                     STRING COMMENT '',
    `gender`                        STRING COMMENT '',
    `address`                       STRING COMMENT '',
    `email`                         STRING COMMENT ''
)
COMMENT 'feature_demo_final_03 regression test table'
PARTITIONED BY (pt STRING);
