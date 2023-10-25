CREATE TABLE rise.prefecture
(
    `db_name` STRING,
    `prefecture_id` BIGINT,
    `name` STRING,
    `name_kana` STRING
)
PARTITIONED BY (
    `dt` STRING
)
  STORED AS ORC
  TBLPROPERTIES ('transactional'='false')
;