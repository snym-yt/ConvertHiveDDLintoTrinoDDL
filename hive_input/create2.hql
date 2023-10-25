CREATE TABLE db.table (
  column_name int,
  column_name int
)
PARTITIONED BY (
  dt string
)
CLUSTERED BY ( xxx )
INTO 24 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='false');