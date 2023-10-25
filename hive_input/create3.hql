create TABLE db.table (
  column_name1 INT,
  column_name2 STRING
)
STORED AS ORC
CLUSTERED BY ( xxx )
SORTED BY ( xxx )
INTO 24 BUCKETS
TBLPROPERTIES ('transactional'='false');