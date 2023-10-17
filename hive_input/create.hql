create 
TABLE tmp.table (
  column_name1 INT,
  column_name2 string
)
PARTITIONED BY (dt STRING)
STORED AS ORC
CLUSTErED   BY  (
  aaa
)
SOrTEd  BY  (
  xxx
)
INTO 24 BUCKETS
TBLPROPERTIES ('transactional'='false');