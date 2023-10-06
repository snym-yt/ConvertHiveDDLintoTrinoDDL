CREATE TABLE tmp.table(
  column_name1 INT,
  column_name2 VARCHAR,
  dt VARCHAR
)
WITH(
  partitioned_by= ARRAY['dt'],
  format = 'ORC',
  bucketed by = ARRAY['aaa'],
  sorted by = ARRAY['xxx'],
  bucket_count = 24,
  'transactional'='false'
);