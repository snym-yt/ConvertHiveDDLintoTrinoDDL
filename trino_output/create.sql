CREATE TABLE tmp.table(
  column_name1 INT,
  column_name2 string,
  dt VARCHAR
)
WITH(
  partitioned_by = ARRAY['dt'],
  format = 'ORC'
);