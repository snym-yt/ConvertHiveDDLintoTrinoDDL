import re
import sys
 
# <Prerequisites>***************************************************************
# (1) One semicolon per query.
# (2) CREATE TABLE always includes "TBLPROPERTIES ('transactional'='false');".
# (3) Don"t use for external table.
# (4) "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'" is not supported
# (5) Structures, Array and Map data types are not supported.
# ******************************************************************************
 
input_path = "./hive_input/use.hql"
filename = re.findall(r'input/(\S+).hql', input_path)
output_path = "./trino_output/" + filename[0] + ".sql"
 
# Hive's data type + VARCHAR(Trino' data type)
DATA_TYPE_LIST = ["TINYINT", "SMALLINT", "INT", "BIGINT", "BOOLEAN", "FLOAT", "DOUBLE", "STRING", "VARCHAR"]
 
column_name_list = []
 
def hive_to_trino_ddl():
 
    # read file as string
    with open(input_path) as f:
        hive_ddl = f.read()
 
    # adjust hive create format
    hive_ddl = format_create_hql(hive_ddl)
 
    # カラム名とデータ型のタプルをリストとして格納
    columns = re.findall(r'(\S+) +(\S+)', hive_ddl)

    if (columns == []):
        hive_ddl = format_func_use_explain (hive_ddl)
        columns = re.findall(r'(\S+) +(\S+)', hive_ddl)
 
    print(columns)
 
    trino_ddl = f""
 
    # DDL CREATE
    if (columns[0][0].upper() == 'CREATE'):
        trino_ddl = convert_create(hive_ddl, columns, trino_ddl)
 
    # SHOW TABLES IN itemx LIKE
    elif (columns[0][0].upper() == 'SHOW') and (columns[0][1].upper() == 'TABLES'):
        trino_ddl += "SHOW TABLES FROM " + columns[1][1] + " LIKE " + columns[2][1].rstrip(";").replace('*', '%') + ";\n"
     
    # SHOW PARTITIONS
    elif (columns[0][0].upper() == 'SHOW') and (columns[0][1].upper() == 'PARTITIONS'):
        db = re.findall(r'(\S+)[.]', hive_ddl)
        table = re.findall(r'[.](\S+)', hive_ddl)
        trino_ddl += "SELECT * FROM " + db[0] + '."' + table[0] + '$partitions"'
     
    # describe
    elif (columns[0][0].upper() == 'DESC') or (columns[0][0].upper() == 'DESCRIBE'):
        trino_ddl += "SHOW COLUMNS FROM " + columns[0][1].rstrip(';') + ";\n"
     
    # function, use, explain
    elif (len(columns) <= 1):
        if ("FUNCTION" in columns[0][1].upper()) or ("USE" in columns[0][0].upper()) or ("EXPLAIN" in columns[0][0].upper()):
            trino_ddl += columns[0][0] + " " + columns[0][1].rstrip(';') + ";\n"

        else:
            sys.exit(f"Error: Used DDL is not supported or format is not appropriate.\n " + hive_ddl)
 
    else:
        sys.exit(f"Error: Used DDL is not supported. " + columns[0][0])
 
    with open(output_path, mode='w') as fout:
        fout.write(trino_ddl)
 
    return trino_ddl
 
 
def convert_create(hive_ddl, columns, trino_ddl):
 
    column_name_list = []
 
    # if read the last column or CREATE with LIKE, increment this flag
    last_column_flag = 0
 
    # LIKEだとCREATE TABELの後に”("がないので分岐
    if ("LIKE" in columns[1][0].upper()):
        table_name = re.findall(r'(\w{6}) (\w{5}) (\S+)', hive_ddl)[0][2]
        last_column_flag = 1
    else:
        table_name = columns[1][0]
 
    trino_ddl = f"CREATE TABLE " + table_name + "(\n"
 
    for column in columns:
 
        # TBLPROPERTIES ('transactional'='false')
        if ('TBLPROPERTIES' in column[0].upper()) and last_column_flag == 1:
            trino_ddl += "  " + column[1].rstrip(';').lstrip('(').rstrip(')') + "\n"
 
        # CLUSTERED BY
        elif ('CLUSTERED' in column[0].upper()) and last_column_flag == 1:
            trino_ddl = convert_clustered(hive_ddl, trino_ddl)
 
        # SORTED BY
        elif ('SORTED' in column[0].upper()) and last_column_flag == 1:
            trino_ddl = convert_sorted(hive_ddl, trino_ddl)
 
        # INTO x BUSCKETS
        elif ('INTO' in column[0].upper()) and last_column_flag == 1:
            trino_ddl += "  " + "bucket_count = " + column[1] + ",\n"
 
        # ( xxx )
        elif ('(' in column[0]):
            # do nothing
            trino_ddl += ''
 
        # PARTITIONED BY(dt string)
        elif ('PARTITIONED' in column[0].upper()) and last_column_flag == 1:
            trino_ddl += "  partitioned_by= ARRAY['dt'],\n"
            trino_ddl = convert_partitioned(hive_ddl, trino_ddl)
 
         # partitioned by のdt用       
        elif ('dt' in column[0]) and last_column_flag == 1:
            # do nothing
            trino_ddl += ''
 
        # LIKE db.source_table
        elif ('LIKE' in column[0].upper()) and last_column_flag == 1:
            trino_ddl += "  " + column[0] + " " + column[1] + "\n)\nWITH(\n"
 
        # STORED AS ORC
        elif ('STORED' in column[0].upper()):
            trino_ddl += "  format = 'ORC',\n"
             
        elif ('TABLE' not in column[1].upper()) and ('(' not in column[1]):
            result_convert_column = convert_column(hive_ddl, trino_ddl, column, last_column_flag)
            trino_ddl = result_convert_column[0]
            last_column_flag = result_convert_column[1]
 
    trino_ddl += ");"
 
    return trino_ddl
 
def convert_clustered(hive_ddl, trino_ddl):
    match = re.search(r'CLUSTERED +BY +\(\s*([^)]+)\s*\)', hive_ddl, re.IGNORECASE)
    if match:
        clustered_by_value = match.group(1).strip()
        if clustered_by_value:
            trino_ddl += f"  bucketed by = ARRAY['{clustered_by_value}'],\n"
        else:
            sys.exit("Error: dt string was not found.(CLUSTERED)")
    else:
        sys.exit("Error: CLUSTERED format was not suitable for this tool.")
 
    return trino_ddl
 
def convert_sorted(hive_ddl, trino_ddl):
    match = re.search(r'SORTED +BY +\(\s*([^)]+)\s*\)', hive_ddl, re.IGNORECASE)
    if match:
        sorted_by_value = match.group(1).strip()
        if sorted_by_value:
            trino_ddl += f"  sorted by = ARRAY['{sorted_by_value}'],\n"
        else:
            sys.exit("Error: string was not found.(SORTED)")
    else:
        sys.exit("Error: SORTED format was not suitable for this tool.")
 
    return trino_ddl
 
def convert_partitioned(hive_ddl, trino_ddl):
    match = re.search(r'PARTITIONED +BY +\(\s*([^)]+)\s*\)', hive_ddl, re.IGNORECASE)
    if match:
        partitioned_by_value = match.group(1).strip()
        if partitioned_by_value:
            index = trino_ddl.find('\n)')
            data_type = f"{partitioned_by_value}"
            data_type = data_type.upper().replace('DT', 'dt').replace('STRING', 'VARCHAR')
            trino_ddl = trino_ddl[:index+1] + "  " + data_type + trino_ddl[index:]
        else:
            sys.exit("Error: string was not found.(PARTITIONED)")
    else:
        sys.exit("Error: PARTITIONED format was not suitable for this tool.")
 
    return trino_ddl
     
def convert_column(hive_ddl, trino_ddl, column, last_column_flag):
    column_name = column[0]
    if (column_name not in column_name_list):
        column_name_list.append(column_name)
    else:
        sys.exit(f"Error: Duplicate column name. ({column_name})")
 
    data_type = column[1].upper().replace('STRING', 'VARCHAR')
 
    # last column
    # 最後のカラムには","がなく、partitionedの"dt string"とも分ける
    if (',' not in column[1]) and ('dt' not in column[0]) and (last_column_flag == 0):
        match = re.search(r'partitioned', hive_ddl, flags=re.IGNORECASE)
        # judge whether partitioned is included (yes -> need ",", no -> not need)
        if match:
            # judge whether data type is correct
            if (data_type.upper() in DATA_TYPE_LIST):
                trino_ddl += f"  {column_name} {data_type},\n" + ")\nWITH(\n"
                last_column_flag = 1
            else:
                sys.exit(f"Error: Data type ({data_type}) is not supported by Hive. {column_name}")
        else:
            # judge whether data type is correct
            if (data_type.upper() in DATA_TYPE_LIST):
                trino_ddl += f"  {column_name} {data_type}\n" + ")\nWITH(\n"
                last_column_flag = 1
            else:
                sys.exit(f"Error: Data type ({data_type}) is not supported by Hive. {column_name}")
             
    # column without last one
    elif ('dt' not in column[0]) and (last_column_flag == 0):
        # judge whether data type is correct
        if (data_type.upper().rstrip(',') in DATA_TYPE_LIST):
            trino_ddl += f"  {column_name} {data_type}\n"
        else:
            data_type = data_type.rstrip(",")
            sys.exit(f"Error: Data type ({data_type}) is not supported by Hive")
     
    else:
        sys.exit("Error: format or spel is wrong. " + column[0] + " " + column[1])
 
    return trino_ddl, last_column_flag
 
# Adjust comma formatting
def format_create_hql(hive_ddl):
    pattern = r'CREATE +TABLE +([^\s]+)\s+\(([\s\S]+?)\)\s*([\s\S]+?);'
    matches = re.finditer(pattern, hive_ddl, re.IGNORECASE)
 
    formatted_hql = ""
 
    match = None
 
    for match in matches:
        table_name = match.group(1)
        table_body = match.group(2)
        table_property = match.group(3)
 
        # insert newline after comma
        formatted_table_body = re.sub(r',\s*', ',\n', table_body)
 
        # delete space character before comma
        formatted_table_body = re.sub(r'\s*(?=\S),', ',', formatted_table_body)
 
        formatted_hql += f"CREATE TABLE {table_name} (\n{formatted_table_body}\n)\n{table_property};\n\n"
 
    # don't match and return pure hive_ddl
    if match is None:
        return hive_ddl
 
    return formatted_hql
 

def format_func_use_explain (hive_ddl):
    pattern = r'([^\s]+)\s+([\s\S]+?\s*;)'
    matches = re.finditer(pattern, hive_ddl)

    formatted_hql = ""

    match = None
    for match in matches:
        first = match.group(1)
        second = match.group(2)
        formatted_hql += f"{first} {second};"
    
    if match is None:
        return hive_ddl
    
    return formatted_hql
 
# convert
trino_ddl = hive_to_trino_ddl()
print(trino_ddl)