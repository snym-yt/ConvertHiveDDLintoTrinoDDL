import re
import sys
 
# <Prerequisites>***************************************************************
# (1) One semicolon per query.
# (2) Don"t use for external table.
# (3) "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'" is not supported
# (4) Structures, Array and Map data types are not supported.
# ******************************************************************************
 
input_path = "./hive_input/show_tables.hql"
filename = re.findall(r'input/(\S+).hql', input_path)
output_path = "./trino_output/" + filename[0] + ".sql"
 
# Hive's data type + VARCHAR(Trino' data type)
DATA_TYPE_LIST = ["TINYINT", "SMALLINT", "INT", "BIGINT", "BOOLEAN", "FLOAT", "DOUBLE", "STRING", "VARCHAR"]
 
column_name_list = []

PATTERN_CREATE = r'CREATE\s+TABLE\s+([^\s]+)\s+'
PATTERN_SHOWTABLES = r'SHOW\s+TABLES\s+IN\s+itemx\s+LIKE\s+([^\s]+)\s*;'
PATTERN_SHOWPARTITIONS = r'SHOW\s+PARTITIONS\s+([^\s]+)\s*;'
PATTERN_LIKE = r'LIKE\s+([^\s]+)'
PATTERN_PARTITION = r'PARTITIONED\s+BY\s+\(\s*(\S+ +\S+)\s*\)'
PATTERN_WITH = r'WITH\s+\('
PATTERN_FORMAT = r'STORED\s+AS\s+(\S+)\s'
PATTERN_CLUSTERED = r'CLUSTERED\s+BY\s+\(\s*(\S+)\s*\)'
PATTERN_INTOBUCKETS = r'INTO\s+(\d+)\s+BUCKETS'
PATTERN_SORTED = r'SORTED\s+BY\s+\(\s*(\S+)\s*\)'

 
def hive_to_trino_ddl():

    trino_ddl = ""
 
    # read file as string
    with open(input_path) as f:
        hive_ddl = f.read()
 
    # adjust hive create format
    hive_ddl = format_create_hql(hive_ddl)

    # CREATE
    searches = determine_query(hive_ddl)
    # print(searches)

    if (searches == "CREATE"):
        table_name = re.findall(r'(\w{6})\s+(\w{5})\s+(\S+)\s', hive_ddl)[0][2]
        trino_ddl = f"CREATE TABLE " + table_name + "(\n"
        pattern = r'CREATE\s+TABLE\s+([^\s]+)\s+\('
        searches = None
        seraches = re.search(pattern, hive_ddl, re.IGNORECASE)
        # LIKEがない
        if (seraches != None):
            trino_ddl = convert_create(hive_ddl, trino_ddl)
        # LIKEあり(columnの指定がないためcolumn nameがLIKEになることがない)
        else:
            trino_ddl = convert_like(hive_ddl, trino_ddl)

        trino_ddl = convert_properties(hive_ddl, trino_ddl)
    elif (searches == "SHOW TABLES"):
        print("show tables")
 

    trino_ddl += "\n);"
 
    with open(output_path, mode='w') as fout:
        fout.write(trino_ddl)
 
    return trino_ddl


def convert_properties(hive_ddl, trino_ddl):
    # PARTITIONED
    searches = None
    searches = re.search(PATTERN_PARTITION, hive_ddl, re.IGNORECASE)
    if (searches != None):
        trino_ddl = convert_partitioned(hive_ddl, trino_ddl)

    # STORED AS
    searches = None
    searches = re.search(PATTERN_FORMAT, hive_ddl, re.IGNORECASE)
    if (searches != None):
        trino_ddl = convert_dataformat(hive_ddl, trino_ddl)

    # CLUSTERED 
    searches = None
    searches = re.search(PATTERN_CLUSTERED, hive_ddl, re.IGNORECASE)
    if (searches != None):
        trino_ddl = convert_clustered(hive_ddl, trino_ddl)

    # INTO BUCKETS 
    searches = None
    searches = re.search(PATTERN_INTOBUCKETS, hive_ddl, re.IGNORECASE)
    if (searches != None):
        trino_ddl = convert_INTOBUCKETS(hive_ddl, trino_ddl)

    # SORTED
    searches = None
    searches = re.search(PATTERN_SORTED, hive_ddl, re.IGNORECASE)
    if (searches != None):
        trino_ddl = convert_sorted(hive_ddl, trino_ddl)

    return trino_ddl

def convert_create(hive_ddl, trino_ddl):
 
    column_name_list = []

    pattern = r'CREATE\s+TABLE\s+([^\s]+)\s+\(([\s\S]+?)\)'
    matches = re.finditer(pattern, hive_ddl, re.IGNORECASE)

    # ()の中身を文字列で取得
    for match in matches:
        columns_string = match.group(2).replace("\n", "").replace(",", ", ")

    # カラム名とデータ型をタプルで格納してリストを作成
    columns = re.findall(r'(\S+)\s+(\S+)', columns_string)
 
    for column in columns:
        if (column[0] not in column_name_list):
            column_name_list.append(column[0])
            trino_ddl += "  " + column[0] + " " + column[1] + "\n"
        else:
            sys.exit(f"Error: Duplicate column name. ({column[0]})")
 
    return trino_ddl 

def convert_like(hive_ddl, trino_ddl):
    matches = None
    matches = re.finditer(PATTERN_LIKE, hive_ddl, re.IGNORECASE)

    if (matches == None):
        sys.exit(f"Error: LIKE format is wrong.")

    table_name = ""

    # LIKEの参照先を取得
    for match in matches:
        table_name = match.group(1)
    
    print(table_name)
    
    trino_ddl += "  LIKE " + table_name
 
    return trino_ddl 

def convert_partitioned(hive_ddl, trino_ddl):
    match = re.search(PATTERN_PARTITION, hive_ddl, re.IGNORECASE)
    partitioned_by_value = match.group(1).strip()
    if partitioned_by_value:
        index = trino_ddl.find('\n)')
        data_type = f"{partitioned_by_value}"
        data_type = data_type.upper().replace('DT', 'dt').replace('STRING', 'VARCHAR')
        trino_ddl = trino_ddl[:index] + ",\n  " + data_type + trino_ddl[index:]
    else:
        sys.exit("Error: string was not found.(PARTITIONED)")

    match = re.search(PATTERN_WITH, hive_ddl, re.IGNORECASE)
    # withがすでにある
    if match:
        trino_ddl += ",\n  partitioned_by = ARRAY['dt']"
    # withがまだない
    else:
        trino_ddl += ")\nWITH(\n  partitioned_by = ARRAY['dt']"
 
    return trino_ddl

def convert_dataformat(hive_ddl, trino_ddl):
    matches = re.finditer(PATTERN_FORMAT, hive_ddl, re.IGNORECASE)
    for match in matches:
        data_format = match.group(1)
    # withがすでにある
    if match:
        trino_ddl += f",\n  format = '{data_format}'"
    # withがまだない
    else:
        trino_ddl += f")\nWITH(\n  format = '{data_format}'"
 
    return trino_ddl

def convert_clustered(hive_ddl, trino_ddl):
    match = re.search(PATTERN_CLUSTERED, hive_ddl, re.IGNORECASE)
    clustered_by_value = match.group(1).strip()
    # withがすでにある
    if match:
        trino_ddl += f",\n  bucketed by = ARRAY['{clustered_by_value}']"
    # withがまだない
    else:
        trino_ddl += f")\nWITH(\n  bucketed by = ARRAY['{clustered_by_value}']"
 
    return trino_ddl

def convert_INTOBUCKETS(hive_ddl, trino_ddl):
    match = re.search(PATTERN_INTOBUCKETS, hive_ddl, re.IGNORECASE)
    bucket_count = match.group(1).strip()
    trino_ddl += f",\n  bucket_count = {bucket_count}"

    return trino_ddl
 
def convert_sorted(hive_ddl, trino_ddl):
    match = re.search(PATTERN_SORTED, hive_ddl, re.IGNORECASE)
    sorted_by_value = match.group(1).strip()
    # withがすでにある
    if match:
        trino_ddl += f",\n  sorted by = ARRAY['{sorted_by_value}']"
    # withがまだない
    else:
        trino_ddl += f")\nWITH(\n  sorted by = ARRAY['{sorted_by_value}']"
 
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
 
def determine_query(hive_ddl):
    searches = None
    searches = re.search(PATTERN_CREATE, hive_ddl, re.IGNORECASE)
    if (searches != None):
        return "CREATE"
    
    searches = re.search(PATTERN_SHOWTABLES, hive_ddl, re.IGNORECASE)
    if (searches != ""):
        return "SHOW TABLES"


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