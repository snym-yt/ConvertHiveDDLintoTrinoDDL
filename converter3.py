import re
import sys
 
# <Prerequisites>***************************************************************
# (1) One semicolon per query.
# (2) Don"t use for external table.
# (3) "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'" is not supported
# (4) LOCATION '/user/aaa/xxx'; is not supported
# (5) Structures, Array and Map data types are not supported.
# ******************************************************************************

# Commandline argument
args = sys.argv
argc = len(args)
if(argc != 2):
    sys.exit("Error: One filename of the hql file is required as a command line argument.")
 
input_path = "./hive_input/" + args[1]
filename = re.findall(r'input/(\S+).hql', input_path)
output_path = "./trino_output/" + filename[0] + ".sql"
 
# Hive's data type + VARCHAR(Trino' data type)
DATA_TYPE_LIST = ["TINYINT", "SMALLINT", "INT", "BIGINT", "BOOLEAN", "FLOAT", "DOUBLE", "STRING", "VARCHAR"]
 
column_name_list = []

PATTERN_CREATE = r'CREATE\s+TABLE\s+([^\s]+)\s+'
PATTERN_SHOWTABLES = r'SHOW\s+TABLES\s+IN\s+itemx\s+LIKE\s+([^\s]+)\s*;'
PATTERN_SHOWPARTITIONS = r'SHOW\s+PARTITIONS\s+([^\s]+)\s*;'
PATTERN_DESC = r'DESC(RIBE)*\s+([^\s]+)\s*;'
PATTERN_FUNCTION = r'SHOW\s+FUNCTIONS\s*;'
PATTERN_USE = r'USE\s+([^\s]+)\s*;'
PATTERN_EXPLAIN = r'EXPLAIN\s+([^\s]+)\s*;'
PATTERN_LIKE = r'LIKE\s+([^\s]+)'
PATTERN_PARTITION = r'PARTITIONED\s+BY\s+\(\s*(\S+ +\S+)\s*\)'
PATTERN_WITH = r'WITH\s*\('
PATTERN_FORMAT = r'STORED\s+AS\s+(\S+)\s'
PATTERN_CLUSTERED = r'CLUSTERED\s+BY\s+\(\s*(\S+)\s*\)'
PATTERN_INTOBUCKETS = r'INTO\s+(\d+)\s+BUCKETS'
PATTERN_SORTED = r'SORTED\s+BY\s+\(\s*(\S+)\s*\)'
PATTERN_TBLPROPERTIES = r'TBLPROPERTIES\s+\(\s*(\S+)\s*\)'

 
def hive_to_trino_ddl():

    if (len(args) != 2):
        sys.exit("The number of command line argument should be 1, but this time; ", len(args))

    trino_ddl = ""
 
    # read file as string
    with open(input_path) as f:
        hive_ddl = f.read()
 
    # adjust hive create format
    hive_ddl = format_create_hql(hive_ddl)

    searches = determine_query(hive_ddl)

    # CREATE
    if (searches == "CREATE"):
        table_name = re.findall(r'(\w{6})\s+(\w{5})\s+(\S+)\s', hive_ddl)[0][2]
        trino_ddl = f"CREATE TABLE " + table_name + "(\n"
        pattern = r'CREATE\s+TABLE\s+([^\s]+)\s+\('
        searches = None
        seraches = re.search(pattern, hive_ddl, re.IGNORECASE)
        # No LIKE
        if (seraches != None):
            trino_ddl = convert_create(hive_ddl, trino_ddl)
        # LIKE (columnの指定がないためcolumn nameがLIKEになることがない)
        else:
            trino_ddl = convert_like(hive_ddl, trino_ddl)

        trino_ddl = convert_properties(hive_ddl, trino_ddl)
        trino_ddl += "\n)"

    elif (searches == "SHOW TABLES"):
        trino_ddl = convert_showtables(hive_ddl, trino_ddl)

    elif (searches == "SHOW PARTITIONS"):
        trino_ddl = convert_showpartitions(hive_ddl, trino_ddl)

    elif (searches == "DESC"):
        trino_ddl = convert_desc(hive_ddl, trino_ddl)
    
    elif (searches == "FUNCTION"):
        trino_ddl = convert_function(hive_ddl, trino_ddl)

    elif (searches == "USE"):
        trino_ddl = convert_use(hive_ddl, trino_ddl)

    elif (searches == "EXPLAIN"):
        trino_ddl = convert_explain(hive_ddl, trino_ddl)

    else:
        sys.exit(f"Error: Don't support this query.")
 

    trino_ddl += ";"
 
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

    # TBLPROPERTIES
    searches = None
    searches = re.search(PATTERN_TBLPROPERTIES, hive_ddl, re.IGNORECASE)
    if (searches != None):
        trino_ddl = convert_tblproperties(hive_ddl, trino_ddl)

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
            trino_ddl += "  " + column[0] + " " + column[1].upper().replace('STRING', 'VARCHAR') + "\n"
        else:
            sys.exit(f"Error: Duplicate column name. ({column[0]})")
 
    return trino_ddl 

def convert_like(hive_ddl, trino_ddl):
    match = re.search(PATTERN_LIKE, hive_ddl, re.IGNORECASE)

    table_name = ""

    # LIKEの参照先を取得
    table_name = match.group(1)
    
    trino_ddl += "  LIKE " + table_name + "\n"
 
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

    match_with = re.search(PATTERN_WITH, hive_ddl, re.IGNORECASE)
    # withがすでにある
    if match_with:
        trino_ddl += ",\n  partitioned_by = ARRAY['dt']"
    # withがまだない
    else:
        trino_ddl += ")\nWITH(\n  partitioned_by = ARRAY['dt']"
 
    return trino_ddl

def convert_dataformat(hive_ddl, trino_ddl):
    match = re.search(PATTERN_FORMAT, hive_ddl, re.IGNORECASE)
    data_format = match.group(1)

    match_with = re.search(PATTERN_WITH, trino_ddl, re.IGNORECASE)
    # withがすでにある
    if match_with:
        trino_ddl += f",\n  format = '{data_format}'"
    # withがまだない
    else:
        trino_ddl += f")\nWITH(\n  format = '{data_format}'"
 
    return trino_ddl

def convert_clustered(hive_ddl, trino_ddl):
    match = re.search(PATTERN_CLUSTERED, hive_ddl, re.IGNORECASE)
    clustered_by_value = match.group(1).strip()

    match_with = re.search(PATTERN_WITH, trino_ddl, re.IGNORECASE)
    # withがすでにある
    if match_with:
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

    match_with = re.search(PATTERN_WITH, trino_ddl, re.IGNORECASE)
    # withがすでにある
    if match_with:
        trino_ddl += f",\n  sorted by = ARRAY['{sorted_by_value}']"
    # withがまだない
    else:
        trino_ddl += f")\nWITH(\n  sorted by = ARRAY['{sorted_by_value}']"
 
    return trino_ddl

def convert_tblproperties(hive_ddl, trino_ddl):
    match = re.search(PATTERN_TBLPROPERTIES, hive_ddl, re.IGNORECASE)
    property = match.group(1).strip()

    match_with = re.search(PATTERN_WITH, trino_ddl, re.IGNORECASE)
    # withがすでにある
    if match_with:
        trino_ddl += f",\n  {property}"
    # withがまだない
    else:
        trino_ddl += f")\nWITH(\n  {property}"
 
    return trino_ddl
     
def convert_showtables(hive_ddl, trino_ddl):
    match = re.search(PATTERN_SHOWTABLES, hive_ddl, re.IGNORECASE)
    tablename = match.group(1).replace('*', '%')
    trino_ddl += f"SHOW TABLES FROM catalog.db LIKE {tablename}"
    return trino_ddl

def convert_showpartitions(hive_ddl, trino_ddl):
    match = re.search(PATTERN_SHOWPARTITIONS, hive_ddl, re.IGNORECASE)
    tablename = match.group(1).replace('.', '."')
    trino_ddl += f"SELECT * FROM catalog.{tablename}$partitions\""
    return trino_ddl

def convert_desc(hive_ddl, trino_ddl):
    match = re.search(PATTERN_DESC, hive_ddl, re.IGNORECASE)
    tablename = match.group(2)
    trino_ddl += f"SHOW COLUMNS FROM catalog.{tablename}"
    return trino_ddl

def convert_function(hive_ddl, trino_ddl):
    trino_ddl += f"SHOW FUNCTIONS"
    return trino_ddl

def convert_use(hive_ddl, trino_ddl):
    match = re.search(PATTERN_USE, hive_ddl, re.IGNORECASE)
    db_name = match.group(1)
    trino_ddl += f"USE {db_name}"
    return trino_ddl

def convert_explain(hive_ddl, trino_ddl):
    match = re.search(PATTERN_EXPLAIN, hive_ddl, re.IGNORECASE)
    query = match.group(1)
    trino_ddl += f"EXPLAIN {query}"
    return trino_ddl

def determine_query(hive_ddl):
    searches = None
    searches = re.search(PATTERN_CREATE, hive_ddl, re.IGNORECASE)
    if (searches != None):
        return "CREATE"
    
    searches = re.search(PATTERN_SHOWTABLES, hive_ddl, re.IGNORECASE)
    if (searches != None):
        return "SHOW TABLES"

    searches = re.search(PATTERN_SHOWPARTITIONS, hive_ddl, re.IGNORECASE)
    if (searches != None):
        return "SHOW PARTITIONS"
    
    searches = re.search(PATTERN_DESC, hive_ddl, re.IGNORECASE)
    if (searches != None):
        return "DESC"
    
    searches = re.search(PATTERN_FUNCTION, hive_ddl, re.IGNORECASE)
    if (searches != None):
        return "FUNCTION"
    
    searches = re.search(PATTERN_USE, hive_ddl, re.IGNORECASE)
    if (searches != None):
        return "USE"
    
    searches = re.search(PATTERN_EXPLAIN, hive_ddl, re.IGNORECASE)
    if (searches != None):
        return "EXPLAIN"
    
    return "NOT SUPPORT"


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