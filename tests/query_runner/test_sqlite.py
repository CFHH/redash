import sqlite3
import simplejson
import random

TYPE_INTEGER = "integer"
TYPE_FLOAT = "float"
TYPE_BOOLEAN = "boolean"
TYPE_STRING = "string"
TYPE_DATETIME = "datetime"
TYPE_DATE = "date"
SQLITE_TYPES_MAP = {TYPE_STRING: "VARCHAR(255)", TYPE_INTEGER: "BIGINT", TYPE_FLOAT: "DOUBLE"}


querystr = '''X{
    "tables": [
    {
        "table_name": "tablea",
        "datetime_column": "daytime",
        "query":
{
  "aggregations": [
    {
      "type": "doubleSum",
      "name": "showCount",
      "fieldName": "AD_SHOW_COUNT"
    },
    {
      "type": "doubleSum",
      "name": "realcost",
      "fieldName": null,
      "expression": "(AD_CLICK_COUNT * KW_AVG_COST)"
    },
    {
      "type": "doubleSum",
      "name": "a1",
      "fieldName": "AD_CLICK_COUNT"
    }
  ],
  "postAggregations": [
    {
      "type": "expression",
      "name": "click_per_cost",
      "expression": "(realcost / a1)",
      "ordering": null
    }
  ],
  "filter": {
    "type": "selector",
    "dimension": "EVENT_TYPE",
    "value": "数据报告"
  },
  "dataSource": "travels_demo",
  "dimension": "KEYWORD",
  "granularity": "day",
  "intervals": [
    "1900-01-09T00:00:00.000Z/2992-01-10T00:00:00.000Z"
  ],
  "metric": "realcost",
  "queryType": "topN",
  "threshold": 30
}
    }
    ],
    "final_sql": "SELECT * FROM tablea;"
}
'''
querystr = querystr[1:] #去掉X
#print(querystr)
input_obj = simplejson.loads(querystr)
tables = input_obj.get("tables")
for table_cofig in tables:
    sub_query = table_cofig.get("query")
    if type(sub_query).__name__ =="dict":
        sub_query_str = simplejson.dumps(sub_query)
        print(sub_query_str)

a["sadas"]


def fetch_columns(columns):
    column_names = []
    duplicates_counter = 1
    new_columns = []
    for col in columns:
        column_name = col[0]
        if column_name in column_names:
            column_name = "{}{}".format(column_name, duplicates_counter)
            duplicates_counter += 1

        column_names.append(column_name)
        new_columns.append(
            {"name": column_name, "friendly_name": column_name, "type": col[1]}
        )
    return new_columns

table_name = "table_test"
datetime_column = "daytime"

json_data = {
    "columns":
    [
        {"name": "daytime", "friendly_name": "daytime", "type": "string"},
        {"name": "TOUR_DEST", "friendly_name": "TOUR_DEST", "type": "string"},
        {"name": "orders", "friendly_name": "orders", "type": "integer"},
        {"name": "cpo", "friendly_name": "cpo", "type": "integer"}
    ],
    "rows":
    [
        {"daytime": "2020-01-02T00:00:00.000Z", "TOUR_DEST": "", "orders": 1.0, "cpo": 0.0},
        {"daytime": "2020-01-22T00:00:00.000Z", "TOUR_DEST": "\u8d35\u5dde", "orders": 1.9999999675783329, "cpo": 297.29051284564537}
    ]
}


class CustomException(Exception):
    def __init__(self, info):
        self.info = info
    def __str__(self):
        return self.info
    def read(self):
        return self.info
class CustomException2(Exception):
    def __init__(self, info):
        self.info = info
    def __str__(self):
        return self.info
    def read(self):
        return self.info


try:
    raise CustomException("asdf!!!")
except CustomException as e:
    error = str(e)
    print(error)
except:
    error = "except"
    print(error)
finally:
    print("finally!!!!")

a["sad"]


json_str = simplejson.dumps(json_data)
#print(json_str)

num = random.randint(10000,99999)
table_name = table_name + str(num)

sqlite_connection = sqlite3.connect('test.db')
sqlite_cursor = sqlite_connection.cursor()

#删除表
drop_table_sql = "DROP TABLE IF EXISTS " + table_name + ";"
print(drop_table_sql)
sqlite_cursor.execute(drop_table_sql)
print(sqlite_cursor.rowcount)

#创建表
create_table_sql = "CREATE TABLE " + table_name + "("
colume_index = 0
for colume in json_data["columns"]:
    if colume["name"] == datetime_column:
        type_str = "DATETIME"
    else:
        type_str = SQLITE_TYPES_MAP[colume["type"]]
    if colume_index != 0:
        create_table_sql = create_table_sql + ", "
    colume_index += 1
    create_table_sql = create_table_sql + colume["name"] + " " + type_str
create_table_sql = create_table_sql + ");"
print(create_table_sql)
sqlite_cursor.execute(create_table_sql)
print(sqlite_cursor.rowcount)

#插入数据
for row in json_data["rows"]:
    insert_sql = "INSERT INTO " + table_name + " VALUES("
    colume_index = 0
    for colume in json_data["columns"]:
        if colume_index != 0:
            insert_sql = insert_sql + ", "
        colume_index += 1
        value = row[colume["name"]]
        if colume["type"] == "string":
            value = "\"" + value + "\""
        else:
            value = str(value)
        insert_sql = insert_sql + value
    insert_sql = insert_sql + ");"
    print(insert_sql)
    sqlite_cursor.execute(insert_sql)
    print(sqlite_cursor.rowcount)

#查询
query_sql = "SELECT * from table_test;"
query_sql = query_sql.replace("table_test", table_name)
print(query_sql)
sqlite_cursor.execute(query_sql)
#values = sqlite_cursor.fetchall()
#print(values)



if sqlite_cursor.description is not None:
    print(sqlite_cursor.description)
    columns = fetch_columns([(i[0], None) for i in sqlite_cursor.description])
    rows = [
        dict(zip((column["name"] for column in columns), row))
        for row in sqlite_cursor
    ]
    data = {"columns": columns, "rows": rows}
    json_str = simplejson.dumps(data)
    print(json_str)


#删除表
sqlite_cursor.execute(drop_table_sql)
print(sqlite_cursor.rowcount)


sqlite_cursor.close()
sqlite_connection.close()

'''
#数据库文件是test.db，不存在，则自动创建
conn = sqlite3.connect('test.db')
#创建一个cursor：
cursor = conn.cursor()

#执行一条SQL语句：创建user表
cursor.execute('create table user(id varchar(20) primary key,name varchar(20))')
#通过rowcount获得插入的行数：
print(cursor.rowcount) #reusult 1

#插入一条记录：
cursor.execute('insert into user (id, name) values (\'1\', \'Michael\')')
cursor.execute('insert into user (id, name) values (\'2\', \'zzw\')')

#执行查询语句：
cursor.execute('select name from user')
#使用featchall获得结果集（list）
values = cursor.fetchall()
print(values) #result:[('1', 'Michael')]

#关闭Cursor:
cursor.close()
#提交事务：
conn.commit()
#关闭connection：
conn.close()
'''