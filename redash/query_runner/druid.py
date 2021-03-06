try:
    from pydruid.db import connect
    enabled = True
except ImportError:
    enabled = False

from redash.query_runner import BaseQueryRunner, register, JobTimeoutException
from redash.query_runner import TYPE_STRING, TYPE_INTEGER, TYPE_BOOLEAN, TYPE_FLOAT
from redash.utils import enum, json_dumps, json_loads

from six.moves import urllib
from base64 import b64encode

import random
import sqlite3
import re
import threading

#import logging
from redash.worker import get_job_logger


TYPES_MAP = {1: TYPE_STRING, 2: TYPE_INTEGER, 3: TYPE_BOOLEAN}
PYTHON_TYPES_MAP = {"str": TYPE_STRING, "int": TYPE_INTEGER, "bool": TYPE_BOOLEAN, "float": TYPE_FLOAT}
SQLITE_TYPES_MAP = {TYPE_STRING: "TEXT", TYPE_INTEGER: "INTEGER", TYPE_FLOAT: "NUMERIC"}

QueryMode = enum(
    'QueryMode',
    DRUID_SQL='DruidSql',       #基本查询：向druid发起sql查询，带不带context两种方式
    DRUID_JSON='DruidJson',     #基本查询：向druid发起原生json格式的查询
    SQLITE='Sqlite',            #基本查询：向sqlite发起查询
    CUSTOM='Custom'             #复杂查询
)

QUERY_MODE_SQLITE_PREFIX = "SQLITE:"

def get_logger():
    return get_job_logger(__name__)

#表名转换
def ReplaceTableName(querystr, old_name, new_name):
    '''
    #return querystr.replace(old_name, new_name)
    TABLE_NAME_REPL_REG = "(\s|\))(TABLE_NAME)(\s|\(|\)|\.|$)"
    正则替换时，索引0是整体，接下来是按次序出现的每个(，这里保留1和3，把2换掉
    '''
    pattern = "(\s|\))(" + old_name + ")(\s|\(|\)|\.|$)"
    return re.sub(pattern, lambda x:x.group(1) + new_name + x.group(3), querystr, flags=re.I)

#判断是否是创建表的SQL语句
CREATE_TABLE_SQL_REG = re.compile("(\s*CREATE\s+(TEMPORARY\s+)*TABLE\s+(IF\s+NOT\s+EXISTS\s+)?)", flags=re.I)
def IsCreateTableSql(querystr):
    m = CREATE_TABLE_SQL_REG.findall(querystr)
    if m:
        return True
    else:
        return False

#找出CREATE TABLE语句中的表名
TABLE_NAME_TO_CREATE_REG = re.compile("\s*CREATE\s+(TEMPORARY\s+)*TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(\w+)[\s\(]", flags=re.I)
REG_MATCH_TABLE_NAME_INDEX = 2
def GetTableNameToCreate(querystr):
    m = TABLE_NAME_TO_CREATE_REG.findall(querystr)
    if m:
        tabel_name = ''
        for i in range(0, len(m)):
            tabel_name = tabel_name + m[i][2]
        return tabel_name
    else:
        return None

#禁止执行的SQL行为：DATABASE、ALTER、RENAME
FORBIDDEN_SQL_REG = re.compile("(\s+(DATABASE)\s+)|((\s+|:{1}|^)(ALTER|RENAME)\s+)", flags=re.I)
def CheckForbiddenSql(querystr):
    m = FORBIDDEN_SQL_REG.findall(querystr)
    if m:
        forbidden_part = "%s%s" % (m[0][1], m[0][4])
        return forbidden_part
    else:
        return None

#删除注释(/**/)
COMMENT_REG = re.compile("(/\*([\S\s]*?)\*/)")


class CustomException(Exception):
    def __init__(self, info):
        self.info = info
    def __str__(self):
        return self.info
    def read(self):
        return self.info


class Result(object):
    def __init__(self):
        pass


class Druid(BaseQueryRunner):
    noop_query = "SELECT 1"
    sqlite_dbpath = "druid_sqlite.db"
    '''
    {"Username": "13436361@qq.com", "Query ID": "7", "Queue": "queries", "Enqueue Time": 1597988440.3357646,
    "Job ID": "20529e79-80da-4de7-bfe0-bea63a35c9e8", "Query Hash": "add2ea64feea932bc1a12a20cdb29bc5", "Scheduled": false}
    '''
    metadata = {}

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "host": {"type": "string", "default": "localhost"},
                "port": {"type": "number", "default": 8082},
                "scheme": {"type": "string", "default": "http"},
                "user": {"type": "string"},
                "password": {"type": "string"},
            },
            "order": ["scheme", "host", "port", "user", "password"],
            "required": ["host"],
            "secret": ["password"],
        }

    @classmethod
    def enabled(cls):
        return enabled

    def get_logger(self):
        #logger = logging.getLogger("druid")
        return get_job_logger(__name__)

    def _log_debug(self, message):
        get_logger().debug("###druid### [query_id=%s] [query_hash=%s], %s",
            self.metadata.get("Query ID", "unknown"),
            self.metadata.get("Query Hash", "unknown"),
            message,
        )

    def _log_info(self, message):
        get_logger().info("###druid### [query_id=%s] [query_hash=%s], %s",
            self.metadata.get("Query ID", "unknown"),
            self.metadata.get("Query Hash", "unknown"),
            message,
        )

    def _log_warning(self, message):
        get_logger().warning("###druid### [query_id=%s] [query_hash=%s], %s",
            self.metadata.get("Query ID", "unknown"),
            self.metadata.get("Query Hash", "unknown"),
            message,
        )

    def _log_error(self, message):
        get_logger().error("###druid### [query_id=%s] [query_hash=%s], %s",
            self.metadata.get("Query ID", "unknown"),
            self.metadata.get("Query Hash", "unknown"),
            message,
        )

    def run_query(self, query, user):
        json_data, error = self.run_query_obj_result(query, user, sqlite_query_param={})
        if error is not None:
            self._log_error(error)

        if json_data is not None:
            json_str = json_dumps(json_data)
            #print(json_str)
        else:
            json_str = ""
        return json_str, error

    def run_query_obj_result(self, query, user, sqlite_query_param):
        '''
        postman方式：
        POST http://10.15.101.10:5000/api/queries/7/results
        body格式
        {
            "id": "7",
            "parameters": {
                "start_time_bc": "2020-02-01T00:00:00",
                "end_time_bc": "2020-03-01T00:00:00",
                "start_time_tb": "2019-02-01T00:00:00",
                "end_time_tb": "2019-03-01T00:00:00",
                "start_time_hb": "2020-01-01T00:00:00",
                "end_time_hb": "2020-02-01T00:00:00"
            },
            "max_age": -1   //0表示强制刷新；-1表示有缓存就取缓存；其他正数表示一定秒数内的缓存
        }

        输出这样的格式：
        {
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
        '''
        querystr = self.remove_comments(query)
        query_mode, query_obj = self.get_query_mode(querystr)
        self._log_info("query=#####%s#####, mode=%s" % (querystr, query_mode))

        if query_mode == QueryMode.DRUID_SQL:
            if query_obj is not None:
                querystr = query_obj["sql"]
                context = query_obj["context"]
            else:
                context = {}
            json_data, error = self.run_sql_query(querystr, context, user)
        elif query_mode == QueryMode.DRUID_JSON:
            json_data, error = self.run_native_query(querystr, user)
        elif query_mode == QueryMode.SQLITE:
            json_data, error = self.run_sqlite_query(querystr, sqlite_query_param)
        else:
            json_data, error = self.run_custom_query(querystr, user)

        return json_data, error

    def _run_query_threading(self, query, user, sqlite_query_param, result):
        result.json_data = None
        result.error = None
        try:
            result.json_data, result.error = self.run_query_obj_result(query, user, sqlite_query_param)
        except Exception as e:
            result.error = str(e)
        finally:
            pass

    def remove_comments(self, querystr):
        '''
        参见_annotate_query，开头加了类似这样的注释：
        /* Username: 13436361@qq.com, Query ID: 4, Queue: queries,
        Job ID: 51003672-2c5b-4705-850e-27efc8b0b881,
        Query Hash: a79e88ed1a8adf112794e614966d547e, Scheduled: False */
        现在已经被我改成json了, 如下
        /* {"Username": "13436361@qq.com", "Query ID": "7", "Queue": "queries", "Enqueue Time": 1597988440.3357646,
        "Job ID": "20529e79-80da-4de7-bfe0-bea63a35c9e8", "Query Hash": "add2ea64feea932bc1a12a20cdb29bc5", "Scheduled": false} */
        '''
        if querystr[0:2] == "/*":
            index = querystr.find("*/")
            comment = querystr[2:index]
            self.metadata = json_loads(comment)
            querystr = querystr[index+2:]
        querystr = COMMENT_REG.sub(" ", querystr)
        querystr = querystr.strip()
        return querystr

    def get_query_mode(self, querystr):
        first_char = querystr[0]

        if first_char == "{":
            query_obj = json_loads(querystr)
            if query_obj.get("context") != None and query_obj.get("sql") != None:
                return QueryMode.DRUID_SQL, query_obj
            else:
                return QueryMode.DRUID_JSON, None
        elif first_char == "X":
            return QueryMode.CUSTOM, None
        elif querystr.find(QUERY_MODE_SQLITE_PREFIX) == 0:
            return QueryMode.SQLITE, None
        else:
            return QueryMode.DRUID_SQL, None

    def run_sql_query(self, query, context, user):
        #context = {"useApproximateCountDistinct": False}
        connection = connect(
            host=self.configuration["host"],
            port=self.configuration["port"],
            path="/druid/v2/sql/",
            scheme=(self.configuration.get("scheme") or "http"),
            user=(self.configuration.get("user") or None),
            password=(self.configuration.get("password") or None),
            context=context,
        )

        cursor = connection.cursor()

        try:
            cursor.execute(query)
            if cursor.description is not None:
                columns = self.fetch_columns(
                    [(i[0], TYPES_MAP.get(i[1], None)) for i in cursor.description]
                )
                rows = [
                    dict(zip((column["name"] for column in columns), row)) for row in cursor
                ]
                data = {"columns": columns, "rows": rows}
                error = None
                #json_data = json_dumps(data)
                #print(json_data)
            else:
                data = {"columns": [], "rows": []}
                error = None #如果结果就是没数据，那么不返会错误
        finally:
            connection.close()

        return data, error

    def run_native_query(self, querystr, user):
        #pydruid搜索_prepare_url_headers_and_body和_stream_query

        host = self.configuration["host"]
        port = self.configuration["port"]
        username = (self.configuration.get("user") or None)
        password = (self.configuration.get("password") or None)

        url = "http://{}:{}/druid/v2/?pretty".format(host, port)

        headers = {"Content-Type": "application/json"}
        if (username is not None) and (password is not None):
            authstring = "{}:{}".format(username, password)
            b64string = b64encode(authstring.encode()).decode()
            headers["Authorization"] = "Basic {}".format(b64string)

        error = None
        try:
            b = querystr.encode('utf-8')
            req = urllib.request.Request(url, b, headers, method="POST")
            res = urllib.request.urlopen(url=req, cafile=None)
            data = res.read().decode("utf-8")
            res.close()
        except urllib.error.HTTPError as e:
            error = e.read()
            json_str = None
            raise
        except (KeyboardInterrupt, InterruptException, JobTimeoutException):
            raise
        else:
            raw_json_data = json_loads(data)
            final_json_data = self.post_process_native_result(raw_json_data)
            #json_str = json_dumps(final_json_data)

        return final_json_data, error

    def post_process_native_result(self, raw_json_data):
        '''
        最终的输出目标格式
        {
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
        '''
        columns = []
        rows = []
        final_json_data = {"columns" : columns, "rows" : rows}

        for obj1 in iter(raw_json_data):
            if not "result" in obj1.keys():
                rows.append(obj1)
                continue
            result = obj1["result"]
            if type(result).__name__ !="list":
                rows.append(obj1)
                continue

            row_common = {}
            for (k,v) in obj1.items():
                if k != "result":
                    row_common[k] = v
            for obj2 in iter(result):
                row = row_common.copy();
                for (k,v) in obj2.items():
                    row[k] = v
                rows.append(row)

        if len(rows) > 0:
            row = rows[0]
            for (column_name, column_value) in row.items():
                columns.append(
                    {"name": column_name, "friendly_name": column_name, "type": PYTHON_TYPES_MAP[type(column_value).__name__]}
                )

        return final_json_data

    def run_custom_query(self, querystr, user):
        '''
        例子1，子查询是个sql：
X{
    "store_to_db": false,
    "tables": [
    {
        "table_name": "tablea",
        "datetime_column": "daytime",
        "query": {
            "context": {"useApproximateCountDistinct": false},
            "sql": "SELECT DATE_TRUNC('day', __time) as daytime,PV_SRC_GEO_LOCATION,sum(AD_CLICK_COUNT) as click, sum(AD_CLICK_COUNT*KW_AVG_COST) as cost FROM travels_demo where EVENT_TYPE='被展现'  group by PV_SRC_GEO_LOCATION,DATE_TRUNC('day', __time) order by daytime"
        },
        "nodata_procs": [
        "SQLITE:CREATE TABLE tablea (daytime DATETIME, PV_SRC_GEO_LOCATION TEXT, click INTEGER, cost NUMERIC)",
        "SQLITE:INSERT INTO tablea VALUES('2020-01-01T00:00:00.000Z', 'CHINA', 252, 848.74)"
        ]
    },
    {
        "table_name": "tableb",
        "datetime_column": "daytime",
        "query": "SQLITE:SELECT * FROM tablea"
    }
    ],
    "main_query": "SQLITE:SELECT daytime, PV_SRC_GEO_LOCATION, click, cost FROM tableb",
    "final_sql": "SELECT daytime, PV_SRC_GEO_LOCATION, click, cost FROM tableb",
    "persist_table_name": "some_long_name_table_1",
    "persist_datetime_column": "daytime",
    "sub_queries":[
    {
        "name": "exdata1",
        "query":"SQLITE:SELECT daytime, click, cost FROM tablea",
        "persist_table_name": "some_long_name_table_2",
        "persist_datetime_column": "daytime"
    }
    ]
}
        例子2，子查询是个json：
X{
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
    "main_query": "SQLITE:SELECT * FROM tablea"
}
        '''
        error = None
        json_data = None

        #解析
        querystr = querystr[1:] #去掉X
        try:
            input_obj = json_loads(querystr)
        except:
            error = "Incorrect Json format."
        if error is not None:
            raise CustomException(error)

        #threading: 是否使用多线程进行查询
        use_threading = input_obj.get("threading")
        if use_threading is None:
            use_threading = True

        #store_to_db: 查询结果是否保存为sqlite的表，如果是，后续还得指定persist_table_name
        #   不需要可以不填，默认是False
        store_to_db = input_obj.get("store_to_db")
        if store_to_db is None:
            store_to_db = False
        #tables: 一系列辅助查询的过渡表，顺序执行，后续的表可以以来前面的表
        #   不需要可以不填
        tables = input_obj.get("tables")
        if (tables is not None) and (type(tables).__name__ !="list"):
            raise CustomException("Incorrect Json data: tables must be a list.")
        #main_query: 主查询，查询结果存放在query_result["data"]中
        #   不需要可以不填
        main_query = input_obj.get("main_query")
        if main_query is not None:
            if type(main_query).__name__ =="str":
                pass
            elif type(main_query).__name__ =="dict":
                main_query = json_dumps(main_query)
            else:
                raise CustomException("Incorrect Json data: main_query must be a string or json format.")
        #final_sql: 兼容，也是主查询，但只能从SQLITE中查结果；在有main_query的情况下，此项无效
        #   不需要可以不填
        final_sqlite_query = input_obj.get("final_sql")
        if (final_sqlite_query is not None) and (type(final_sqlite_query).__name__ !="str"):
            raise CustomException("Incorrect Json data: final_sql must be a string.")
        #persist_table_name: store_to_db为true的情况下，保存主查询数据的表名
        #persist_datetime_column: 查询结果中的时间项
        #   不需要可以不填
        persist_table_name = None
        persist_datetime_column = None
        if store_to_db and (main_query is not None or final_sqlite_query is not None):
            persist_table_name = input_obj.get("persist_table_name")
            if persist_table_name is None or type(persist_table_name).__name__ !="str":
                raise CustomException("Incorrect Json data: persist_table_name for main query must be a string.")
            persist_datetime_column = input_obj.get("persist_datetime_column")
            if persist_datetime_column is not None and type(persist_datetime_column).__name__ !="str":
                raise CustomException("Incorrect Json data: persist_datetime_column for main query must be a string.")
        #sub_queries: 子查询，查询结果存放在query_result["data_ex"]中
        #   不需要可以不填
        sub_queries = input_obj.get("sub_queries")
        if (sub_queries is not None) and (type(sub_queries).__name__ !="list"):
            raise CustomException("Incorrect Json data: sub_queries must be a string.")

        #对tables中的临时表名的随机化
        table_name_map = {}
        #创建sqlite
        sqlite_connection = sqlite3.connect(self.sqlite_dbpath)
        sqlite_cursor = sqlite_connection.cursor()
        sqlite_query_param = {"table_name_map": table_name_map, "can_create_table": False}
        try:
            #一、依次处理单个表
            if tables is not None:
                for table_cofig in tables:
                    #json配置
                    name = table_cofig.get("table_name")
                    if (name is None) or (type(name).__name__ !="str"):
                        raise CustomException("Incorrect Json data: table_name can't be none and must be a string.")
                    self._log_info("Processing Table[%s]" % name)
                    datetime_column = table_cofig.get("datetime_column")
                    if (datetime_column is not None) and (type(datetime_column).__name__ !="str"):
                        raise CustomException("Incorrect Json data in table %s: datetime_column must be a string." % name)
                    table_query = table_cofig.get("query")
                    if table_query is None:
                        raise CustomException("Incorrect Json data in table %s: query must exist." % name)
                    if type(table_query).__name__ =="str":
                        pass
                    elif type(table_query).__name__ =="dict":
                        table_query = json_dumps(table_query)
                    else:
                        raise CustomException("Incorrect Json data in table %s: query must be a string or json format." % name)
                    nodata_procs = table_cofig.get("nodata_procs")
                    if (nodata_procs is not None) and (type(nodata_procs).__name__ !="list"):
                        raise CustomException("Incorrect Json data in table %s: nodata_procs must be a list." % name)
                    #查询
                    query_data, query_error = self.run_query_obj_result(table_query, user, sqlite_query_param)
                    if query_error is not None:
                        raise CustomException(query_error)
                    if (query_data is None) or query_data.get("columns") is None:
                        raise CustomException("Incorrect query data for table %s." % name)
                    #存储
                    rand_num = random.randint(100000,999999)
                    table_name = name + str(rand_num)
                    table_name_map[name] = table_name
                    if len(query_data["columns"]) > 0:
                        self.store_data_to_sqlite(sqlite_connection, sqlite_cursor, query_data, table_name, datetime_column, drop_before_create = False)
                    #查询返回无数据的处理
                    elif nodata_procs is not None:
                        self._log_info("Using nodata_procs to build table: %s." % name)
                        sqlite_query_param["can_create_table"] = True
                        for proc in nodata_procs:
                            if type(proc).__name__ !="str":
                                raise CustomException("Incorrect Json data in table %s: nodata_procs must be a string list." % name)
                            t = GetTableNameToCreate(proc)
                            if t is not None and t != name:
                                raise CustomException("[nodata_procs]Invalid table name(%s) to create in table %s." % (t, name))
                            query_data, query_error = self.run_query_obj_result(proc, user, sqlite_query_param)
                            if query_error is not None:
                                raise CustomException(query_error)
                        sqlite_query_param["can_create_table"] = False
            else:
                pass

            #二、执行主查询
            if main_query is not None:
                self._log_info("Processing Main Query:#####%s#####" % main_query)
                json_data, error = self.run_query_obj_result(main_query, user, sqlite_query_param)
                if error is not None:
                    raise CustomException(error)
                if (json_data is None) or json_data.get("columns") is None:
                    raise CustomException("Incorrect query_data for main query.")
            elif final_sqlite_query is not None:
                for (k,v) in table_name_map.items():
                    final_sqlite_query = ReplaceTableName(final_sqlite_query, k, v)
                self._log_info("Processing Final SQL:#####%s#####" % final_sqlite_query)
                sqlite_cursor.execute(final_sqlite_query)
                if sqlite_cursor.description is not None:
                    columns = self.fetch_columns([(i[0], None) for i in sqlite_cursor.description])
                    rows = [
                        dict(zip((column["name"] for column in columns), row))
                        for row in sqlite_cursor
                    ]
                    error = None
                    #columns里的type全是null
                    columns = []
                    if len(rows) > 0:
                        row = rows[0]
                        for (column_name, column_value) in row.items():
                            columns.append(
                                {"name": column_name, "friendly_name": column_name, "type": PYTHON_TYPES_MAP[type(column_value).__name__]}
                            )
                    json_data = {"columns": columns, "rows": rows}
                else:
                    #error = "Query completed but it returned no data."
                    #json_data = None
                    error = None
                    json_data = {"columns": [], "rows": []}
            else:
                json_data = {"columns": [], "rows": []}
                error = None
            #存储
            if store_to_db and error is None and len(json_data["columns"]) > 0:
                self.store_data_to_sqlite(sqlite_connection, sqlite_cursor, json_data, persist_table_name, persist_datetime_column, drop_before_create = True)
                json_data = {"columns": [], "rows": []}


            #三、执行子查询
            if sub_queries is not None:
                json_data["data_ex"] = []
                if use_threading:
                    threads = []
                for query_config in sub_queries:
                    #json配置
                    name = query_config.get("name")
                    if (name is None) or (type(name).__name__ !="str"):
                        raise CustomException("Incorrect Json data in sub_queries: name must be exist and must be a string.")
                    sub_query = query_config.get("query")
                    if sub_query is None:
                        raise CustomException("Incorrect Json data in sub_query %s: query must be exist." % name)
                    if type(sub_query).__name__ =="str":
                        pass
                    elif type(sub_query).__name__ =="dict":
                        sub_query = json_dumps(sub_query)
                    else:
                        raise CustomException("Incorrect Json data in sub_query %s: query must be a string or json format." % name)
                    sub_persist_table_name = None
                    sub_persist_datetime_column = None
                    if store_to_db:
                        sub_persist_table_name = query_config.get("persist_table_name")
                        if sub_persist_table_name is None or type(sub_persist_table_name).__name__ !="str":
                            raise CustomException("Incorrect Json data in sub_query %s: persist_table_name must be a string." % name)
                        sub_persist_datetime_column = query_config.get("persist_datetime_column")
                        if sub_persist_datetime_column is not None and type(sub_persist_datetime_column).__name__ !="str":
                            raise CustomException("Incorrect Json data in sub_query %s: persist_datetime_column must be a string." % name)
                    if use_threading:
                        r = Result()
                        r.config = query_config
                        t = threading.Thread(target=self._run_query_threading, args=(sub_query, user, sqlite_query_param, r))
                        threads.append({"t": t, "r": r})
                        t.start()
                    else:
                        #查询
                        self._log_info("Processing Sub Query:#####%s#####" % sub_query)
                        query_data, query_error = self.run_query_obj_result(sub_query, user, sqlite_query_param)
                        if query_error is not None:
                            raise CustomException(query_error)
                        if (query_data is None) or query_data.get("columns") is None:
                            raise CustomException("Incorrect query data for sub query %s." % name)
                        #存储
                        if store_to_db:
                            if query_error is None and len(query_data["columns"]) > 0:
                                self.store_data_to_sqlite(sqlite_connection, sqlite_cursor, query_data, sub_persist_table_name, sub_persist_datetime_column, drop_before_create = True)
                        else:
                            json_data["data_ex"].append({"name": name, "data": query_data})

                if use_threading:
                    for itor in threads:
                        itor["t"].join()
                    for itor in threads:
                        r = itor["r"]
                        query_data = r.json_data
                        query_error = r.error
                        if query_error is not None:
                            raise CustomException(query_error)
                        if (query_data is None) or query_data.get("columns") is None:
                            name = r.config["name"]
                            raise CustomException("Incorrect query data for sub query %s." % name)
                    for itor in threads:
                        r = itor["r"]
                        query_data = r.json_data
                        query_error = r.error
                        if store_to_db:
                            if query_error is None and len(query_data["columns"]) > 0:
                                sub_persist_table_name = r.config["persist_table_name"]
                                sub_persist_datetime_column = r.config.get("persist_datetime_column")
                                self.store_data_to_sqlite(sqlite_connection, sqlite_cursor, query_data, sub_persist_table_name, sub_persist_datetime_column, drop_before_create = True)
                        else:
                            name = r.config["name"]
                            json_data["data_ex"].append({"name": name, "data": query_data})

        except CustomException as e:
            error = e.read()
            #sqlite_connection.cancel()
        except JobTimeoutException:
            error = "Query exceeded Redash query execution time limit."
            #sqlite_connection.cancel()
        except Exception as e:
            error = str(e)
            #sqlite_connection.cancel()
        finally:
            #删除所有数据表
            for (k,v) in table_name_map.items():
                drop_table_sql = "DROP TABLE IF EXISTS " + v + ";"
                self._log_info(drop_table_sql)
                sqlite_cursor.execute(drop_table_sql)
            sqlite_connection.commit()
            sqlite_connection.close()

        if error is not None:
            raise CustomException(error)
        return json_data, error

    def store_data_to_sqlite(self, sqlite_connection, sqlite_cursor, query_data, table_name, datetime_column, drop_before_create = False):
        #删表
        if drop_before_create:
            drop_table_sql = "DROP TABLE IF EXISTS " + table_name + ";"
            self._log_info(drop_table_sql)
            sqlite_cursor.execute(drop_table_sql)
            sqlite_connection.commit()
        #创建表
        create_table_sql = "CREATE TABLE " + table_name + "("
        colume_index = 0
        for colume in query_data["columns"]:
            if datetime_column is not None and colume["name"] == datetime_column:
                type_str = "DATETIME"
            else:
                type_str = SQLITE_TYPES_MAP.get(colume["type"])
                if type_str is None:
                    type_str = "TEXT"
            if colume_index != 0:
                create_table_sql = create_table_sql + ", "
            colume_index += 1
            create_table_sql = create_table_sql + colume["name"] + " " + type_str
        create_table_sql = create_table_sql + ");"
        self._log_info(create_table_sql)
        sqlite_cursor.execute(create_table_sql)
        #插入数据
        row_index = 0
        for row in query_data["rows"]:
            insert_sql = "INSERT INTO " + table_name + " VALUES("
            colume_index = 0
            for colume in query_data["columns"]:
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
            if row_index == 0:
                self._log_info(insert_sql)
            sqlite_cursor.execute(insert_sql)
            row_index += 1
        #提交：不然接下来的别的Cursor可能查不到更新的数据
        sqlite_connection.commit()

    def run_sqlite_query(self, querystr, sqlite_query_param):
        tables = []
        querystr = querystr.replace(QUERY_MODE_SQLITE_PREFIX, '')
        table_name_map = sqlite_query_param.get("table_name_map")
        if table_name_map is not None:
            for (k,v) in table_name_map.items():
                querystr = ReplaceTableName(querystr, k, v)
            self._log_info(querystr)

        error = None
        json_data = None

        can_create_table = sqlite_query_param.get("can_create_table")
        if not can_create_table:
            table_name = GetTableNameToCreate(querystr)
            if table_name is not None:
                raise CustomException("No permission to create table %s!" % table_name)
        forbidden_part = CheckForbiddenSql(querystr)
        if forbidden_part is not None:
            raise CustomException("No permission to %s " % forbidden_part)

        sqlite_connection = sqlite3.connect(self.sqlite_dbpath)
        sqlite_cursor = sqlite_connection.cursor()
        try:
            sqlite_cursor.execute(querystr)
            sqlite_connection.commit()
            if sqlite_cursor.description is not None:
                columns = self.fetch_columns([(i[0], None) for i in sqlite_cursor.description])
                rows = [
                    dict(zip((column["name"] for column in columns), row))
                    for row in sqlite_cursor
                ]
                #columns里的type全是null
                columns = []
                if len(rows) > 0:
                    row = rows[0]
                    for (column_name, column_value) in row.items():
                        columns.append(
                            {"name": column_name, "friendly_name": column_name, "type": PYTHON_TYPES_MAP[type(column_value).__name__]}
                        )
                else:
                    self._log_warning("run_sqlite_query, NO DATA IN rows")
                json_data = {"columns": columns, "rows": rows}
            else:
                #error = "Query completed but it returned no data."
                #json_data = None
                error = None
                json_data = {"columns": [], "rows": []}
        except Exception as e:
            error = str(e)
            #sqlite_connection.cancel()
        finally:
            sqlite_connection.close()

        if error is not None:
            raise CustomException(error)
        return json_data, error

    def get_schema(self, get_stats=False):
        query = """
        SELECT TABLE_SCHEMA,
               TABLE_NAME,
               COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
        """

        results, error = self.run_query(query, None)

        if error is not None:
            raise Exception("Failed getting schema.")

        schema = {}
        results = json_loads(results)

        for row in results["rows"]:
            table_name = "{}.{}".format(row["TABLE_SCHEMA"], row["TABLE_NAME"])

            if table_name not in schema:
                schema[table_name] = {"name": table_name, "columns": []}

            schema[table_name]["columns"].append(row["COLUMN_NAME"])

        return list(schema.values())


register(Druid)
