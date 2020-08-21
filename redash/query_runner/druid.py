try:
    from pydruid.db import connect

    enabled = True
except ImportError:
    enabled = False

from redash.query_runner import BaseQueryRunner, register, JobTimeoutException
from redash.query_runner import TYPE_STRING, TYPE_INTEGER, TYPE_BOOLEAN, TYPE_FLOAT
from redash.utils import json_dumps, json_loads

from six.moves import urllib
from base64 import b64encode

TYPES_MAP = {1: TYPE_STRING, 2: TYPE_INTEGER, 3: TYPE_BOOLEAN}
PYTHON_TYPES_MAP = {"str": TYPE_STRING, "int": TYPE_INTEGER, "bool": TYPE_BOOLEAN, "float": TYPE_FLOAT}
SQLITE_TYPES_MAP = {TYPE_STRING: "TEXT", TYPE_INTEGER: "INTEGER", TYPE_FLOAT: "NUMERIC"}

QUERY_MODE_SQL = 1      #向druid发起sql查询
QUERY_MODE_NATIVE = 2   #向druid发起json格式的查询
QUERY_MODE_CUSTOM = 3   #自定义，总和模式
QUERY_MODE_SQLITE = 4   #向临时SQLITE的查新

QUERY_MODE_SQLITE_PREFIX = "SQLITE:"

import sqlite3
import random

#import logging
#logger = logging.getLogger("druid")
from redash.worker import get_job_logger
logger = get_job_logger(__name__)


class CustomException(Exception):
    def __init__(self, info):
        self.info = info
    def __str__(self):
        return self.info
    def read(self):
        return self.info


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

    def _log_debug(self, message):
        logger.debug("###druid### [query_id=%s] [query_hash=%s], %s",
            self.metadata.get("Query ID", "unknown"),
            self.metadata.get("Query Hash", "unknown"),
            message,
        )

    def _log_info(self, message):
        logger.info("###druid### [query_id=%s] [query_hash=%s], %s",
            self.metadata.get("Query ID", "unknown"),
            self.metadata.get("Query Hash", "unknown"),
            message,
        )

    def _log_warning(self, message):
        logger.warning("###druid### [query_id=%s] [query_hash=%s], %s",
            self.metadata.get("Query ID", "unknown"),
            self.metadata.get("Query Hash", "unknown"),
            message,
        )

    def _log_error(self, message):
        logger.error("###druid### [query_id=%s] [query_hash=%s], %s",
            self.metadata.get("Query ID", "unknown"),
            self.metadata.get("Query Hash", "unknown"),
            message,
        )

    def run_query(self, query, user):
        json_data, error = self.run_query_obj_result(query, user, {})
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
        self._log_info("query=#####%s#####, mode=%d" % (querystr, query_mode))

        if query_mode == QUERY_MODE_SQL:
            if query_obj is not None:
                querystr = query_obj["sql"]
                context = query_obj["context"]
            else:
                context = {}
            json_data, error = self.run_sql_query(querystr, context, user)
        elif query_mode == QUERY_MODE_NATIVE:
            json_data, error = self.run_native_query(querystr, user)
        elif query_mode == QUERY_MODE_SQLITE:
            json_data, error = self.run_sqlite_query(querystr, sqlite_query_param)
        else:
            json_data, error = self.run_custom_query(querystr, user)

        return json_data, error

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
            index += 2
            for i in range(index, len(querystr)):
                if querystr[i] != " ":
                    querystr = querystr[i:]
                    break
        return querystr

    def get_query_mode(self, querystr):
        '''
        三种模式:
        1、SQL: QUERY_MODE_SQL
        2、JSON: QUERY_MODE_NATIVE
        3、自定义: QUERY_MODE_CUSTOM
        '''
        first_char = querystr[0]

        if first_char == "{":
            query_obj = json_loads(querystr)
            if query_obj.get("context") != None and query_obj.get("sql") != None:
                return QUERY_MODE_SQL, query_obj
            else:
                return QUERY_MODE_NATIVE, None
        elif first_char == "X":
            return QUERY_MODE_CUSTOM, None
        elif querystr.find(QUERY_MODE_SQLITE_PREFIX) == 0:
            return QUERY_MODE_SQLITE, None
        else:
            return QUERY_MODE_SQL, None

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
    "tables": [
    {
        "table_name": "tablea",
        "datetime_column": "daytime",
        "query": {
            "context": {"useApproximateCountDistinct": false},
            "sql": "SELECT DATE_TRUNC('day', __time) as daytime,PV_SRC_GEO_LOCATION,sum(AD_CLICK_COUNT) as click, sum(AD_CLICK_COUNT*KW_AVG_COST) as cost FROM travels_demo where EVENT_TYPE='被展现'  group by PV_SRC_GEO_LOCATION,DATE_TRUNC('day', __time) order by daytime"
        }
    },
    {
        "table_name": "tableb",
        "datetime_column": "daytime",
        "query": "SQLITE:SELECT * FROM tablea"
    }
    ],
    "main_query": "SQLITE:SELECT daytime, PV_SRC_GEO_LOCATION, click, cost FROM tableb",
    "final_sql": "SELECT daytime, PV_SRC_GEO_LOCATION, click, cost FROM tableb",
    "sub_queries":[
    {
        "name": "exdata1",
        "query":"SQLITE:SELECT daytime, PV_SRC_GEO_LOCATION, click, cost FROM tableb"
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
    "final_sql": "SELECT * FROM tablea;"
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

        tables = input_obj.get("tables")
        main_query = input_obj.get("main_query")
        final_sqlite_query = input_obj.get("final_sql")
        sub_queries = input_obj.get("sub_queries")
        if (tables is not None) and (type(tables).__name__ !="list"):
            raise CustomException("Incorrect Json data: tables must be a list.")
        if main_query is not None:
            if type(main_query).__name__ =="str":
                pass
            elif type(main_query).__name__ =="dict":
                main_query = json_dumps(main_query)
            else:
                raise CustomException("Incorrect Json data: main_query must be a string or json format.")
        if (final_sqlite_query is not None) and (type(final_sqlite_query).__name__ !="str"):
            raise CustomException("Incorrect Json data: final_sql must be a string.")
        if (sub_queries is not None) and (type(sub_queries).__name__ !="list"):
            raise CustomException("Incorrect Json data: sub_queries must be a string.")

        #对表名的随机化
        table_name_map = {}
        #创建sqlite
        sqlite_connection = sqlite3.connect(self.sqlite_dbpath)
        sqlite_cursor = sqlite_connection.cursor()
        sqlite_query_param = {"table_name_map": table_name_map}
        try:
            #一、依次处理单个表
            if tables is not None:
                for table_cofig in tables:
                    name = table_cofig.get("table_name")
                    if (name is None) or (type(name).__name__ !="str"):
                        raise CustomException("Incorrect Json data: table_name can't be none and must be a string.")
                    self._log_info("Processing Table[%s]" % name)
                    datetime_column = table_cofig.get("datetime_column")
                    if (datetime_column is not None) and (type(datetime_column).__name__ !="str"):
                        raise CustomException("Incorrect Json data in table %s: datetime_column must be a string." % name)
                    sub_query = table_cofig.get("query")
                    if sub_query is None:
                        raise CustomException("Incorrect Json data in table %s: query must exist." % name)
                    if type(sub_query).__name__ =="str":
                        pass
                    elif type(sub_query).__name__ =="dict":
                        sub_query = json_dumps(sub_query)
                    else:
                        raise CustomException("Incorrect Json data in table %s: query must be a string or json format." % name)
                    query_data, query_error = self.run_query_obj_result(sub_query, user, sqlite_query_param)
                    if query_error is not None:
                        raise CustomException(query_error)
                    if (query_data is None) or query_data.get("columns") is None:
                        raise CustomException("Incorrect query data for table %s." % name)

                    #创建表
                    if len(query_data["columns"]) == 0:
                        continue
                    rand_num = random.randint(100000,999999)
                    table_name = name + str(rand_num)
                    table_name_map[name] = table_name
                    create_table_sql = "CREATE TABLE " + table_name + "("
                    colume_index = 0
                    for colume in query_data["columns"]:
                        if colume["name"] == datetime_column:
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
                #直接旧的
                for (k,v) in table_name_map.items():
                    final_sqlite_query = final_sqlite_query.replace(k, v)
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
                    error = "Query completed but it returned no data."
                    json_data = None
            else:
                json_data = {"columns": [], "rows": []}
                error = None

            #三、执行子查询
            if sub_queries is not None:
                json_data["data_ex"] = []
                for query_config in sub_queries:
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
                    self._log_info("Processing Sub Query:#####%s#####" % sub_query)
                    query_data, query_error = self.run_query_obj_result(sub_query, user, sqlite_query_param)
                    if query_error is not None:
                        raise CustomException(query_error)
                    if (query_data is None) or query_data.get("columns") is None:
                        raise CustomException("Incorrect query data for sub query %s." % name)
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
            sqlite_connection.close()

        if error is not None:
            raise CustomException(error)
        return json_data, error

    def run_sqlite_query(self, querystr, sqlite_query_param):
        tables = []
        querystr = querystr.replace(QUERY_MODE_SQLITE_PREFIX, '')
        table_name_map = sqlite_query_param.get("table_name_map")
        if table_name_map is not None:
            for (k,v) in table_name_map.items():
                querystr = querystr.replace(k, v)

        error = None
        json_data = None
        sqlite_connection = sqlite3.connect(self.sqlite_dbpath)
        sqlite_cursor = sqlite_connection.cursor()
        try:
            sqlite_cursor.execute(querystr)
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
                error = "Query completed but it returned no data."
                json_data = None
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
