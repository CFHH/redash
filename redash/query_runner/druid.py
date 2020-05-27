try:
    from pydruid.db import connect

    enabled = True
except ImportError:
    enabled = False

from redash.query_runner import BaseQueryRunner, register
from redash.query_runner import TYPE_STRING, TYPE_INTEGER, TYPE_BOOLEAN, TYPE_FLOAT
from redash.utils import json_dumps, json_loads

from six.moves import urllib
from base64 import b64encode

TYPES_MAP = {1: TYPE_STRING, 2: TYPE_INTEGER, 3: TYPE_BOOLEAN}
PYTHON_TYPES_MAP = {"str": TYPE_STRING, "int": TYPE_INTEGER, "bool": TYPE_BOOLEAN, "float": TYPE_FLOAT}


class Druid(BaseQueryRunner):
    noop_query = "SELECT 1"

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

    def run_query(self, query, user):
        if self.is_native_query(query):
            return self.run_native_query(query, user)

        connection = connect(
            host=self.configuration["host"],
            port=self.configuration["port"],
            path="/druid/v2/sql/",
            scheme=(self.configuration.get("scheme") or "http"),
            user=(self.configuration.get("user") or None),
            password=(self.configuration.get("password") or None),
        )

        cursor = connection.cursor()

        try:
            cursor.execute(query)
            columns = self.fetch_columns(
                [(i[0], TYPES_MAP.get(i[1], None)) for i in cursor.description]
            )
            rows = [
                dict(zip((column["name"] for column in columns), row)) for row in cursor
            ]

            data = {"columns": columns, "rows": rows}
            error = None
            json_data = json_dumps(data)
            print(json_data)
        finally:
            connection.close()

        return json_data, error

    def is_native_query(self, querystr):
        #开头加了类似这样的注释：/* Username: 13436361@qq.com, Query ID: 4, Queue: queries, Job ID: 51003672-2c5b-4705-850e-27efc8b0b881, Query Hash: a79e88ed1a8adf112794e614966d547e, Scheduled: False */
        first_char = ""
        if querystr[0:2] == "/*":
            index = querystr.find("*/") + 2
            for i in range(index, len(querystr)):
                if querystr[i] != " ":
                    first_char = querystr[i]
                    break
        if first_char == "{":
            return True
        else:
            return False

    def run_native_query(self, querystr, user):
        #pydruid搜索_prepare_url_headers_and_body和_stream_query
        if querystr[0:2] == "/*":
            index = querystr.find("*/") + 2
            querystr = querystr[index:]

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
        except (KeyboardInterrupt, InterruptException, JobTimeoutException):
            raise
        else:
            raw_json_data = json_loads(data)
            final_json_data = self.post_process_native_result(raw_json_data)
            json_str = json_dumps(final_json_data)

        return json_str, error

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
