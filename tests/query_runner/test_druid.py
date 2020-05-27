import simplejson

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

TYPE_INTEGER = "integer"
TYPE_FLOAT = "float"
TYPE_BOOLEAN = "boolean"
TYPE_STRING = "string"
TYPE_DATETIME = "datetime"
TYPE_DATE = "date"

TYPES_MAP = {1: "string", 2: "integer", 3: "boolean"}
PYTHON_TYPES_MAP = {"str": TYPE_STRING, "int": TYPE_INTEGER, "bool": TYPE_BOOLEAN, "float": TYPE_FLOAT}


json_str = """
[
  {
    "timestamp": "2020-01-01T00:00:00.000Z",
    "result": [
      {
        "a1": 61.0000012665987,
        "realcost": 316.2400087690851,
        "KEYWORD": "贵州旅游景点",
        "showCount": 1287.9999949932098,
        "click_per_cost": 5.184262331191888
      },
      {
        "a1": 48.00000162422657,
        "realcost": 255.72001092068356,
        "KEYWORD": "贵州旅游攻略",
        "showCount": 1361.9999964237213,
        "click_per_cost": 5.327500047242009
      },
      {
        "a1": 17.000000551342964,
        "realcost": 100.60000411977373,
        "KEYWORD": "贵州旅游线路",
        "showCount": 325.99999809265137,
        "click_per_cost": 5.91764710924239
      }
    ]
  },
  {
    "timestamp": "2020-01-02T00:00:00.000Z",
    "result": [
      {
        "a1": 65.00000217556953,
        "realcost": 440.75001389697195,
        "KEYWORD": "贵州旅游攻略",
        "showCount": 2729.0000071525574,
        "click_per_cost": 6.780769217614416
      },
      {
        "a1": 68.00000157952309,
        "realcost": 346.84000778745565,
        "KEYWORD": "贵州旅游景点",
        "showCount": 2177.9999861717224,
        "click_per_cost": 5.100588231337629
      },
      {
        "a1": 27.000000789761543,
        "realcost": 141.75000537971667,
        "KEYWORD": "贵州旅游线路",
        "showCount": 632.9999945014715,
        "click_per_cost": 5.250000045684019
      }
    ]
  }
]
"""

'''
json_str = """
{
        "a1": 61.0000012665987,
        "realcost": 316.2400087690851,
        "KEYWORD": "贵州旅游景点",
        "showCount": 1287.9999949932098,
        "click_per_cost": 5.184262331191888
      }
"""
'''


def post_process_native_result(raw_json_data):
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

host = "127.0.0.1"
port = 3379
url = "http://{}:{}/druid/v2/?pretty".format(host, port)
print(url)


b=host.encode('utf-8')
print(b)
print(type(b))

raw_json_data = simplejson.loads(json_str)
if type(raw_json_data).__name__ =="list":
    print(type(raw_json_data))
elif type(raw_json_data).__name__ =="dict":
    print(type(raw_json_data))

final_json_data = post_process_native_result(raw_json_data)
json_str = simplejson.dumps(final_json_data)
print(json_str)







print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")


from pydruid.db import connect
from sqlalchemy.orm.query import Query
import datetime
import decimal
import uuid
import binascii
from six.moves import urllib
from base64 import b64encode

class JSONEncoder(simplejson.JSONEncoder):
    """Adapter for `simplejson.dumps`."""

    def default(self, o):
        # Some SQLAlchemy collections are lazy.
        if isinstance(o, Query):
            result = list(o)
        elif isinstance(o, decimal.Decimal):
            result = float(o)
        elif isinstance(o, (datetime.timedelta, uuid.UUID)):
            result = str(o)
        # See "Date Time String Format" in the ECMA-262 specification.
        elif isinstance(o, datetime.datetime):
            result = o.isoformat()
            if o.microsecond:
                result = result[:23] + result[26:]
            if result.endswith("+00:00"):
                result = result[:-6] + "Z"
        elif isinstance(o, datetime.date):
            result = o.isoformat()
        elif isinstance(o, datetime.time):
            if o.utcoffset() is not None:
                raise ValueError("JSON can't represent timezone-aware times.")
            result = o.isoformat()
            if o.microsecond:
                result = result[:12]
        elif isinstance(o, memoryview):
            result = binascii.hexlify(o).decode()
        elif isinstance(o, bytes):
            result = binascii.hexlify(o).decode()
        else:
            result = super(JSONEncoder, self).default(o)
        return result


def json_loads(data, *args, **kwargs):
    """A custom JSON loading function which passes all parameters to the
    simplejson.loads function."""
    return simplejson.loads(data, *args, **kwargs)

def json_dumps(data, *args, **kwargs):
    """A custom JSON dumping function which passes all parameters to the
    simplejson.dumps function."""
    kwargs.setdefault("cls", JSONEncoder)
    kwargs.setdefault("encoding", None)
    return simplejson.dumps(data, *args, **kwargs)

def run_native_query(querystr):
    #pydruid搜索_prepare_url_headers_and_body和_stream_query
    host = "10.15.101.88"
    port = "8888"
    username = None
    password = None

    url = "http://{}:{}/druid/v2/?pretty".format(host, port)

    headers = {"Content-Type": "application/json"}
    if (username is not None) and (password is not None):
        authstring = "{}:{}".format(username, password)
        b64string = b64encode(authstring.encode()).decode()
        headers["Authorization"] = "Basic {}".format(b64string)

    try:
        req = urllib.request.Request(url, querystr, headers, method="POST")
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
        final_json_data = post_process_native_result(raw_json_data)
        json_str = json_dumps(final_json_data)

    return json_str


querystr = """
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
      "expression": "(\"AD_CLICK_COUNT\" * \"KW_AVG_COST\")"
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
      "expression": "(\"realcost\" / \"a1\")",
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
  "threshold": 2
}
"""

json_str = run_native_query(querystr)
print(json_str)







'''
connection = connect(
    host="10.15.101.88",
    port="8888",
    path="/druid/v2/sql/",
    scheme="http",
    user=None,
    password=None,
)

cursor = connection.cursor()

query = """
SELECT DATE_TRUNC('day', __time) AS daytime,
       TOUR_DEST,
       sum(ORDER_COUNT) AS orders,
       CASE
           WHEN sum(ORDER_COUNT) = 0 THEN 0
           ELSE (sum(AD_CLICK_COUNT * KW_AVG_COST_MONEY)/sum(ORDER_COUNT))
       END AS cpo
FROM "travels_demo"
WHERE EVENT_TYPE='数据报告'
GROUP BY DATE_TRUNC('day', __time),
         TOUR_DEST
"""

try:
    cursor.execute(query)
    columns = fetch_columns(
        [(i[0], TYPES_MAP.get(i[1], None)) for i in cursor.description]
    )
    rows = [
        dict(zip((column["name"] for column in columns), row)) for row in cursor
    ]

    data = {"columns": columns, "rows": rows}
    json_data = json_dumps(data)
    print(json_data)
finally:
    connection.close()
'''
