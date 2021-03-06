import io
import csv
import xlsxwriter
from funcy import rpartial, project
from dateutil.parser import isoparse as parse_date
from redash.utils import json_loads, UnicodeWriter
from redash.query_runner import TYPE_BOOLEAN, TYPE_DATE, TYPE_DATETIME
from redash.authentication.org_resolving import current_org


def _convert_format(fmt):
    return (
        fmt.replace("DD", "%d")
        .replace("MM", "%m")
        .replace("YYYY", "%Y")
        .replace("YY", "%y")
        .replace("HH", "%H")
        .replace("mm", "%M")
        .replace("ss", "%s")
    )


def _convert_bool(value):
    if value is True:
        return "true"
    elif value is False:
        return "false"

    return value


def _convert_datetime(value, fmt):
    if not value:
        return value

    try:
        parsed = parse_date(value)
        ret = parsed.strftime(fmt)
    except Exception:
        return value

    return ret


def _get_column_lists(columns):
    date_format = _convert_format(current_org.get_setting("date_format"))
    datetime_format = _convert_format(
        "{} {}".format(
            current_org.get_setting("date_format"),
            current_org.get_setting("time_format"),
        )
    )

    special_types = {
        TYPE_BOOLEAN: _convert_bool,
        TYPE_DATE: rpartial(_convert_datetime, date_format),
        TYPE_DATETIME: rpartial(_convert_datetime, datetime_format),
    }

    fieldnames = []
    special_columns = dict()

    for col in columns:
        fieldnames.append(col["name"])

        for col_type in special_types.keys():
            if col["type"] == col_type:
                special_columns[col["name"]] = special_types[col_type]

    return fieldnames, special_columns


def serialize_query_result(query_result, is_api_user):
    if is_api_user:
        publicly_needed_keys = ["data", "retrieved_at"]
        return project(query_result.to_dict(), publicly_needed_keys)
    else:
        return query_result.to_dict()


def serialize_query_result_to_dsv(query_result, delimiter):
    s = io.StringIO()

    query_data = query_result.data
    data_ex = query_data.get("data_ex")

    datas = []
    datas.append(query_data)
    if data_ex != None:
        for item in data_ex:
            data = item.get("data")
            if data is not None:
                datas.append(data)

    for data in datas:
        columns = data.get("columns")
        if columns is None or len(columns) == 0:
            continue

        fieldnames, special_columns = _get_column_lists(data["columns"] or [])

        writer = csv.DictWriter(s, extrasaction="ignore", fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()

        for row in data["rows"]:
            for col_name, converter in special_columns.items():
                if col_name in row:
                    row[col_name] = converter(row[col_name])

            writer.writerow(row)

    return s.getvalue()


def serialize_query_result_to_xlsx(query_result):
    output = io.BytesIO()
    book = xlsxwriter.Workbook(output, {"constant_memory": True})

    query_data = query_result.data
    data_ex = query_data.get("data_ex")

    datas = []
    datas.append({"name": "result", "data": query_data})
    if data_ex != None:
        for item in data_ex:
            name = item.get("name")
            data = item.get("data")
            if name is not None and data is not None:
                datas.append({"name": name, "data": data})

    for item in datas:
        name = item["name"]
        data = item["data"]
        columns = data.get("columns")
        if columns is None or len(columns) == 0:
            continue

        sheet = book.add_worksheet(name)

        column_names = []
        for c, col in enumerate(data["columns"]):
            sheet.write(0, c, col["name"])
            column_names.append(col["name"])

        for r, row in enumerate(data["rows"]):
            for c, name in enumerate(column_names):
                v = row.get(name)
                if isinstance(v, (dict, list)):
                    v = str(v)
                sheet.write(r + 1, c, v)

    book.close()

    return output.getvalue()
