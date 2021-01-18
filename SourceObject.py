import Metadata
import SQLScript
import SourceConnection
import pyodbc
import sys
import datetime
import json


#Объекты источника
class SourceObject:

    @staticmethod
    def GetSourceObject(input=None):
        # input имеет следующий вид:
        # {"input":[
        # {"source_id":"source_id",
        #  "database":"database",
        #  "schema":"schema",
        #  "table":"table"
        #}]}
        #source_id всегда заполнен
        #Если database, schema, table не заполнены, будут выведены все базы данных со всеми объектами источника
        #Можно указывать только одно значение атрибута

        error = []

        if input == None:
            error.clear()
            error.append({"error_flg":1,"error_code":"000020","error_text":"Not enough parameters"})

        input = json.loads(input)
        source_id=input["input"][0].get(["source_id"],None)
        database=input["input"][0].get(["database"],None)
        schema=input["input"][0].get(["schema"],None)
        table=input["input"][0].get(["table"],None)

        select_qr = SQLScript.SQLScript.ListSourceObject(database, schema, table)

        crsr = SourceConnection.SourceConnection.SourceConnect(source_id)
        crsr.execute(select_qr)
        source_object = crsr.fetchall()

        databases = []
        schemas = []
        tables = []
        attributes = []

        objects = []
        result = {}

        for obj in source_object:
            if obj[0] not in databases:
                databases_arr = {}
                databases_arr.update({"database":obj[0],"schemas":[]})
                objects.append(databases_arr)
                databases.append(obj[0])
            if obj[0]+"&"+obj[1] not in schemas:
                for dtbs in objects:
                    if dtbs["database"] == obj[0]:
                        schemas_list = dtbs["schemas"]
                        schemas_list.append({"schema":obj[1], "tables":[]})
                        dtbs["schemas"]=schemas_list
                schemas.append(obj[0]+"&"+obj[1])
            if obj[0]+"&"+obj[1]+obj[2] not in tables:
                for dtbs in objects:
                    if dtbs["database"] == obj[0]:
                        for schms in dtbs["schemas"]:
                            if schms["schema"] == obj[1]:
                                tables_list = schms["tables"]
                                tables_list.append({"table":obj[2],"table_type":obj[3],"attributes":[]})
                                schms["tables"]=tables_list
                tables.append(obj[0]+"&"+obj[1]+obj[2])
            if obj[0]+"&"+obj[1]+obj[2]+"&"+obj[4] not in attributes:
                for dtbs in objects:
                    if dtbs["database"] == obj[0]:
                        for schms in dtbs["schemas"]:
                            if schms["schema"] == obj[1]:
                                for tbls in schms["tables"]:
                                    if tbls["table"] == obj[2]:
                                        attributes_list = tbls["attributes"]
                                        attributes_list.append({"attribute":obj[4],"datatype":obj[5],"key":obj[9]})
                                        tbls["attributes"]=attributes_list
            attributes.append(obj[0]+"&"+obj[1]+obj[2]+"&"+obj[4])

        result.update({"result":[{"objects":objects, "errors":error}]})

        result = json.dumps(result)

        return result

    @staticmethod
    def GetTableRaw(input):
        # input имеет следующий вид:
        # {"input":[
        # {"source_id":"source_id",
        #  "database":"database",
        #  "schema":"schema",
        #  "table":"table"
        #}]}
        #Все атрибуты должны быть заполнены

        result = {}
        table_raw = []
        error = []

        input = json.loads(input)
        source_id=input["input"][0].get(["source_id"],None)
        database=input["input"][0].get(["database"],None)
        schema=input["input"][0].get(["schema"],None)
        table=input["input"][0].get(["table"],None)

        if source_id is None or database is None or schema is None or table is None:
            error.clear()
            error.append({"error_flg":1,"error_code":"000020","error_text":"Not enough parameters"})

        select_qr = SQLScript.SQLScript.TableRaw(source_id, database, schema, table)
        col_qr = SQLScript.SQLScript.ListSourceObject(database, schema, table)

        crsr = SourceConnection.SourceConnection.SourceConnect(source_id)
        crsr.execute(select_qr)
        raws = crsr.fetchall()

        crsr.execute(col_qr)
        columns = crsr.fetchall()

        col_num = len(columns)
        raw_num = len(raws)

        test = []
        i = 0
        while i < raw_num:
            raw = {}
            j = 0
            while j < col_num:
                raw.update({columns[j][4]:str(raws[i][j])})
                j = j + 1
            # print(raw)
            table_raw.append(raw)
            i = i + 1

        result.update({"result":[{"table_raw":table_raw,"error":error}]})

        result = json.dumps(result)

        return result


