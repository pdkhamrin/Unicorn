import Metadata
import SQLScript
import pyodbc
import sys
import datetime
import json


#Подключения к источнику
class SourceConnection:

 #Все свойства подключения к источнику (API)
 @staticmethod
 def GetSourceParams(input=None):
     #input имеет следующий вид:
     # {"input":[
     # {"source_id":"source_id",
     #  "disable_flg":"disable_flg"
     #}]}
     #Если source_id не заполнен, будут выведены все подключения
     #Дефолтное значение для disable_flg - 0

    result = ""
    if input is not None:
        input = json.loads(input)
        source_id=input["input"][0].get("source_id")
        disable_flg=input["input"][0].get("disable_flg")
    else:
        source_id=None
        disable_flg=None

    source_qr = SQLScript.SQLScript.SourceId(source_id=source_id,disable_flg=disable_flg)
    source_attr_qr = SQLScript.SQLScript.SourceAttr(source_id=source_id,disable_flg=disable_flg)

    Metadata.Metadata.crsr.execute(source_qr)
    sources = Metadata.Metadata.crsr.fetchall()
    Metadata.Metadata.crsr.execute(source_attr_qr)
    sources_attr = Metadata.Metadata.crsr.fetchall()

    result = {"result":[]}
    i=0
    for source in sources:
        result["result"].append({"source_id":source[0]})
        for source_attr in sources_attr:
            if source[0]==source_attr[0]:
                result["result"][i].update({source_attr[1]:source_attr[2]})
        i=i+1


    if result["result"] == "[]":
        result.update({"error":[{"error_flg":"0","error_code":None,"error_text":None}]})
    else: result.update({"error":[{"error_flg":"1","error_code":"000001","error_text":"Source is not found"}]})

    result = json.dumps(result)

    return result

 @staticmethod
 def SourceConnectionCheck(server,database,user,password,port):

     try:
         pyodbc.connect(
             server=server,
             database=database,
             uid=user,
             tds_version='7.3',
             pwd=password,
             port=port,
             driver='/usr/local/lib/libtdsodbc.so'
         )
     except Exception:
         result = 0
     else: result = 1

     return result

 #проверка подключения к источнику (API)
 @staticmethod
 def GetSourceConnectionCheck(input):
     #input имеет следующий вид:
     # {"input":[
     # {"server_name":"server_name",
     #  "database":"database",
     #  "user":"user",
     #  "password":"password",
     #  "port_number":"port_number"
     #}]}
     #Все атрибуты должны быть заполнены

     input = json.loads(input)
     server = input["input"][0]["server_name"]
     database = input["input"][0]["database"]
     user = input["input"][0]["user"]
     password = input["input"][0]["password"]
     port = input["input"][0]["port_number"]
     check = SourceConnection.SourceConnectionCheck(server, database, user, password, port)

     if check == 1:
         result = """{"result":[{"connection_flg:"1","connection_status":"Connection succeeded"}], "error":[{"error_flg":"0","error_code":None,"error_text":None}]}"""
     else: result = """{"result":[{"connection_flg:"0","connection_status":"Connection failed"}], "error":[{"error_flg":"1","error_code":"000002","error_text":"Connection failed"}]}"""

     return result


 #проверка имени источника
 @staticmethod
 def SourceNameCheck(source_name):

     result = ""
     qr = "select coalesce(count(*),0) from metadata.source_attr where attr_type='source_name' and attr_value='"+str(source_name)+"'"
     Metadata.Metadata.crsr.execute(qr)
     qr_result = Metadata.Metadata.crsr.fetchall()

     if qr_result[0][0] != 0:
         return 0
     else: return 1


 #создание подключения к источнику
 @staticmethod
 def SourceCreate(source_type, source_name, server_name, database, user, password, port_number):

     result = ""
     crsr = Metadata.Metadata.crsr

     #проверка подключения
     cnct_check = SourceConnection.SourceConnectionCheck(server_name,database,user,password,port_number)
     if cnct_check == 0:
         return """{"result":[{"creation_flg:"0","creation_status":"Connection failed"}], "error":[{"error_flg":"1","error_code":"000003","error_text":"Connection failed"}]}"""

     #проверка имени источника
     nm_check = SourceConnection.SourceNameCheck(source_name)
     if nm_check == 0:
         return '{"result": [{"creation_flg":"0","creation_status":"Creation failed"}], "error":[{"error_flg":"1","error_code":"000004","error_text":"Source name already exists"}]}'

     #запись в метаданные
     max_source_id_qr = SQLScript.SQLScript.MaxSourceId()
     Metadata.Metadata.crsr.execute(max_source_id_qr)
     source_id = str(Metadata.Metadata.crsr.fetchall()[0][0]+1)
     source_attr = '{"server_name":"'+server_name + '",' \
                   ' "database":"'+database + '",' \
                   ' "user":"'+user + '",' \
                   ' "password":"'+password + '",' \
                   ' "port_number":"'+port_number + '",' \
                   ' "source_name":"'+source_name + '",' \
                   ' "source_type":"'+source_type + '"}' \

     insert_source_qr = SQLScript.SQLScript.InsertSource(source_id)
     insert_sourceprm_qr = SQLScript.SQLScript.InsertSourceAttr(source_id, source_attr)
     create_qr = insert_source_qr + insert_sourceprm_qr

     #выполнение запроса
     try:
         crsr.execute(create_qr)
         result = '{"result": [{"creation_flg":"1","creation_status":"Creation succeeded"}], "error":[{"error_flg":"0","error_code":None,"error_text":None}]}'
     except:
         crsr.rollback()
         result = '{"result": [{"creation_flg":"0","creation_status":"Creation failed"}], "error":[{"error_flg":"1","error_code":"000005","error_text":"Creation failed"}]}'

     return result

 #изменение подключения к источнику
 @staticmethod
 def SourceAlter(source_id, source_type, source_name, server_name, database, user, password, port_number):
     result = ""

     if source_id == None or source_id == "":
         return '{"result": [{"creation_flg":"0","alter":"Alter failed"}], "error":[{"error_flg":"1","error_code":"000006","error_text":"Source_id is empty"}]}'

     #проверка подключения
     cnct_check = SourceConnection.SourceConnectionCheck(server_name,database,user,password,port_number)
     if cnct_check == 0:
         return """{"result":[{"alter flg":"0","alter status":"Alter failed"}], "error":[{"error_flg":"1","error_code":"000007","error_text":"Connection failed"}]}"""

     #изменение метаданных
     del_qr = SQLScript.SQLScript.DeleteSourceAttr(source_id)
     source_attr = '{"server_name":"'+server_name + '",' \
                   ' "database":"'+database + '",' \
                   ' "user":"'+user + '",' \
                   ' "password":"'+password + '",' \
                   ' "port_number":"'+port_number + '",' \
                   ' "source_name":"'+source_name + '",' \
                   ' "source_type":"'+source_type + '"}'
     ins_qr = SQLScript.SQLScript.InsertSourceAttr(source_id, source_attr)
     alter_qr = del_qr + ins_qr

     #выполнение запроса
     try:
         Metadata.Metadata.crsr.execute(alter_qr)
         result = '{"result": [{"bit":"1","text":"Alter succeed"}], "error_flg":"0","error_code":null,"error_text":null}'
     except:
         Metadata.Metadata.crsr.rollback()
         result = '{"result": [{"bit":"0","text":"Alter failed"}], "error_flg":"1","error_code":"000010","error_text":"Source alter failed"}'

     return result


 #удаление подключения к источнику
 @staticmethod
 def SourceDelete(source_id):

     result = ""
     del_qr = SQLScript.SQLScript.DeleteSource(source_id)

     #выполнение запроса
     try:
         Metadata.Metadata.crsr.execute(del_qr)
         result = '{"result": [{"bit":"1","text":"Delete succeed"}], "error_flg":"0","error_code":null,"error_text":null}'
     except:
         Metadata.Metadata.crsr.rollback()
         result = '{"result": [{"bit":"0","text":"Delete failed"}], "error_flg":"1","error_code":"000010","error_text":"Source delete failed"}'

     return result



 #редактирование подключения к источнику (API)
 @staticmethod
 def SetSourceEdit(input):
     #params имеет следующий вид:
     # {"input":[
     # {"source_id":"source_id",
     #  "source_name":"source_name",
     #  "source_type":"source_type",
     #  "server_name":"server_name",
     #  "database":"database",
     #  "user":"user",
     #  "password":"password",
     #  "port_number":"port_number",
     #  "change_type":"1"
     #}]}
     #change_type: 1 - создание, 2 - изменение, 3 - удаление
     #при change_type = 1: заполняется все, кроме source_id
     #при change_type = 2: заполняется все (даже если атрибут не требуется изменять!)
     #при change_type = 3: может быть заполнен только source_id

     input = json.loads(input)
     input_len = input["input"].__len__()

     input_i = 0
     while input_i < input_len:
         source_id = input["input"][input_i]["source_id"]
         source_name = input["input"][input_i]["source_name"]
         source_type = input["input"][input_i]["source_type"]
         server_name = input["input"][input_i]["server_name"]
         database = input["input"][input_i]["database"]
         user = input["input"][input_i]["user"]
         password = input["input"][input_i]["password"]
         port_number = input["input"][input_i]["port_number"]
         change_type = input["input"][input_i]["change_type"]
         input_i = input_i + 1

         if change_type=="1":
             return SourceConnection.SourceCreate(source_type, source_name, server_name, database, user, password, port_number)
         if change_type=="2":
             return SourceConnection.SourceAlter(source_id,source_type, source_name, server_name, database, user, password, port_number)
         if change_type=="3":
             return SourceConnection.SourceDelete(source_id)

 @staticmethod
 def SourceConnect(source_id):

     source_attr_qr = SQLScript.SQLScript.SourceAttr(source_id)
     Metadata.Metadata.crsr.execute(source_attr_qr)
     source_attr = Metadata.Metadata.crsr.fetchall()

     for attr in source_attr:
         if attr[1]=="server_name":
             server_name=attr[2]
         if attr[1]=="database":
             database=attr[2]
         if attr[1]=="user":
             user=attr[2]
         if attr[1]=="password":
             password=attr[2]
         if attr[1]=="port_number":
             port_number=attr[2]

     cnct = pyodbc.connect(
             server=server_name,
             database=database,
             uid=user,
             tds_version='7.3',
             pwd=password,
             port=port_number,
             driver='/usr/local/lib/libtdsodbc.so'
     )

     crsr = cnct.cursor()

     return crsr
