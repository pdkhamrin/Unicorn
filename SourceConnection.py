import Metadata
import pyodbc
import sys
import datetime
import json


#Подключения к источнику
class SourceConnection:

 #Все свойства подключения к источнику
 @staticmethod
 def SourceParam():

    result = ""
    sources_qr = "select " \
                 "s.source_id, " \
                 "sa.attr_type, " \
                 "sa.attr_value " \
                 "from metadata.source s " \
                 "left join metadata.source_attr sa " \
                 "on s.source_id=sa.source_id " \
                 "where s.disable_flg='0'"

    Metadata.Metadata.crsr.execute(sources_qr)
    sources = Metadata.Metadata.crsr.fetchall()

    if sources == None:
        result = '{"result": null, "error_flg":"1","error_code":"000001","error_text":"No source was found"}'
        return result

    sources_len = sources.__len__()
    i = 0
    result = '{"result":['

    while i < sources_len:

        if i == sources_len - 1:
            comma = ""
        else: comma = ","

        result = result + \
                 '{"source_id":"'+str(sources[i][0])+'",' + \
                 '"attr_type":"'+str(sources[i][1])+'",' + \
                 '"attr_value":"'+str(sources[i][2])+'"}'+comma
        i += 1

    result = result + '], "error_flg":"0","error_code":null,"error_text":null}'

    return result

 #создание подключения к источнику
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
         result = '{"result": [{"bit":"0","text":"Connection failed"}], "error_flg":"0","error_code":null,"error_text":null}'
     else: result = '{"result": [{"bit":"1","text":"Successful connection"}], "error_flg":"0","error_code":null,"error_text":null}'

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
     cnct_check = json.loads(cnct_check)
     if cnct_check["result"][0]["bit"] == "0":
         return json.dumps(cnct_check)

     #проверка имени источника
     nm_check = SourceConnection.SourceNameCheck(source_name)
     if nm_check == 0:
         return '{"result": [{"bit":"1","text":"Creation failed"}], "error_flg":"1","error_code":"000006","error_text":"Source name already exists"}'

     #запись в метаданные
     max_source_id_qr = "select coalesce(max(source_id),0) from metadata.source"
     Metadata.Metadata.crsr.execute(max_source_id_qr)
     max_source_id = str(Metadata.Metadata.crsr.fetchall()[0][0]+1)


     insert_source_qr = "insert into metadata.source (source_id, disable_flg) values ("+max_source_id+",'0');"
     insert_sourceprm_qr = "insert into metadata.source_attr (source_id, attr_type, attr_value) values " + \
                           "("+max_source_id+","+"'source_name','"+str(source_name)+"')," + \
                           "("+max_source_id+","+"'source_type','"+str(source_type)+"')," + \
                           "("+max_source_id+","+"'server_name','"+str(server_name)+"')," + \
                           "("+max_source_id+","+"'database','"+str(database)+"')," + \
                           "("+max_source_id+","+"'user','"+str(user)+"')," + \
                           "("+max_source_id+","+"'password','"+str(password)+"')," + \
                           "("+max_source_id+","+"'port_number','"+str(port_number)+"');"
     create_qr = insert_source_qr + insert_sourceprm_qr

     #выполнение запроса
     try:
         crsr.execute(create_qr)
         result = '{"result": [{"bit":"1","text":"Creation succeed"}], "error_flg":"0","error_code":null,"error_text":null}'
     except:
         crsr.rollback()
         result = '{"result": [{"bit":"0","text":"Creation failed"}], "error_flg":"1","error_code":"000008","error_text":"Source creation failed"}'

     return result

 #изменение подключения к источнику
 @staticmethod
 def SourceAlter(source_id, params):
     result = ""

     if source_id == None or source_id == "":
         return '{"result": [{"bit":"0","text":"Alter failed"}], "error_flg":"1","error_code":"000009","error_text":"Source_id is empty"}'

     #проверка подключения
     cnct_check = SourceConnection.SourceConnectionCheck(params["server_name"],params["database"],params["user"],params["password"],params["port_number"])
     cnct_check = json.loads(cnct_check)
     if cnct_check["result"][0]["bit"] == "0":
         return json.dumps(cnct_check)

     #изменение метаданных
     del_qr = "delete from metadata.source_attr where source_id="+source_id+";"
     ins_qr = "insert into metadata.source_attr (source_id, attr_type, attr_value) values " + \
              "("+source_id+","+"'source_name','"+str(params["source_name"])+"')," + \
              "("+source_id+","+"'source_type','"+str(params["source_type"])+"')," + \
              "("+source_id+","+"'server_name','"+str(params["server_name"])+"')," + \
              "("+source_id+","+"'database','"+str(params["database"])+"')," + \
              "("+source_id+","+"'user','"+str(params["user"])+"')," + \
              "("+source_id+","+"'password','"+str(params["password"])+"')," + \
              "("+source_id+","+"'port_number','"+str(params["port_number"])+"');"
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
     del_qr = "update metadata.source set disable_flg='1' where source_id="+source_id+";"
     #выполнение запроса
     try:
         Metadata.Metadata.crsr.execute(del_qr)
         result = '{"result": [{"bit":"1","text":"Delete succeed"}], "error_flg":"0","error_code":null,"error_text":null}'
     except:
         Metadata.Metadata.crsr.rollback()
         result = '{"result": [{"bit":"0","text":"Delete failed"}], "error_flg":"1","error_code":"000010","error_text":"Source delete failed"}'

     return result



 #редактирование подключения к источнику
 @staticmethod
 def SourceEdit(params):
     #source_params имеет следующий вид:
     # {"input":[
     # {"source_id":"source_id",
     #  "source_param":[
     #             {
     #              "attr_type":"",
     #              "attr_value":""
     #             }
     #           ]
     #  ,"change_type":"1"
     #}]}
     #change_type: 1 - создание, 2 - изменение, 3 - удаление
     #при change_type = 3: может быть заполнен только source_id
     #attr_type: source_name, source_type, server_name, database, port_number, user, password

     params = json.loads(params)
     input_len = params["input"].__len__()

     input_i = 0
     while input_i < input_len:
         #ключ и тип изменения
         source_id = params["input"][input_i]["source_id"]
         change_type = params["input"][input_i]["change_type"]
         #параметры
         param_i = 0
         params_len = params["input"][input_i]["source_param"].__len__()
         server_name = ""
         database = ""
         user = ""
         password = ""
         source_name = ""
         source_type = ""
         port_number = ""
         while param_i < params_len:
             if params["input"][input_i]["source_param"][param_i]["attr_type"] == "server_name":
                 server_name = params["input"][input_i]["source_param"][param_i]["attr_value"]
             elif params["input"][input_i]["source_param"][param_i]["attr_type"] == "database":
                 database = params["input"][input_i]["source_param"][param_i]["attr_value"]
             elif params["input"][input_i]["source_param"][param_i]["attr_type"] == "user":
                user = params["input"][input_i]["source_param"][param_i]["attr_value"]
             elif params["input"][input_i]["source_param"][param_i]["attr_type"] == "password":
                 password = params["input"][input_i]["source_param"][param_i]["attr_value"]
             elif params["input"][input_i]["source_param"][param_i]["attr_type"] == "source_name":
                 source_name = params["input"][input_i]["source_param"][param_i]["attr_value"]
             elif params["input"][input_i]["source_param"][param_i]["attr_type"] == "source_type":
                 source_type = params["input"][input_i]["source_param"][param_i]["attr_value"]
             elif params["input"][input_i]["source_param"][param_i]["attr_type"] == "port_number":
                 port_number = params["input"][input_i]["source_param"][param_i]["attr_value"]
             param_i = param_i + 1

         input_i = input_i + 1

         if change_type=="1":
             return SourceConnection.SourceCreate(source_type, source_name, server_name, database, user, password, port_number)
         if change_type=="2":
             return SourceConnection.SourceAlter(source_id,
                                                 {"source_type":source_type
                                                 ,"source_name":source_name
                                                 ,"server_name":server_name
                                                 ,"database":database
                                                 ,"user":user
                                                 ,"password":password
                                                 ,"port_number":port_number})
         if change_type=="3":
             return SourceConnection.SourceDelete(source_id)


