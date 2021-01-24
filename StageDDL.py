import Metadata
import SQLScript
import Support
import SourceConnection
import pyodbc
import sys
import datetime
import json


#Генерация DDL слоя STAGE
class StageDDL:
    @staticmethod
    def CreateStageTable(table_type, table,attr, source_id):
        #Создает таблицу очереди или таблицу обработанных данных в зависимости от table_type
        #attr: {
        #           "attribute":"наименование атрибута"
        #           ,"datatype":"тип данных"
        #           ,"length":"Размер десятичного числа/строки"
        #           ,"scale":"Количество знаков после запятой"}]
        # table_type: queue - таблица очереди, storage - таблица обработанных данных

        result = {}
        creation = []
        error = []
        source_table = table
        table = str(source_id)+"_"+table + "_"+table_type # добавляем префикс (id источника) и постфикс таблицы очереди
        # добавляем технические атрибуты
        attr.append({"attribute":"deleted_flg","datatype":"INT","length":None,"scale":None})
        attr.append({"attribute":"update_dttm","datatype":"DATETIME","length":None,"scale":None})
        attr.append({"attribute":"processed_dttm","datatype":"DATETIME","length":None,"scale":None})
        attr.append({"attribute":"status_id","datatype":"INT","length":None,"scale":None})
        if table_type == "storage":
            attr.append({"attribute":"row_id","datatype":"INT","length":None,"scale":None})
            attr.append({"attribute":"check_flg","datatype":"INT","length":None,"scale":None})


        # вытаскиваем тип источника
        src_type_qr = SQLScript.SQLScript.ObjectAttr("source",source_id, None,"source_type")
        Metadata.Metadata.crsr.execute(src_type_qr)
        src_type = Metadata.Metadata.crsr.fetchall()
        src_type = src_type[0][2]

        # проверка на наличие источника
        source_exst_sql = SQLScript.SQLScript.ObjectCheck(source_id,"source")
        Metadata.Metadata.crsr.execute(source_exst_sql)
        source_exst = Metadata.Metadata.crsr.fetchall()
        if int(source_exst[0][0]) == 0:
            error.clear()
            error.append({"error_flg":1,"error_code":"0000150","error_text":"Source isn't found"})
            result.update({"result":[{"result":None,"error":error}]})
            return result

        # проверяем наличие уже созданной таблицы
        table_exst_sql = SQLScript.SQLScript.ObjectNameCheck(table, "table","stg")
        Metadata.Metadata.crsr.execute(table_exst_sql)
        table_exst = Metadata.Metadata.crsr.fetchall()

        if int(table_exst[0][0]) > 0:
            error.clear()
            error.append({"error_flg":1,"error_code":"0000150","error_text":"Queue table already exists"})
            result.update({"result":[{"result":None,"error":error}]})
            return result

        # вычисляем макс. table_id
        tbl_id_sql = SQLScript.SQLScript.MaxObjectId("table")
        Metadata.Metadata.crsr.execute(tbl_id_sql)
        tbl_id = Metadata.Metadata.crsr.fetchall()
        tbl_id = int(tbl_id[0][0]) + 1

        # вставляем id новой таблице очереди
        tbl_id_qr = SQLScript.SQLScript.InsertObject("table",tbl_id)

        #выполнение запроса
        try:
            Metadata.Metadata.crsr.execute(tbl_id_qr)
        except:
            Metadata.Metadata.crsr.rollback()
            error.clear()
            error.append({"error_flg":"1","error_code":"0000151","error_text":"Something is wrong during queue table creation"})
            result.clear()
            result.update({"result":[{"result":None,"error":error}]})
            return result

        # вставляем параметры новой таблицы очереди
        table_attr = {"table_name":table,"table_type":table_type,"schema":"stg","source_table_name":source_table}
        tbl_prm_qr = SQLScript.SQLScript.InsertObjectAttr("table",tbl_id, table_attr)

        #выполнение запроса
        try:
            Metadata.Metadata.crsr.execute(tbl_prm_qr)
        except:
            Metadata.Metadata.crsr.rollback()
            error.clear()
            error.append({"error_flg":"1","error_code":"0000151","error_text":"Something is wrong during queue table creation"})
            result.clear()
            result.update({"result":[{"result":None,"error":error}]})
            return result

         # вставляем связь таблицы и источника
        src_tbl_qr = SQLScript.SQLScript.InsertLinkObject("source","table",source_id,tbl_id)
        #выполнение запроса
        try:
            Metadata.Metadata.crsr.execute(src_tbl_qr)
        except:
            Metadata.Metadata.crsr.rollback()
            error.clear()
            error.append({"error_flg":"1","error_code":"0000151","error_text":"Something is wrong during queue table creation"})
            result.clear()
            result.update({"result":[{"result":None,"error":error}]})
            return result

        # определяем макс. id атрибута
        max_attr_qr = SQLScript.SQLScript.MaxObjectId("column")
        Metadata.Metadata.crsr.execute(max_attr_qr)
        column_id = Metadata.Metadata.crsr.fetchall()
        column_id = int(column_id[0][0]) + 1

        # вставка атрибутов и их параметров
        attr_len = len(attr)
        i=0
        while i<attr_len:
            attr_qr = SQLScript.SQLScript.InsertObject("column",column_id)
            try:
                Metadata.Metadata.crsr.execute(attr_qr)
            except:
                Metadata.Metadata.crsr.rollback()
                error.clear()
                error.append({"error_flg":"1","error_code":"0000151","error_text":"Something is wrong during queue table creation"})
                result.clear()
                result.update({"result":[{"result":None,"error":error}]})
                return result
            # маппинг типов данных
            datatype = attr[i].get("datatype",None)
            length = attr[i].get("length",None)
            if src_type != "POSTGRESQL":
                datatype = Support.Support.DataTypeMapping(datatype, length)["datatype"]
                length = Support.Support.DataTypeMapping(datatype, length)["length"]
            scale = attr[i]["scale"]
            # вставка параметров атрибута
            if length is None:
                length="none"
            if scale is None:
                scale="none"
            if attr[i]["attribute"] in {"deleted_flg","update_dttm","processed_dttm","status_id"}:
                column_type="tech"
            else:
                column_type="basic"
            column_attr = {"column_name":attr[i]["attribute"],"column_type":column_type,"datatype":datatype,"length":length,"scale":scale,"source_column_name":attr[i]["attribute"]}
            attr_prm_qr = SQLScript.SQLScript.InsertObjectAttr("column",column_id,column_attr)
            try:
                Metadata.Metadata.crsr.execute(attr_prm_qr)
            except:
                Metadata.Metadata.crsr.rollback()
                error.clear()
                error.append({"error_flg":"1","error_code":"0000151","error_text":"Something is wrong during queue table creation"})
                result.clear()
                result.update({"result":[{"result":None,"error":error}]})
                return result
            #вставка связи атрибута и таблицы
            attr_link_qr = SQLScript.SQLScript.InsertLinkObject("table","column",tbl_id,column_id)
            try:
                Metadata.Metadata.crsr.execute(attr_link_qr)
            except:
                Metadata.Metadata.crsr.rollback()
                error.clear()
                error.append({"error_flg":"1","error_code":"0000151","error_text":"Something is wrong during queue table creation"})
                result.clear()
                result.update({"result":[{"result":None,"error":error}]})
                return result
            column_id=column_id+1
            i=i+1

        # создаем таблицу
        create_qr = SQLScript.SQLScript.DDLCreateTable("stg",table,attr,1)
        try:
            Metadata.Metadata.crsr.execute(create_qr)
        except:
            Metadata.Metadata.crsr.rollback()
            error.clear()
            error.append({"error_flg":"1","error_code":"0000151","error_text":"Something is wrong during queue table creation"})
            result.clear()
            result.update({"result":[{"result":None,"error":error}]})
            return result

        error.clear()
        error.append({"error_flg":"0","error_code":None,"error_text":None})
        creation.clear()
        creation.append({"creation_flg":"1","creation_status":"Successful queue table creation"})
        result.clear()
        result.update({"result":[{"result":creation,"error":error}]})


        return result
