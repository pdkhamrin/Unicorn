import pyodbc
import sys
import datetime
import json
import SourceConnection
import Metadata
import Support
#SQL скрипты
class SQLScript:

    @staticmethod
    def SourceId(source_id=None, disable_flg=None):

        select = "select " \
                 "distinct " \
                 "source_id " \
                 "from metadata.source "
        filter="where 1=1 "
        script=""

        if source_id is not None:
            filter=filter+" and source_id="+str(source_id)
        if disable_flg is not None:
            filter=filter+" and disable_flg='"+str(disable_flg)+"'"

        script= select + filter + ";"

        return script

    @staticmethod
    def ObjectCheck(object_id, object_type):
        script = "select count(*) from metadata."+object_type+" where "+object_type+"_id="+str(object_id)+" and disable_flg='0';"
        return script

    @staticmethod
    def ObjectNameCheck(object_name, object_type, object_schema=None):

        obj_schm_script = ""
        if object_schema is not None:
            obj_schm_script = " and obj_schm.attr_value='"+object_schema+"'"
        script = "select count(*)" \
                 " from metadata."+object_type+" obj" \
                 " left join metadata."+object_type+"_attr obj_nm" \
                 " on obj."+object_type+"_id=obj_nm."+object_type+"_id" \
                 " and obj_nm.attr_type='"+object_type+"_name'" \
                 " left join metadata."+object_type+"_attr obj_schm" \
                 " on obj."+object_type+"_id=obj_schm."+object_type+"_id" \
                 " and obj_schm.attr_type='schema'" \
                 " where obj_nm.attr_value='"+object_name+"'"+obj_schm_script+" and obj.disable_flg=0;"
        return script

    @staticmethod
    def ObjectAttr(object_type, object_id=None, disable_flg=None, object_attr_type=None):

        select= "select " \
                "obj."+object_type+"_id, " \
                "obj_attr.attr_type, " \
                "obj_attr.attr_value " \
                "from metadata."+object_type+" obj " \
                "left join metadata."+object_type+"_attr obj_attr " \
                "on obj."+object_type+"_id=obj_attr."+object_type+"_id "
        filter="where 1=1 "
        script=""

        if object_id is not None:
            filter=filter+" and obj."+object_type+"_id="+str(object_id)
        if disable_flg is not None:
            filter=filter+" and obj.disable_flg='"+str(disable_flg)+"'"
        if object_attr_type is not None:
            filter = filter+" and obj_attr.attr_type='"+object_attr_type+"'"

        script=select + filter + ";"

        return script

    @staticmethod
    def MaxObjectId(object_type):

        script = "select coalesce(max("+object_type+"_id),0) from metadata."+object_type+";"
        return script

    @staticmethod
    def InsertObject(object_type,object_id):

        script = "insert into metadata."+object_type+" ("+object_type+"_id, disable_flg) values ("+str(object_id)+",'0');"
        return script

    @staticmethod
    def InsertLinkObject(object1, object2,object1_id, object2_id):

        object_type = object1+"_"+object2
        script = "insert into metadata."+object_type+" ("+object1+"_id, "+object2+"_id) values ("+str(object1_id)+","+str(object2_id)+");"
        return script

    @staticmethod
    def InsertObjectAttr(object_type, object_id, object_attr):

        insert = "insert into metadata."+object_type+"_attr ("+object_type+"_id, attr_type, attr_value) values "

        values = ""
        attr_key = list(object_attr.keys())
        source_attr_len = len(list(object_attr.keys()))
        i=0
        while i < source_attr_len:
            if i!=source_attr_len-1:
                comma = ","
            else: comma=""
            values = values +"("+str(object_id)+",'"+attr_key[i]+"','"+object_attr[attr_key[i]]+"')"+comma
            i = i + 1

        script = insert + values + ";"

        return script

    @staticmethod
    def DeleteObjectAttr(object_type, object_id):

        script = "delete from metadata."+object_type+"_attr where "+object_type+"_id="+str(object_id)+";"
        return script

    @staticmethod
    def DeleteObject(object_type, object_id):

        script = "update metadata."+object_type+" set disable_flg='1' where "+object_type+"_id="+str(object_id)+";"
        return script

    @staticmethod
    def ListSourceObject(database=None, schema=None, table=None):

        select = "select " \
                 "tab.table_catalog, " \
                 "tab.table_schema, " \
                 "tab.table_name, " \
                 "tab.table_type, " \
                 "col.column_name, " \
                 "col.data_type, " \
                 "col.character_maximum_length, " \
                 "col.numeric_precision, " \
                 "col.numeric_scale, " \
                 "case when ky.constraint_name is not null then 1 else 0 end " \
                 "from information_schema.tables tab " \
                 "left join information_schema.columns col " \
                 "on 1=1 " \
                 "and tab.table_catalog=col.table_catalog " \
                 "and tab.table_schema=col.table_schema " \
                 "and tab.table_name=col.table_name " \
                 "left join information_schema.key_column_usage ky " \
                 "on 1=1 " \
                 "and tab.table_catalog=ky.table_catalog " \
                 "and tab.table_schema=ky.table_schema " \
                 "and tab.table_name=ky.table_name " \
                 "and col.column_name=ky.column_name " \
                 "and substring(ky.constraint_name,1,2)='PK'"
        filter = " where 1=1"
        if database is not None:
            filter = filter + " and tab.table_catalog='"+database+"'"
        if schema is not None:
            filter = filter + " and tab.table_schema='"+schema+"'"
        if table is not None:
            filter = filter + " and tab.table_name='"+table+"'"

        script = select + filter + ";"

        return script

    @staticmethod
    def TableRaw(database, schema, table):

        script = "select top 10 * from "+database+"."+schema+"."+table

        return script

    @staticmethod
    def DDLCreateTable (schema, table, attr, source):
        #attr: {
        #           "attribute":"наименование атрибута"
        #           ,"datatype":"тип данных"
        #           ,"length":"Размер десятичного числа/строки"
        #           ,"scale":"Количество знаков после запятой"}]
        # source - указатель, что типы данных из сторонней СУБД (не PostgreSQL)

        attr_len = len(attr)

        script = "create table "+schema+"."+'"'+table+'"'+" ("
        attr_script = ""

        i=0
        while i<attr_len:
            datatype_length = ""
            scale = ""
            datatype = ""
            comma = ","
            if source == 1:
                datatype = Support.Support.DataTypeMapping(attr[i].get("datatype",None),attr[i].get("length",None))["datatype"]
                datatype_length = Support.Support.DataTypeMapping(attr[i].get("datatype",None),attr[i].get("length",None))["length"]
            if attr[i].get("scale",None) is not None:
                scale = ","+str(attr[i].get("scale",None))
            if datatype_length is not None:
                datatype_length = "("+str(datatype_length)+scale+")"
            else:
                datatype_length = ""
            if i == attr_len - 1:
                comma=""
            attr_script = attr_script + attr[i].get("attribute",None)+" "+datatype+datatype_length+comma
            i=i+1

        script = script + attr_script + ");"
        return script







