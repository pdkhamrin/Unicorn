import pyodbc
import sys
import datetime
import json
import SourceConnection
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
    def SourceAttr(source_id=None, disable_flg=None):

        select= "select " \
                "s.source_id, " \
                "sa.attr_type, " \
                "sa.attr_value " \
                "from metadata.source s " \
                "left join metadata.source_attr sa " \
                "on s.source_id=sa.source_id "
        filter="where 1=1 "
        script=""

        if source_id is not None:
            filter=filter+" and s.source_id="+str(source_id)
        if disable_flg is not None:
            filter=filter+" and s.disable_flg='"+str(disable_flg)+"'"

        script=select + filter + ";"

        return script

    @staticmethod
    def MaxSourceId():

        script = "select coalesce(max(source_id),0) from metadata.source;"
        return script

    @staticmethod
    def InsertSource(source_id):

        script = "insert into metadata.source (source_id, disable_flg) values ("+source_id+",'0');"
        return script

    @staticmethod
    def InsertSourceAttr(source_id, source_attr):

        insert = "insert into metadata.source_attr (source_id, attr_type, attr_value) values "

        values = ""
        source_attr = json.loads(source_attr)
        attr_key = list(source_attr.keys())
        source_attr_len = len(list(source_attr.keys()))
        i=0
        while i < source_attr_len:
            if i!=source_attr_len-1:
                comma = ","
            else: comma=""
            values = values +"("+str(source_id)+",'"+attr_key[i]+"','"+source_attr[attr_key[i]]+"')"+comma
            i = i + 1

        script = insert + values + ";"

        return script

    @staticmethod
    def DeleteSourceAttr(source_id):

        script = "delete from metadata.source_attr where source_id="+str(source_id)+";"
        return script

    @staticmethod
    def DeleteSource(source_id):

        script = "update metadata.source set disable_flg='1' where source_id="+str(source_id)+";"
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
    def TableRaw(source_id, database, schema, table):

        script = "select top 10 * from "+database+"."+schema+"."+table

        return script



