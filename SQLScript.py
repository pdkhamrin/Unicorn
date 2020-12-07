import pyodbc
import sys
import datetime
import json

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

