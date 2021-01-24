import psycopg2
import Metadata

#Работа с метаданными
class Support:

    @staticmethod
    def DataTypeMapping(datatype, length):
        # Маппинг типов данных сторонних источников на типы данных PostgreSQL
        result = {}
        dttp_map_sql = "select" \
                       "  source_data_type" \
                       ", source_data_type_length" \
                       ", anchor_data_type" \
                       ", anchor_data_type_length" \
                       " from metadata.data_type_mapping where source_type='MSSQL'" \
                       " order by source_data_type, case when source_data_type_length is not null then 1 else 0 end desc"

        Metadata.Metadata.crsr.execute(dttp_map_sql)
        dttp_map = Metadata.Metadata.crsr.fetchall()

        dttp_map_len = len(dttp_map)
        map=0
        j=0
        while j<dttp_map_len:
            if datatype.upper() == dttp_map[j][0] and length == str(dttp_map[j][1]) and map==0:
                datatype = dttp_map[j][2]
                length = dttp_map[j][3]
                map=1
            elif datatype.upper() == dttp_map[j][0] and dttp_map[j][1] is None and map==0:
                datatype = dttp_map[j][2]
                if dttp_map[j][3] is not None:
                    length = dttp_map[j][3]
                map=1
            j=j+1
        result = {"datatype":datatype,"length":length}
        return result

# print(Support.DataTypeMapping("smalldatetime",None)["length"])
