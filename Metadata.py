# coding=utf-8
import psycopg2
import sys
import datetime
import json

#Работа с метаданными
class Metadata:

    # Переменная подключения к метаданным
    server = "localhost"
    database = "anchorbi"
    user = "pavelhamrin"
    password = ""
    cnct = psycopg2.connect(dbname=database, user=user,
                             password=password, host=server)
    cnct.autocommit = True
    crsr = cnct.cursor()
