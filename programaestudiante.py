import start
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import funciones #todo: ver que hacer aqui
import json

# antes era porque siempre llegan 4 cosas, ahora puede variar entonces hay que validar diferente
if len(sys.argv) < 2:
    print("argumentos incorrectos, se requiere: spark-submit programaestudiante.py *.json")
    sys.exit(1)

json_list = []


for file in sys.argv[1:]:
    with open(file, 'r') as current_json:
        # todo: poner validacion para json erroneos
        try:
            myJson = json.load(current_json)
            json_list.append(myJson)
        except json.JSONDecodeError:
            print(f"El archivo {file} es un JSON malo D:")        
        current_json.close()



# spark = SparkSession.builder.appName("Tarea-2-Big Data").getOrCreate()
# spark.sparkContext.setLogLevel("WARN")
