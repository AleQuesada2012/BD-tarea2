import start
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import funciones as funcs
import json
import generar_csv

spark = SparkSession.builder.appName("Tarea-2-Big Data").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# antes era porque siempre llegan 4 cosas, ahora puede variar entonces hay que validar diferente
if len(sys.argv) < 2:
    print("argumentos incorrectos, se requiere: spark-submit programaestudiante.py *.json")
    sys.exit(1)

json_list = []


for file in sys.argv[1:]:
    with open(file, 'r') as current_json:
        try:
            myJson = json.load(current_json) # con esto se valida el formato
            json_list.append(file)
        except json.JSONDecodeError:
            print(f"El archivo {file} es un JSON de formato no valido")        
        current_json.close()
# funciona, todo: separarlos para que quede tabular  

dataframes = []
for json_file in json_list:
    df = spark.read.option("multiLine", "true").json(json_file)
    dataframes.append(df)

normalizados = []
for datafa in dataframes:
    newDF = funcs.normalizar(datafa)
    normalizados.append(newDF)
    #newDF.show(truncate=False)



df_final = funcs.unificar(normalizados)
df_final.printSchema()
df_final.show(df_final.count())

print(f"cantidad de compras: {df_final.count()}")

generar_csv.total_productos(df_final)

generar_csv.total_vendido_caja(df_final)

generar_csv.generar_metricas(df_final)
