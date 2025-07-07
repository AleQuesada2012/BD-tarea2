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

json_list = funcs.leer_json(sys.argv[1:])

dataframes = funcs.cargar_json(json_list, spark)
for df in dataframes:
    df.show()

normalizados = []
for datafa in dataframes:
    newDF = funcs.normalizar(datafa)
    normalizados.append(newDF)

for df in normalizados:
    df.show()

df_final = funcs.unificar(normalizados)
df_final.printSchema()
df_final.show(df_final.count())

# ya con ese DF con toda la info se sacan los necesarios para los CSVs
df_tot_prods = funcs.calcular_total_productos(df_final)
print("tabla (DF) para cada producto y su total")
df_tot_prods.show(truncate=False)

df_tot_cajas = funcs.calcular_total_vendido_caja(df_final)
print("Tabla (DF) para cada caja y su total de ventas")
df_tot_cajas.show(truncate=False)


df_metricas = funcs.calcular_metricas(df_final, spark)
print("Tabla de metricas")
df_metricas.show(truncate=False)


generar_csv.generar_archivo1(df_tot_prods)

generar_csv.generar_archivo2(df_tot_cajas)

generar_csv.generar_archivo3(df_metricas)
