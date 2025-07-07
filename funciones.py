from pyspark.sql.functions import col, explode, sum
from pyspark.sql import DataFrame
import json


def leer_json(argv):
    json_list = []
    for file in argv:
        with open(file, 'r') as current_json:
            try:
                json.load(current_json) # con esto se valida el formato
                json_list.append(file)
            except json.JSONDecodeError:
                print(f"El archivo {file} es un JSON de formato no valido; no fue cargado a memoria.")        
            current_json.close()
    return json_list

def cargar_json(lista_nombres, spark):
    dataframes = []
    for json_file in lista_nombres:
        df = spark.read.option("multiLine", "true").json(json_file)
        dataframes.append(df)
    return dataframes


def normalizar(df):
    # Separación del grupo de compras
    df_exploded_outer = df.withColumn("compra_group", explode(col("compras")))
    

    # expansión de las compras usando explode
    df_exploded_inner = df_exploded_outer.withColumn("compra", explode(col("compra_group")))

    final_df = df_exploded_inner.select(
        col("numero_caja"),
        col("compra.nombre").alias("producto"),
        col("compra.cantidad").cast("int").alias("cantidad"),
        col("compra.precio_unitario").cast("int").alias("precio_unitario")
    )

    return final_df



def unificar(list_df: list) -> DataFrame:
    unified_df = list_df[0]
    for df in list_df[1:]:
        unified_df = unified_df.union(df)

    return unified_df

"""
Migré las funciones que generaban los CSV aquí porque se ocupa probar por aparte el DF que genera y luego
probar el CSV que escribe, entonces no me servía que estén ambas cosas en la misma función
ahora en las que hay acá abajo solo se crea el DF, y en las que hay en generar_csv.py se llaman las de acá para
generarlo como archivo
"""

def calcular_total_productos(df_final : DataFrame) -> DataFrame:
    df_productos = df_final.groupBy("producto").agg(
        sum("cantidad").alias("total_comprado")
    )
    #save_as_csv(df_productos, "total_productos.csv")
    return df_productos


def calcular_total_vendido_caja(df_final : DataFrame):

    df_con_ingreso = df_final.withColumn(
        "ingreso", 
        col("cantidad") * col("precio_unitario")
    )
    
    df_vendidos = df_con_ingreso.groupBy("numero_caja").agg(
        sum("ingreso").alias("total_vendido")
    )
    return df_vendidos


# -------- funciones para las metricas ---------

def calcular_caja_max(df_vendidos):
    df_caja_max = df_vendidos.orderBy(col("total_vendido").desc()).first()[0]
    return df_caja_max


def calcular_caja_min(df_vendidos):
    df_caja_min = df_vendidos.orderBy(col("total_vendido").asc()).first()[0]
    return df_caja_min


def calc_percentiles(df_vendidos):
    percentiles = df_vendidos.approxQuantile(
        "total_vendido", [0.25, 0.50, 0.75], 0.01  # Error relativo
    )
    return percentiles


def calc_prod_mas_vendido(df_final):
    df_unidades = df_final.groupBy("producto").agg(
        sum("cantidad").alias("total_unidades")
    )
    prod_mas_vendido = df_unidades.orderBy(col("total_unidades").desc()).first()[0]
    return prod_mas_vendido


def calc_prod_mayor_ingreso(df_final):
    # Producto con mayor ingreso (dinero)
    df_ingresos = df_final.withColumn(
        "ingreso", col("cantidad") * col("precio_unitario")
    ).groupBy("producto").agg(sum("ingreso").alias("total_ingreso"))

    prod_mayor_ingreso = df_ingresos.orderBy(col("total_ingreso").desc()).first()[0]
    return prod_mayor_ingreso


def calcular_metricas(df_final, spark_sesh):

    # Métricas por caja
    df_cajas = calcular_total_vendido_caja(df_final)
    
    caja_max = calcular_caja_max(df_cajas)
    caja_min = calcular_caja_min(df_cajas)
    
    # Métricas por producto
    producto_mas_vendido = calc_prod_mas_vendido(df_final)
    producto_mayor_ingreso = calc_prod_mayor_ingreso(df_final)
    
    # DF final
    percentiles = calc_percentiles(df_cajas)
    metricas_data = [
        ("caja_con_mas_ventas", str(caja_max)),
        ("caja_con_menos_ventas", str(caja_min)),
        ("percentil_25_por_caja", str(percentiles[0])),
        ("percentil_50_por_caja", str(percentiles[1])),
        ("percentil_75_por_caja", str(percentiles[2])),
        ("producto_mas_vendido_por_unidad", producto_mas_vendido),
        ("producto_de_mayor_ingreso", producto_mayor_ingreso)
    ]
        # Crear DataFrame
    df_metricas = spark_sesh.createDataFrame(
        metricas_data, 
        ["metrica", "valor"]
    )
    #df_metricas.show(truncate=False) # mejor dejarle el show al programa principal
    # si lo dejara aqui, luego en cada prueba unitaria que lo use va a estarse mostrando
    return df_metricas