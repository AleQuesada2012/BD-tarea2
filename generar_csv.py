from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum 
import pyspark.sql.functions as F



def total_productos(df_final : DataFrame):
    df_productos = df_final.groupBy("producto").agg(
        sum("cantidad").alias("total_comprado")
    )
    df_productos.show()
    save_as_csv(df_productos, "total_productos.csv")
    return df_productos


def total_vendido_caja(df_final : DataFrame):
    df_vendidos = df_final.groupBy("numero_caja").agg(
        sum("precio unitario").alias("total_vendido")
    )
    df_vendidos.show()
    save_as_csv(df_vendidos, "total_vendidos.csv")
    return df_vendidos


def generar_metricas(df_final: DataFrame):
    spark = SparkSession.builder.getOrCreate()

    # 1. Métricas por caja
    df_cajas = df_final.groupBy("numero_caja").agg(
        sum("precio unitario").alias("total_vendido")
    )
    
    # Calcular max y min
    caja_max = df_cajas.orderBy(col("total_vendido").desc()).first()[0]
    caja_min = df_cajas.orderBy(col("total_vendido").asc()).first()[0]
    
    # Calcular percentiles
    percentiles = df_cajas.approxQuantile(
        "total_vendido", [0.25, 0.50, 0.75], 0.01  # Error 
    )
    
    # 2. Métricas por producto
    # Producto más vendido (unidades)
    df_unidades = df_final.groupBy("producto").agg(
        sum("cantidad").alias("total_unidades")
    )
    producto_mas_vendido = df_unidades.orderBy(col("total_unidades").desc()).first()[0]
    
    # Producto con mayor ingreso (dinero)
    df_ingresos = df_final.withColumn(
        "ingreso", col("cantidad") * col("precio unitario")
    ).groupBy("producto").agg(sum("ingreso").alias("total_ingreso"))
    
    
    producto_mayor_ingreso = df_ingresos.orderBy(col("total_ingreso").desc()).first()[0]
    
    # 3. Construir DataFrame de métricas
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
    df_metricas = spark.createDataFrame(
        metricas_data, 
        ["metrica", "valor"]
    )
        # Mostrar y guardar
    df_metricas.show(truncate=False)
    save_as_csv(df_metricas, "metricas.csv")
    return df_metricas

# Esta función ayudante la tuvimos que buscar para no romper el modelo de spark y usar pandas para guardar
def save_as_csv(df, filename):
    from tempfile import mkdtemp
    import os, shutil

    temp_dir = mkdtemp()
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(temp_dir)

    for file in os.listdir(temp_dir):
        if file.endswith(".csv"):
            shutil.move(os.path.join(temp_dir, file), filename)
            break
    shutil.rmtree(temp_dir)
