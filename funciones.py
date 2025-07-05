from pyspark.sql.functions import col, explode
from pyspark.sql import DataFrame

def normalizar(df):
    # Explode outer array
    df_exploded_outer = df.withColumn("compra_group", explode(col("compras")))

# Explode inner array
    df_exploded_inner = df_exploded_outer.withColumn("compra", explode(col("compra_group")))

# Select fields
    final_df = df_exploded_inner.select(
        col("numero_caja"),
        col("compra.nombre").alias("producto"),
        col("compra.cantidad").cast("int").alias("cantidad"),
        col("compra.precio_unitario").cast("int").alias("precio unitario")
    )

    return final_df



def unificar(list_df: list):
    unified_df = list_df[0]
    for df in list_df[1:]:
        unified_df = unified_df.union(df)

    return unified_df