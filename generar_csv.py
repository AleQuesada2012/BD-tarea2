from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum
def total_productos(df_final : DataFrame):
    df_productos = df_final.groupBy("producto").agg(
        sum("cantidad").alias("total_comprado")
    )
    df_productos.show()
    save_as_csv(df_productos, "total_productos.csv")
    return df_productos


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
