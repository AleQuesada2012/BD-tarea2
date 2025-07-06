from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum

import funciones



def generar_archivo1(df_csv1 : DataFrame):
    save_as_csv(df_csv1, "total_productos.csv")


def generar_archivo2(df_csv2 : DataFrame):
    save_as_csv(df_csv2, "total_vendidos.csv")


def generar_archivo3(df_csv3 : DataFrame):
    save_as_csv(df_csv3, "metricas.csv")



# Esta función ayudante la tuvimos que buscar para no romper el modelo de spark y no recurrir 
# a usar pandas para guardar
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
