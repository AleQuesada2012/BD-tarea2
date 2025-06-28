from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
import glob

def main():
    # Encendemos el motor de Spark 
    spark = SparkSession.builder.appName("ProcesamientoVentas").getOrCreate()
    
    try:
        # Buscamos todos los archivos JSON que nos pasaron como argumento
        # Usamos glob porque los asteriscos no se expanden solos en Python
        input_files = []
        for pattern in sys.argv[1:]:
            input_files.extend(glob.glob(pattern))
        
        # Si no encontramos nada, tiramos un error 
        if not input_files:
            raise ValueError("No se encontraron los archivos")
        
        print(f"Procesando estos archivos: {input_files}")
        # Leemos todos los JSONs y los cargamos en un DataFrame de Spark
        df = spark.read.json(input_files)
        
        # Cada registro tiene una caja y una lista de productos. Aquí hacemos que tenga
        # una fila por cada producto en cada caja
        df_exploded = df.select(
            F.col("caja").alias("caja_id"),  # Renombramos para que quede más claro
            F.explode("productos").alias("producto")  # Abrimos la bolsa de productos
        )
        
        # Ahora extraemos los datos individuales de cada producto
        df_products = df_exploded.select(
            "caja_id",
            F.col("producto.nombre").alias("producto_nombre"),  # Nombre del producto
            F.col("producto.precio").cast("float"),  # Precio como decimal
            F.col("producto.cantidad").cast("int")   # Cantidad como entero
        ).withColumn("monto", F.col("precio") * F.col("cantidad"))  # Calculamos el total por ítem
        
        # 1. TOTAL POR PRODUCTO: Agrupamos por nombre y sumamos cuántas unidades se vendieron
        total_productos = df_products.groupBy("producto_nombre") \
            .agg(F.sum("cantidad").alias("total_unidades")) \
            .orderBy("producto_nombre")  # Orden alfabético para que sea bonito
        
        # 2. TOTAL POR CAJA: Agrupamos por caja y sumamos todo el dinero que recaudó
        total_cajas = df_products.groupBy("caja_id") \
            .agg(F.sum("monto").alias("total_ventas")) \
            .orderBy("caja_id")  # Orden por ID de caja
        
        # 3. MÉTRICAS ESPECIALES 
        # a) CAJA ESTRELLA: La que más vendió (orden descendente y tomamos la primera)
        caja_max = total_cajas.orderBy(F.desc("total_ventas")).first()
        
        # b) CAJA FLOJA: La que menos vendió (orden ascendente y primera)
        caja_min = total_cajas.orderBy(F.asc("total_ventas")).first()
        
        # c) PERCENTILES: Calculamos los puntos de corte del 25%, 50% y 75%
        percentiles = total_cajas.approxQuantile("total_ventas", [0.25, 0.5, 0.75], 0.01)
        
        # d) PRODUCTO POPULAR: El más vendido por unidades 
        producto_mas_vendido = total_productos.orderBy(F.desc("total_unidades")).first()
        
        # e) PRODUCTO MILLONARIO: El que más dinero generó (cantidad * precio)
        producto_ingreso = df_products.groupBy("producto_nombre") \
            .agg(F.sum("monto").alias("ingreso_total")) \
            .orderBy(F.desc("ingreso_total")).first()
        
        # Preparamos la lista de métricas como pares (nombre, valor)
        metricas_data = [
            ("caja_con_mas_ventas", caja_max["caja_id"]),
            ("caja_con_menos_ventas", caja_min["caja_id"]),
            ("percentil_25_por_caja", percentiles[0]),
            ("percentil_50_por_caja", percentiles[1]),
            ("percentil_75_por_caja", percentiles[2]),
            ("producto_mas_vendido_por_unidad", producto_mas_vendido["producto_nombre"]),
            ("producto_de_mayor_ingreso", producto_ingreso["producto_nombre"])
        ]
        
        # Convertimos la lista de métricas en un DataFrame de Spark
        metricas_df = spark.createDataFrame(metricas_data, ["metrica", "valor"])
        
        # Guardamos los resultados en CSV (usamos Pandas para simplificar)
        total_productos.toPandas().to_csv("/salida/total_productos.csv", index=False)
        total_cajas.toPandas().to_csv("/salida/total_cajas.csv", index=False)
        metricas_df.toPandas().to_csv("/salida/metricas.csv", index=False)
        
        print("¡Todo listo! Archivos generados en /salida")
        
    except Exception as e:
        # Por si algo sale mal
        print(f"Algo falló: {str(e)}")
        raise e
        
    finally:
        # Apagamos Spark
        spark.stop()

if __name__ == "__main__":
    main()