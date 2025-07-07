import math
import pytest
from faker import Faker
from pyspark.sql import Row, DataFrame
from pyspark.sql.functions import sum as sum_
import funciones
fake = Faker()

@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]").appName("TestTarea2").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def dataframes_productos(spark):
    archivos = ["sample1.json", "sample2.json", "sample3.json", "sample4.json", "sample5.json"]
    return funciones.cargar_json(archivos, spark)
    

@pytest.fixture(scope="module")
def dataframes_normalizados(dataframes_productos):
    separados = []
    for df in dataframes_productos:
        separados.append(funciones.normalizar(df))
    return separados

@pytest.fixture(scope="module")
def dataframe_unificado(dataframes_normalizados):
    return funciones.unificar(dataframes_normalizados)

@pytest.fixture(scope="module")
def mock_total_cajas(dataframe_unificado):
    return funciones.calcular_total_vendido_caja(dataframe_unificado)

@pytest.fixture(scope="module")
def mock_total_productos(dataframe_unificado) -> DataFrame:
    return funciones.calcular_total_productos(dataframe_unificado)

@pytest.fixture(scope="module")
def mock_df_metricas(dataframe_unificado, spark) -> DataFrame:
    return funciones.calcular_metricas(dataframe_unificado, spark)
# -------------------------------------


def test_columnas_df(mock_df_metricas):
    columnas_esperadas = ["metrica", "valor"]
    columnas_obtenidas = mock_df_metricas.columns
    assert columnas_esperadas == columnas_obtenidas, "las columnas del DF de metricas deben ser metrica y valor"

def test_metricas_presentes(mock_df_metricas):
    metricas_esperadas = ["caja_con_mas_ventas", "caja_con_menos_ventas", "percentil_25_por_caja",
                          "percentil_50_por_caja", "percentil_75_por_caja",
                          "producto_mas_vendido_por_unidad", "producto_de_mayor_ingreso"]
    lista_metricas_obtenidas = mock_df_metricas.select("metrica").collect()
    obtenidas = [metrica["metrica"] for metrica in lista_metricas_obtenidas]
    for esperada in metricas_esperadas:
        if esperada not in obtenidas:
            raise AssertionError(f"la metrica {esperada} no se encuentra en el DF de metricas")


def test_caja_mas_ventas(mock_df_metricas, mock_total_cajas):
    num_caja_mas_ventas = mock_df_metricas.filter(mock_df_metricas.metrica == "caja_con_mas_ventas").collect()[0]["valor"]
    cantVendida = 0
    superada = False
    for fila in mock_total_cajas.collect():
        if fila["numero_caja"] == num_caja_mas_ventas:
            cantVendida = fila["total_vendido"]
    
    cajas = mock_total_cajas.collect()
    num_cajas = [caja["numero_caja"] for caja in cajas]
    montos = [caja["total_vendido"] for caja in cajas]

    for i in range(len(num_cajas)):
        caja_actual = num_cajas[i]
        monto_actual = montos[i]
        if caja_actual != num_caja_mas_ventas and monto_actual > cantVendida:
            superada = True
            raise AssertionError(f"Metricas dice que la caja con mas ventas es la {num_caja_mas_ventas}\
                                  pero la supera la {caja_actual} con {monto_actual}")
        
    assert superada == False, "Se espera que la caja con más ventas en las métricas coincida con los resultados de las cajas"
    

def test_caja_menos_ventas(mock_df_metricas, mock_total_cajas):
    num_caja_menos_ventas = mock_df_metricas.filter(mock_df_metricas.metrica == "caja_con_menos_ventas").collect()[0]["valor"]
    cantVendida = 0
    minimo = True
    for fila in mock_total_cajas.collect():
        if fila["numero_caja"] == num_caja_menos_ventas:
            cantVendida = fila["total_vendido"]
    
    cajas = mock_total_cajas.collect()
    num_cajas = [caja["numero_caja"] for caja in cajas]
    montos = [caja["total_vendido"] for caja in cajas]

    for i in range(len(num_cajas)):
        caja_actual = num_cajas[i]
        monto_actual = montos[i]
        if caja_actual != num_caja_menos_ventas and monto_actual < cantVendida:
            minimo = False
            raise AssertionError(f"Metricas dice que la caja con menos ventas es la {num_caja_menos_ventas}\
                                  pero la supera la {caja_actual} con {monto_actual}")
        
    assert minimo == True, "Se espera que la caja con menos ventas en las métricas coincida con los resultados de las cajas"
    

def test_producto_uds_mas_vendidas(mock_df_metricas, mock_total_productos):
    prod_mas_vendido = mock_df_metricas.filter(mock_df_metricas.metrica == "producto_mas_vendido_por_unidad").collect()[0]["valor"]
    cant_vendidas = mock_total_productos.filter(mock_total_productos.producto == prod_mas_vendido).collect()[0]["total_comprado"]
    superado = False
    for fila in mock_total_productos.collect():
        if fila["producto"] != prod_mas_vendido and fila["total_comprado"] > cant_vendidas:
            superado = True
            raise AssertionError(f"Metricas dice que el más vendido es {prod_mas_vendido} pero \
                                 lo supera el producto: {fila['producto']} con unidades: {fila['total_comprado']}")
    assert superado == False, "Se espera que el producto denotado como mas vendido tenga el maximo de unidades o que a lo sumo\
        esté empate con otro"
    

def test_producto_mas_ingresos(mock_df_metricas, dataframe_unificado):
        prod_mas_ingresos = mock_df_metricas.filter(mock_df_metricas.metrica == "producto_de_mayor_ingreso").collect()[0]["valor"]
        assert prod_mas_ingresos == funciones.calc_prod_mayor_ingreso(dataframe_unificado), f"Se espera que el producto con\
             mayor ingreso sea {prod_mas_ingresos} segun la tabla de metricas"
        

def test_percentiles(mock_df_metricas, mock_total_cajas):
    perc75 = mock_df_metricas.filter(mock_df_metricas.metrica == "percentil_75_por_caja").collect()[0]["valor"]
    perc50 = mock_df_metricas.filter(mock_df_metricas.metrica == "percentil_50_por_caja").collect()[0]["valor"]
    perc25 = mock_df_metricas.filter(mock_df_metricas.metrica == "percentil_25_por_caja").collect()[0]["valor"]
    percentiles_esperados = [float(perc25), float(perc50), float(perc75)]

    percentiles_calculados = funciones.calc_percentiles(mock_total_cajas) # usa error de 0,01
    
    for esperado in percentiles_esperados:
        encontrado = any(math.isclose(esperado, calculado, rel_tol=1e-2) for calculado in percentiles_calculados)
        if not encontrado:
            raise AssertionError(f"El valor percentil {esperado} no se encuentra en los calculados, "
                f"los calculados son: {percentiles_calculados}")