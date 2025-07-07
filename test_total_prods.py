import pytest
from faker import Faker
from pyspark.sql import Row, DataFrame
from pyspark.sql.functions import sum
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
def mock_total_productos(dataframe_unificado) -> DataFrame:
    return funciones.calcular_total_productos(dataframe_unificado)


def test_productos(dataframes_productos: list[DataFrame]):
    for df in dataframes_productos:
        assert(df.columns == ["compras", "numero_caja"])
        assert(len(df.collect()) > 0)


# asumiendo que todos los json que entran son validos, debe haber un dataframe por cada json
# si alguno no es valido entonces no lo va a cargar, eso se valida como una protección del sistema
def test_cant_dfs_creados():
    mock_argv = ["sample1.json", "sample2.json", "sample3.json"]
    json_cargados = funciones.leer_json(mock_argv)
    assert len(json_cargados) == 3, f"cantidad real: {len(json_cargados)}"



def test_cant_compras(dataframes_normalizados: list[DataFrame]):
    total = 0
    for df in dataframes_normalizados:
        total += df.count()
    assert total >= 50, f"Hay {total} compra(s) en los datos, se esperan al menos 50"


def test_columnas_esperadas(dataframes_normalizados: list[DataFrame]):
    columnas = ["numero_caja", "producto", "cantidad", "precio_unitario"]
    todos_bien = True
    for df in dataframes_normalizados:
        if (df.columns != columnas):
            todos_bien = False
            raise AssertionError("las columnas de un dataframe no cumplen el formato esperado")
    assert todos_bien, "se espera que las columnas cumplan el formato al ser expandidos los datos"

def test_misma_caja_cada_fila(dataframes_normalizados: list[DataFrame]):
    cajas_esperadas = True
    for df in dataframes_normalizados:
        filas = df.collect()
        num_caja = filas[0]["numero_caja"]
        for fila in filas:
            if fila["numero_caja"] != num_caja:
                cajas_esperadas = False
                raise AssertionError("Todas las compras de un archivo deben estar asociadas a la misma caja")
    assert cajas_esperadas, f"Los datos vienen con las compras asociados a una tabla por archivo: {cajas_esperadas}"

def test_ningun_producto_en_cero(dataframes_normalizados: list[DataFrame]):
    productos_en_cero = 0
    for df in dataframes_normalizados:
        for fila in df.collect():
            if fila["cantidad"] <= 0:
                productos_en_cero += 1
                raise AssertionError("No puede haber un producto en la compra que tenga 0 o menos de cantidad")
    assert productos_en_cero == 0, f"Se esperan 0 productos con 0 o menos de cantidad, se recibieron: {productos_en_cero}"


def test_ningun_precio_cero(dataframes_normalizados: list[DataFrame]):
    precios_erroneos = 0
    for df in dataframes_normalizados:
        for fila in df.collect():
            if fila["precio_unitario"] <= 0:
                precios_erroneos += 1
                raise AssertionError("No puede haber un producto con precio 0 o negativo")
    assert precios_erroneos == 0, f"Se esperan 0 productos con precio <= 0, se recibieron: {precios_erroneos}"


def test_todos_presentes_en_datos(dataframe_unificado, mock_total_productos):
    for fila in mock_total_productos.collect():
        if dataframe_unificado.filter(dataframe_unificado.producto == fila["producto"]).count() < 1:
            raise AssertionError(f"El producto {fila["producto"]} del total de productos no se encuentra en los datos")


def test_columnas_resultante(mock_total_productos: DataFrame):
    columnas = ["producto", "total_comprado"]
    assert mock_total_productos.columns == columnas, "se espera que las columnas sean producto y total_comprado"


def test_cantidades_compradas_calzan(dataframe_unificado, mock_total_productos):
    cantidades_esperadas = dataframe_unificado.groupBy("producto").agg(sum("cantidad").alias("cantidad"))
    for fila in mock_total_productos.collect():
        producto = fila["producto"]
        cantidad_dada = fila["total_comprado"]
        cantidad_esperada = cantidades_esperadas.filter(cantidades_esperadas.producto == producto).collect()[0]
        if cantidad_dada != cantidad_esperada["cantidad"]:
            raise AssertionError(f"Se esperaba que el total de {producto} sea {cantidad_esperada}, \
                                 pero se tiene: {cantidad_dada} en el total.")


