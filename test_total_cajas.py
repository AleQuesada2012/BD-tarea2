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


def test_no_se_repiten_al_unificar(dataframe_unificado : DataFrame):
    unicos = dataframe_unificado.distinct()
    assert unicos.count() == dataframe_unificado.count(), "Se espera que no se dupliquen registros al unificar los DFs"


def test_persiste_informacion_de_cada_caja(dataframes_normalizados, dataframe_unificado):
    cant_final = dataframe_unificado.count()
    cant_por_caja = 0
    for df in dataframes_normalizados:
        cant_por_caja += df.count()
    assert cant_por_caja == cant_final, "Se espera que no se pierdan registros al unificar los dataframes"


def test_columnas_resultante(mock_total_cajas: DataFrame):
    columnas = ["numero_caja", "total_vendido"]
    assert mock_total_cajas.columns == columnas, "se espera que las columnas sean numero_caja y total_vendido"


def test_monto_por_caja(mock_total_cajas, dataframe_unificado):
    todas_bien = True
    for caja in mock_total_cajas.collect():
        num_caja = caja["numero_caja"]
        monto_dado = caja["total_vendido"]
        monto_esperado = 0
        filtrado = dataframe_unificado.filter("numero_caja = " + num_caja).collect()
        for compra in filtrado:
            monto_esperado += (int(compra["cantidad"]) * int(compra["precio_unitario"]))
        if monto_dado != monto_esperado:
            todas_bien = False
            raise AssertionError(f"El monto esperado par la caja {num_caja} es {monto_esperado}.\
                                  el monto recibido fue: {monto_dado}")
    assert todas_bien, "Se espera que en todas las cajas coincida el monto dado con el calculado en la prueba"


def test_una_fila_por_caja(mock_total_cajas, dataframes_productos):
    cajas = []
    for df in dataframes_productos:
        num_caja = df.collect()[0]["numero_caja"]
        if num_caja not in cajas:
            cajas.append(num_caja)
    
    todas_bien = True
    for num in cajas:
        apariciones = len(mock_total_cajas.filter("numero_caja = " + num).collect())
        if apariciones > 1 or apariciones <= 0:
            todas_bien = False
            raise AssertionError("En el total de cajas debe aparecer una fila por caja, pero\
                                  la caja {num} aparece {apariciones} veces.")
    assert todas_bien, "Se espera que aparezca cada caja exactamente una vez"


def test_monto_total(mock_total_cajas, dataframe_unificado):
    filas_total_dado = mock_total_cajas.select(sum_(mock_total_cajas["total_vendido"])).collect()
    total_dado = filas_total_dado[0]["sum(total_vendido)"]
    total_esperado = 0
    for fila in dataframe_unificado.select("cantidad", "precio_unitario").collect():
        total_esperado += fila["cantidad"] * fila["precio_unitario"]
    assert total_esperado == total_dado, f"El monto total de ventas dado es: {total_dado} pero el esperado \
        corresponde a {total_esperado}"