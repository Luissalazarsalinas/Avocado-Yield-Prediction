from pyspark.sql.types import *

schema_avocado = StructType([
    StructField("CÓD. DEP.", IntegerType(), False),
    StructField("DEPARTAMENTO", StringType(), False),
    StructField("CÓD. MUN.", IntegerType(), False),
    StructField("MUNICIPIO", StringType(), False),
    StructField("GRUPO DE CULTIVO", StringType(), False),
    StructField("SUBGRUPO DE CULTIVO", StringType(), False),
    StructField("CULTIVO", StringType(), False),
    StructField("DESAGREGACIÓN REGIONAL Y/O SISTEMA PRODUCTIVO", StringType(), False),
    StructField("AÑO", IntegerType(), False),
    StructField("PERIODO", StringType(), False),
    StructField("Área Sembrada (ha)", IntegerType(), False),
    StructField("Área Cosechada (ha)", IntegerType(), False),
    StructField("Producción (t)", FloatType(), False),
    StructField("Rendimiento (t/ha)", FloatType(), False),
    StructField("ESTADO FISICO PRODUCCION",StringType(), False),
    StructField("NOMBRE CIENTIFICO", StringType(), False),
    StructField("CICLO DE CULTIVO", StringType(), False),
  
])

# Schema of precipitations and temperature
# Create schema
schema_p_t = StructType(
    [
        StructField("CodigoEstacion",IntegerType(), False),
        StructField("CodigoSensor", IntegerType(),False),
        StructField("FechaObservacion", StringType(),False),
        StructField("ValorObservado", FloatType(),False),
        StructField("NombreEstacion", StringType(),False),
        StructField("Departamento", StringType(),False),
        StructField("Municipio", StringType(),False),
        StructField("ZonaHidrografica", StringType(),False),
        StructField("Latitud", DecimalType(10,8),False),
        StructField("Longitud", DecimalType(10,8),False),
        StructField("DescripcionSensor", StringType(),False),
        StructField("UnidadMedida", StringType(),False)
    ]
)

schema_s = StructType(
    [
        StructField("numfila",StringType(), False),
        StructField("Departamento", StringType(),False),
        StructField("Municipio", StringType(),False),
        StructField("Cultivo", StringType(),False),
        StructField("Estado", StringType(),False),
        StructField("Tiempo Establecimiento", StringType(),False),
        StructField("Topografia", StringType(),False),
        StructField("Drenaje", StringType(),False),
        StructField("Riego", StringType(),False),
        StructField("Fertilizantes aplicados", StringType(),False),
        StructField("FechaAnalisis", StringType(),False),
        StructField("pH agua:suelo 2,5:1,0", StringType(),False),
        StructField("Materia orgánica (MO) %", StringType(),False),
        StructField("Fósforo (P) Bray II mg/kg", StringType(),False),
        StructField("Azufre (S) Fosfato monocalcico mg/kg", StringType(),False),
        StructField("Acidez (Al+H) KCL cmol(+)/kg", StringType(),False),
        StructField("Aluminio (Al) intercambiable cmol(+)/kg", StringType(),False),
        StructField("Calcio (Ca) intercambiable cmol(+)/kg", StringType(),False),
        StructField("Magnesio (Mg) intercambiable cmol(+)/kg", StringType(),False),
        StructField("Potasio (K) intercambiable cmol(+)/kg", StringType(),False),
        StructField("Sodio (Na) intercambiable cmol(+)/kg", StringType(),False),
        StructField("capacidad de intercambio cationico (CICE) suma de bases cmol(+)/kg", StringType(),False),
        StructField("Conductividad el‚ctrica (CE) relacion 2,5:1,0 dS/m", StringType(),False),
        StructField("Hierro (Fe) disponible olsen mg/kg", StringType(),False),
        StructField("Cobre (Cu) disponible mg/kg", StringType(),False),
        StructField("Manganeso (Mn) disponible Olsen mg/kg", StringType(),False),
        StructField("Zinc (Zn) disponible Olsen mg/kg", StringType(),False),
        StructField("Boro (B) disponible mg/kg", StringType(),False),
        StructField("Hierro (Fe) disponible doble  cido mg/kg", StringType(),False),
        StructField("Cobre (Cu) disponible doble acido mg/kg", StringType(),False),
        StructField("Manganeso (Mn) disponible doble acido mg/kg", StringType(),False),
        StructField("Zinc (Zn) disponible doble  cido mg/kg", StringType(),False),
        StructField("Secuencial", StringType(),False),
    ]
)