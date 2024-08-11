from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession


# Crear una sesión de Spark
spark = SparkSession.builder.master("local").appName("SMK Example").getOrCreate()

# Define la función

def n_max_smk(n, df_clientes, df_vacio):
    for i in n:
        # Filtra el dataframe de las categoría rewarding y rw_count igual al número
        df_rw = df_clientes.filter((F.col('EST') == 'Rewarding') & (F.col('RW_COUNT') == i))
        
        # Define una ventana de partición y ordena las probabilidades de forma DESC
        windowSpec = Window.partitionBy('PARTY_ID').orderBy(F.col('PROB').desc())
        
        # Agregar una columna con el número de fila para cada partición por PARTY_ID
        df_rw = df_rw.withColumn('N_FILA', F.row_number().over(windowSpec))
        
        # Filtra la base para mantener las filas con N_FILA <= 3 para cupones Rewarding
        df_rw_ajust = df_rw.filter(F.col('N_FILA') <= i).drop('N_FILA')
        
        # Ahora seleccionamos los cupones no Rewarding
        df_non_rw = df_clientes.filter((F.col('EST') != 'Rewarding') & (F.col('PARTY_ID').isin(df_rw_ajust.select("PARTY_ID").distinct().collect())))
        
        # Añadimos la columna N_FILA a los cupones no Rewarding
        df_non_rw = df_non_rw.withColumn('N_FILA', F.row_number().over(windowSpec))
        
        # Filtramos para completar hasta 12 cupones en total, restando los ya seleccionados cupones Rewarding
        df_non_rw_ajust = df_non_rw.filter(F.col('N_FILA') <= (12 - i)).drop('N_FILA')
        
        # Unimos los cupones Rewarding ajustados con los no Rewarding ajustados
        df_final = df_rw_ajust.unionByName(df_non_rw_ajust)
        
        # Almacena el dataframe ajustado
        df_vacio = df_vacio.unionByName(df_final)

    # Guarda el registro filtrado
    return df_vacio


# Crear la lista n con los valores 0, 1, 2, 3
n = [0, 1, 2, 3]

# Crear un DataFrame de ejemplo para df_clientes
data = [
    (1, 101, "Rewarding", 12, 3, 0.95),
    (1, 102, "Rewarding", 12, 3, 0.90),
    (1, 103, "Rewarding", 12, 3, 0.85),
    (1, 104, "Categoría 3", 12, 3, 0.80),
    (1, 105, "Categoría 2", 12, 3, 0.75),
    (1, 106, "Categoría 1", 12, 3, 0.70),
    (1, 107, "Categoría 2", 12, 3, 0.65),
    (1, 108, "Categoría 3", 12, 3, 0.60),
    (1, 109, "Categoría 1", 12, 3, 0.55),
    (1, 110, "Categoría 3", 12, 3, 0.50),
    (2, 201, "Rewarding", 8, 2, 0.95),
    (2, 202, "Rewarding", 8, 2, 0.85),
    (2, 203, "Categoría 3", 8, 2, 0.80),
    (2, 204, "Categoría 2", 8, 2, 0.75),
    (2, 205, "Categoría 1", 8, 2, 0.70),
    (2, 206, "Categoría 3", 8, 2, 0.65),
    (3, 301, "Categoría 3", 5, 0, 0.90),
    (3, 302, "Categoría 2", 5, 0, 0.80),
    (3, 303, "Categoría 1", 5, 0, 0.75),
    (3, 304, "Categoría 2", 5, 0, 0.70),
    (3, 305, "Categoría 1", 5, 0, 0.65)
]

columns = ["PARTY_ID", "ID_CATEGORIA", "EST", "SMK_COUNT", "RW_COUNT", "PROB"]

# Crear el DataFrame de clientes
df_clientes = spark.createDataFrame(data, columns)

# Crear un DataFrame vacío con el mismo esquema
df_vacio = spark.createDataFrame([], df_clientes.schema)

# Mostrar los DataFrames creados
df_clientes.show()
df_vacio.show()

# Probamos la función
print(df_clientes)