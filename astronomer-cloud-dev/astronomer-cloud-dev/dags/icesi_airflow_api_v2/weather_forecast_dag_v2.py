"""
DAG para obtener y procesar datos meteorológicos.

Este DAG se encarga de:
1. Obtener datos del clima desde una API.
2. Guardar los datos en una tabla de Snowflake.
3. Ejecutar una consulta SQL para limpiar o transformar los datos almacenados.

Configuraciones:
    - URL: URL de la API de CONAGUA.
    - SNOWFLAKE_CONN_ID: Identificador de la conexión a Snowflake.
    - DATABASE: Nombre de la base de datos en Snowflake.
    - SCHEMA: Nombre del esquema en Snowflake.
    - TABLE: Nombre de la tabla en Snowflake.
    - QUERIES_BASE_PATH: Ruta base para los archivos SQL.
"""

import os
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from dags.icesi_airflow_api.utils.weather_api import run_weather_forecast_pipeline
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Configuraciones básicas para la ejecución
URL = 'https://www.dian.gov.co/dian/cifras/Basesestadisticasimportaciones/12_Importaciones_2024_Diciembre.zip'
SNOWFLAKE_CONN_ID = 'snowflake_conn_id'  # ID de conexión configurado en Airflow
DATABASE = 'DEV_ICESI'                   # Nombre de la base de datos en Snowflake
SCHEMA = 'SYSTEM_RECOMMENDATION'         # Nombre del esquema en Snowflake
TABLE = 'CONAGUA_WEATHER_RAW_v2'            # Nombre de la tabla de destino en Snowflake
QUERIES_BASE_PATH = os.path.join(os.path.dirname(__file__), 'queries')


@dag(
    schedule_interval='0 */12 * * *',
    start_date=days_ago(1),
    catchup=False,
    default_args={'owner': 'airflow', 'retries': 1},
    tags=['weather', 'snowflake'],
    template_searchpath=QUERIES_BASE_PATH
)
def weather_forecast_dag_v2():
    """
    DAG para obtener datos meteorológicos y ejecutar una consulta en Snowflake.

    Este DAG realiza las siguientes tareas en secuencia:
        1. Obtención y almacenamiento de datos meteorológicos en Snowflake.
        2. Ejecución de una consulta SQL para limpiar o transformar los datos.

    La ejecución de la segunda tarea depende del éxito de la primera.
    """

    @task()
    def fetch_and_save_weather_data():
        """
        Obtiene datos meteorológicos y los almacena en Snowflake.

        Llama a 'run_weather_forecast_pipeline' para conectarse a la API,
        obtener los datos del clima y guardarlos en la tabla definida en Snowflake.
        """
        run_weather_forecast_pipeline(
            url=URL,
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            database=DATABASE,
            schema=SCHEMA,
            table=TABLE
        )

    @task()
    def execute_snowflake_query():
        """
        Ejecuta una consulta SQL en Snowflake para procesar los datos.

        Se conecta a Snowflake utilizando 'SnowflakeHook', lee el contenido del
        archivo 'limpiar.sql' ubicado en la carpeta de queries y ejecuta la consulta.
        """
        # Inicializar el hook para conectarse a Snowflake.
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # Construir la ruta completa al archivo SQL 'limpiar.sql'.
        sql_file_path = os.path.join(QUERIES_BASE_PATH, 'limpiar.sql')

        # Leer el contenido del archivo SQL.
        with open(sql_file_path, 'r') as file:
            sql_query = file.read()

        # Ejecutar la consulta SQL en Snowflake.
        hook.run(sql_query)

    # Definir la secuencia de ejecución de las tareas:
    # Primero se ejecuta la tarea de obtención y almacenamiento de datos,
    # luego se ejecuta la tarea para la consulta en Snowflake.
    fetch_task = fetch_and_save_weather_data()
    execute_task = execute_snowflake_query()
    fetch_task >> execute_task


# Instanciar el DAG.
dag = weather_forecast_dag_v2()