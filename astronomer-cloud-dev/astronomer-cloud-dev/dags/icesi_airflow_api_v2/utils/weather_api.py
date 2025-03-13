"""
Módulo para obtener, procesar y cargar datos meteorológicos en Snowflake.

Este módulo define la clase WeatherForecast, la cual:
    - Obtiene datos del clima desde una API.
    - Descomprime y procesa los datos JSON.
    - Convierte los datos en un DataFrame de pandas.
    - Guarda el DataFrame en un archivo CSV temporal.
    - Crea una tabla en Snowflake basada en la estructura del DataFrame.
    - Carga los datos del CSV a la tabla en Snowflake.

Función:
    run_weather_forecast_pipeline: Orquesta el proceso completo.
"""

import requests
import json
import gzip
import io
import pandas as pd
import logging
import tempfile
import os
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class WeatherForecast:
    """
    Clase para gestionar la obtención, procesamiento y carga de datos meteorológicos en Snowflake.

    Atributos:
        url (str): URL de la API para obtener datos del clima.
        snowflake_conn_id (str): ID de conexión configurado en Airflow para acceder a Snowflake.
        database (str): Nombre de la base de datos en Snowflake.
        schema (str): Nombre del esquema en Snowflake.
        table (str): Nombre de la tabla destino en Snowflake.
    """

    def __init__(self, url, snowflake_conn_id, database=None, schema=None, table=None):
        """
        Inicializa la instancia de WeatherForecast.

        Args:
            url (str): URL de la API.
            snowflake_conn_id (str): ID de conexión para Snowflake.
            database (str, opcional): Nombre de la base de datos en Snowflake.
            schema (str, opcional): Nombre del esquema en Snowflake.
            table (str, opcional): Nombre de la tabla destino en Snowflake.
        """
        self.url = url
        self.snowflake_conn_id = snowflake_conn_id
        self.database = database
        self.schema = schema
        self.table = table

    def fetch_weather_data(self):
        """
        Obtiene datos del clima desde la API y los convierte en un DataFrame.

        Realiza una solicitud GET a la URL especificada, descomprime el contenido GZIP
        y carga el JSON resultante en un DataFrame de pandas.

        Returns:
            pd.DataFrame: DataFrame con los datos meteorológicos, o None si ocurre algún error.
        """
        try:
            response = requests.get(self.url)
            logging.info("Código de estado HTTP: %s", response.status_code)

            if response.status_code != 200:
                logging.error("Error al obtener datos: %s", response.content)
                return None

            # Descomprimir y leer el contenido GZIP
            gz_stream = io.BytesIO(response.content)
            with gzip.GzipFile(fileobj=gz_stream, mode="rb") as f:
                json_data = json.load(f)

            if not json_data:
                logging.error("Los datos JSON están vacíos")
                return None

            # Convertir el JSON en un DataFrame de pandas
            df_data = [item for item in json_data]
            data = pd.DataFrame(df_data)
            return data

        except Exception as e:
            logging.error("Error al obtener datos del clima: %s", str(e))
            return None

    def save_to_temp_csv(self, df):
        """
        Guarda un DataFrame en un archivo CSV temporal sin encabezado.

        Args:
            df (pd.DataFrame): DataFrame a guardar en CSV.

        Returns:
            str: Ruta del archivo CSV temporal creado.
        """
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        df.to_csv(temp_file.name, index=False, header=False)
        logging.info("Datos guardados en archivo temporal sin encabezado: %s", temp_file.name)
        return temp_file.name

    def create_table_in_snowflake(self, df):
        """
        Crea o reemplaza una tabla en Snowflake basada en la estructura del DataFrame.

        Analiza cada columna del DataFrame para determinar el tipo de dato correspondiente en Snowflake
        (INTEGER, FLOAT o TEXT) y ejecuta una instrucción SQL para crear la tabla.

        Args:
            df (pd.DataFrame): DataFrame que se usará para definir la estructura de la tabla.
        """
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        columns = []
        for column in df.columns:
            if pd.api.types.is_integer_dtype(df[column]):
                col_type = "INTEGER"
            elif pd.api.types.is_float_dtype(df[column]):
                col_type = "FLOAT"
            else:
                col_type = "TEXT"
            columns.append(f"{column} {col_type}")

        create_table_sql = f"CREATE OR REPLACE TABLE {self.table} ({', '.join(columns)});"
        cursor.execute(f"USE DATABASE {self.database}")
        cursor.execute(f"USE SCHEMA {self.schema}")
        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Tabla %s creada en Snowflake.", self.table)

    def load_to_snowflake(self, file_path):
        """
        Carga datos desde un archivo CSV a la tabla en Snowflake.

        Se utiliza una etapa temporal en Snowflake para cargar el archivo CSV y
        luego se ejecuta una consulta COPY INTO para insertar los datos en la tabla.
        Finalmente, elimina el archivo tanto de la etapa de Snowflake como localmente.

        Args:
            file_path (str): Ruta del archivo CSV a cargar.

        Returns:
            bool: True si la carga se realizó correctamente, False en caso de error.
        """
        try:
            snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
            conn = snowflake_hook.get_conn()
            cursor = conn.cursor()

            cursor.execute(f"USE DATABASE {self.database}")
            cursor.execute(f"USE SCHEMA {self.schema}")

            stage_name = f"@%{self.table}"
            cursor.execute(f"PUT file://{file_path} {stage_name}")

            copy_into_query = f"""
                COPY INTO {self.table}
                FROM {stage_name}/{os.path.basename(file_path)}
                FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"')
                ON_ERROR='CONTINUE'
            """
            cursor.execute(copy_into_query)
            cursor.execute(f"REMOVE {stage_name}")
            logging.info("Datos cargados en Snowflake y archivo eliminado de la etapa.")
            cursor.close()
            conn.close()

            os.remove(file_path)
            logging.info("Archivo temporal eliminado: %s", file_path)
            return True

        except Exception as e:
            logging.error("Error al cargar datos en Snowflake: %s", str(e))
            return False


def run_weather_forecast_pipeline(url, snowflake_conn_id, database, schema, table):
    """
    Ejecuta el pipeline para obtener, procesar y cargar datos meteorológicos en Snowflake.

    Orquesta los siguientes pasos:
        1. Instancia la clase WeatherForecast con los parámetros necesarios.
        2. Obtiene los datos del clima y los convierte en un DataFrame.
        3. Crea la tabla en Snowflake basada en la estructura del DataFrame.
        4. Guarda los datos en un archivo CSV temporal.
        5. Carga los datos del CSV en la tabla de Snowflake.

    Args:
        url (str): URL de la API para obtener datos del clima.
        snowflake_conn_id (str): ID de conexión configurado en Airflow para Snowflake.
        database (str): Nombre de la base de datos en Snowflake.
        schema (str): Nombre del esquema en Snowflake.
        table (str): Nombre de la tabla destino en Snowflake.
    """
    weather_forecast = WeatherForecast(
        url,
        snowflake_conn_id=snowflake_conn_id,
        database=database,
        schema=schema,
        table=table
    )

    data = weather_forecast.fetch_weather_data()
    if data is not None:
        weather_forecast.create_table_in_snowflake(data)
        temp_file_path = weather_forecast.save_to_temp_csv(data)
        weather_forecast.load_to_snowflake(temp_file_path)