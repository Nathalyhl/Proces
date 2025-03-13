-- Crear la tabla con comentarios en cada columna
CREATE OR REPLACE TABLE DEV_ICESI.SYSTEM_RECOMMENDATION.transformed_weather_data_v2 (
    DESCIEL STRING COMMENT 'Estado del cielo (e.g., despejado, nublado)',
    HLOC STRING COMMENT 'Fecha y hora de registro del clima en formato DD-MM-YYYY HH24',
    DIRVIENC STRING COMMENT 'Dirección del viento en términos textuales (e.g., Noreste)',
    DIRVIENG NUMBER COMMENT 'Dirección del viento en grados (numérica)',
    DPT NUMBER COMMENT 'Punto de rocío en grados Celsius',
    DSEM STRING COMMENT 'Día de la semana en que se registró el clima',
    HR NUMBER COMMENT 'Humedad relativa en porcentaje',
    IDES STRING COMMENT 'ID del estado en el que se realizó el registro',
    IDMUN NUMBER COMMENT 'ID del municipio donde se registraron los datos',
    LAT FLOAT COMMENT 'Latitud geográfica del registro del clima',
    LON FLOAT COMMENT 'Longitud geográfica del registro del clima',
    NES STRING COMMENT 'Nombre del estado donde se realizó el registro',
    NHOR NUMBER COMMENT 'Número de horas desde el inicio del día en que se registraron los datos',
    NMUN STRING COMMENT 'Nombre del municipio donde se realizó el registro',
    PREC FLOAT COMMENT 'Cantidad de precipitación registrada en milímetros',
    PROBPREC FLOAT COMMENT 'Probabilidad de precipitación en porcentaje',
    RAF FLOAT COMMENT 'Velocidad de ráfagas de viento en km/h',
    TEMP FLOAT COMMENT 'Temperatura en grados Celsius',
    VELVIEN FLOAT COMMENT 'Velocidad del viento en km/h'
);

-- Insertar datos transformados desde la tabla original con formato de fecha adecuado
INSERT INTO DEV_ICESI.SYSTEM_RECOMMENDATION.transformed_weather_data_v2
SELECT DISTINCT
    DESCIEL,
    TO_CHAR(TO_TIMESTAMP(HLOC, 'YYYYMMDDTHH'), 'DD-MM-YYYY HH24') AS HLOC,  -- Formatear HLOC a DD-MM-YYYY HH24
    DIRVIENC,
    TRY_TO_NUMBER(DIRVIENG, 0) AS DIRVIENG,
    TRY_TO_NUMBER(DPT, 0) AS DPT,
    DSEM,
    TRY_TO_NUMBER(HR, 0) AS HR,
    IDES,
    TRY_TO_NUMBER(IDMUN, 0) AS IDMUN,  -- Cambiado a 0 en lugar de -1
    TRY_TO_NUMBER(LAT, 0.0) AS LAT,
    TRY_TO_NUMBER(LON, 0.0) AS LON,
    NES,
    TRY_TO_NUMBER(NHOR, 0) AS NHOR,
    NMUN,
    TRY_TO_NUMBER(PREC, 0.0) AS PREC,
    TRY_TO_NUMBER(PROBPREC, 0.0) AS PROBPREC,
    TRY_TO_NUMBER(RAF, 0.0) AS RAF,
    TRY_TO_NUMBER(TEMP, 0.0) AS TEMP,
    TRY_TO_NUMBER(VELVIEN, 0.0) AS VELVIEN
FROM DEV_ICESI.SYSTEM_RECOMMENDATION.CONAGUA_WEATHER_RAW_v2;

