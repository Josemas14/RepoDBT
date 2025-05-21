import os
import snowflake.connector

# Conexión Snowflake
conn = snowflake.connector.connect(
    account="HHFXICV-CIVICAPARTNER",
    user="ALUMNO16_DBT",
    password="*********",
    authenticator="snowflake",
    role="CURSO_DATA_ENGINEERING",
    warehouse='WH_CURSO_DATA_ENGINEERING',
    database='ALUMNO16_DEV_BRONZE_DB',
    schema='Meteo'
)

cursor = conn.cursor()

# Carpeta donde están los CSV
folder_path = './csv_output'

# Crear stage interna si no existe
cursor.execute("CREATE OR REPLACE STAGE stage_csv_files;")

# Subir todos los CSV al stage
for file in os.listdir(folder_path):
    if file.endswith(".csv"):
        file_path = os.path.join(folder_path, file)
        put_query = f"PUT file://{file_path} @stage_csv_files OVERWRITE = TRUE;"
        cursor.execute(put_query)
        print(f"✅ Subido {file} al stage.")

# Para cada archivo CSV, crear tabla y cargarla
for file in os.listdir(folder_path):
    if file.endswith(".csv"):
        table_name = os.path.splitext(file)[0]

        # Crear tabla (asumiendo todo STRING por simplicidad)
        # Extraer cabeceras
        with open(os.path.join(folder_path, file), 'r', encoding='utf-8') as f:
            headers = f.readline().strip().split(",")
        columns = ", ".join([f'"{col}" STRING' for col in headers])

        create_table_query = f'CREATE OR REPLACE TABLE "{table_name}" ({columns});'
        cursor.execute(create_table_query)
        print(f"✅ Tabla {table_name} creada.")

        # Cargar datos desde stage
        copy_query = f"""
        COPY INTO "{table_name}"
        FROM @stage_csv_files/{file}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE';
        """
        cursor.execute(copy_query)
        print(f"📊 Datos cargados en {table_name}")

cursor.close()
conn.close()
print("🎉 Todos los CSV han sido subidos correctamente a Snowflake.")
