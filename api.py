import requests
import pandas as pd
import snowflake.connector
import re

def dms_to_decimal(dms_str):
    """Convierte coordenadas en formato DMS (e.g. '430528N') a decimal."""
    if not isinstance(dms_str, str):
        return None
    m = re.match(r"(\d{2})(\d{2})(\d{2})([NSEW])", dms_str)
    if not m:
        return None
    grados, minutos, segundos, direccion = m.groups()
    dec = int(grados) + int(minutos)/60 + int(segundos)/3600
    if direccion in ['S', 'W']:
        dec = -dec
    return round(dec, 6)

API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJrb3plNDQ0QGdtYWlsLmNvbSIsImp0aSI6IjRlNmEyYzAzLWUyMWItNDE3OS1hYmRkLWY2Y2NhNDA1MjQ5YSIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNzQ3MzIxMTU1LCJ1c2VySWQiOiI0ZTZhMmMwMy1lMjFiLTQxNzktYWJkZC1mNmNjYTQwNTI0OWEiLCJyb2xlIjoiIn0.GKc1F1SzDVof2IfrYr8HPQJ4gb7YfrPmOxCGz55DkEI"  # Cambia por tu API key real

url = "https://opendata.aemet.es/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones"
headers = {"api_key": API_KEY}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    res_json = response.json()
except Exception as e:
    print(f"❌ Error general: {e}")
    exit()

if res_json.get("estado") != 200:
    print(f"❌ No hay datos disponibles o error en la solicitud: {res_json}")
    exit()

url_datos = res_json.get("datos")
if not url_datos:
    print("❌ No se encontró URL de descarga en la respuesta.")
    exit()

try:
    response_datos = requests.get(url_datos)
    response_datos.raise_for_status()
    datos = response_datos.json()
except Exception as e:
    print(f"❌ Error al obtener datos reales: {e}")
    exit()

if not isinstance(datos, list):
    print("❌ Los datos descargados no son una lista.")
    exit()

df = pd.DataFrame(datos)

df['latitud_decimal'] = df['latitud'].apply(dms_to_decimal)
df['longitud_decimal'] = df['longitud'].apply(dms_to_decimal)

# Conexión a Snowflake
try:
    conn = snowflake.connector.connect(
        account="HHFXICV-CIVICAPARTNER",
        user="ALUMNO16_DBT",
        password="*******",
        authenticator="snowflake",
        role="CURSO_DATA_ENGINEERING",
        warehouse='WH_CURSO_DATA_ENGINEERING',
        database='ALUMNO16_DEV_BRONZE_DB',
        schema='METEO'
    )
except Exception as e:
    print(f"❌ Error conectando a Snowflake: {e}")
    exit()

cursor = conn.cursor()

for idx, row in df.iterrows():
    id_estacion = idx  # O genera un ID único basado en el índice o algún campo

    latitud = row['latitud'] if pd.notnull(row['latitud']) else 'DESCONOCIDO'
    longitud = row['longitud'] if pd.notnull(row['longitud']) else 'DESCONOCIDO'
    altitud = row['altitud'] if pd.notnull(row['altitud']) else 0
    indicativo = row['indicativo'] if pd.notnull(row['indicativo']) else 'DESCONOCIDO'
    nombre = row['nombre'] if pd.notnull(row['nombre']) else 'DESCONOCIDO'
    indsinop = row['indsinop'] if isinstance(row['indsinop'], str) else 'DESCONOCIDO'
    provincia = row['provincia'] if pd.notnull(row['provincia']) else 'DESCONOCIDO'

    # Sanitizamos strings
    latitud_sql = latitud.replace("'", "''")
    longitud_sql = longitud.replace("'", "''")
    indicativo_sql = indicativo.replace("'", "''")
    nombre_sql = nombre.replace("'", "''")
    indsinop_sql = indsinop.replace("'", "''")
    provincia_sql = provincia.replace("'", "''")

    try:
        cursor.execute(f"""
            INSERT INTO raw_aemet_inventario (
                id_estacion, latitud, longitud, altitud, indicativo, nombre, indsinop, provincia
            ) VALUES (
                {id_estacion},
                '{latitud_sql}',
                '{longitud_sql}',
                {altitud},
                '{indicativo_sql}',
                '{nombre_sql}',
                '{indsinop_sql}',
                '{provincia_sql}'
            )
        """)
    except Exception as e:
        print(f"⚠️ Error al insertar fila {idx}: {e}")
        print(f"Datos: {row}")
