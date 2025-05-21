import json
import pandas as pd
import os
import glob

def json_a_csv(nombre_archivo_json, nombre_archivo_csv):
    """
    Convierte un archivo JSON a un archivo CSV.

    Args:
        nombre_archivo_json (str): El nombre del archivo JSON de entrada.
        nombre_archivo_csv (str): El nombre del archivo CSV de salida.
    """
    try:
        # 1. Leer el archivo JSON
        with open(nombre_archivo_json, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # 2. Comprobar si es lista de registros o un único diccionario
        if isinstance(data, list):
            registros = data
        elif isinstance(data, dict):
            registros = [data]
        else:
            print(f"⚠️ Formato no reconocido en {nombre_archivo_json}")
            return

        # 3. Crear un DataFrame de Pandas
        df = pd.DataFrame(registros)

        # 4. Escribir el DataFrame a un archivo CSV
        df.to_csv(nombre_archivo_csv, index=False, encoding='utf-8')

        print(f"✅ CSV creado: {nombre_archivo_csv}")

    except FileNotFoundError:
        print(f"❌ Error: El archivo JSON '{nombre_archivo_json}' no se encontró.")
    except json.JSONDecodeError as e:
        print(f"❌ Error al parsear el JSON: {e}")
    except Exception as e:
        print(f"❌ Ocurrió un error inesperado: {e}")

if __name__ == "__main__":
    directorio_json = r"C:\Users\josem\Desktop\PROYECTO\pythonProject\venv\aemet_data" 
    directorio_csv = "csv_output"  # Directorio de salida para los CSV
    os.makedirs(directorio_csv, exist_ok=True)

    # Usar glob para encontrar todos los archivos JSON en el directorio
    archivos_json = glob.glob(os.path.join(directorio_json, "*.json"))

    if not archivos_json:
        print(f"❌ No se encontraron archivos JSON en el directorio: {directorio_json}")
    else:
        for archivo_json in archivos_json:
            nombre_base = os.path.splitext(os.path.basename(archivo_json))[0]
            nombre_archivo_csv = os.path.join(directorio_csv, f"{nombre_base}.csv")
            json_a_csv(archivo_json, nombre_archivo_csv)
