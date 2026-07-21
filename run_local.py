import os
import sys

# 1. Configura tu API Key directamente en el código para no depender de PowerShell
# (Reemplaza el texto de abajo con tu clave real de Gemini)
os.environ["GEMINI_API_KEY"] = "TU_API_KEY_AQUI"

# 2. Configurar la ruta absoluta de las credenciales de GCP
ruta_credenciales = os.path.abspath("gcp-key.json")

if not os.path.exists(ruta_credenciales):
    print(f"[!] Error: No se encuentra el archivo de credenciales en: {ruta_credenciales}")
    print("Asegúrate de haber guardado tu JSON como 'gcp-key.json' en la raíz del proyecto.")
    sys.exit(1)

# Inyectamos la variable de Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ruta_credenciales

# 3. Importar y ejecutar tu main.py
from main import generate_sqlx_from_json

mock_event = {
    'bucket': 'automatizacion-bucket-diego',  # <-- ASEGÚRATE DE CAMBIAR ESTO POR TU BUCKET REAL
    'name': 'stg_customers.json'
}

print("Iniciando simulación de Cloud Function de manera local...")
try:
    generate_sqlx_from_json(mock_event, None)
    print("\n[ÉXITO] ¡Proceso completado! Revisa la carpeta 'output_sqlx/main/' en tu bucket.")
except Exception as e:
    print(f"\n[ERROR] La ejecución falló: {e}")