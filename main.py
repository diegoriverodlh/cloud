import os
import json
from google.cloud import storage
from google import genai
from google.genai import types

def generate_sqlx_from_json(event, context):
    """
    Función activada por evento de Cloud Storage (subida de JSON).
    Lee el JSON, llama a Gemini en Vertex AI y escribe el resultado .sqlx.
    """
    # 1. Obtener detalles del archivo subido
    bucket_name = event['bucket']
    file_name = event['name']
    
    if not file_name.endswith('.json'):
        print(f"Ignorando archivo no JSON: {file_name}")
        return

    print(f"Procesando archivo: {file_name} del bucket: {bucket_name}")

    # 2. Inicializar clientes de GCP
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Leer y deserializar el contenido JSON
    json_data = json.loads(blob.download_as_text())
    
    # 3. Construir el Prompt del sistema y del usuario para Gemini
    system_instruction = (
        "Eres un ingeniero de datos experto especializado en Google Cloud Dataform y BigQuery.\n"
        "Tu tarea es transformar una estructura JSON de entrada en un código SQLX perfectamente formateado y válido para Dataform.\n"
        "Reglas estrictas:\n"
        "1. Genera únicamente el código de Dataform (.sqlx).\n"
        "2. No incluyas explicaciones en lenguaje natural, preámbulos ni bloques de Markdown (como ```sqlx ... ```) en tu respuesta. Devuelve exclusivamente texto plano ejecutable.\n"
        "3. Estructura el bloque config {} utilizando las propiedades dadas en el JSON.\n"
        "4. Usa la función ref() para declarar y usar las dependencias proporcionadas en la consulta SQL principal.\n"
        "5. Documenta las columnas dentro del bloque config empleando la estructura de Dataform."
    )
    
    user_prompt = f"""
    Genera el archivo SQLX para Dataform a partir de los siguientes metadatos JSON:
    
    {json.dumps(json_data, indent=2)}
    """

    # 4. Llamar a Vertex AI (Gemini 3.5 Flash) utilizando el SDK oficial de Google GenAI
    # Nota: Asegúrate de tener configurada la variable de entorno de GCP o el Service Account con permisos.
    ai_client = genai.Client()
    
    try:
        response = ai_client.models.generate_content(
            model='gemini-3.5-flash',
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                temperature=0.1 # Temperatura baja para código determinista
            )
        )
        
        sqlx_code = response.text.strip()
        print("Código SQLX generado exitosamente por Gemini.")
        
        # 5. Almacenar el archivo generado (.sqlx) en un bucket de salida
        output_bucket_name = os.environ.get("OUTPUT_BUCKET_NAME", bucket_name)
        output_bucket = storage_client.bucket(output_bucket_name)
        
        output_file_name = file_name.replace('.json', '.sqlx')
        output_blob = output_bucket.blob(f"output_sqlx/main/{output_file_name}")
        
        output_blob.upload_from_string(sqlx_code, content_type='text/plain')
        print(f"Archivo SQLX guardado en: gs://{output_bucket_name}/output_sqlx/{output_file_name}")
        
    except Exception as e:
        print(f"Error durante la generación o almacenamiento de SQLX: {str(e)}")
        raise e