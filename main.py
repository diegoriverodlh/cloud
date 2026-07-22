import os
import json
from google.cloud import storage
from google import genai
from google.genai import types

def generate_sqlx_from_json(event, context=None):
    """
    Función activada por evento de Cloud Storage (2ª Gen o local).
    Lee el JSON, adapta dinámicamente las instrucciones para Gemini según
    las opciones especificadas (CTEs, particiones, tags, etc.) y guarda el .sqlx.
    """
    # 1. Extraer datos según el formato de origen (CloudEvent o Diccionario)
    if hasattr(event, "data"):
        data = event.data
    elif isinstance(event, dict):
        data = event
    else:
        data = event

    bucket_name = data.get('bucket')
    file_name = data.get('name')

    if not file_name or not file_name.endswith('.json'):
        print(f"Ignorando archivo no JSON o evento inválido: {file_name}")
        return

    print(f"Procesando archivo: {file_name} del bucket: {bucket_name}")

    # 2. Inicializar cliente de Storage y descargar el JSON
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    json_data = json.loads(blob.download_as_text(encoding="utf-8"))

    # 3. Construcción DINÁMICA del Prompt del Sistema
    base_instructions = [
        "Eres un ingeniero de datos experto especializado en Google Cloud Dataform y BigQuery.",
        "Tu tarea es transformar una estructura JSON de entrada en un código SQLX perfectamente formateado y válido para Dataform.",
        "Reglas estrictas:",
        "1. Genera únicamente el código de Dataform (.sqlx).",
        "2. No incluyas explicaciones en lenguaje natural, preámbulos ni bloques de Markdown (como ```sqlx ... ```) en tu respuesta. Devuelve exclusivamente texto plano ejecutable.",
        "3. Estructura el bloque config {} utilizando las propiedades dadas en el JSON.",
        "4. Usa la función ref() para declarar y usar las dependencias proporcionadas en la consulta SQL principal.",
        "5. Documenta las columnas dentro del bloque config empleando la estructura de Dataform."
    ]

    # Inspect si el JSON trae requerimientos especiales de 2ª generación
    options = json_data.get("sqlx_options", {})

    if options:
        print("Detectadas opciones avanzadas (v2). Adaptando las reglas de Gemini...")
        
        # Opción 1: Uso explícito de CTEs (Common Table Expressions)
        if options.get("use_cte"):
            base_instructions.append(
                "6. Estructura la consulta SQL utilizando Common Table Expressions (CTEs) con nombres claros (ej. WITH source_data AS (...))."
            )

        # Opción 2: Particionamiento de tablas en BigQuery
        if options.get("partition_by"):
            base_instructions.append(
                f"7. Incluye obligatoriamente la propiedad 'partitionBy: \"{options.get('partition_by')}\"' dentro del bloque config {{}}."
            )

        # Opción 3: Clustering de tablas en BigQuery
        if options.get("cluster_by"):
            cluster_list = json.dumps(options.get("cluster_by"))
            base_instructions.append(
                f"8. Incluye obligatoriamente la propiedad 'clusterBy: {cluster_list}' dentro del bloque config {{}}."
            )

        # Opción 4: Etiquetas (Tags) de Dataform
        if options.get("tags"):
            tags_list = json.dumps(options.get("tags"))
            base_instructions.append(
                f"9. Agrega la propiedad 'tags: {tags_list}' dentro del bloque config {{}}."
            )

        # Opción 5: Instrucciones o reglas de negocio personalizadas en texto libre
        if options.get("custom_instructions"):
            base_instructions.append(
                f"10. REGLA ADICIONAL OBLIGATORIA: {options.get('custom_instructions')}"
            )

    system_instruction = "\n".join(base_instructions)

    user_prompt = f"""
    Genera el archivo SQLX para Dataform a partir de los siguientes metadatos JSON:

    {json.dumps(json_data, indent=2)}
    """

    # 4. Llamar a Gemini mediante SDK oficial
    api_key = os.environ.get("GEMINI_API_KEY")
    ai_client = genai.Client(api_key=api_key) if api_key else genai.Client()

    try:
        response = ai_client.models.generate_content(
            model='gemini-3.5-flash',
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                temperature=0.1
            )
        )

        sqlx_code = response.text.strip()
        print("Código SQLX generado exitosamente por Gemini.")

        # 5. Almacenar el archivo generado (.sqlx) en el bucket de salida
        output_bucket_name = os.environ.get("OUTPUT_BUCKET_NAME", bucket_name)
        output_bucket = storage_client.bucket(output_bucket_name)

        base_name = os.path.basename(file_name)
        output_file_name = base_name.replace('.json', '.sqlx')

        target_path = f"output_sqlx/main/{output_file_name}"
        output_blob = output_bucket.blob(target_path)

        output_blob.upload_from_string(sqlx_code, content_type='text/plain')
        print(f"Archivo SQLX guardado exitosamente en: gs://{output_bucket_name}/{target_path}")

    except Exception as e:
        print(f"Error durante la generación o almacenamiento de SQLX: {str(e)}")
        raise e








# import os
# import json
# from google.cloud import storage
# from google import genai
# from google.genai import types

# def generate_sqlx_from_json(event, context):
#     """
#     Función activada por evento de Cloud Storage (subida de JSON).
#     Lee el JSON, llama a Gemini en Vertex AI y escribe el resultado .sqlx.
#     """
#     # 1. Obtener detalles del archivo subido
#     bucket_name = event['bucket']
#     file_name = event['name']
    
#     if not file_name.endswith('.json'):
#         print(f"Ignorando archivo no JSON: {file_name}")
#         return

#     print(f"Procesando archivo: {file_name} del bucket: {bucket_name}")

#     # 2. Inicializar clientes de GCP
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(file_name)
    
#     # Leer y deserializar el contenido JSON
#     json_data = json.loads(blob.download_as_text())
    
#     # 3. Construir el Prompt del sistema y del usuario para Gemini
#     system_instruction = (
#         "Eres un ingeniero de datos experto especializado en Google Cloud Dataform y BigQuery.\n"
#         "Tu tarea es transformar una estructura JSON de entrada en un código SQLX perfectamente formateado y válido para Dataform.\n"
#         "Reglas estrictas:\n"
#         "1. Genera únicamente el código de Dataform (.sqlx).\n"
#         "2. No incluyas explicaciones en lenguaje natural, preámbulos ni bloques de Markdown (como ```sqlx ... ```) en tu respuesta. Devuelve exclusivamente texto plano ejecutable.\n"
#         "3. Estructura el bloque config {} utilizando las propiedades dadas en el JSON.\n"
#         "4. Usa la función ref() para declarar y usar las dependencias proporcionadas en la consulta SQL principal.\n"
#         "5. Documenta las columnas dentro del bloque config empleando la estructura de Dataform."
#     )
    
#     user_prompt = f"""
#     Genera el archivo SQLX para Dataform a partir de los siguientes metadatos JSON:
    
#     {json.dumps(json_data, indent=2)}
#     """

#     # 4. Llamar a Vertex AI (Gemini 3.5 Flash) utilizando el SDK oficial de Google GenAI
#     # Nota: Asegúrate de tener configurada la variable de entorno de GCP o el Service Account con permisos.
#     ai_client = genai.Client()
    
#     try:
#         response = ai_client.models.generate_content(
#             model='gemini-3.5-flash',
#             contents=user_prompt,
#             config=types.GenerateContentConfig(
#                 system_instruction=system_instruction,
#                 temperature=0.1 # Temperatura baja para código determinista
#             )
#         )
        
#         sqlx_code = response.text.strip()
#         print("Código SQLX generado exitosamente por Gemini.")
        
#         # 5. Almacenar el archivo generado (.sqlx) en un bucket de salida
#         output_bucket_name = os.environ.get("OUTPUT_BUCKET_NAME", bucket_name)
#         output_bucket = storage_client.bucket(output_bucket_name)
        
#         output_file_name = file_name.replace('.json', '.sqlx')
#         output_blob = output_bucket.blob(f"output_sqlx/main/{output_file_name}")
        
#         output_blob.upload_from_string(sqlx_code, content_type='text/plain')
#         print(f"Archivo SQLX guardado en: gs://{output_bucket_name}/output_sqlx/{output_file_name}")
        
#     except Exception as e:
#         print(f"Error durante la generación o almacenamiento de SQLX: {str(e)}")
#         raise e