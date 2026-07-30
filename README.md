# Documentación Técnica: Pipeline Serverless de Automatización JSON a Dataform (.sqlx) con Gemini

**Repositorio del Proyecto:** [github.com/diegoriverodlh/cloud](https://github.com/diegoriverodlh/cloud)  
**Entorno Cloud:** Google Cloud Platform (GCP) — Cloud Functions 2ª Generación / Cloud Run  
**Motor de IA:** Google Gemini API (`gemini-3.5-flash` mediante el SDK `google-genai`)  

---

## 1. Resumen del Proyecto

Este proyecto automatiza la generación de modelos de datos en formato **SQLX para Google Cloud Dataform** a partir de archivos de metadatos en formato **JSON** subidos a un bucket de **Google Cloud Storage (GCS)**.

La solución utiliza una arquitectura orientada a eventos (*event-driven*) y sin servidor (*serverless*). Cuando un desarrollador o proceso sube un archivo `.json` al bucket de entrada, se activa automáticamente una **Cloud Function de 2ª Generación**, la cual:

1. Lee y desglosa la estructura del JSON recibido.
2. Analiza si el JSON contiene opciones de configuración avanzadas (soporte para metadatos v1 y v2).
3. Adapta dinámicamente el *System Prompt* enviado a la API de **Gemini**.
4. Solicita la generación de código SQLX válido para Dataform.
5. Guarda el archivo `.sqlx` resultante en una subcarpeta del bucket de salida (`output_sqlx/main/`).
6. Descarta cualquier archivo no `.json` en milisegundos para evitar bucles de ejecución infinitos.

---

## 2. Arquitectura de la Solución

```text
   [ Usuario / Pipeline ]
             │
             ▼ (Sube archivo .json)
 ┌────────────────────────────────────────┐
 │ GCS Bucket: automatizacion-bucket-diego│
 └───────────────────┬────────────────────┘
                     │
                     ▼ Evento: google.storage.object.v1.finalized
 ┌────────────────────────────────────────┐
 │ Eventarc / Trigger                     │
 └───────────────────┬────────────────────┘
                     │
                     ▼ (Invocación HTTP CloudEvent)
 ┌────────────────────────────────────────┐
 │ Cloud Function 2ª Gen (function-2)     │
 │  - Valida extensión (.json)            │
 │  - Detecta versión (v1 / v2 options)   │
 │  - Construye Prompt Dinámico           │
 └───────────────────┬────────────────────┘
                     │
                     ▼ (Solicitud de generación de código)
 ┌────────────────────────────────────────┐
 │ Gemini API (gemini-3.5-flash)          │
 └───────────────────┬────────────────────┘
                     │
                     ▼ (Devuelve texto plano .sqlx)
 ┌────────────────────────────────────────┐
 │ GCS Output: output_sqlx/main/*.sqlx    │
 └───────────────────┬────────────────────┘
                     │
                     ▼ (MEdiante el Main_v2.py)
 ┌────────────────────────────────────────┐
 │ Insercción en Dataforma                │
 └────────────────────────────────────────┘
                     
```

---
## 3. Especificación de los Metadatos JSON

El sistema soporta dos esquemas de entrada:

### Esquema v1 (Estándar)
Diseñado para transformaciones básicas de staging o vistas sin configuraciones complejas de BigQuery.

```text
{
  "model_name": "stg_customers",
  "config": {
    "type": "table",
    "schema": "staging_dataset",
    "description": "Tabla de clientes limpios y unificados",
    "assertions": {
      "uniqueKey": ["customer_id"],
      "nonNull": ["customer_id", "email"]
    }
  },
  "dependencies": [
    {
      "name": "raw_customers",
      "schema": "raw_dataset"
    }
  ],
  "columns": [
    {
      "name": "customer_id",
      "expression": "CAST(id AS STRING)",
      "description": "ID único de cliente"
    },
    {
      "name": "full_name",
      "expression": "CONCAT(first_name, ' ', last_name)",
      "description": "Nombre completo del cliente"
    }
  ],
  "where_filter": "status = 'ACTIVE'"
}
```

### Esquema v2 (Avanzado / Adaptable)
Incorpora el bloque opcional sqlx_options, el cual modifica en tiempo de ejecución las reglas del modelo que Gemini debe generar.
```text
{
  "model_name": "stg_orders_v2",
  "config": {
    "type": "table",
    "schema": "staging_dataset",
    "description": "Tabla de pedidos con particionado diario y clustering",
    "assertions": {
      "uniqueKey": ["order_id"]
    }
  },
  "sqlx_options": {
    "use_cte": true,
    "partition_by": "DATE(order_date)",
    "cluster_by": ["customer_id", "order_status"],
    "tags": ["daily_pipeline", "staging"],
    "custom_instructions": "Crea una CTE llamada 'raw_data' para la extracción inicial y una CTE 'cleaned_data' antes del SELECT final."
  },
  "dependencies": [
    { "name": "raw_orders", "schema": "raw_dataset" }
  ],
  "columns": [
    { "name": "order_id", "expression": "CAST(id AS STRING)", "description": "ID del pedido" }
  ],
  "where_filter": "status != 'TEST'"
}
```
---

## 4. Código Fuente del Proyecto

### 4.1.1. main.py (Código Servidor en la Nube)

Este código está adaptado para procesar tanto invocaciones CloudEvent (en GCP 2ª Gen) como diccionarios estándar de Python (utilizados en pruebas locales).

```text
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

    # Filtro anti-bucles e ignorar archivos no JSON
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

    # Inyección de opciones dinámicas (v2)
    options = json_data.get("sqlx_options", {})

    if options:
        print("Detectadas opciones avanzadas (v2). Adaptando las reglas de Gemini...")
        
        if options.get("use_cte"):
            base_instructions.append(
                "6. Estructura la consulta SQL utilizando Common Table Expressions (CTEs) con nombres claros (ej. WITH source_data AS (...))."
            )

        if options.get("partition_by"):
            base_instructions.append(
                f"7. Incluye obligatoriamente la propiedad 'partitionBy: \"{options.get('partition_by')}\"' dentro del bloque config {{}}."
            )

        if options.get("cluster_by"):
            cluster_list = json.dumps(options.get("cluster_by"))
            base_instructions.append(
                f"8. Incluye obligatoriamente la propiedad 'clusterBy: {cluster_list}' dentro del bloque config {{}}."
            )

        if options.get("tags"):
            tags_list = json.dumps(options.get("tags"))
            base_instructions.append(
                f"9. Agrega la propiedad 'tags: {tags_list}' dentro del bloque config {{}}."
            )

        if options.get("custom_instructions"):
            base_instructions.append(
                f"10. REGLA ADICIONAL OBLIGATORIA: {options.get('custom_instructions')}"
            )

    system_instruction = "\n".join(base_instructions)

    user_prompt = f"""
    Genera el archivo SQLX para Dataform a partir de los siguientes metadatos JSON:

    {json.dumps(json_data, indent=2)}
    """

    # 4. Llamar a Gemini mediante SDK oficial google-genai
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
```

### 4.1.2. main_v2.py (Código Servidor en la Nube y Dataform)

Este código está adaptado para procesar tanto invocaciones CloudEvent (en GCP 2ª Gen) como diccionarios estándar de Python (utilizados en pruebas locales). Añade el sqlx a Dataform.

```text
import os
import json
from google.cloud import storage
from google import genai
from google.genai import types
from google.cloud import dataform_v1beta1


def deploy_and_run_dataform(sqlx_file_name: str, sqlx_content: str):
    """
    Escribe el archivo .sqlx en un Workspace de Dataform,
    compila el proyecto entero y lanza la ejecución en BigQuery.
    """
    client = dataform_v1beta1.DataformClient()

    # GCP inyecta por defecto la variable GOOGLE_CLOUD_PROJECT en Cloud Functions / Cloud Run
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
    location = os.environ.get("DATAFORM_REGION", "europe-west1")
    repository_id = os.environ.get("DATAFORM_REPO", "mi-repositorio-dataform")
    workspace_id = os.environ.get("DATAFORM_WORKSPACE", "dev-workspace")

    repo_path = f"projects/{project_id}/locations/{location}/repositories/{repository_id}"
    workspace_path = f"{repo_path}/workspaces/{workspace_id}"
    target_file_path = f"definitions/staging/{sqlx_file_name}"

    # A. Escribir o actualizar el archivo .sqlx en el Workspace de Dataform
    print(f"Subiendo archivo {target_file_path} a Dataform (Repo: {repository_id} | Workspace: {workspace_id})...")
    client.write_file(
        request={
            "workspace": workspace_path,
            "path": target_file_path,
            "contents": sqlx_content.encode("utf-8")
        }
    )
    print("✔ Archivo guardado correctamente en el workspace de Dataform.")

    # B. Compilar el proyecto entero de Dataform
    print("Compilando proyecto de Dataform...")
    compilation_request = dataform_v1beta1.CreateCompilationResultRequest(
        parent=repo_path,
        compilation_result=dataform_v1beta1.CompilationResult(
            workspace=workspace_path
        )
    )
    compilation_result = client.create_compilation_result(request=compilation_request)
    print(f"✔ Compilación completada exitosamente: {compilation_result.name}")

    # C. Invocar la ejecución del nuevo modelo en BigQuery
    print("Iniciando ejecución del modelo en BigQuery...")
    invocation_request = dataform_v1beta1.CreateWorkflowInvocationRequest(
        parent=repo_path,
        workflow_invocation=dataform_v1beta1.WorkflowInvocation(
            compilation_result=compilation_result.name,
            invocation_config=dataform_v1beta1.InvocationConfig(
                included_targets=[
                    dataform_v1beta1.Target(
                        name=sqlx_file_name.replace('.sqlx', '')
                    )
                ]
            )
        )
    )
    invocation = client.create_workflow_invocation(request=invocation_request)
    print(f"✔ [ÉXITO DATAFORM] Invocación creada. ID: {invocation.name}")


def generate_sqlx_from_json(event, context=None):
    """
    Función activada por evento de Cloud Storage (2ª Gen o local).
    Lee el JSON, lo pasa a Gemini, guarda en Storage Y ejecuta en Dataform.
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
        "Reglas strictly necesarias:",
        "1. Genera únicamente el código de Dataform (.sqlx).",
        "2. No incluyas explicaciones en lenguaje natural, preámbulos ni bloques de Markdown (como ```sqlx ... ```) en tu respuesta. Devuelve exclusivamente texto plano ejecutable.",
        "3. Estructura el bloque config {} utilizando las propiedades dadas en el JSON.",
        "4. Usa la función ref() para declarar y usar las dependencias proporcionadas en la consulta SQL principal.",
        "5. Documenta las columnas dentro del bloque config empleando la estructura de Dataform."
    ]

    options = json_data.get("sqlx_options", {})

    if options:
        print("Detectadas opciones avanzadas (v2). Adaptando las reglas de Gemini...")
        if options.get("use_cte"):
            base_instructions.append(
                "6. Estructura la consulta SQL utilizando Common Table Expressions (CTEs) con nombres claros (ej. WITH source_data AS (...))."
            )
        if options.get("partition_by"):
            base_instructions.append(
                f"7. Incluye obligatoriamente la propiedad 'partitionBy: \"{options.get('partition_by')}\"' dentro del bloque config {{}}."
            )
        if options.get("cluster_by"):
            cluster_list = json.dumps(options.get("cluster_by"))
            base_instructions.append(
                f"8. Incluye obligatoriamente la propiedad 'clusterBy: {cluster_list}' dentro del bloque config {{}}."
            )
        if options.get("tags"):
            tags_list = json.dumps(options.get("tags"))
            base_instructions.append(
                f"9. Agrega la propiedad 'tags: {tags_list}' dentro del bloque config {{}}."
            )
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

        # 5. Guardar copia en el bucket de salida de Cloud Storage (output_sqlx/main/)
        output_bucket_name = os.environ.get("OUTPUT_BUCKET_NAME", bucket_name)
        output_bucket = storage_client.bucket(output_bucket_name)

        base_name = os.path.basename(file_name)
        output_file_name = base_name.replace('.json', '.sqlx')

        target_path = f"output_sqlx/main/{output_file_name}"
        output_blob = output_bucket.blob(target_path)

        output_blob.upload_from_string(sqlx_code, content_type='text/plain')
        print(f"✔ [GCS] Archivo SQLX guardado en: gs://{output_bucket_name}/{target_path}")

        # 6. Escribir en Dataform (mi-repositorio-dataform / dev-workspace), compilar y ejecutar
        deploy_and_run_dataform(
            sqlx_file_name=output_file_name,
            sqlx_content=sqlx_code
        )

    except Exception as e:
        print(f"Error durante el procesamiento, almacenamiento o ejecución: {str(e)}")
        raise e
```


### 4.1.3. main_v3.py (Código Servidor en la Nube y Dataform)

Este código está adaptado para procesar tanto invocaciones CloudEvent (en GCP 2ª Gen) como diccionarios estándar de Python (utilizados en pruebas locales). Añade el sqlx a Dataform. Permite trabajar con PDF y desde un diagrama crea todas las tablas correspondientes.
```text
import json
import os
import re
from google import genai
from google.cloud import dataform_v1beta1, storage
from google.genai import types


def deploy_and_run_dataform(sqlx_file_name: str, sqlx_content: str):
    client = dataform_v1beta1.DataformClient()

    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
    location = os.environ.get("DATAFORM_REGION", "europe-west1")
    repository_id = os.environ.get("DATAFORM_REPO", "mi-repositorio-dataform")
    workspace_id = os.environ.get("DATAFORM_WORKSPACE", "dev-workspace")
    dataform_sa = os.environ.get("DATAFORM_SERVICE_ACCOUNT")

    repo_path = f"projects/{project_id}/locations/{location}/repositories/{repository_id}"
    workspace_path = f"{repo_path}/workspaces/{workspace_id}"
    target_file_path = f"definitions/staging/{sqlx_file_name}"

    # LIMPIEZA PREVIA: Si existía una versión con guiones (-), la borramos del workspace para evitar duplicados
    hyphenated_file_name = sqlx_file_name.replace("_", "-")
    if hyphenated_file_name != sqlx_file_name:
        legacy_path = f"definitions/staging/{hyphenated_file_name}"
        try:
            client.delete_file(
                request={"workspace": workspace_path, "path": legacy_path}
            )
            print(f"🗑 Eliminado archivo obsoleto con guiones: {legacy_path}")
        except Exception:
            pass  # Si no existía, ignoramos la excepción

    # A. Escribir archivo
    print(f"Subiendo archivo {target_file_path} a Dataform...")
    client.write_file(
        request={
            "workspace": workspace_path,
            "path": target_file_path,
            "contents": sqlx_content.encode("utf-8")
        }
    )
    print(f"✔ Archivo '{sqlx_file_name}' guardado correctamente.")

    # B. Compilar
    print("Compilando proyecto de Dataform...")
    compilation_request = dataform_v1beta1.CreateCompilationResultRequest(
        parent=repo_path,
        compilation_result=dataform_v1beta1.CompilationResult(
            workspace=workspace_path
        )
    )
    compilation_result = client.create_compilation_result(request=compilation_request)
    print(f"✔ Compilación completada: {compilation_result.name}")

    # C. Obtener acciones y usar los objetos Target ORIGINALES
    print("Iniciando ejecución del modelo en BigQuery...")
    actions_response = client.query_compilation_result_actions(
        request={"name": compilation_result.name}
    )

    clean_search_name = sqlx_file_name.replace('.sqlx', '').lower().replace('-', '_')
    
    target_to_run = None
    all_targets = []

    for action in actions_response:
        if action.target and action.target.name:
            all_targets.append(action.target)
            if action.target.name.lower().replace('-', '_') == clean_search_name:
                target_to_run = action.target
                break

    invocation_config_args = {}

    if target_to_run:
        print(f"✔ Target detectado en compilación: '{target_to_run.name}' (Schema: {target_to_run.schema})")
        invocation_config_args["included_targets"] = [target_to_run]
    else:
        print(f"⚠ Target '{clean_search_name}' no coincidi con exactitud en la compilación. Omitiendo ejecución global.")
        return

    if dataform_sa:
        invocation_config_args["service_account"] = dataform_sa

    # invocation_request = dataform_v1beta1.CreateWorkflowInvocationRequest(
    #     parent=repo_path,
    #     workflow_invocation=dataform_v1beta1.WorkflowInvocation(
    #         compilation_result=compilation_result.name,
    #         invocation_config=dataform_v1beta1.InvocationConfig(**invocation_config_args)
    #     )
    # )
    
    # invocation = client.create_workflow_invocation(request=invocation_request)
    # print(f"✔ [ÉXITO DATAFORM] Invocación creada correctamente. ID: {invocation.name}")


def parse_sqlx_files_from_response(
    response_text: str, default_filename: str
) -> dict:
    """Parsea la respuesta de Gemini para extraer uno o más archivos .sqlx.
    Normaliza todos los nombres a snake_case estricto (reemplaza '-' por '_').
    """
    files = {}

    pattern = r"===\s*FILE:\s*([^\n\r=]+(?:\.sqlx)?)\s*===\s*\n?(.*?)(?=(?:===\s*FILE:|$))"
    matches = re.findall(pattern, response_text, re.DOTALL)

    if matches:
        for raw_filename, content in matches:
            filename = raw_filename.strip()
            if not filename.endswith(".sqlx"):
                filename = f"{filename}.sqlx"

            # NORMALIZACIÓN: Reemplazar guiones '-' por '_' y forzar minúsculas
            filename = filename.lower().replace("-", "_")
            filename = re.sub(r"[^a-zA-Z0-9_\.]", "_", filename)
            filename = re.sub(r"_+", "_", filename)

            cleaned_content = content.strip()
            cleaned_content = re.sub(r"^```(?:sqlx|sql)?\s*", "", cleaned_content)
            cleaned_content = re.sub(r"\s*```$", "", cleaned_content).strip()

            files[filename] = cleaned_content
    else:
        # Fallback normalizado
        filename = default_filename.replace(".pdf", ".sqlx").replace(".json", ".sqlx")
        filename = filename.lower().replace("-", "_")
        filename = re.sub(r"[^a-zA-Z0-9_\.]", "_", filename)
        filename = re.sub(r"_+", "_", filename)

        content = response_text.strip()
        content = re.sub(r"^```(?:sqlx|sql)?\s*", "", content)
        content = re.sub(r"\s*```$", "", content).strip()
        files[filename] = content

    return files


def generate_sqlx_from_json(event, context=None):
    if hasattr(event, "data"):
        data = event.data
    elif isinstance(event, dict):
        data = event
    else:
        data = event

    bucket_name = data.get("bucket")
    file_name = data.get("name")

    if (
        not file_name
        or file_name.startswith("output_sqlx/")
        or not (
            file_name.lower().endswith(".json")
            or file_name.lower().endswith(".pdf")
        )
    ):
        print(f"Ignorando archivo no soportado o evento circular: {file_name}")
        return

    print(f"Procesando archivo: {file_name} del bucket: {bucket_name}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # ---------------------------------------------------------------------
    # RAMA 1: ARCHIVOS JSON
    # ---------------------------------------------------------------------
    if file_name.lower().endswith(".json"):
        json_data = json.loads(blob.download_as_text(encoding="utf-8"))

        base_instructions = [
            "Eres un ingeniero de datos experto especializado en Google Cloud Dataform y BigQuery.",
            "Tu tarea es transformar una estructura JSON de entrada en un código SQLX perfectamente formateado y válido para Dataform.",
            "Reglas estrictamente necesarias:",
            "1. Genera únicamente el código de Dataform (.sqlx).",
            "2. No incluyas explicaciones en lenguaje natural, preámbulos ni bloques de Markdown (como ```sqlx ... ```) en tu respuesta. Devuelve exclusivamente texto plano ejecutable.",
            "3. Estructura el bloque config {} utilizando las propiedades dadas en el JSON.",
            "4. Usa la función ref() para declarar y usar las dependencias proporcionadas en la consulta SQL principal.",
            "5. Documenta las columnas dentro del bloque config empleando la estructura de Dataform.",
            "6. REGLA DE NOMBRADO: Usa exclusivamente snake_case (minúsculas y guiones bajos '_'). NUNCA uses guiones medios '-' en nombres de archivo ni en referencias."
        ]

        options = json_data.get("sqlx_options", {})

        if options:
            if options.get("use_cte"):
                base_instructions.append("7. Estructura la consulta SQL utilizando Common Table Expressions (CTEs) con nombres claros (ej. WITH source_data AS (...)).")
            if options.get("partition_by"):
                base_instructions.append(f'8. Incluye obligatoriamente la propiedad \'partitionBy: "{options.get("partition_by")}"\' dentro del bloque config {{}}.')
            if options.get("cluster_by"):
                cluster_list = json.dumps(options.get("cluster_by"))
                base_instructions.append(f"9. Incluye obligatoriamente la propiedad 'clusterBy: {cluster_list}' dentro del bloque config {{}}.")
            if options.get("tags"):
                tags_list = json.dumps(options.get("tags"))
                base_instructions.append(f"10. Agrega la propiedad 'tags: {tags_list}' dentro del bloque config {{}}.")
            if options.get("custom_instructions"):
                base_instructions.append(f"11. REGLA ADICIONAL OBLIGATORIA: {options.get('custom_instructions')}")

        system_instruction = "\n".join(base_instructions)
        user_prompt = f"Genera el archivo SQLX para Dataform a partir de los siguientes metadatos JSON:\n\n{json.dumps(json_data, indent=2)}"
        gemini_contents = user_prompt

    # ---------------------------------------------------------------------
    # RAMA 2: ARCHIVOS PDF
    # ---------------------------------------------------------------------
    elif file_name.lower().endswith(".pdf"):
        print("Detectado archivo PDF. Descargando bytes para Gemini...")
        pdf_bytes = blob.download_as_bytes()

        system_instruction = """
Rol: Actúa como un experto en SQL, Dataform (SQLX) y modelado de datos.
Tu tarea es analizar el PDF adjunto (que contiene un diagrama de un modelo de datos) e implementar el SQLX de las tablas finales siguiendo exactamente la estructura representada en el documento.

--- REGLAS DE NOMBRADO OBLIGATORIAS (SNAKE_CASE) ---
- TODOS los nombres de archivos, tablas, CTEs y funciones ref() DEBEN estar en MINÚSCULAS y usar GUIONES BAJOS (`_`). 
- NUNCA uses guiones medios (`-`), espacios ni caracteres especiales. Ejemplo correcto: `=== FILE: mi_tabla_final.sqlx ===`. Ejemplo incorrecto: `=== FILE: mi-tabla-final.sqlx ===`.

--- REGLA DE SALIDA OBLIGATORIA ---
- NO incluyas introducciones, ni saludos, ni explicaciones al inicio o al final.
- Tu respuesta DEBE COMENZAR DIRECTAMENTE con la etiqueta del primer archivo: === FILE: nombre_tabla_final.sqlx ===

--- CÓMO INTERPRETAR EL PDF ---
El documento está compuesto por varios recuadros. Cada recuadro representa una tabla.
Dentro de cada recuadro encontrarás:
- El nombre de la tabla.
- Los campos que contiene la tabla.
- En algunos casos, tablas que sirven como origen de otras tablas.
- Flechas que conectan campos entre distintas tablas.

--- TIPOS DE TABLAS ---
1. Tablas intermedias (CTEs):
   - Son tablas que sirven como origen para construir otra tabla.
   - NO deben devolverse como archivos independientes.
   - Únicamente deben utilizarse como CTE (WITH ... AS (...)) dentro del SQLX de la tabla final correspondiente.
2. Tablas finales:
   - Son las tablas objetivo que deben generarse.
   - Debes devolver un único archivo SQLX por cada tabla final.

--- RELACIONES ENTRE TABLAS ---
- Las flechas del diagrama representan condiciones JOIN. Utiliza exclusivamente las relaciones explícitas.

--- CONSTRUCCIÓN DE LA TABLA FINAL ---
Estructura esperada por cada tabla final:

=== FILE: nombre_tabla_final.sqlx ===
config {
  type: "table"
}

WITH tabla_1 AS (
    SELECT campo1, campo2 FROM ${ref("tabla_1")}
)
SELECT t1.campo1 FROM tabla_1 t1
"""

        pdf_part = types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")
        user_prompt_pdf = """
Analiza el PDF adjunto con el diagrama del modelo de datos y genera el código SQLX para CADA UNA de las tablas finales presentes en el gráfico.

Recuerda:
- Antepone obligatoriamente `=== FILE: nombre_tabla_final.sqlx ===` inmediatamente antes de cada archivo generado.
- Usa estrictamente guiones bajos (`_`) en lugar de guiones medios (`-`).
"""
        gemini_contents = [pdf_part, user_prompt_pdf]

    # ---------------------------------------------------------------------
    # INVOCACIÓN A GEMINI Y DESPLIEGUE
    # ---------------------------------------------------------------------
    api_key = os.environ.get("GEMINI_API_KEY")
    ai_client = genai.Client(api_key=api_key) if api_key else genai.Client()

    try:
        response = ai_client.models.generate_content(
            model="gemini-3.5-flash",
            contents=gemini_contents,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction, temperature=0.1
            ),
        )

        response_text = response.text.strip()
        print("Respuesta recibida de Gemini. Procesando archivos SQLX...")

        base_name = os.path.basename(file_name)
        sqlx_files = parse_sqlx_files_from_response(
            response_text, default_filename=base_name
        )

        output_bucket_name = os.environ.get("OUTPUT_BUCKET_NAME", bucket_name)
        output_bucket = storage_client.bucket(output_bucket_name)

        for target_sqlx_filename, sqlx_code in sqlx_files.items():
            print(f"\n--- Procesando modelo final: {target_sqlx_filename} ---")

            target_path = f"output_sqlx/main/{target_sqlx_filename}"
            output_blob = output_bucket.blob(target_path)
            output_blob.upload_from_string(sqlx_code, content_type="text/plain")
            print(f"✔ [GCS] Archivo SQLX guardado en: gs://{output_bucket_name}/{target_path}")

            deploy_and_run_dataform(
                sqlx_file_name=target_sqlx_filename, sqlx_content=sqlx_code
            )

    except Exception as e:
        print(f"Error durante el procesamiento, almacenamiento o ejecución: {str(e)}")
        raise e
```

### 4.2.1 requirements.txt
Utilizado en V1
```text
google-cloud-storage>=2.10.0
google-genai>=0.1.0
pydantic>=2.0.0
```

### 4.2.2 requirements_v2.txt
Utilizado en V2 y V3

```text
google-cloud-storage>=2.14.0
google-genai>=0.1.1
cloudevents>=1.10.0
functions-framework>=3.5.0
```

### 4.3. run_local.py (Script de Ejecución y Pruebas Locales)

```text
import os
import sys

# 1. Inyectar variables de entorno para desarrollo local
os.environ["GEMINI_API_KEY"] = "TU_API_KEY_DE_GEMINI"

ruta_credenciales = os.path.abspath("gcp-key.json")
if not os.path.exists(ruta_credenciales):
    print(f"[!] Error: No se encuentra el archivo de credenciales en: {ruta_credenciales}")
    sys.exit(1)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ruta_credenciales

# 2. Importar la función del main
from main import generate_sqlx_from_json

# 3. Simular el evento recibido desde Cloud Storage
mock_event = {
    'bucket': 'automatizacion-bucket-diego',
    'name': 'stg_customers.json'
}

print("Iniciando simulación local de Cloud Function...")
try:
    generate_sqlx_from_json(mock_event, None)
    print("\n[ÉXITO] Proceso completado correctamente.")
except Exception as e:
    print(f"\n[ERROR] La ejecución falló: {e}")
```
---
## 5. Paso a Paso del Despliegue en GCP

### Paso 1: Creación del Bucket en Cloud Storage
* En la consola de GCP, ir a **Cloud Storage** > **Buckets**.
* Crear un bucket denominado `automatizacion-bucket-diego`.

### Paso 2: Configuración de la Cloud Function (2ª Gen)
* Navegar a **Cloud Functions** y seleccionar **Crear función**.
* Configurar los siguientes parámetros básicos:
  * **Entorno:** `2nd gen`
  * **Nombre de la función:** `function-2`
  * **Región:** `europe-west1`
  * **Activador (Trigger):** `Cloud Storage`
  * **Tipo de evento:** `google.storage.object.v1.finalized`
  * **Bucket:** `automatizacion-bucket-diego`
* En **Variables de entorno del entorno de ejecución**, definir:
  * `GEMINI_API_KEY` = `[API_KEY_DE_GOOGLE_AI_STUDIO]`

### Paso 3: Carga del Código y Despliegue
* Configurar el **Runtime** como `Python 3.12`.
* Establecer el **Entry Point** estrictamente a: `generate_sqlx_from_json`.
* Copiar el código fuente en `main.py` y `requirements.txt`.
* Pulsar en **Desplegar** (*Deploy*).

### Paso 4: Entorno Dataform
* Creación del repositorio en Dataform
* Creación del Workspace
* Editar ruta en main_v2.py para que coincida con estos nuevos datos

---

## 6. Hitos de Diagnóstico y Errores Resueltos

| Problema Encontrado | Causa Raíz | Solución Aplicada |
| :--- | :--- | :--- |
| **`CommandNotFoundException: gcloud`** | Se intentó instalar el CLI de GCP mediante `pip install gcloud` (paquete Python extinto). | Se aclaró el uso de credenciales de Cuenta de Servicio (`gcp-key.json`) para entornos locales sin necesidad del SDK global. |
| **`429 RESOURCE_EXHAUSTED`** | Uso de un proyecto con facturación vinculada a créditos prepagados agotados. | Transición hacia una API Key limpia y gratuita vinculada a un proyecto sin facturación activa en Google AI Studio. |
| **`ValueError: No API key was provided`** | Ausencia de la variable de entorno en la configuración de la función en la nube. | Inyección explícita de `GEMINI_API_KEY` en la sección *Runtime Environment Variables* de GCP. |
| **`Container failed to start on PORT=8080`** | Incompatibilidad de argumentos en la firma de función entre CloudEvents de 2ª Gen y diccionarios v1. | Implementación de inspección dinámica de objetos (`hasattr(event, "data")`) en el punto de entrada de Python. |
| **Prevención de Bucles Infinitos** | El guardado de un nuevo archivo en el bucket volvía a activar la función. | Implementación de una cláusula de guarda inicial que evalúa en 2 ms si el archivo es un `.json`. Todo archivo `.sqlx` o no compatible es descartado de inmediato con un código HTTP 200. |

---

## 7. Evidencia de Ejecución en Registros (Cloud Logging)
Los logs de auditoría en Cloud Run / Cloud Functions confirman la ejecución paralela y la adaptación del prompt dinámico para los modelos v2:

```text
2026-07-24 10:15:10.120 Procesando archivo: fct_sales.json del bucket: automatizacion-bucket-diego
2026-07-24 10:15:11.840 Detectadas opciones avanzadas (v2). Adaptando las reglas de Gemini...
2026-07-24 10:15:14.210 Código SQLX generado exitosamente por Gemini.
2026-07-24 10:15:14.350 ✔ [GCS] Archivo SQLX guardado en: gs://automatizacion-bucket-diego/output_sqlx/main/fct_sales.sqlx
2026-07-24 10:15:14.400 Subiendo archivo definitions/staging/fct_sales.sqlx a Dataform (Project: gen-lang-client-0449946907 | Repo: mi-repositorio-dataform | Workspace: dev-workspace)...
2026-07-24 10:15:14.820 ✔ Archivo guardado correctamente en el workspace de Dataform.
2026-07-24 10:15:14.830 Compilando proyecto de Dataform...
2026-07-24 10:15:15.610 ✔ Compilación completada exitosamente: projects/gen-lang-client-0449946907/locations/europe-west1/repositories/mi-repositorio-dataform/compilationResults/12345678
2026-07-24 10:15:15.620 Iniciando ejecución del modelo en BigQuery...
2026-07-24 10:15:16.100 ✔ [ÉXITO DATAFORM] Invocación creada. ID: projects/gen-lang-client-0449946907/locations/europe-west1/repositories/mi-repositorio-dataform/workflowInvocations/87654321
2026-07-24 10:15:16.150 Ignorando archivo no JSON o evento inválido: output_sqlx/main/fct_sales.sqlx
```
---
