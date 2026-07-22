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

### 4.1. main.py (Código Servidor en la Nube)

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

### 4.2. requirements.txt

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
2026-07-22 10:47:12.573 Procesando archivo: fct_sales_v2.json del bucket: automatizacion-bucket-diego
2026-07-22 10:47:14.333 Detectadas opciones avanzadas (v2). Adaptando las reglas de Gemini...
2026-07-22 10:47:36.329 Código SQLX generado exitosamente por Gemini.
2026-07-22 10:47:36.444 Archivo SQLX guardado exitosamente en: gs://automatizacion-bucket-diego/output_sqlx/main/fct_sales_v2.sqlx
2026-07-22 10:47:36.562 Ignorando archivo no JSON o evento inválido: output_sqlx/main/fct
```

---