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
