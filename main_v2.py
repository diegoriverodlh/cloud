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