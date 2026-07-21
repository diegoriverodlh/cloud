import os
import json
from google import genai
from google.genai import types

def ejecutar_prueba_local():
    # 1. En lugar de leer de GCS, leemos el archivo stg_customers.json de tu carpeta local
    ruta_json = "stg_customers.json"
    
    if not os.path.exists(ruta_json):
        print(f"Error: No se encuentra el archivo {ruta_json} en esta carpeta.")
        return

    print(f"Procesando archivo local: {ruta_json}")
    
    with open(ruta_json, "r", encoding="utf-8") as f:
        json_data = json.load(f)

    # 2. Configurar el prompt
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

    # 3. Inicializar el cliente de Gemini
    # Buscaremos la API Key en las variables de entorno para no dejar credenciales hardcodeadas
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("\n[!] ADVERTENCIA: No se detectó la variable de entorno GEMINI_API_KEY.")
        print("Para que funcione, necesitas definirla en tu terminal. Ver el Paso 2 abajo.\n")
        return

    ai_client = genai.Client(api_key=api_key)
    
    try:
        print("Llamando a la API de Gemini (gemini-3.5-flash)...")
        response = ai_client.models.generate_content(
            model='gemini-3.5-flash',
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=system_instruction,
                temperature=0.1
            )
        )
        
        sqlx_code = response.text.strip()
        print("\n¡Código SQLX generado exitosamente por Gemini!:\n")
        print("--------------------------------------------------")
        print(sqlx_code)
        print("--------------------------------------------------")
        
        # Guardar localmente
        output_file = "stg_customers_test.sqlx"
        with open(output_file, "w", encoding="utf-8") as out:
            out.write(sqlx_code)
        print(f"\nArchivo guardado localmente como: {output_file}")
        
    except Exception as e:
        print(f"Error durante la generación: {str(e)}")

if __name__ == "__main__":
    ejecutar_prueba_local()