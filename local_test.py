from main import generate_sqlx_from_json

# Simulación de un evento de GCS
event = {
    'bucket': 'tu-nombre-de-bucket',
    'name': 'stg_customers.json'
}

# Ejecución local
if __name__ == "__main__":
    # Nota: Para esto, asegúrate de estar autenticado con 'gcloud auth application-default login'
    # y de que el archivo realmente exista en el bucket indicado.
    generate_sqlx_from_json(event, None)