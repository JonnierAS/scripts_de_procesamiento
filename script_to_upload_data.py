from elasticsearch import Elasticsearch, helpers
import json
from tqdm import tqdm  # Barra de progreso
from dotenv import load_dotenv
import os

load_dotenv()
# Configuración de Elasticsearch
es = Elasticsearch(
    os.getenv('ElASTIC_API'),
    basic_auth=(os.getenv('ELASTIC_USER'), os.getenv('ELASTIC_PASSWORD'))  # Sustituye "usuario" y "contraseña" por las credenciales correctas
)

index_name = "calles_numero_de_puerta"  # Nombre del índice donde se cargarán los datos
file_path = "ndjson/manzanas_trujillo.ndjson"  # Ruta al archivo NDJSON a cargar
mapping_path = "mapping/calles.json"  # Ruta al archivo JSON del mapeo

def read_mapping(file_path):
    """Lee el archivo de mapeo JSON y lo convierte a un diccionario."""
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)

def create_index(es, index_name, mapping):
    """Crea el índice con el mapeo proporcionado."""
    # Verificar si el índice ya existe
    if not es.indices.exists(index=index_name):
        # print(f"El índice {index_name} ya existe. Eliminándolo para recrearlo...")
        # es.indices.delete(index=index_name)
        # Crear el índice con el mapeo
        es.indices.create(index=index_name, body=mapping)
        print(f"Índice {index_name} creado correctamente.")
    

def bulk_load_to_elasticsearch(es, index_name, file_path, chunk_size=1000):
    """Carga datos en Elasticsearch usando la operación bulk con barra de progreso."""
    # Contar el total de líneas en el archivo NDJSON
    with open(file_path, "r", encoding="utf-8") as file:
        total_lines = sum(1 for _ in file)

    # Procesar datos con barra de progreso
    with open(file_path, "r", encoding="utf-8") as file:
        actions = []
        for line in tqdm(file, total=total_lines, desc="Cargando datos en Elasticsearch"):
            try:
                data = json.loads(line)
                actions.append({"_index": index_name, "_source": data})
                # Enviar en lotes para evitar problemas de memoria
                if len(actions) == chunk_size:
                    helpers.bulk(es, actions)
                    actions = []
            except json.JSONDecodeError as e:
                print(f"Línea inválida encontrada: {line}. Error: {e}")
        # Enviar los últimos datos si hay menos de chunk_size
        if actions:
            try:
                success, failed = helpers.bulk(es, actions, raise_on_error=False, stats_only=False)
                print(f"Documentos indexados correctamente: {success}")
                if failed:
                    print(f"⚠️ {len(failed)} documentos fallaron.")
                    for error in failed:
                        print(error)
            except Exception as e:
                print(f"Error durante la carga en Elasticsearch: {e}")
    print("Datos cargados correctamente.")

# Leer el mapeo del archivo JSON
mapping = read_mapping(mapping_path)

# Crear el índice antes de cargar datos
create_index(es, index_name, mapping)

# Ejecutar la función de carga
bulk_load_to_elasticsearch(es, index_name, file_path)
