from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os

load_dotenv()
# Configuración de Elasticsearch

es = Elasticsearch(
    os.getenv('ElASTIC_API'),
    basic_auth=(os.getenv('ELASTIC_USER'), os.getenv('ELASTIC_PASSWORD'))  # Sustituye "usuario" y "contraseña" por las credenciales correctas
)
doc_id = "rw8SJZUBKZn991UEoPk1"  # ID del documento
index_name = "calles_numero_de_puerta"

es.update(index=index_name, id=doc_id, body={"doc": {"name": "EDUARDO GIRALDO"}})
print("registro actualizado")