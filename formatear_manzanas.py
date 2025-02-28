import pandas as pd
import json
import pickle
from tqdm import tqdm  # Importar tqdm para la barra de progreso

# Cargar el archivo NDJSON en un DataFrame
input_file = "manzanas_sin_formato/CALLAO_MAZANAS_LOTES_1.ndjson"
output_file = "ndjson/manzanas_callao.ndjson"
ubigeo_file = "ubigeo_dict.pkl"  # Archivo con los ubigeos

# Leer el archivo NDJSON
df = pd.read_json(input_file, lines=True)

# Cargar el diccionario de ubigeos desde el archivo .pkl
with open(ubigeo_file, "rb") as f:
    ubigeo_dict = pickle.load(f)  # Carga el diccionario desde el archivo .pkl

# Filtrar registros donde "manzana" está vacío o solo tiene espacios
df = df[df["manzana"].str.strip() != ""]
df = df[df["lote"].str.strip() != ""]

# Función para obtener los códigos de departamento, provincia y distrito desde el postcode
def get_ubigeo(postcode):
    ubigeo = ubigeo_dict.get(postcode, {})
    return (
        ubigeo.get("cod_departament", None),
        ubigeo.get("cod_province", None),
        ubigeo.get("cod_district", None),
    )

# Aplicar la función a cada fila y descomprimir los valores en tres columnas
df[["cod_departament", "cod_province", "cod_district"]] = df["postcode"].apply(get_ubigeo).apply(pd.Series)

# Agrupar por urbanización y manzana
grouped = df.groupby(["nombre_urbanizacion", "manzana", "id_urb", "postcode", "cod_departament", "cod_province", "cod_district"])

# Transformar los datos en el formato deseado
output_data = []

# Usar tqdm para mostrar el progreso del procesamiento
for (urbanizacion, manzana, id_urb, postcode, cod_departament, cod_province, cod_district), group in tqdm(grouped, desc="Procesando manzanas"):
    # Crear estructura de salida
    obj = {
        "id_via": int(id_urb) if str(id_urb).isdigit() else None,
        "name": urbanizacion,
        "postcode": postcode,
        "cod_departament": cod_departament,
        "cod_province": cod_province,
        "cod_district": cod_district,
        "context": manzana,
        "type": "street",
        "housenumbers": []
    }

    # Agregar los lotes a housenumbers
    obj["housenumbers"] = [
        {"number": row["lote"], "location": {"lon": row["lon"], "lat": row["lat"]}}
        for _, row in group.iterrows()
    ]

    output_data.append(obj)

# Guardar la salida en NDJSON con barra de progreso
with open(output_file, "w", encoding="utf-8") as file:
    for data in tqdm(output_data, desc="Escribiendo archivo"):
        file.write(json.dumps(data, ensure_ascii=False, default=str) + "\n")

print(f"Archivo formateado guardado en {output_file}")
