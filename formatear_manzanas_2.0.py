import pandas as pd
import json
import pickle
from shapely import wkb
import geopandas as gpd
from tqdm import tqdm  # Importar tqdm para la barra de progreso
import warnings

# Archivos de entrada y salida
input_file = "manzanas_sin_formato/manzanas_feb_27_4326.parquet"
output_file = "ndjson/manzanas_feb_27.ndjson"
ubigeo_file = "ubigeo_dict.pkl"  # Archivo con los ubigeos

#Columnas a usar
name_manzana="name_manzana"
name_lote="name_lote"
name_urb="name_urb"
postcode="postcode"

# Detectar formato del archivo de entrada
if input_file.endswith(".ndjson"):
    df = pd.read_json(input_file, lines=True)
elif input_file.endswith(".parquet"):
    df = pd.read_parquet(input_file)
elif input_file.endswith(".gpkg"):
    df = gpd.read_file(input_file)
else:
    raise ValueError("Formato de archivo no soportado. Debe ser .ndjson, .parquet o .gpkg")

# Cargar el diccionario de ubigeos desde el archivo .pkl
with open(ubigeo_file, "rb") as f:
    ubigeo_dict = pickle.load(f)

# Filtrar registros donde "manzana" y name_lote no sean vacíos
if name_manzana in df.columns and name_lote in df.columns:
    df = df[df[name_manzana].astype(str).str.strip() != ""]
    df = df[df[name_lote].astype(str).str.strip() != ""]
else:
    raise ValueError("Las columnas 'manzana' y 'lote' son obligatorias en el archivo de entrada")

# Extraer lon y lat de la geometría si es un archivo geoespacial
geometry_col = None
for col in ["geometry", "geom"]:
    if col in df.columns:
        geometry_col = col
        break
    
if input_file.endswith(".parquet"):
    df[geometry_col] = df[geometry_col].apply(lambda x: wkb.loads(x) if isinstance(x, bytes) else x)

# Si se encontró una columna de geometría, calcular centroides
if geometry_col:
    df = gpd.GeoDataFrame(df, geometry=df[geometry_col], crs="EPSG:4326")  # Asegurar que la geometría se reconozca
    warnings.simplefilter("ignore", category=UserWarning)
    df["lon"] = df.geometry.centroid.x
    df["lat"] = df.geometry.centroid.y
    df.drop(columns=[geometry_col], inplace=True)  
else:
    raise ValueError("No se encontró una columna de geometría ('geometry' o 'geom').")


# Función para obtener códigos de departamento, provincia y distrito desde el postcode
def get_ubigeo(postcode):
    ubigeo = ubigeo_dict.get(int(postcode), {})
    return (
        ubigeo.get("cod_departament", None),
        ubigeo.get("cod_province", None),
        ubigeo.get("cod_district", None),
    )

# Aplicar la función a cada fila y crear nuevas columnas
df[["cod_departament", "cod_province", "cod_district"]] = df[postcode].astype(str).apply(get_ubigeo).apply(pd.Series)

# Agrupar por urbanización y manzana
grouped = df.groupby([name_urb, name_manzana, postcode, "cod_departament", "cod_province", "cod_district"])

# Transformar los datos en el formato deseado
output_data = []
for (urbanizacion, manzana, postcode, cod_departament, cod_province, cod_district), group in tqdm(grouped, desc="Procesando manzanas"):
    obj = {
        "id_via":None,
        "name": urbanizacion,
        "postcode": postcode,
        "cod_departament": cod_departament,
        "cod_province": cod_province,
        "cod_district": cod_district,
        "context": manzana,
        "type": "street",
        "housenumbers": []
    }
    obj["housenumbers"] = [
        {"number": row[name_lote], "location": {"lon": row["lon"], "lat": row["lat"]}}
        for _, row in group.iterrows()
    ]
    output_data.append(obj)

# Guardar la salida en NDJSON
with open(output_file, "w", encoding="utf-8") as file:
    for data in tqdm(output_data, desc="Escribiendo archivo"):
        file.write(json.dumps(data, ensure_ascii=False, default=str) + "\n")

print(f"Archivo formateado guardado en {output_file}")
