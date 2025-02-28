import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
# Cargar archivo NDJSON
ndjson_file = "ndjson/calles.ndjson"  # Reemplaza con tu archivo
output_name_file ="calles"
# Leer el NDJSON línea por línea
data = []
with open(ndjson_file, "r", encoding="utf-8") as f:
    for line in f:
        data.append(json.loads(line))

# Extraer puntos de housenumbers
rows = []
for entry in tqdm(data, desc="Extrayendo puntos"):
    for house in entry.get("housenumbers", []):
        rows.append({
            "id_via": entry.get("id_via"),
            "name": entry.get("name"),
            "postcode": entry.get("postcode"),
            "cod_departament": entry.get("cod_departament"),
            "cod_province": entry.get("cod_province"),
            "cod_district": entry.get("cod_district"),
            "context": entry.get("context"),
            "type": entry.get("type"),
            "number": house["number"],
            "geometry": Point(house["location"]["lon"], house["location"]["lat"])
        })

# Convertir a GeoDataFrame
gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")

# Guardar como Parquet
# gdf.to_parquet("output.parquet", index=False)

# Guardar como GPKG
gdf.to_file(f"manzanas_con_geometria/{output_name_file}.gpkg", driver="GPKG")

print(f"Conversión completada. Archivos generados: {output_name_file}.gpkg")
