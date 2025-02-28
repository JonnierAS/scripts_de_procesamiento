# import geopandas as gpd
# import h3
# import pandas as pd
# from shapely.geometry import shape
# import json

# # 1. Cargar el Shapefile desde el archivo .zip
# # Asegúrate de que el archivo .zip esté en la misma carpeta o proporciona la ruta correcta.
# shapefile_path = "DISTRITOS_inei_geogpsperu_suyopomalia.zip"
# gdf = gpd.read_file(f"zip://{shapefile_path}")

# # 2. Función para convertir geometrías a índices H3
# def geometry_to_h3(geometry, resolution=7):
#     """
#     Convierte una geometría (Point, Polygon, MultiPolygon, etc.) a un índice H3.
#     :param geometry: Geometría de Shapely.
#     :param resolution: Resolución de H3 (por defecto 7).
#     :return: Índice H3.
#     """
#     if geometry.geom_type == "Point":
#         # Si es un punto, convertir directamente
#         return h3.latlng_to_cell(geometry.y, geometry.x, resolution)
#     elif geometry.geom_type == "Polygon":
#         # Si es un polígono, obtener el centroide y luego convertir
#         centroid = geometry.centroid
#         return h3.latlng_to_cell(centroid.y, centroid.x, resolution)
#     elif geometry.geom_type == "MultiPolygon":
#         # Si es un MultiPolygon, procesar cada polígono individualmente
#         h3_indexes = []
#         for poly in geometry.geoms:  # Usar .geoms para iterar sobre los polígonos
#             centroid = poly.centroid
#             h3_index = h3.latlng_to_cell(centroid.y, centroid.x, resolution)
#             h3_indexes.append(h3_index)
#         return h3_indexes
#     else:
#         # Para otros tipos de geometría, manejar según sea necesario
#         raise ValueError(f"Tipo de geometría no soportado: {geometry.geom_type}")

# # 3. Aplicar la función a todas las geometrías del GeoDataFrame
# gdf["h3_index"] = gdf.geometry.apply(geometry_to_h3)

# # Convertir la lista de índices H3 en una cadena (si es una lista)
# gdf["h3_index"] = gdf["h3_index"].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)

# # 4. Convertir el GeoDataFrame a un DataFrame de Pandas (para guardar en Parquet)
# df = pd.DataFrame(gdf.drop(columns="geometry"))

# # 5. Guardar el DataFrame en un archivo .parquet
# output_path = "distritos_indexado.parquet"
# df.to_parquet(output_path, engine="pyarrow")

# print(f"Archivo guardado en: {output_path}")

import geopandas as gpd
import h3
import pandas as pd
from shapely.geometry import Polygon, MultiPolygon

# Cargar el archivo Shapefile desde el ZIP
file_path = "zip://c:/Users/jhonn/Documents/Capa_Demografica.zip"
output_path = "Capa_Demografica_indexado_con_hexagono.parquet"
gdf = gpd.read_file(file_path)
resolution=8
# Función para convertir geometría a H3 y devolver el hexágono
def geometry_to_h3(geometry):
    """
    Convierte una geometría en un índice H3 y su geometría hexagonal.
    """
    if geometry.geom_type == "Point":
        h3_index = h3.latlng_to_cell(geometry.y, geometry.x, resolution)
        hex_coords = [[lng, lat] for lat, lng in h3.cell_to_boundary(h3_index)]
        hex_coords.append(hex_coords[0])  # Cerrar polígono
        hex_geom = Polygon(hex_coords)
        return h3_index, hex_geom
    
    elif geometry.geom_type == "Polygon":
        centroid = geometry.centroid
        h3_index = h3.latlng_to_cell(centroid.y, centroid.x, resolution)
        hex_coords = [[lng, lat] for lat, lng in h3.cell_to_boundary(h3_index)]
        hex_coords.append(hex_coords[0])  # Cerrar polígono
        hex_geom = Polygon(hex_coords)
        return h3_index, hex_geom

    elif geometry.geom_type == "MultiPolygon":
        h3_indexes = []
        hex_geoms = []
        for poly in geometry.geoms:
            centroid = poly.centroid
            h3_index = h3.latlng_to_cell(centroid.y, centroid.x, resolution)
            hex_coords = [[lng, lat] for lat, lng in h3.cell_to_boundary(h3_index)]
            hex_coords.append(hex_coords[0])  # Cerrar polígono
            hex_geoms.append(Polygon(hex_coords))
            h3_indexes.append(h3_index)
        
        return h3_indexes, MultiPolygon(hex_geoms)

    else:
        raise ValueError(f"Tipo de geometría no soportado: {geometry.geom_type}")

# Aplicamos la conversión
print(f'Indexando: {gdf.geometry.geom_type[0]}...')
h3_results = gdf.geometry.apply(geometry_to_h3)

# Extraemos el índice H3 y la geometría generada
gdf['h3_index'] = h3_results.apply(lambda x: str(x[0]) if isinstance(x[0], list) else x[0])
gdf['geometry'] = h3_results.apply(lambda x: x[1])  # Guardamos la geometría del hexágono

# Convertimos a GeoDataFrame con las geometrías hexagonales
print('Estableciendo CRS a EPSG:4326')
df = gpd.GeoDataFrame(gdf, crs="EPSG:4326")

# Establecer CRS si no está definido
if df.crs is None:
    df.crs = 'EPSG:4326'

# Guardar en Parquet
df.to_parquet(output_path, engine="pyarrow")

print(f"Archivo guardado en: {output_path}")

