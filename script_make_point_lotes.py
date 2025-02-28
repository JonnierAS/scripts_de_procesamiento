import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString, Polygon, MultiPolygon
from shapely.ops import nearest_points
from tqdm import tqdm  # Para barra de progreso

# Cargar los archivos .parquet
lotes = gpd.read_parquet("c:/Users/jhonn/Documents/LOTES_CERCA_DE_VIAS_ULTIMO_2024.parquet")
vias = gpd.read_parquet("c:/Users/jhonn/Documents/VIAS_LOTES_ULTIMA_2024.parquet")

# Convertir geometrías a CRS métrico si es necesario
lotes = lotes.to_crs(epsg=32718)  # Cambiar según la zona UTM correspondiente
vias = vias.to_crs(epsg=32718)

puntos_generados = []

# Distancia de desplazamiento hacia el interior del lote
DESPLAZAMIENTO = 2  # en metros

def obtener_lado_mas_cercano(poly, vias_geom, max_dist=15): #max_dist=15
    lado_mas_cercano = None
    menor_distancia = float("inf")
    
    for i in range(len(poly.exterior.coords) - 1):
        segmento = LineString([poly.exterior.coords[i], poly.exterior.coords[i + 1]])
        for via_geom in vias_geom:
            distancia = segmento.distance(via_geom)
            if distancia < menor_distancia and distancia <= max_dist:
                menor_distancia = distancia
                lado_mas_cercano = segmento
    
    return lado_mas_cercano

# Iterar sobre cada lote con barra de progreso
print("Procesando lotes...")
for lote in tqdm(lotes.itertuples(), total=len(lotes)):
    lote_geom = lote.geometry
    
    if isinstance(lote_geom, MultiPolygon):
        lotes_polygons = list(lote_geom.geoms)  # Extraer los polígonos
    else:
        lotes_polygons = [lote_geom]  # Convertir a lista para tratarlo igual
    
    for poly in lotes_polygons:
        vias_cercanas = [via.geometry for via in vias.itertuples()]
        best_segment = obtener_lado_mas_cercano(poly, vias_cercanas)
        
        if best_segment:
            midpoint = best_segment.interpolate(0.5, normalized=True)
            dx, dy = best_segment.xy[0][1] - best_segment.xy[0][0], best_segment.xy[1][1] - best_segment.xy[1][0]
            norm = (dx**2 + dy**2)**0.5
            unit_vector = (-dy/norm, dx/norm)  # Perpendicular
            
            # Asegurar que el punto se aleje del borde hacia el interior del lote
            test_point = Point(midpoint.x + unit_vector[0] * DESPLAZAMIENTO, 
                               midpoint.y + unit_vector[1] * DESPLAZAMIENTO)
            
            if poly.contains(test_point):
                puntos_generados.append(test_point)
            else:
                # Si el punto cae fuera, invertir la dirección del desplazamiento
                new_point = Point(midpoint.x - unit_vector[0] * DESPLAZAMIENTO, 
                                  midpoint.y - unit_vector[1] * DESPLAZAMIENTO)
                if poly.contains(new_point):
                    puntos_generados.append(new_point)

# Crear un GeoDataFrame con los puntos generados
puntos_gdf = gpd.GeoDataFrame(geometry=puntos_generados, crs=lotes.crs)

# Guardar en un archivo .parquet
puntos_gdf.to_parquet("puntos_lotes.parquet")

print("Procesamiento completado. Archivo guardado: puntos_lotes.parquet")