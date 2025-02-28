import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

def xlsx_to_geoparquet(input_file: str, output_file: str, x_col: str, y_col: str, crs: str = "EPSG:4326"):
    """
    Convierte un archivo XLSX con columnas de coordenadas en un archivo GeoParquet.

    Args:
        input_file (str): Ruta del archivo XLSX de entrada.
        output_file (str): Ruta del archivo GeoParquet de salida.
        x_col (str): Nombre de la columna con las coordenadas X (longitud).
        y_col (str): Nombre de la columna con las coordenadas Y (latitud).
        crs (str): Sistema de referencia de coordenadas (por defecto "EPSG:4326").
    """
    # Leer el archivo XLSX
    df = pd.read_excel(input_file)

    # Convertir todos los nombres de las columnas a cadenas de texto
    df.columns = df.columns.map(str)
    
    # Verificar que las columnas de coordenadas existan
    if x_col not in df.columns or y_col not in df.columns:
        raise ValueError(f"El archivo XLSX debe contener las columnas '{x_col}' y '{y_col}'.")

    # Limpiar las columnas de coordenadas
    df[x_col] = pd.to_numeric(df[x_col].astype(str).str.replace('\xa0', '').str.strip(), errors='coerce')
    df[y_col] = pd.to_numeric(df[y_col].astype(str).str.replace('\xa0', '').str.strip(), errors='coerce')

    # Filtrar filas con valores nulos o faltantes en las columnas de coordenadas
    df = df.dropna(subset=[x_col, y_col])

    # Crear geometrías de puntos solo para filas con datos válidos
    geometry = [Point(xy) for xy in zip(df[x_col], df[y_col])]
    
    # Convertir a GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=crs)
    
    # Guardar en formato GeoParquet
    gdf.to_parquet(output_file, engine='pyarrow')
    print(f"Archivo GeoParquet creado exitosamente en: {output_file}")

# Ejemplo de uso
input_file = "municipalidad_casas.xlsx"  # Ruta al archivo XLSX de entrada
output_file = "municipalidad_casas.parquet"  # Ruta al archivo GeoParquet de salida
xlsx_to_geoparquet(input_file, output_file, x_col="LONGITUD", y_col="LATITUD")
