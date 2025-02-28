import pandas as pd
import os
import pickle

# Ruta del archivo en Windows
csv_file = r"C:\Users\jhonn\Desktop\proyectos\script_para_procesar_data_elastic\UBIGEOS_2022_1891_distritos.csv"
pkl_file = "ubigeo_dict.pkl"
# Verificar si el archivo existe
if os.path.exists(csv_file):
    print("Archivo encontrado, procesando...")
    
    # Leer el archivo CSV
try:
    df = pd.read_csv(csv_file, delimiter=';')
    print("Archivo encontrado, procesando...")

    # Renombrar columnas para estandarizar con los nombres esperados
    df.rename(columns={
        "ubigeo": "postcode",
        "NOMBDEP": "cod_departament",
        "NOMBPROV": "cod_province",
        "NOMBDIST": "cod_district",
        "REGION NATURAL": "context"
    }, inplace=True)

    # Convertir DataFrame a diccionario con el formato adecuado
    ubigeo_dict = df[["postcode", "cod_departament", "cod_province", "cod_district", "context"]].set_index("postcode").T.to_dict()

    # Guardar el diccionario en formato PKL
    pd.to_pickle(ubigeo_dict, pkl_file)
    print(f"Archivo {pkl_file} generado correctamente.")

except Exception as e:
    print(f"Error al procesar el archivo: {e}")
else:
    print("Archivo no encontrado. Verifica la ruta.")
