import pandas as pd
import json
import unicodedata

def remove_accents(text):
    """Elimina los acentos de un texto."""
    return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))

def excel_to_json(excel_file, output_json):
    # Leer el archivo Excel
    df = pd.read_excel(excel_file, dtype=str, engine="xlrd")
    
    # Asegurar que no haya valores nulos y eliminar espacios extra
    df = df.dropna().applymap(lambda x: x.strip())

    # Eliminar acentos en las columnas
    df["Departamento"] = df["Departamento"].apply(remove_accents)
    df["Provincia"] = df["Provincia"].apply(remove_accents)
    
    # Agrupar por departamento y asegurarse de que las provincias sean únicas
    grouped = df.groupby("Departamento")["Provincia"].agg(lambda x: sorted(set(x))).reset_index()
    
    # Formatear la salida en una lista de diccionarios
    data = grouped.rename(columns={"Departamento": "departamento", "Provincia": "provincias"}).to_dict(orient="records")
    
    # Guardar en un archivo JSON
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"Archivo JSON guardado en {output_json}")

# Uso del script
excel_to_json("Ubigeo-Descripción.xls", "departamentos.json")
