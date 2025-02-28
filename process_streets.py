import duckdb
import json
import pandas as pd
import numpy as np

file_path = "data/puertas_para_elasti.parquet"
postcode_file = "ubigeo_dict.pkl"
output_file = "ndjson/calles.ndjson"
limit = 100000000

def info(file_path, limit):
    with duckdb.connect(database=':memory:') as conn:
        # Ejecutar la consulta para contar los IDs únicos en las primeras filas limitadas
        query = f"""
            SELECT 
                COUNT(DISTINCT id_via) AS unique_id_count
            FROM (
                SELECT id_via
                FROM '{file_path}'
                LIMIT {limit}
            )
        """
        # Ejecutar la consulta y obtener el resultado
        result = conn.execute(query).fetchone()[0]
        print(f"Cantidad de IDs únicos en las primeras {limit} filas: {result}")

def find_duplicates(file_path, limit):
    with duckdb.connect(database=':memory:') as conn:
        # Ejecutar la consulta para encontrar duplicados en numpuerta para el mismo id_via
        query = f"""
            WITH grouped_data AS (
                SELECT 
                    id_via,
                    numpuerta
                FROM '{file_path}'
                LIMIT {limit}
            )
            SELECT id_via, numpuerta, COUNT(*) as count
            FROM grouped_data
            GROUP BY id_via, numpuerta
            HAVING COUNT(*) > 1
        """
        # Ejecutar la consulta y obtener el resultado
        duplicates_df = conn.execute(query).fetchdf()
        if not duplicates_df.empty:
            print("Valores duplicados encontrados:")
            print(duplicates_df)
        else:
            print("No se encontraron valores duplicados.")

def find_duplicate_details(file_path, limit):
    with duckdb.connect(database=':memory:') as conn:
        # Ejecutar la consulta para obtener el detalle de las filas duplicadas, solucionando el problema de subconsulta
        query = f"""
            WITH grouped_data AS (
                SELECT 
                    id_via,
                    numpuerta,
                    tipo_via,
                    nom_via,
                    ubigeo,
                    lon_x,
                    lat_y
                FROM '{file_path}'
                LIMIT {limit}
            ),
            duplicates AS (
                SELECT id_via, numpuerta
                FROM grouped_data
                GROUP BY id_via, numpuerta
                HAVING COUNT(*) > 1
            )
            SELECT g.*
            FROM grouped_data g
            INNER JOIN duplicates d
            ON g.id_via = d.id_via AND g.numpuerta = d.numpuerta
            ORDER BY g.id_via, g.numpuerta
        """
        # Ejecutar la consulta y obtener el resultado
        duplicate_details_df = conn.execute(query).fetchdf()
        if not duplicate_details_df.empty:
            print("Detalles de las filas duplicadas:")
            print(duplicate_details_df)
        else:
            print("No se encontraron filas duplicadas.")

def split_duplicates_into_files(file_path, limit, output_prefix):
    with duckdb.connect(database=':memory:') as conn:
        # Ejecutar la consulta para obtener los datos duplicados
        query = f"""
            WITH grouped_data AS (
                SELECT 
                    id_via,
                    numpuerta,
                    tipo_via,
                    nom_via,
                    ubigeo,
                    lon_x,
                    lat_y
                FROM '{file_path}'
                LIMIT {limit}
            ),
            duplicates AS (
                SELECT id_via, numpuerta
                FROM grouped_data
                GROUP BY id_via, numpuerta
                HAVING COUNT(*) > 1
            )
            SELECT g.*
            FROM grouped_data g
            INNER JOIN duplicates d
            ON g.id_via = d.id_via AND g.numpuerta = d.numpuerta
            ORDER BY g.id_via, g.numpuerta
        """
        
        # Obtener los detalles de los duplicados en un DataFrame
        duplicate_details_df = conn.execute(query).fetchdf()

    if not duplicate_details_df.empty:
        # Obtener los duplicados agrupados por id_via y numpuerta
        grouped_duplicates = duplicate_details_df.groupby(['id_via', 'numpuerta'])

        # Determinar el número máximo de duplicados para dividir en esa cantidad de grupos
        max_duplicates = grouped_duplicates.size().max()

        # Crear una lista de DataFrames para almacenar los grupos divididos
        groups = [pd.DataFrame() for _ in range(max_duplicates)]

        # Iterar sobre los grupos y dividir los datos en la cantidad de grupos determinada
        for _, group in grouped_duplicates:
            for i, row in enumerate(group.itertuples(index=False)):
                groups[i % max_duplicates] = pd.concat([groups[i % max_duplicates], pd.DataFrame([row._asdict()])], ignore_index=True)

        # Guardar los datos en archivos CSV separados
        for i, group_df in enumerate(groups):
            if not group_df.empty:
                output_file = f"{output_prefix}_group_{i + 1}.csv"
                group_df.to_csv(output_file, index=False)

        print(f"Datos duplicados divididos en {len(groups)} archivos.")
    else:
        print("No se encontraron filas duplicadas.")

def get_unique_rows(file_path, limit):
    with duckdb.connect(database=':memory:') as conn:
        # Ejecutar la consulta para obtener las filas sin duplicados adicionales
        unique_rows_query = f"""
            WITH grouped_data AS (
                SELECT 
                    id_via,
                    numpuerta,
                    tipo_via,
                    nom_via,
                    ubigeo,
                    lon_x,
                    lat_y
                FROM '{file_path}'
                LIMIT {limit}
            ),
            duplicates AS (
                SELECT id_via, numpuerta
                FROM grouped_data
                GROUP BY id_via, numpuerta
                HAVING COUNT(*) > 1
            )
            SELECT DISTINCT ON (id_via, numpuerta)
                id_via,
                tipo_via || ' ' || nom_via AS name,
                ubigeo AS postcode,
                numpuerta,
                lon_x AS lon,
                lat_y AS lat
            FROM grouped_data
        """
        unique_rows_df = conn.execute(unique_rows_query).fetchdf()
        print(unique_rows_df)

def get_unique_rows_with_mapping(file_path, limit):
    with duckdb.connect(database=':memory:') as conn:
        # Crear una tabla temporal para simplificar la consulta
        conn.execute(f"""
            CREATE TEMPORARY TABLE temp_unique_rows AS
            WITH grouped_data AS (
                SELECT 
                    id_via,
                    numpuerta,
                    tipo_via || ' ' || nom_via AS name,
                    ubigeo AS postcode,
                    lon_x AS lon,
                    lat_y AS lat
                FROM '{file_path}'
                LIMIT {limit}
            ),
            duplicates AS (
                SELECT id_via, numpuerta
                FROM grouped_data
                GROUP BY id_via, numpuerta
                HAVING COUNT(*) > 1
            )
            SELECT DISTINCT ON (id_via, numpuerta)
                id_via,
                name,
                postcode,
                numpuerta,
                lon,
                lat
            FROM grouped_data
        """)

        # Ejecutar la consulta para obtener las filas con el SELECT solicitado
        unique_rows_query = f"""
            SELECT 
                id_via,
                name,
                postcode,
                MAP_FROM_ENTRIES(ARRAY_AGG(STRUCT_PACK(
                    key := CAST(numpuerta AS VARCHAR),
                    value := STRUCT_PACK(lon := lon, lat := lat)
                ))) AS housenumbers
            FROM temp_unique_rows
            GROUP BY id_via, name, postcode
        """

        unique_rows_query = f"""
            SELECT 
                *
            FROM temp_unique_rows
        """

        unique_rows_with_mapping_df = conn.execute(unique_rows_query).fetchdf()
        print(unique_rows_with_mapping_df)

def find_duplicate_details_of_unique(file_path, limit):
    with duckdb.connect(database=':memory:') as conn:
        
        # Crear una tabla temporal para simplificar la consulta
        conn.execute(f"""
            CREATE TEMPORARY TABLE temp_unique_rows AS
            WITH grouped_data AS (
                SELECT 
                    id_via,
                    numpuerta,
                    tipo_via || ' ' || nom_via AS name,
                    ubigeo AS postcode,
                    lon_x AS lon,
                    lat_y AS lat
                FROM '{file_path}'
                LIMIT {limit}
            ),
            duplicates AS (
                SELECT id_via, numpuerta
                FROM grouped_data
                GROUP BY id_via, numpuerta
                HAVING COUNT(*) > 1
            )
            SELECT DISTINCT ON (id_via, numpuerta)
                id_via,
                name,
                postcode,
                numpuerta,
                lon,
                lat
            FROM grouped_data
        """)

        # Ejecutar la consulta para obtener el detalle de las filas duplicadas, solucionando el problema de subconsulta
        query = f"""
            WITH grouped_data AS (
                SELECT 
                    id_via,
                    name,
                    postcode,
                    numpuerta,
                    lon,
                    lat
                FROM temp_unique_rows
            ),
            duplicates AS (
                SELECT id_via, numpuerta
                FROM grouped_data
                GROUP BY id_via, numpuerta
                HAVING COUNT(*) > 1
            )
            SELECT g.*
            FROM grouped_data g
            INNER JOIN duplicates d
            ON g.id_via = d.id_via AND g.numpuerta = d.numpuerta
            ORDER BY g.id_via, g.numpuerta
        """
        # Ejecutar la consulta y obtener el resultado
        duplicate_details_df = conn.execute(query).fetchdf()
        if not duplicate_details_df.empty:
            print("Detalles de las filas duplicadas:")
            print(duplicate_details_df)
        else:
            print("No se encontraron filas duplicadas.")

def use_duckdb_unique(file_path, postcode_file, output_file, limit):
    # Cargar el archivo .pkl con información adicional de postcode
    postcode_data = pd.read_pickle(postcode_file)

    # Convertir el diccionario de postcode a un DataFrame de DuckDB
    postcode_df = pd.DataFrame(postcode_data).T.reset_index()
    postcode_df.columns = ['postcode', 'cod_departament', 'cod_province', 'cod_district', 'context']

    with duckdb.connect(database=':memory:') as conn:
        # Registrar el DataFrame de postcode en DuckDB
        conn.register('postcode_data', postcode_df)

        # Crear una tabla temporal para simplificar la consulta
        conn.execute(f"""
            CREATE TEMPORARY TABLE temp_unique_rows AS
            WITH grouped_data AS (
                SELECT 
                    id_via,
                    numpuerta,
                    tipo_via || ' ' || nom_via AS name,
                    ubigeo AS postcode,
                    lon_x AS lon,
                    lat_y AS lat
                FROM '{file_path}'
            ),
            duplicates AS (
                SELECT id_via, numpuerta
                FROM grouped_data
                GROUP BY id_via, numpuerta
                HAVING COUNT(*) > 1
            )
            SELECT DISTINCT ON (id_via, numpuerta)
                id_via,
                name,
                postcode,
                numpuerta,
                lon,
                lat
            FROM grouped_data
        """)
        
        # Ejecutar una única consulta que haga el join y el agrupamiento necesario
        query = f"""
            WITH grouped_data AS (
                SELECT 
                    id_via,
                    name,
                    postcode,
                    numpuerta,
                    lon,
                    lat
                FROM temp_unique_rows
            )
            SELECT
                g.id_via,
                g.name,
                g.postcode,
                p.cod_departament,
                p.cod_province,
                p.cod_district,
                p.context,
                ARRAY_AGG(
                    STRUCT_PACK(
                        number := CAST(g.numpuerta AS VARCHAR),
                        location := STRUCT_PACK(lon := g.lon, lat := g.lat)
                    )
                ) AS housenumbers
            FROM grouped_data g
            LEFT JOIN postcode_data p
            ON g.postcode = p.postcode
            GROUP BY g.id_via, g.name, g.postcode, p.cod_departament, p.cod_province, p.cod_district, p.context
        """

        result_df = conn.execute(query).fetchdf()

    # Crear y guardar cada registro directamente en el archivo NDJSON
    with open(output_file, 'w', encoding='utf-8') as ndjson_file:
        for row in result_df.itertuples():
            # Crear un diccionario para cada calle
            housenumbers = row.housenumbers.tolist() if isinstance(row.housenumbers, np.ndarray) else row.housenumbers

            street_data = {
                "id_via": row.id_via,
                "name": row.name,
                "postcode": row.postcode,
                "cod_departament": row.cod_departament,
                "cod_province": row.cod_province,
                "cod_district": row.cod_district,
                "context": row.context,
                "type": "street",
                "housenumbers": housenumbers
            }

            # Guardar el registro directamente en el archivo
            ndjson_line = json.dumps(street_data, ensure_ascii=False)
            ndjson_file.write(ndjson_line + '\n')

    print(f"NDJSON guardado como {output_file}")


#info(file_path, limit)
#find_duplicate_details(file_path, limit)
use_duckdb_unique(file_path, postcode_file, output_file, limit)
