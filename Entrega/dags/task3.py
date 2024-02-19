from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import subprocess

def task3():
    # Ruta base donde se encuentran los archivos .csv
    base_dir_path = "/opt/airflow/data/postgres/"
    # Itera sobre las tablas en el directorio base

    # Carga los datos desde el archivo .csv usando psql
    subprocess.run(["psql", "-h", "postgres", "-U", "northwind_user", "-d", "postgres", "-f", "/opt/airflow/data/postgres/schema.sql"])

    for table_name in os.listdir(base_dir_path):
        table_dir_path = os.path.join(base_dir_path, table_name)

        # Verifica si es un directorio (correspondiente a una tabla)
        if os.path.isdir(table_dir_path):
 
            # Encuentra el directorio más reciente dentro de la tabla
            latest_date_dir = max(os.listdir(table_dir_path))
            date_dir_path = os.path.join(table_dir_path, latest_date_dir)

            # Restaura el archivo .csv en la base de datos de destino
            ruta_csv = os.path.join(date_dir_path, f"{table_name}.csv")   
            nombre_tabla = table_name

            # Crea la consulta SQL 
            load_data_query = f"\copy {nombre_tabla} FROM '{ruta_csv}' DELIMITER ',' CSV HEADER;"

            # Carga los datos desde el archivo .csv usando psql
            result = subprocess.run(["psql", "-h", "postgres", "-U", "northwind_user", "-d", "postgres", "-c", load_data_query])

    # Carga los datos desde el archivo .csv usando psql
    subprocess.run(["psql", "-h", "postgres", "-U", "northwind_user", "-d", "postgres", "-f", "/opt/airflow/data/postgres/schema2.sql"])