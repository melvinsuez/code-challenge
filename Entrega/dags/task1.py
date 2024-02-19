from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess

# Definir la función para realizar el respaldo en formato CSV usando psql
def task1(*args, **kwargs):
    # Obtén la fecha de ejecución del DAG
    fecha_tabla = kwargs['dag_run'].conf['fecha_tabla']

    # Define la carpeta base de destino
    base_destination_folder = f"/opt/airflow/data/postgres"

    # Modifica la cadena de conexión según tus necesidades
    connection_string = "postgresql://northwind_user:thewindisblowing@postgres:5432/northwind"

    # Obtén la lista de tablas
    table_names_query = f'psql {connection_string} -t -c "SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\';"'
    table_names = subprocess.run(table_names_query, shell=True, stdout=subprocess.PIPE, text=True).stdout.splitlines()

    # Realiza el respaldo de cada tabla y guarda en formato CSV
    i=1
    for table_name in table_names:
        table_name = table_name.strip()
        # Define la carpeta de destino específica para cada tabla y fecha
        destination_folder = f"{base_destination_folder}/{table_name}/{fecha_tabla}"

        if table_name != "":
            # Crea la carpeta de destino si no existe
            subprocess.run(f'mkdir -p {destination_folder}', shell=True)

            # Define la ruta completa del archivo CSV
            csv_file_path = f"{destination_folder}/{table_name}.csv"
            
            #Realiza el respaldo y guarda en el archivo CSV usando psql
            subprocess.run(f'psql {connection_string} -c "COPY {table_name} TO STDOUT WITH CSV HEADER" > {csv_file_path}', shell=True)
