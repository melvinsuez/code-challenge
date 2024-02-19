from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess

# Definir la función para realizar el respaldo en formato CSV usando psql
def task2(*args, **kwargs):
    # Obtén la fecha de ejecución del DAG
    fecha_tabla = kwargs['dag_run'].conf['fecha_tabla']

    # Rutas de los archivos
    ruta_origen = '/opt/airflow/data/postgres/order_details.csv'
    ruta_destino = f"/opt/airflow/data/postgres/order_details/{fecha_tabla}/"

    # Copiar el archivo utilizando mv
    subprocess.run(f'mkdir -p {ruta_destino}', shell=True)
    subprocess.run(['mv', ruta_origen, ruta_destino])

    # Opcional: Eliminar el archivo original
    subprocess.run(['rm', ruta_origen])
