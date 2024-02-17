from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import psycopg2

def extract_and_save_table_names(*args, **kwargs):
    # Configura la conexión a PostgreSQL
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        user="northwind_user",
        password="thewindisblowing",
        database="northwind"
    )
    
    # Crea un cursor para ejecutar consultas
    cursor = conn.cursor()

    # Define la consulta SQL
    sql_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"

    # Ejecuta la consulta
    cursor.execute(sql_query)

    # Obtiene los resultados
    table_names = cursor.fetchall()

    # Cierra la conexión
    cursor.close()
    conn.close()

    # Guarda los nombres de las tablas en un archivo .txt
    with open("/opt/airflow/data/postgres/{nombre-tabla}/{fecha-tabla}/nombre-tabla.sql", "w") as file:
        for table_name in table_names:
            file.write(f"{table_name[0]}\n")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'get_and_save_table_names_dag',
    default_args=default_args,
    description='DAG to extract table names from PostgreSQL and save to file',
    schedule_interval=timedelta(days=1),
)

# Tarea para obtener los nombres de las tablas y guardar en archivo .txt
get_and_save_table_names_task = PythonOperator(
    task_id='get_and_save_table_names_task',
    python_callable=extract_and_save_table_names,
    provide_context=True,
    dag=dag,
)

