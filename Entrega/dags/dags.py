from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from task1 import task1
from task2 import task2
from task3 import task3
from task4 import task4

# Configuración predeterminada del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG combinado
dag = DAG(
    'Indicium',
    default_args=default_args,
    description='DAG combinado con tareas de backup, colar archivo, restaurar tablas y generar datos JSON',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Tarea para respaldar las tablas
backup_tables_task = PythonOperator(
    task_id='backup_tables_task',
    python_callable=task1,
    provide_context=True,
    dag=dag,
)

# Tarea para colar el archivo local
colar_archivo_task = PythonOperator(
    task_id='colar_archivo_local',
    python_callable=task2,
    provide_context=True,
    dag=dag,
)

# Tarea para restaurar las tablas
restore_tables_task = PythonOperator(
    task_id='restore_tables_task',
    python_callable=task3,
    provide_context=True,
    dag=dag,
)

# Tarea para generar datos JSON
generate_json_data_task = PythonOperator(
    task_id='generate_json_data_task',
    python_callable=task4,
    provide_context=True,
    dag=dag,
)

# Define la relación de ejecución entre las tareas
backup_tables_task >> colar_archivo_task >> restore_tables_task >> generate_json_data_task
# Puedes agregar más dependencias según tus necesidades
