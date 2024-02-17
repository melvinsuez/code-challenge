from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2

def extract_and_save_table_names(*args, **kwargs):
    # Configura a conexão com o PostgreSQL de origem
    conn_source = psycopg2.connect(
        host="postgres",
        port=5432,
        user="northwind_user",
        password="thewindisblowing",
        database="northwind"
    )

    # Cria um cursor para executar consultas
    cursor_source = conn_source.cursor()

    # Define a consulta SQL
    sql_query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"

    # Executa a consulta
    cursor_source.execute(sql_query)

    # Obtém os resultados
    table_names = cursor_source.fetchall()

    # Fecha a conexão
    cursor_source.close()
    conn_source.close()

    # Salva os nomes das tabelas em um arquivo .txt
    with open("/opt/airflow/data/postgres/{nome-tabela}/{data-tabela}/nome-tabela.sql", "w") as file:
        for table_name in table_names:
            file.write(f"{table_name[0]}\n")

def load_tables_to_another_database(*args, **kwargs):
    # Configura a conexão com o outro banco de dados PostgreSQL de destino
    conn_target = psycopg2.connect(
        host="postgres",
        port=5432,
        user="northwind_user",
        password="thewindisblowing",
        database="postgres"
    )

    # Cria um cursor para executar consultas
    cursor_target = conn_target.cursor()

    # Obtém a lista de nomes de tabelas do arquivo criado na primeira tarefa
    with open("/opt/airflow/data/postgres/{nome-tabela}/{data-tabela}/nome-tabela.sql", "r") as file:
        table_names = [line.strip() for line in file]

    # Itera sobre a lista de nomes de tabelas e realiza o carregamento no outro banco de dados
    for table_name in table_names:
        # Define a consulta SQL para carregar a tabela
        sql_query = f"INSERT INTO {table_name} SELECT * FROM source_database.public.{table_name};"

        # Executa a consulta
        cursor_target.execute(sql_query)

    # Faz o commit das alterações e fecha a conexão
    conn_target.commit()
    cursor_target.close()
    conn_target.close()

# Configurações padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(days=1),
}

dag = DAG(
    'extrair_e_carregar_tabelas_dag',
    default_args=default_args,
    description='DAG para extrair e carregar tabelas entre bancos de dados PostgreSQL',
    schedule_interval=timedelta(days=1),
)

# Tarefa para obter os nomes das tabelas e salvar em um arquivo .txt
tarefa_extrair_e_salvar = PythonOperator(
    task_id='tarefa_extrair_e_salvar',
    python_callable=extract_and_save_table_names,
    provide_context=True,
    dag=dag,
)

# Tarefa para carregar as tabelas em outro banco de dados PostgreSQL
tarefa_carregar_para_outro_banco = PythonOperator(
    task_id='tarefa_carregar_para_outro_banco',
    python_callable=load_tables_to_another_database,
    provide_context=True,
    dag=dag,
)

# Define a sequência de execução das tarefas
tarefa_extrair_e_salvar >> tarefa_carregar_para_outro_banco
