
No diretório "entrega" está o meu código desenvolvido para este exercício.

Primeiramente, um arquivo docker-compose.yml, onde criei um contêiner para cada serviço: airflow, postgres e um gerenciador web do postgres.

No diretório "dags"

Tenho um arquivo chamado parte1.py que realiza perfeitamente a primeira tarefa de extrair as tabelas do banco de dados e salvá-las no disco local com a rota dinâmica conforme solicitado. Além disso, o DAG tem a opção de inserir a data para dias anteriores, como solicitado.

No arquivo chamado parte2.py, eu estava desenvolvendo a segunda tarefa de mover cada arquivo correspondente a uma tabela para outro banco de dados, para finalmente realizar o join e obter o resultado esperado.
