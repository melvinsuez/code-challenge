
Para a resolução do desafio, utilizei o Docker.

Primeiramente, criei um docker-compose com todos os serviços necessários: Airflow, PostgreSQL e um servidor web para cada um deles.

Ao iniciar o serviço do Docker, são criados diretórios para tornar persistentes os dados importantes. Supondo que o banco de dados a ser backup diariamente seja o fornecido no desafio, criei um serviço independente para esse banco de dados (northwind) e outro para o uso interno do Airflow (postgres), que posteriormente utilizarei como banco de dados de destino para completar o desafio.

P.S.: Estou ciente de que restaurar dados de um banco de dados para outro que contém informações totalmente diferentes é impensável em um ambiente de trabalho. No entanto, por questões de tempo, fiz isso como um ambiente puramente experimental.

Então, definindo as funções criadas:

**Task1:**
Para seu funcionamento correto, requer uma variável fornecida no momento da execução do DAG. Conforme solicitado no desafio, é necessário indicar a data para realizar o backup de dias anteriores. Em seguida, a função realiza uma consulta a todas as tabelas do banco de dados e começa a criar os diretórios de acordo com o formato da ruta solicitada no desafio:
/data/postgres/{nome-tabela}/{data}/arquivo.csv

**Task2:**
Para executar esta tarefa com sucesso, é necessário utilizar o mesmo valor da variável de data indicado no início da execução das tarefas. Fazer o download do arquivo fornecido no desafio (order_details.csv) e armazene-o no diretório específico, onde será posteriormente extraído e colocado na pasta da tabela e data correspondente.
Diretório onde o arquivo (order_details.csv) deve ser baixado: `/data/postgres/`

Ao finalizar esta tarefa, o passo 1 é concluído, onde o backup do banco de dados e do arquivo é feito no armazenamento local.

**Task3:**
Aqui começamos o processo de transferir do armazenamento local para um novo banco de dados. Novamente, esclareço que, por falta de tempo, utilizei o banco de dados que contém as informações do Airflow, o que não gera nenhum conflito funcional, mas certamente seria inadequado profissionalmente. Destaco que o banco de dados de destino pode ser qualquer outro, local, remoto, em nuvem.
Primeiramente, utilizando a estrutura fornecida pelo desafio (northwind.sql), usei a primeira parte do arquivo onde estão definidas as entidades e seus atributos para criar a estrutura do novo banco de dados. Em seguida, utilizando os backups gerados nas Task1 e Task2, faço a restauração dos dados no novo banco de dados. Finalmente, utilizo a última parte do arquivo fornecido pelo desafio para criar as chaves primárias e estrangeiras de cada entidade e assim estabelecer suas relações.

**Task4:**
Na última etapa e para concluir os objetivos do desafio, criei uma consulta utilizando JOIN para unir os detalhes de cada pedido e, ao mesmo tempo, convertê-los para o formato JSON e salvá-los no mesmo diretório para facilitar sua análise e utilização.

Naturalmente, tudo isso é orquestrado a partir do arquivo dags.py utilizando o Airflow. Neste arquivo, está a invocação de cada tarefa e é responsável por executá-las na ordem estabelecida.

Considerações finais: Espero que os resultados da minha entrega sejam suficientes para demonstrar meu interesse em continuar aprendendo e continuar no processo seletivo, desde já agradeço.
