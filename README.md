# Spark Learning

Ambiente local para estudar Apache Spark com Docker Compose: cluster Spark, History Server e Jupyter Notebook com PySpark.

Written for: estudantes que vão usar o ambiente pela primeira vez.

## Pré-requisitos

- Docker e Docker Compose instalados
- Pelo menos 8 GB de RAM livres (o stack limita os serviços Spark a cerca de 6 GB)

## Subindo o ambiente

Na raiz do projeto:

```bash
docker compose up -d
```

Para acompanhar os logs de todos os serviços:

```bash
docker compose logs -f
```

Para parar tudo:

```bash
docker compose down
```

## Acessando o notebook

1. Suba o ambiente com `docker compose up -d`.
2. Pegue o link de acesso com token:

   ```bash
   docker compose logs notebook | grep token= | tail -1
   ```

3. Abra no navegador o link que aparecer, no formato:

   ```
   http://localhost:8888/lab?token=<TOKEN>
   ```

O token muda sempre que o container do notebook é recriado. Se o link antigo não funcionar, rode o comando do passo 2 de novo.

Os notebooks ficam na pasta `notebooks/` do projeto (montada em `work/` dentro do Jupyter), então o que você criar ali permanece mesmo se o container for recriado.

## Conectando o PySpark ao cluster

O Jupyter usa o kernel **Python 3**. O PySpark já está instalado na imagem, mas precisa ser importado pelo caminho do `SPARK_HOME`. Coloque a célula abaixo no início de cada notebook:

```python
import glob
import os
import sys

spark_home = os.environ["SPARK_HOME"]  # /usr/local/spark
sys.path.insert(0, f"{spark_home}/python")
sys.path.insert(0, glob.glob(f"{spark_home}/python/lib/py4j-*.zip")[0])

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("notebook")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
print(spark.version)
spark.range(5).show()
```

A saída esperada é a versão `4.0.0` seguida de uma tabela com 5 linhas.

## Endereços úteis

| Serviço | Endereço | Para quê |
|---|---|---|
| Jupyter Lab | http://localhost:8888 | Notebooks (usar o link com token) |
| Spark Master UI | http://localhost:8081 | Estado do cluster e workers |
| Spark Worker UI | http://localhost:8082 | Executores do worker |
| Spark Application UI | http://localhost:4040 | Jobs e stages de uma sessão ativa |
| History Server | http://localhost:18080 | Aplicações que já terminaram |
| Spark Master (RPC) | `spark://localhost:7077` | Conexão de aplicações externas |

A porta 4040 só responde enquanto uma `SparkSession` estiver rodando no notebook.

## Dados de exemplo

Os datasets ficam em `data/` e aparecem como somente leitura dentro dos containers em `/data` (no Spark) e `/home/jovyan/data` (no notebook).

Se a pasta `data/` não existir, gere os arquivos com:

```bash
python3 scripts/generate_sample_data.py
```

Arquivos gerados:

- `customers.csv`: 10.000 clientes
- `products.csv`: 500 produtos
- `orders.csv`: 200.000 pedidos
- `web_events.jsonl`: 100.000 eventos de navegação

## Estrutura de pastas

```
.
├── docker-compose.yml        # Definição dos serviços
├── conf/spark-defaults.conf  # Configuração do Spark (logs de eventos)
├── data/                     # Datasets de exemplo
├── notebooks/                # Seus notebooks
├── scripts/                  # Geração dos dados de exemplo
└── spark-events/             # Logs de eventos para o History Server
```

## Problemas comuns

- **Página do Jupyter não abre:** confira se o container está rodando com `docker compose ps` e se a porta 8888 não está em uso por outro programa.
- **Token inválido:** pegue o token novo com o comando da seção de acesso ao notebook.
- **`pyspark` não encontrado:** use a célula de conexão acima em vez de `import pyspark` direto; a biblioteca fica em `$SPARK_HOME/python`.
- **Aplicações não aparecem no History Server:** a sessão precisa terminar (`spark.stop()`) para o log ser finalizado, e o History Server atualiza a lista a cada poucos segundos.
- **Erro de permissão em `spark-events/`:** rode `chmod -R a+rwX spark-events` na raiz do projeto.
