# Raw Data Loader

Um pipeline de ELT (Extract, Load, Transform) simples e robusto, projetado para extrair dados brutos de forma incremental de um banco de dados SQL (SQL Server) e carregá-los no Google Cloud Storage (GCS) em formato Parquet particionado.

O projeto é construído para rodar como um job containerizado (ex: no Cloud Run Jobs) e é totalmente configurável via argumentos de linha de comando e variáveis de ambiente, tornando-o facilmente reutilizável para diferentes tabelas.

## ✨ Principais Funcionalidades

-   **Extração Incremental:** Utiliza uma coluna de cursor (ex: `dt_atualizacao`) para buscar de forma eficiente apenas os dados novos ou atualizados.
-   **Tratamento de *Schema Drift*:** Lida de forma resiliente com mudanças no schema da origem, ignorando novas colunas e preenchendo colunas deletadas com nulos, prevenindo falhas no pipeline.
-   **Saída em Parquet Particionado:** Carrega os dados no GCS utilizando particionamento no estilo Hive (`year=.../month=.../day=...`) e o eficiente formato Parquet, otimizado para analytics.
-   **Containerizado e Nativo para Nuvem:** Construído com Docker e projetado para execução *serverless* em plataformas como o Cloud Run Jobs.
-   **Configuração Dinâmica:** Adapte facilmente o pipeline para diferentes tabelas de origem alterando os argumentos de linha de comando, sem necessidade de mudar o código.


## 💻 Stack de Tecnologia

-   **Linguagem:** Python 3.13
-   **Gerenciador de Pacotes:** `uv`
-   **Processamento de Dados:** Pandas, PyArrow
-   **Conectividade:** SQLAlchemy, pyodbc
-   **Containerização:** Docker
-   **Cloud:** Google Cloud (Cloud Run, GCS, Secret Manager, Cloud Build, Artifact Registry)


## 🏗️ Estrutura do Projeto

```
.
├── app/                # Código fonte da aplicação
│   ├── config/         # Módulos de configuração (variáveis de ambiente, etc.)
│   ├── controller/     # Controladores para extração, transformação e carga
│   ├── core/           # Lógica de negócios principal (conexão com DB, GCP)
│   └── utils/          # Funções utilitárias (logger, schema, etc.)
├── secret/             # Arquivos de segredo e configuração (não versionados)
├── .dockerignore       # Arquivos a serem ignorados pelo Docker
├── .gitignore          # Arquivos a serem ignorados pelo Git
├── docker-compose.yml  # Configuração do Docker Compose para ambiente local
├── Dockerfile          # Dockerfile para a imagem de produção
├── Dockerfile.dev      # Dockerfile para o ambiente de desenvolvimento
├── pyproject.toml      # Arquivo de configuração do projeto Python e dependências
├── README.md           # Este arquivo
└── uv.lock             # Arquivo de lock do gerenciador de pacotes uv
```

## ⚙️ Configuração

O pipeline é configurado principalmente por um arquivo de manifesto (`manifest.json`) e por variáveis de ambiente.

### Arquivo de Manifesto (`manifest.json`)

O arquivo de manifesto é um arquivo JSON que define a lista de jobs a serem executados. Cada objeto na lista representa um job e contém os seguintes parâmetros:

| Parâmetro | Obrigatório | Padrão | Descrição |
| :--- | :--- | :--- | :--- |
| `schema_name` | **Sim** | - | Nome do schema do banco de dados a ser processado. |
| `table_name` | **Sim** | - | Nome da tabela a ser processada. |
| `cursor_column` | **Sim** | - | Nome da coluna de cursor para a carga incremental. |
| `chunk_size` | Não | `1000000` | Número de linhas a serem extraídas em cada lote (chunk). |

O job a ser executado é selecionado pela variável de ambiente `CLOUD_RUN_TASK_INDEX`.

### Variáveis de Ambiente

Estas variáveis configuram o ambiente de execução e as conexões com os serviços.

| Variável | Descrição |
| :--- | :--- |
| `DB_USER` | Usuário de acesso ao banco de dados. |
| `DB_PASSWORD` | Senha de acesso ao banco de dados (em produção, usar Secret Manager).|
| `DB_HOST` | Hostname ou endereço IP do servidor do banco de dados. |
| `DB_PORT` | Porta do servidor do banco de dados. |
| `DB_NAME` | Nome do banco de dados. |
| `GCP_PROJECT_ID` | ID do projeto no Google Cloud. |
| `GCS_BUCKET_NAME`| Nome do bucket no Google Cloud Storage para onde os dados serão enviados.|
| `EXECUTION_TS` | Timestamp da execução (usado para particionamento). |
| `CLOUD_RUN_TASK_INDEX` | Índice do job a ser executado a partir do arquivo de manifesto. |


## 🚀 Deploy em Produção (Google Cloud Run Jobs)

O deploy em produção é feito construindo uma imagem Docker e criando um Job no Google Cloud Run, que pode ser executado manualmente ou orquestrado por um scheduler como o Cloud Composer (Airflow).

### Passo 1: Gerenciar Segredos 🤫

Guarde todas as senhas e chaves de acesso no **Google Secret Manager**. Você pode criar um segredo a partir de um arquivo `.env` local.

```bash
# Crie um arquivo .env com as variáveis de ambiente de produção
# Exemplo de conteúdo do .env:
# DB_USER=...
# DB_PASSWORD=...
# ...

# Crie o segredo no Secret Manager
gcloud secrets create NOME_DO_SEGREDO --data-file=./secret/.env
```

### Passo 2: Construir e Enviar a Imagem

Use o Google Cloud Build para construir a imagem Docker a partir do `Dockerfile` e enviá-la para o Google Artifact Registry.

1.  **Ative as APIs necessárias:**
    -   Cloud Build API
    -   Artifact Registry API

2.  **Crie um repositório no Artifact Registry:**
    ```bash
    gcloud artifacts repositories create NOME_DO_REPOSITORIO \
        --repository-format=docker \
        --location=SUA_REGIAO
    ```

3.  **Envie a imagem para o Cloud Build:**
    ```bash
    gcloud builds submit --tag SUA_REGIAO-docker.pkg.dev/SEU_PROJETO/NOME_DO_REPOSITORIO/NOME_DA_IMAGEM:latest
    ```

### Passo 3: Criar/Atualizar o Cloud Run Job

Crie um Job no Cloud Run para executar o pipeline.

```bash
gcloud run jobs deploy NOME_DO_JOB \
    --image=SUA_REGIAO-docker.pkg.dev/SEU_PROJETO/NOME_DO_REPOSITORIO/NOME_DA_IMAGEM:latest \
    --region=SUA_REGIAO \
    --service-account=SUA_CONTA_DE_SERVICO \
    --set-secrets=VARIAVEL_DE_AMBIENTE=NOME_DO_SEGREDO:latest \
    # Adicione outras configurações como memória, CPU, etc.
```

**Importante:**
- Substitua `SUA_REGIAO`, `SEU_PROJETO`, `NOME_DO_REPOSITORIO`, `NOME_DA_IMAGEM`, `NOME_DO_JOB`, `SUA_CONTA_DE_SERVICO`, `VARIAVEL_DE_AMBIENTE` e `NOME_DO_SEGREDO` pelos seus valores.
- A `VARIAVEL_DE_AMBIENTE` no comando `--update-secrets` deve ser o nome da variável de ambiente que o seu código espera (ex: `DB_PASSWORD`).

### Passo 4: Executar o Job

Execute o Job manualmente ou orquestre-o com o Cloud Composer (Airflow).

**Execução Manual:**

Para executar um job específico do manifesto, configure a variável de ambiente `CLOUD_RUN_TASK_INDEX`.

```bash
gcloud run jobs execute NOME_DO_JOB \
    --region=SUA_REGIAO \
    --update-env-vars=CLOUD_RUN_TASK_INDEX=0 # Execute o primeiro job do manifesto
```

**Orquestração com Airflow:**

Use o `CloudRunExecuteJobOperator` para executar o job a partir de um DAG do Airflow.

```python
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

run_task = CloudRunExecuteJobOperator(
    task_id="run_etl_for_my_table",
    project_id="SEU_PROJETO",
    region="SUA_REGIAO",
    job_name="NOME_DO_JOB",
    overrides={
        "container_overrides": [
            {
                "name": "NOME_DO_CONTAINER", # Geralmente 'app' ou 'default'
                "env": [
                    {
                        "name": "CLOUD_RUN_TASK_INDEX",
                        "value": "0"
                    }
                ]
            }
        ]
    },
)
```

## 🔁 CI/CD (GitHub Actions)

O repositório contém um workflow em `.github/workflows/deploy.yml` que:

- Faz build e push da imagem para o Artifact Registry.
- Faz o deploy do Cloud Run Job com `gcloud run jobs deploy` (cria ou atualiza).
- Registra a especificação final do Job e a imagem implantada no log do workflow.
- Usa controle de concorrência para evitar deploys sobrepostos.

Dispare o workflow em pushes no branch `main` que alterem `app/**`, `Dockerfile`, `pyproject.toml`, `uv.lock` ou o próprio workflow.

Secrets necessários no repositório:

- `GOOGLE_CREDENTIALS` (JSON do service account com permissão em Artifact Registry e Cloud Run)
- `GCP_REGION`, `GCP_PROJECT_ID`, `GCP_ARTIFACT_REPO_NAME`, `GCP_IMAGE_NAME`, `GCP_IMAGE_TAG_NAME`
- `GCP_SERVICE_ACCOUNT`, `GCP_NETWORK`, `GCP_SUBNETWORK`

## 🧪 Testes e Qualidade

- Testes unitários: `uv run pytest -q`
- Type-check: `uv run pyright`
- Lint/format: `uv run ruff check` e `uv run ruff format`

## 🧭 Execução Local (Docker Compose)

Para rodar localmente com Docker Compose:

1. Crie `secret/.env` com as variáveis obrigatórias (consulte `app/config/env.py`).
2. Garanta que o arquivo `secret/key-file.json` (ADC) exista localmente.
3. Execute `docker compose up --build`.

O compose injeta `GOOGLE_APPLICATION_CREDENTIALS` e define um `EXECUTION_TS` default. O arquivo de manifesto é lido do GCS em `mssql/manifest.json`.

## 📌 Notas de Produção

- SQL incremental (pyodbc): a query usa marcadores posicionais `?` e `pandas.read_sql` recebe `params=(last_cursor,)`. Isso é essencial para evitar o erro `The SQL contains 0 parameter markers, but 1 parameters were supplied`.
- Particionamento no GCS: os arquivos são gravados em `mssql/tables/<table>/ingestion/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>_<chunk>.parquet`.
- Timestamps: se `EXECUTION_TS` for inválido/missing, o loader usa `datetime.now(timezone.utc)` e loga um aviso/erro apropriado.
- Imagem Docker: use `.dockerignore` para excluir `.venv/`, caches e `secret/` do contexto de build. Para imagens menores, considere multi-stage build (builder com `uv` e runtime em `python:3.13-slim`) caso necessário.
