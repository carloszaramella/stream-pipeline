# Stream Pipeline

Pipeline de Engenharia de Dados desenvolvido com **Apache Spark Structured Streaming**, utilizando **arquitetura Medallion (Bronze, Silver e Gold)**.

O projeto demonstra ingestão incremental de eventos em formato **JSON**, validação e transformação dos dados, agregação utilizando **janelas temporais (Window)** e geração de saída em **Parquet**.

O ambiente de execução é reproduzível por meio de **Docker e Docker Compose**, incluindo serviços auxiliares como **MinIO** e **PostgreSQL**.

---

## 📌 Objetivo do projeto

O objetivo é construir um **Stream Pipeline utilizando Apache Spark Structured Streaming**, realizando:

* ingestão de eventos em streaming;
* processamento incremental;
* transformação dos dados;
* validação e filtragem;
* agregação;
* utilização de janelas temporais;
* geração de dados de saída em formato diferente da origem;
* execução containerizada.

A solução atende ao enunciado principal e contempla os bônus propostos:

| Requisito               | Implementação                       |
| ----------------------- | ----------------------------------- |
| Stream Pipeline         | Apache Spark Structured Streaming   |
| Ingestão                | Eventos JSON/JSONL                  |
| Output em outro formato | Parquet                             |


---

## 📌 Visão geral

O pipeline recebe eventos de financiamento de veículos, processa os dados de forma incremental e os organiza em três camadas da arquitetura Medallion.

```text
                    ┌──────────────────┐
                    │     Producer     │
                    │   Eventos JSON   │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │    BRONZE       │
                    │ Dados originais  │
                    │    em Parquet    │
                    └────────┬─────────┘
                             │
                             ▼
                       Validação
                       e filtro
                             │
                             ▼
                    ┌──────────────────┐
                    │    SILVER       │
                    │ Dados validados  │
                    │ e padronizados   │
                    └────────┬─────────┘
                             │
                             ▼
                  Window + Agregação
                             │
                             ▼
                    ┌──────────────────┐
                    │     GOLD        │
                    │ Dados agregados  │
                    │ para análise     │
                    └──────────────────┘
```

A arquitetura segue o conceito de **Medallion Architecture**, no qual os dados evoluem progressivamente de seu estado bruto para dados confiáveis e preparados para consumo analítico.

---

## 🏗️ Arquitetura

### Bronze

A camada **Bronze** representa os eventos recebidos pelo pipeline em seu formato original.

Nesta etapa o objetivo é preservar os dados recebidos antes das transformações de qualidade.

Principais objetivos:

* preservar os eventos recebidos;
* permitir reprocessamento;
* manter rastreabilidade;
* separar ingestão da lógica de transformação;
* armazenar os dados em Parquet.

---

### Silver

A camada **Silver** contém os dados após aplicação das regras de qualidade e padronização.

Nesta etapa são realizados controles como:

* validação de campos;
* validação de tipos;
* validação de valores;
* normalização dos dados;
* padronização de atributos;
* separação de registros válidos e inválidos.

O objetivo é evitar que dados inconsistentes avancem para as etapas analíticas.

---

### Gold

A camada **Gold** contém os dados preparados para análise.

Nesta etapa são realizadas:

* agregações;
* cálculo de métricas;
* agrupamentos;
* processamento por janelas de tempo;
* preparação dos dados para consumo analítico.

Os resultados também são disponibilizados em **Parquet**.

---

# ⚡ Apache Spark Structured Streaming

O processamento utiliza **Apache Spark Structured Streaming**.

O pipeline trabalha de forma incremental, processando novos eventos conforme eles são disponibilizados na fonte de entrada.

Entre os recursos utilizados estão:

* Structured Streaming;
* processamento em micro-batches;
* triggers;
* watermark;
* janelas de tempo;
* checkpoints;
* transformação com PySpark.

O uso de **watermark** permite controlar a chegada de eventos atrasados durante o processamento das agregações temporais.

---

# 🪟 Window e agregação

Um dos principais recursos demonstrados no projeto é a utilização de **Window** no processamento dos eventos.

Os dados são agrupados em janelas temporais, permitindo calcular métricas sobre intervalos de tempo.

Exemplos de métricas calculadas:

* quantidade de financiamentos;
* valor total financiado;
* parcela média;
* taxa média de juros.

As agregações também podem considerar dimensões do negócio, como:

* segmento;
* região;
* tipo de veículo;
* marca;
* modelo.

Exemplo conceitual:

```text
Eventos
   │
   ▼
Window de tempo
   │
   ├── Segmento
   ├── Região
   ├── Tipo de veículo
   └── Marca / Modelo
            │
            ▼
        Agregações
            │
            ▼
       Dados Gold
```

---

# 🧪 Validação e qualidade dos dados

O pipeline possui uma etapa de validação antes da geração da camada analítica.

São verificadas regras relacionadas a:

* existência de campos;
* tipos de dados;
* valores financeiros;
* atributos dos veículos;
* consistência dos registros.

Registros que não atendem às regras de qualidade são tratados como inválidos e não avançam normalmente para a camada Silver.



---

# 📂 Estrutura do projeto

```text
stream-pipeline/
│
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── requirements.txt
├── README.md
│
├── producer/
│   └── ...
│
├── src/
│   ├── config.py
│   ├── financing_schema.py
│   ├── medallion_pipeline.py
│   ├── pipeline.py
│   ├── bronze.py
│   ├── gold.py
│   ├── schemas.py
│   ├── transformations.py
│   ├── silver.py
│   └── validate_layers.py
│
├── tests/
│   └── test_transformations.py
│
└── data/
    ├── input/
       ├── bronze/
       ├── silver/
       ├── gold/
    └── checkpoint/
```

---

# 🐳 Execução com Docker

O projeto utiliza Docker para disponibilizar um ambiente de execução reproduzível.

O container principal contém o ambiente necessário para executar o Apache Spark e o código Python do pipeline.

## Pré-requisitos

É necessário ter:

* Docker;
* Docker Compose.

Não é necessário instalar manualmente Spark ou Java no ambiente local para executar o pipeline através do Docker.

---

# 🚀 Executando o projeto

Clone o repositório:

```bash
git clone https://github.com/carloszaramella/stream-pipeline.git
```

Entre no diretório:

```bash
cd stream-pipeline
```

Construa as imagens:

```bash
docker compose build
```

Suba o MinIO:

```bash
docker compose up -d minio
```

Inicialize o bucket:

```bash
docker compose run --rm minio-init
```

Execute o pipeline:

```bash
docker compose up stream-pipeline
```

Para reconstruir a imagem antes da execução:

```bash
docker compose up --build stream-pipeline
```

---

# 🪣 MinIO

O projeto utiliza o **MinIO** como object storage compatível com S3.

O serviço é utilizado pelo pipeline para armazenamento dos dados nas camadas do processamento.

A comunicação entre os containers utiliza o nome do serviço:

```text
http://minio:9000
```

Para acesso a partir do ambiente do Codespace ou host:

```text
API:     http://localhost:9000
Console: http://localhost:9001
```

O bucket utilizado pelo projeto é:

```text
stream-pipeline
```

Estrutura conceitual:

```text
stream-pipeline/
├── bronze/
├── silver/
├── gold/
└── checkpoints/
```

---

# 🐘 PostgreSQL e consultas SQL

O PostgreSQL armazena o resultado agregado da camada **GOLD** na tabela
`vehicle_financing_summary`.

Suba o banco, o MinIO e o inicializador do bucket:

```bash
docker compose up -d postgres minio minio-init
```

Execute o pipeline para processar os eventos e materializar a camada GOLD
no PostgreSQL:

```bash
docker compose up --build stream-pipeline
```

Configuração utilizada no ambiente de demonstração:

```text
Database: vehicle_financing
User:     dw_user
Password: dw_password
Port:     5432
```

O pipeline também mantém os dados GOLD em Parquet. A tabela PostgreSQL é
materializada ao final da execução do pipeline; por isso, execute as consultas
somente depois que `stream-pipeline` terminar com código `0`.

## Consulta pelo script

Consulta o resumo por região, tipo de veículo e segmento:

```bash
docker compose run --rm stream-pipeline \
       python3 src/query_dw.py --query resumo
```

Consulta as janelas e os modelos dos veículos:

```bash
docker compose run --rm stream-pipeline \
       python3 src/query_dw.py --query janelas
```

## Consulta SQL direta

Abra o cliente PostgreSQL:

```bash
docker compose exec postgres psql \
       -U dw_user \
       -d vehicle_financing
```

Dentro do `psql`, verifique a tabela e consulte os dados:

```sql
\dt

SELECT *
FROM vehicle_financing_summary
ORDER BY window_start, region, vehicle_type;
```

Consulta agregada para análise:

```sql
SELECT
              region,
              vehicle_type,
              segment,
              SUM(financing_count) AS total_financiamentos,
              SUM(total_financed_amount) AS valor_total_financiado,
              ROUND(AVG(average_monthly_installment), 2) AS parcela_media
FROM vehicle_financing_summary
GROUP BY region, vehicle_type, segment
ORDER BY valor_total_financiado DESC;
```

Também é possível executar uma consulta diretamente pelo terminal:

```bash
docker compose exec postgres psql \
       -U dw_user \
       -d vehicle_financing \
       -c "SELECT COUNT(*) AS total_linhas FROM vehicle_financing_summary;"
```

## O que é o `sitecustomize.py`?

O Python carrega automaticamente um arquivo chamado `sitecustomize.py` quando
ele está disponível no caminho de importação. Neste projeto, esse arquivo
seleciona um `JAVA_HOME` compatível antes da inicialização do Spark.

Isso evita problemas quando o ambiente local usa uma versão recente do Java
incompatível com o Spark 3.5.x. O script procura primeiro um `JAVA_HOME`
compatível e, se necessário, verifica instalações de Java 21, 17, 11 ou 8.

Ele não transforma dados, não configura o PostgreSQL e não participa da lógica
das camadas BRONZE, SILVER ou GOLD. Seu papel é somente preparar o ambiente
Java para executar o Spark localmente.

---

# 🧪 Testes

Os testes automatizados podem ser executados dentro do container Docker:

```bash
docker compose run --rm tests
```

Ou através do Makefile:

```bash
make test
```

Os testes validam principalmente as regras de transformação e processamento dos dados.

Exemplos:

* separação de eventos inválidos;
* validação de valores;
* validação dos atributos dos veículos;
* transformações dos dados;
* agregações;
* processamento utilizando janelas temporais.

---

# ⚙️ Configuração

O comportamento do pipeline pode ser configurado por variáveis de ambiente.

Exemplo:

```bash
PIPELINE_RUNTIME_SECONDS=30
PIPELINE_ROWS_PER_SECOND=5
```

Principais parâmetros:

| Variável                   | Descrição                                               |
| -------------------------- | ------------------------------------------------------- |
| `PIPELINE_RUNTIME_SECONDS` | Tempo de execução do pipeline                           |
| `PIPELINE_ROWS_PER_SECOND` | Quantidade aproximada de eventos produzidos por segundo |
| `PIPELINE_BASE_DIR`        | Diretório base utilizado pelo pipeline                  |
| `MINIO_ENDPOINT`           | Endpoint do MinIO                                       |
| `MINIO_ACCESS_KEY`         | Usuário do MinIO                                        |
| `MINIO_SECRET_KEY`         | Senha do MinIO                                          |
| `MINIO_BUCKET`             | Bucket utilizado pelo pipeline                          |
| `POSTGRES_HOST`            | Host do PostgreSQL                                      |
| `POSTGRES_PORT`            | Porta do PostgreSQL                                     |
| `POSTGRES_DB`              | Banco de dados PostgreSQL                               |
| `POSTGRES_USER`            | Usuário do PostgreSQL                                   |
| `POSTGRES_PASSWORD`        | Senha do PostgreSQL                                     |

Dentro da rede Docker, os serviços são acessados pelos nomes:

```text
MinIO:      minio:9000
PostgreSQL: postgres:5432
```

---

# 📦 Formato dos dados

A fonte escolhida para o projeto é **JSON/JSONL**.

O pipeline utiliza o Apache Spark para ingestão incremental e transforma os dados para **Parquet**.

Dessa forma, o projeto atende diretamente ao requisito do enunciado de realizar a ingestão dos dados e gerar o output em outro formato.

```text
JSON
  │
  ▼
Spark Structured Streaming
  │
  ▼
Parquet
```

O formato Parquet foi escolhido por ser adequado para processamento analítico e armazenamento estruturado.

---

# 🔄 Fluxo completo do processamento

```text
                    Eventos JSON
                         │
                         ▼
                  ┌─────────────┐
                  │  Producer   │
                  └──────┬──────┘
                         │
                         ▼
                  ┌─────────────┐
                  │    BRONZE   │
                  │   Parquet   │
                  └──────┬──────┘
                         │
                         ▼
               ┌──────────────────┐
               │ Validação /      │
               │ Qualidade        │
               └────────┬─────────┘
                        │
                        ▼
                  ┌─────────────┐
                  │    SILVER   │
                  │   Parquet   │
                  └──────┬──────┘
                         │
                         ▼
               ┌──────────────────┐
               │ Window +          │
               │ Agregação         │
               └────────┬─────────┘
                        │
                        ▼
                  ┌─────────────┐
                  │     GOLD    │
                  │   Parquet   │
                  └──────┬──────┘
                         │
                         ▼
                  Dados analíticos
```

---

# 🛠️ Makefile

O `Makefile` fornece atalhos para as operações mais utilizadas no projeto.

Exemplos:

```bash
make test
```

Executa os testes automatizados.

```bash
make run
```

Executa o pipeline.

Comandos Docker disponibilizados pelo projeto podem incluir:

```bash
make docker-build
make docker-test
make docker-run
```

Esses comandos facilitam a execução do projeto sem a necessidade de memorizar todos os comandos Docker.

---

# 🎯 Atendimento ao enunciado

O projeto foi construído para atender aos requisitos da atividade:

### Requisito principal

**Construir um Stream Pipeline utilizando uma ferramenta ou plataforma.**

Implementação:

```text
Apache Spark Structured Streaming
```

### Ingestão dos dados

Fonte:

```text
JSON / JSONL
```

Processamento incremental através do Spark Structured Streaming.

### Output em outro formato

Entrada:

```text
JSON
```

Saída:

```text
Parquet
```

### Bônus 1 — Filtro e/ou validação

Implementado através das regras de qualidade e validação dos eventos na camada Silver.

### Bônus 2 — Agregação

Implementado através de métricas como:

```text
COUNT
SUM
AVG
```

### Bônus 3 — Window

Implementado através de:

```text
Window temporal
+
Watermark
+
Agregações
```

### Bônus 4 — Deploy

Implementado através de:

```text
Docker
Dockerfile
Docker Compose
```

O pipeline pode ser executado em um ambiente containerizado e reproduzível.

---

# 🔍 Qualidade e testes

A qualidade dos dados é tratada durante o processamento para evitar que registros inválidos avancem para as camadas posteriores.

Os testes automatizados permitem validar as principais regras de transformação antes da execução do pipeline.

Execução recomendada:

```bash
docker compose build
docker compose run --rm tests
docker compose up stream-pipeline
```

---

# 🧩 Tecnologias

| Tecnologia                 | Utilização                                  |
| -------------------------- | ------------------------------------------- |
| Python                     | Desenvolvimento do pipeline                 |
| Apache Spark               | Processamento dos dados                     |
| Spark Structured Streaming | Processamento incremental de eventos        |
| PySpark                    | API Python do Spark                         |
| Parquet                    | Formato de saída                            |
| Docker                     | Containerização                             |
| Docker Compose             | Orquestração do ambiente                    |
| Pytest                     | Testes automatizados                        |
| MinIO                      | Object storage compatível com S3            |
| PostgreSQL                 | Serviço opcional para evolução do warehouse |

---

# 📌 Resultados demonstrados

O projeto demonstra na prática:

* ingestão de eventos em streaming;
* processamento incremental;
* arquitetura Medallion;
* validação e qualidade dos dados;
* transformação com PySpark;
* agregações;
* agregações por janela temporal;
* watermark;
* checkpoints;
* geração de arquivos Parquet;
* testes automatizados;
* containerização;
* execução reproduzível do pipeline.

---

# 🚀 Possíveis evoluções

Como extensões futuras, o projeto pode evoluir para:

* integração efetiva do PostgreSQL como Data Warehouse;
* CI/CD;
* monitoramento do pipeline;
* observabilidade;
* métricas de processamento;
* tratamento mais avançado de eventos atrasados;
* execução em ambientes cloud;
* utilização de ferramentas de orquestração;
* implementação de testes de integração.

---

# 👨‍💻 Autor

**Carlos Zaramella**

Projeto desenvolvido para estudo e demonstração de conceitos de Engenharia de Dados, Apache Spark, Structured Streaming, arquitetura Medallion e processamento incremental de dados.
