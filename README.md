# Pipeline CNPJ — Airflow + Docker + PostgreSQL

Pipeline mensal para transformação e carga dos dados públicos do **Cadastro Nacional da Pessoa Jurídica (CNPJ)** da Receita Federal, com execução orquestrada por **Apache Airflow** em ambiente **Docker** e armazenamento no **PostgreSQL**.

O projeto foi estruturado para ser **clonável e reprodutível**, abaixo os detalhes técnicos para utilização. 

## Objetivo do projeto

O objetivo da pipeline é produzir uma base consolidada de empresas e estabelecimentos do **município de São Paulo (capital)**, a partir dos arquivos disponibilizados pela Receita Federal, enriquecendo o dado com:

- descrição da classificação CNAE em múltiplos níveis [CONCLA](https://cnae.ibge.gov.br/?view=estrutura);
- geolocalização aproximada por **CEP**, com associação a **distrito**, **subprefeitura**, **latitude** e **longitude**.

A saída principal da pipeline é a tabela final `cnpj_final` no PostgreSQL.


## Arquitetura

- **Apache Airflow** para orquestração da DAG;
- **Docker Compose** para subir todo o ambiente localmente;
- **PostgreSQL** para staging e tabela final;
- **Python** para limpeza, filtragem e carga dos arquivos;
- **SQL** para a transformação final da base consolidada.



## Estrutura do repositório

```text
cnpj_airflow_pipeline/
├─ dags/
│  ├─ dag_cnpj.py
│  └─ cnpj_pipeline/
│     ├─ __init__.py
│     ├─ transform_cnpj.py
│     └─ sql/
│        └─ transform.sql
├─ cnpj_data/
│  └─ distritos.csv
├─ Dockerfile
├─ docker-compose.yaml
├─ requirements.txt
├─ criacao_tabelas.sql
├─ .env.example
└─ README.md
```

### Principais arquivos

- `dag_cnpj.py`: define o agendamento da pipeline no Airflow.
- `transform_cnpj.py`: implementa as etapas de carga, filtragem, staging e execução do SQL final.
- `transform.sql`: recria, trata e popula a tabela `cnpj_final`.
- `cnpj_data/distritos.csv`: base auxiliar de CEPs e geolocalização usada para enriquecer a base final.
- `criacao_tabelas.sql`: esse código sql deve ser rodado no postgres apenas 1x para criação das tabelas que serão populadas mensalmente pela carga da pipeline.



## Pré-requisitos

Para executar o projeto localmente, é necessário ter instalado:

- **Docker Desktop** (Windows/Mac) ou Docker Engine + Docker Compose (Linux);
- **Git**, para clonar o repositório;
- acesso a um **PostgreSQL** já existente e acessível a partir dos containers do Airflow.

> O projeto **não exige instalação manual do Airflow** fora do Docker.



## Configuração do ambiente

### 1. Clonar o repositório

```bash
git clone <URL_DO_REPOSITORIO>
cd cnpj_airflow_pipeline
```

### 2. Criar o arquivo `.env`

Copie o arquivo de exemplo:

```bash
cp .env.example .env
```

No Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

### 3. Preencher a conexão com o PostgreSQL

Exemplo de `.env`:

```env
AIRFLOW_UID=50000
AIRFLOW_CONN_BANCO_POSTGRES={"conn_type":"postgres","host":"host.docker.internal","port":5432,"login":"postgres","password":"SUA_SENHA","schema":"SEU_BANCO"}
```

#### Observação importante

No código Python, o campo `schema` da connection é usado como `dbname` no `psycopg2.connect(...)`. Ou seja, nesse projeto, o campo `schema` deve conter **o nome do banco de dados**.



## Como subir o projeto

### 1. Construir a imagem customizada

```bash
docker compose build
```

### 2. Inicializar o ambiente

```bash
docker compose up -d
```

### 3. Acessar o Airflow

Abra no navegador:

```text
http://localhost:8081
```

> Se a porta `8081` já estiver em uso, ajuste o mapeamento no `docker-compose.yaml`.

---

## Fonte dos dados do CNPJ

- Os arquivos públicos podem ser baixados no portal da Receita Federal:

```text
https://arquivos.receitafederal.gov.br/index.php/s/gn672Ad4CF8N6TK?dir=/Dados/Cadastros/CNPJ
```

- Coloque os arquivos zipados do CNPJ dentro da pasta:

```text
cnpj_data/
```
- manter também o arquivo `distritos.csv` nessa mesma pasta.



## Como a pipeline funciona

O arquivo transform_cnpj.py executa nessa ordem:

1. conexão com o PostgreSQL via `banco_postgres`;
2. limpeza das tabelas de staging;
3. carga das tabelas pequenas;
4. carga da base auxiliar `distritos.csv`;
5. filtragem dos arquivos grandes para São Paulo capital;
6. carga nas tabelas de staging;
7. criação de índices auxiliares;
8. `ANALYZE` nas tabelas de staging;
9. execução do SQL final que recria a tabela `cnpj_final`.


## Agendamento da DAG

A DAG `cnpj_transform_mensal` está configurada com cron de **toda segunda-feira às 17h**, mas usa uma verificação adicional para permitir a execução apenas quando aquela segunda-feira é a **última do mês**.

Na prática, a transformação roda:

- **na última segunda-feira de cada mês**, às **17h**, no fuso `America/Sao_Paulo`.


A pipeline recria a tabela final `cnpj_final` a cada execução.


## Estrutura da tabela final `cnpj_final`

| Grupo | Colunas |
|---|---|
| **Identificação e estrutura do CNPJ** | `cnpj`, `cnpj_basico`, `matriz_filial`, `capital_social`, `ente_federativo_responsavel`, `razao_social`, `nome_fantasia` |
| **Datas e situação cadastral** | `data_inicio_atividades`, `situacao_cadastral`, `descricao_situacao_cadastral`, `data_situacao_cadastral`, `motivo_situacao_cadastral`, `descricao_motivo_situacao`, `situacao_especial`, `data_situacao_especial` |
| **Natureza jurídica e qualificação do responsável** | `natureza_juridica`, `descricao_natureza_juridica`, `qualificacao_responsavel`, `descricao_qualificacao_responsavel` |
| **Porte da empresa** | `porte_empresa`, `descricao_porte_empresa` |
| **Endereço** | `tipo_logradouro`, `logradouro`, `bairro`, `numero`, `cep`, `cep_formatado`, `uf`, `pais`, `municipio` |
| **CNAE** | `cnae_fiscal`, `cnae_subclasse`, `cnae_fiscal_secundaria`, `cnae_secao`, `cnae_divisao`, `cnae_grupo`, `cnae_classe`, `cnae_subclasse_codigo` |
| **Simples e MEI** | `opcao_simples`, `data_opcao_simples`, `data_exclusao_simples`, `opcao_mei`, `data_opcao_mei`, `data_exclusao_mei` |
| **Metadado de carga** | `data_dados` |
| **Geolocalização por CEP** | `distrito`, `subprefeitura`, `latitude`, `longitude` |



## Geolocalização por CEP

A base final é enriquecida com informação territorial a partir do arquivo `distritos.csv`.

### Como funciona

1. o CEP do estabelecimento é limpo para manter apenas dígitos;
2. o CEP do arquivo `distritos.csv` também é normalizado;
3. a associação entre estabelecimento e distrito é feita por CEP limpo;
4. quando há mais de uma ocorrência possível para o mesmo CEP, o SQL seleciona a combinação **mais frequente** de:
   - distrito
   - subprefeitura
   - latitude
   - longitude

### Resultado

A tabela `cnpj_final` passa a trazer, para cada CNPJ de São Paulo capital, uma localização territorial aproximada com:

- distrito;
- subprefeitura;
- latitude;
- longitude.

### Observação metodológica

Essa geolocalização é uma **aproximação por CEP**, não uma geocodificação ponto a ponto do endereço completo. Portanto, ela é muito útil para análise territorial agregada, mas não deve ser interpretada como coordenada exata do estabelecimento.


## Limitações e observações importantes

### 1. Escopo geográfico

A base final contempla apenas **São Paulo capital**.

### 2. Tabela de sócios

A transformação final **não trata a tabela de sócios**.

### 3. Dependência de estrutura de staging no PostgreSQL

A pipeline pressupõe a existência das tabelas de staging compatíveis com os `COPY` e com o SQL final. O projeto entrega a lógica de carga e transformação, mas o banco de destino deve estar preparado para receber essas cargas, para isso utilize o arquivo criacao_tabelas.sql

---

## Como executar manualmente a transformação

### Via container

```bash
docker compose exec airflow-scheduler python -c "from cnpj_pipeline.transform_cnpj import run; run()"
```

### Testar o import do módulo

```bash
docker compose exec airflow-scheduler python -c "from cnpj_pipeline.transform_cnpj import run; print('import ok')"
```

### Testar a conexão com o PostgreSQL

```bash
docker compose exec airflow-scheduler python -c "from cnpj_pipeline.transform_cnpj import get_connection; conn = get_connection(); print('conectou com sucesso'); conn.close()"
```

---

## Possíveis evoluções futuras

- automatizar a etapa de extração dos arquivos da Receita dentro da própria DAG;
- expandir a pipeline para outros municípios ou para o Brasil inteiro;
- adicionar testes automatizados e validações de qualidade dos dados.

---


