# Código e Governança: Blueprint Mestre

Este blueprint documenta os padrões, boas práticas analíticas, governança de código e gestão contínua de entrega no projeto Case Analytics Unimed.

---

## 1. Padrões de Código (Clean Code Strategy)

### 1.1 Modelagem e Linguagem (SQL)
- **Convenção de Nomes:** Tabelas, schemas e colunas do SQL devem utilizar exclusivamente `snake_case`. Exemplo: `stg_operadoras`, `dim_calendario`.
- **Identificação de Schemas:** 
  - `raw`: Dados sem processamento (Ingestão python livre de SQL de alteração).
  - `staging`: Tabelas prefixadas sob `stg_` - aplicável a normalização, casting e recortes.
  - `dw`: Exclusivo para Tabelas Fato e Dimensões. As Tabelas devem ter o prefixo padronizado `fact_` ou `dim_`.
- **Clareza Qualificadora (Aliases):** Todo join relacional, e.g. `INNER JOIN dim_operadora o`, tem obrigação de ter um Alias de fácil localização. Ao listar colunas de um join no `SELECT`, **todas as colunas base devem referenciar explicitamente seu Alias de origem** (ex: `o.SK_OPERADORA`, `l.SK_LOCALIDADE`).

### 1.2 Programação em Python
- **Padrão PEP-8:** Nome de variáveis nativas locais em `snake_case`. 
- **Idioma:** Código e definições (classes, funções, diretórios) primariamente escritos em Inglês. Docstrings e comentários explícitos em PT-BR.
- **Isolamento de Estado:** Todos os códigos devem tratar seus escopos individualizados, evitando poluir ambientes globais que impeçam o fechamento correto do lock de banco do processo DuckDB.

---

## 2. Governança e Metodologia de DW

### 2.1 Garantia de Idempotência
Todos os scripts do ecossistema ELT (Python para Extração/Mock do Banco e DuckDB scripts para Staging e DW) devem assumir uma característica descartável e reconstruível por tempo integral. 
Módulos construtores dentro do `transform.sql` devem sempre seguir o verbete **`CREATE OR REPLACE TABLE`** em vez de `CREATE TABLE`. Reprocessar competências não pode provocar duplicações.

### 2.2 Estratégia "T-Shirt Sizing" de Integridade
Conforme justificado, as tabelas não sofrem validadores locais como chaves primárias ou restrições `NOT NULL` do banco DuckDB.
A coerência na linhagem está nas metodologias corretas de cruzamento (`INNER JOIN` onde obrigatório, `LEFT JOIN` nas tabelas com dados soltos) durante as etapas SQL de construção das fatos. Campos que não devem ser nulos devem ser tratados nos filtros `WHERE` da staging.

---

## 3. Normatização de Surrogate Keys
O projeto faz uso do modelo de **Surrogate Keys (SK)** para toda abstração contextual nas tabelas analíticas (`dw` Schema).  
O padrão rege que a Surrogate Key deve sempre estar listada na posição de **1ª Coluna de Definição** de uma dimensão. E a convenção de nome dita o prefixo literal `SK_`, como por exemplo: `SK_DATA`, `SK_OPERADORA`.  
Nenhuma SK deve se portar com caráter de dado transacional inteligente (ela é puramente associativa para o Front-End PBI). Informações brutas (chaves naturais como CNPJ, `CD_CONTA_CONTABIL`, Codigo ANS) persistem localmente como características textuais/categóricas anexas nos conjuntos subsequentes da modelagem.

---

## 4. Estrutura e Controle de Versionamento

```text
├── data/
│   ├── raw/                 # Ignorado no .gitignore (contém volumetria bruta não criptografada de clientes)
│   └── dw/                  # Local dos snapshots .parquet compactados preparados pelo Data Warehouse
├── docs/                    # Base de governança, arquitetura e decisões de fluxo
├── db/                      # Arquivos transacionais (.duckdb) descartáveis e de tráfego local 
└── pipeline/                # Single source of truth de ETL e ELT executáveis
```

- Nenhum arquivo local transacional CSV, `.parquet` ou banco persistente (`.duckdb` ou `.db`) deve possuir versionamento nos repositórios git e devem preferencialmente residir em áreas isoladas (.gitignore). A rastreabilidade é exclusiva aos artefatos versionáveis com scripts (`.sql`, `.py`, `.md`).
