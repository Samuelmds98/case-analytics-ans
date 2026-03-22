# Arquitetura do Projeto — Case Analytics Unimed

Este documento descreve a arquitetura de dados e de fluxo do projeto, evidenciando as camadas de processamento, as tecnologias envolvidas e a modelagem final.

## 1. Visão Geral da Arquitetura (ELT)

O projeto adota a arquitetura **ELT (Extract, Load, Transform)**, priorizando o uso de SQL para transformações através do motor analítico DuckDB. 

Abaixo, um diagrama Mermaid ilustrando o fluxo de dados desde os arquivos brutos da ANS até a visualização no Power BI:

```mermaid
flowchart TD
    subgraph Fontes_ANS["Fontes de Dados (ANS)"]
        A1[CSVs de Operadoras]
        A2[CSVs de Produtos]
        A3[CSVs de Beneficiários]
        A4[CSVs Financeiros/DRE]
    end

    subgraph Data_Lake_Local["Local Data Layer"]
        RAW[data/raw/]
    end

    subgraph Ingestao["Ingestão - Python"]
        PY[Scripts Python ingest.py]
    end

    subgraph Engine_DuckDB["DuckDB Engine"]
        SchemaRaw[(Schema: raw)]
        SchemaStg[(Schema: staging)]
        SchemaDW[(Schema: dw)]
        
        SchemaRaw -->|Filtro & Tipagem| SchemaStg
        SchemaStg -->|Modelagem Dimensional| SchemaDW
    end

    subgraph Armazenamento_Otimizado[Exportação Data Lake Layer]
        Parquet[Arquivos .parquet SNAPPY]
    end

    subgraph BI_Storytelling["Data Visualization"]
        PBI[Power BI]
    end

    Fontes_ANS --> RAW
    RAW --> PY
    PY -->|Cópia 1:1 sem transformação| SchemaRaw
    SchemaDW -->|Export Parquet| Parquet
    Parquet --> PBI

    %% Estilização
    style Engine_DuckDB fill:#F5EC42,stroke:#B5A214,color:black
    style PBI fill:#F2C811,stroke:#E0AC00,color:black
    style Parquet fill:#43A047,stroke:#1B5E20,color:white
```

### Tecnologias Utilizadas

- **Fonte Receptora:** Arquivos CSV/Text.
- **Orquestração e Ingestão:** Python puro.
- **Motor Analítico (Processamento e DW):** DuckDB (local único arquivo `unimed.duckdb`).
- **Formato Otimizado de Armazenamento:** Apache Parquet (compressão SNAPPY).
- **Consumo Visual Analytics:** Microsoft Power BI.

---

## 2. Modelagem de Dados — Star Schema

A modelagem de dados no Data Warehouse (schema `dw`) foi formatada sob o padrão **Star Schema**, com base na metodologia de consolidação de dimensões conformadas.

```mermaid
erDiagram
    fact_financeiro {
        INTEGER SK_DATA FK
        INTEGER SK_OPERADORA FK
        INTEGER SK_CONTA FK
        INTEGER SK_LOCALIDADE FK
        DECIMAL VL_SALDO_INICIAL
        DECIMAL VL_SALDO_FINAL
        DECIMAL VL_VARIACAO
    }
    
    fact_beneficiarios {
        INTEGER SK_DATA FK
        INTEGER SK_OPERADORA FK
        INTEGER SK_PRODUTO FK
        INTEGER SK_LOCALIDADE FK
        INTEGER QTD_VIDAS
    }
    
    dim_calendario {
        INTEGER SK_DATA PK
        DATE DATA
        INTEGER ANO
        INTEGER TRIMESTRE
        INTEGER MES
        VARCHAR MES_NOME
    }
    
    dim_operadora {
        INTEGER SK_OPERADORA PK
        VARCHAR REG_ANS
        VARCHAR NOME_RAZAO
        VARCHAR NOME_FANTASIA
        VARCHAR MODALIDADE
        VARCHAR GRUPO_OPERADORA
    }
    
    dim_localidade {
        INTEGER SK_LOCALIDADE PK
        VARCHAR UF
        VARCHAR MUNICIPIO
        VARCHAR REGIAO
    }
    
    dim_produto {
        INTEGER SK_PRODUTO PK
        VARCHAR ID_PRODUTO
        VARCHAR TIPO_CONTRATACAO
        VARCHAR ABRANGENCIA
        VARCHAR SEGMENTACAO
        VARCHAR COBERTURA
        VARCHAR SITUACAO_PLANO
    }
    
    dim_conta_contabil {
        INTEGER SK_CONTA PK
        VARCHAR CD_CONTA_CONTABIL
        VARCHAR DESCRICAO
        VARCHAR TIPO_CONTA
        VARCHAR GRUPO_CONTA
        VARCHAR NIVEL_1_DESC
        VARCHAR NIVEL_2_DESC
        VARCHAR NIVEL_3_DESC
        VARCHAR NIVEL_4_DESC
    }

    fact_financeiro }o--|| dim_calendario : ref
    fact_financeiro }o--|| dim_operadora : ref
    fact_financeiro }o--|| dim_conta_contabil : ref
    fact_financeiro }o--|| dim_localidade : ref

    fact_beneficiarios }o--|| dim_calendario : ref
    fact_beneficiarios }o--|| dim_operadora : ref
    fact_beneficiarios }o--|| dim_produto : ref
    fact_beneficiarios }o--|| dim_localidade : ref
```

### Características Chaves da Modelagem:
- **Ausência de Constraints Físicas:** Não existem Primary Keys, Foreign Keys ou `NOT NULL` nas tabelas finais. O DuckDB no perfil "Data Warehouse" é otimizado para não gastar recursos validando os dados na carga final. A integridade existe devido à lógica validada na camada de Transformação.
- **Campos de Chave Surrogate:** Todas as tabelas fato ligam-se às dimensões por uma Surrogate Key gerada sequencialmente (`SK_NOME`).
- **Idempotência:** Todo o modelo é gerado a partir de `CREATE OR REPLACE TABLE`, tornando a execução reprocessável do início ao fim sem risco de duplicação.
