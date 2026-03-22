# 📊 Case Analytics: Unimed Fortaleza - Painel 360°

[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://duckdb.org/)
[![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)]()
[![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)]()

Bem-vindo(a) ao meu portfólio de Engenharia e Análise de Dados! 

Este projeto é um **Data Warehouse end-to-end** construído a partir de dados abertos da **ANS** (Agência Nacional de Saúde Suplementar). O objetivo de negócios proposto pela demanda foi prover um **Painel Analítico 360°** da operadora Unimed Fortaleza e do mercado ativo, focando na concentração demográfica em mapas, inteligência de market share e saúde financeira baseada em sinistralidade.

---

## 🚀 Por que este projeto se destaca?

Se você é profissional da área técnica (Tech Recruiter, Tech Lead ou Data Engineer), aqui estão as principais soluções de engenharia implementadas que atestam meu domínio prático de arquitetura e qualidade:

- **Arquitetura ELT Moderna:** Substituição do tradicional processo de transformação em lote pelo Python em prol do ELT. O Python aqui serve puramente como maestro da ingestão (`Extract/Load`), escalando eficientemente gigabytes massivos graças ao motor vetorial do **DuckDB** para transformar os dados puramente via SQL transacional.
- **Modelagem Star Schema Fidedigna:** O Data Warehouse obedece aos pilares da modelagem dimensional (Kimball) contendo Dimensões Conformadas (Localidade) e criação de identificadores únicos sintéticos (*Surrogate Keys*).
- **Hierarquia Plana Contábil (Flattened Hierarchy):** Tratamento denso em contas DRE de escopo Pai-Filho. Planifiquei o nível hierárquico no sub-motor do DW (`dw.dim_conta_contabil`) para que as consultas DAX/PBI no front-end do painel não sofram delay computacional para renderizar drill-downs.
- **Filosofia Lean BI & Performance:** Técnicas de rebarbação avançadas. Filtros aplicados durante a etapa `staging` (Early Drop) expurgam saldos zerados da conta e excluem clientes inválidos, reduzindo o volume de processamento.
- **Interoperabilidade:** Todo o output final é injetado localmente sob formatação **Apache Parquet com compressão SNAPPY**. Resultando que o armazenamento consumido perante o CSV tradicional caia substancialmente com ganhos no tempo de varredura das engines analíticas do Dashboard.

---

## 🛠️ Arquitetura e Fluxo de Dados

A infraestrutura foi construída para funcionar sem dependências de subscrições ativas Cloud (puramente local) evidenciando que eficiência se faz no código antes do processador:

1. **Ingestão (`ingest.py`):** Lemos os CSVs contábeis e de cadastro distribuídos pela ANS e enviamos sem filtros para uma Database temporária `raw` garantindo tolerância a falhas ou corrompimentos.
2. **Transformação (`transform.sql`):** Processo analítico SQL limpo de milhares de inserções que modela, refina e audita nulos e distribui em visões `staging` (dados transmutáveis) e finalmente `dw` (Fatos e Dimensões prontas).
3. **Integração:** Os dados da base são extraídos em pacotes `.parquet` compactados numa sub-folder isolada, simulando um Bucket S3 da Amazon.
4. **Data Visualization:** O Microsoft Power BI ingesta nativamente Parquet. Sem nenhuma complexidade matemática sobreposta no PBI, seu único foco se consolida apenas em navegação temporal e Data Storytelling.

---

## 📂 Organização do Repositório

O repositório exposto exclui arquivos temporários ou testes isolados no `.gitignore` e reflete exatamente uma arquitetura madura pronta para ambientes _Production_:

```text
project/
├── data/
│   └── raw/                  ← Input: Onde os CSVs da ANS originais devem repousar
├── db/
│   └── unimed.duckdb         ← O banco de dados em arquivo único alocado pelo DuckDB
├── docs/                     ← Rota de Documentações Técnicas, Arquitetura e Governança Mestre
├── pipeline/
│   ├── ingest.py             ← Script orquestrador de Carga
│   └── transform.sql         ← Arquivo central detentor das regras do ELT
└── notebooks/
    └── exploratory.ipynb     ← Rascunho das análises iniciais descritivas na massa da ANS
```

---

## 📋 Como Avaliar Tecnicamente

Todo o racional de modelagem detalhada, justificativas de adoção de _Features_ (`ADRs`) e documentações do pipeline encontram-se sumarizados na pasta **`docs/`**. Sugiro que um Tech Lead examine-os, em ênfase o `transform_technical_docs.md`.

**Replicação do Pipeline Local:**
```bash
# 1. Active os requerimentos
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt

# 2. Insira os 4 datasets da ANS requisitados em `data/raw/`

# 3. Dispare a Ingestão. O script Python interliga com a pipeline transformacional SQL.
python pipeline/ingest.py
```
