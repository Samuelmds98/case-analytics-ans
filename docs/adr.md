# Architecture Decision Records (ADRs)

Este documento centraliza e documenta todas as decisões importantes de fluxo de projeto, abordagens de engenharia e modelagens submetidas ao longo do desenvolvimento do painel.

---

## ADR-001: Dimensão Conformada para Localidade

### Contexto
O modelo possui duas tabelas fato (`fact_financeiro` e `fact_beneficiarios`) e ambas podem precisar ser filtradas por atributos geográficos (UF, Região, Município). Haviam duas opções:
1. Snowflake Parcial, apontando para `dim_operadora` e derivando atributos geográficos dela.
2. Dimensão conformada externa.

### Decisão
Criar uma **dimensão conformada** `dim_localidade` como tabela independente que possui `SK_LOCALIDADE` atribuído a cada fato.

### Justificativa
Mantém o *Star Schema* puro, excelente padrão no Microsoft Power BI para evitar *joins* em cadeia, e permite agilidade e performance do motor DAX, sem duplicação ou divergência de lógica entre dados financeiros e os de beneficiários.

---

## ADR-002: ELT sobre ETL Tradicional 

### Decisão
Aplicar as cargas brutas pelo Python sem nenhuma transformação. O trabalho de conformação (`staging` para `dw`) é executado em puro SQL pelo motor do **DuckDB**.

### Justificativa
O processo garante separação de responsabilidades (Ingestão vs. Transformação) e permite reprocessamento robusto e veloz da camada `raw` sem a necessidade de efetuar chamadas de I/O de rede ou disco novamente.

---

## ADR-003: Hierarquia Contábil Plana (Flattened Hierarchy)

### Contexto
O balancete advindo da ANS tem dados hierárquicos em Contas Pai e Filho, e montar esse mapeamento usualmente exige fórmulas como `PATH()` em DAX no Power BI.

### Decisão
Resolver e planificar localmente (Flat Hierarchy) a linhagem contábil direto no script `transform.sql`, usando *Left Joins* sucessivos via `SUBSTRING` do código ANS para inferir o nível 1, 2, 3 e 4 das contas.

### Justificativa
Isso retira toda a carga computacional e de processamento do ecossistema front-end (Power BI), trazendo a regra de negócio consolidada para o banco DW, resultando numa performance de visualização melhor no painel.

---

## ADR-004: Deduplicação Contábil Exclusiva por Nível Folha

### Contexto
Dificuldades com contas Sintéticas distorcendo o saldo financeiro bruto. Quando os valores de contas Paí eram integrados junto dos arquivos Filhos ocorria contagem em dobro nos gráficos do report de Saúde Financeira.

### Decisão
Filtragem forçada de granularidade restritamente **Folha** (Leaves). No `transform.sql`, validamos se um `CD_CONTA_CONTABIL` tem um registro dependente. Se houver, a linha é tratada como "Sintética" e suprimida pela cláusula `NOT EXISTS` antes de integrar na `fact_financeiro`.

### Justificativa
Essencial para prevenir acúmulo financeiro estourado e garantir integridade real dos dados e variância descrita no painel DRE, além de limpar o banco e deixar a visualização no Front-End mais linear.

---

## ADR-005: "Lean BI" — Poda e Descarte Cedo (Early Drop)  

### Contexto
A ANS envia volumes extensivos de dados por CSV contendo diversas colunas sem utilidade para a regra de negócio proposta, além de beneficiários sem status ativo ou com volume defasado monetariamente `0`.

### Decisão
Efetuar poda agressiva no passo do SQL de `staging` e carregar em Fatos estritamente dados qualificados, onde:
- Eliminar telefones e colunas genéricas na ingestão de Operadoras.
- Descartar vidas diferentes de ativas ou canceladas prematuramente.
- Filtrar saldos zerados (`0,00`) logo no ingestão do financeiro.

### Justificativa
Evolui amplamente a capacidade de escaneamento colunar nativo do DuckDB e diminui o tamanho binário para arquivamento no export dos Parquets para o modelo Semântico do PowerBI. O foco analítico do projeto preza inteligência ao invés de retenção bruta.