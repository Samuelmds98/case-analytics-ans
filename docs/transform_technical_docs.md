# Documentação Técnica: `transform.sql`

Este documento detalha estritamente o pipeline de transformação analítica implementado no script `pipeline/transform.sql`. O script foi codificado na linguagem SQL (dialeto DuckDB) assumindo operações locais e o padrão **ELT** (Extract, Load, Transform).

---

## 1. Visão Estrutural

O script é dividido em 5 etapas principais:
1. **Definição de Schemas:** Criação dos namespaces lógicos `staging` e `dw`.
2. **Camada Staging:** Sanitização, formatação, filtros de aderência e deduplicação primária dos dados em `raw`.
3. **Dimensões (DW):** Regras de negócio de modelagem e qualificação de entidades.
4. **Fatos (DW):** Cruzamento e sumarização de tabelas métricas e quantitativas.
5. **Exportação Parquet:** Geração dos subprodutos locais em alta compressão (SNAPPY) para o Power BI.

---

## 2. Camada de Preparação (Staging)

Tabelas na camada de `staging` filtram lixo e adequam os Data Types com o prefixo `stg_`.

| Tabela | Otimização Aplicada |
| --- | --- |
| `stg_operadoras` | Remoção de colunas redundantes e não analíticas (como telefones, endereço completo e fax). |
| `stg_produtos` | Filtro limitando apenas as propriedades de produto exigidas pelos requisitos de visualização. |
| `stg_beneficiarios` | **Filtro vital:** Descarta beneficiários inativos (apenas onde `QT_BENEFICIARIO_ATIVO > 0`). Cria base `DATA_COMPETENCIA` do padrão YYYY-MM para o 1º dia do mês. |
| `stg_financeiro` | Parser financeiro nativo numérico (`REPLACE(',', '.')`), e expurgo (Lean BI) excluindo da camada registros nulos (`VL_SALDO_INICIAL` e `FINAL` iterativamente = 0). |

---

## 3. Construção de Dimensões (`dw.dim_*`)

Construção final das abstrações e metadados contextuais, usando chaves sequenciais via subquery `ROW_NUMBER() OVER()` (`SK_`).

* **`dim_calendario`**: Script gerador temporal estendendo dinamicamente o range (`generate_series`) para preencher anos analíticos ativos (2023-2027) com trimestres (`QUARTER`) e nomes de meses consolidados.
* **`dim_operadora`**: Padronização em Propel / *Title Case* ativando `array_to_string/list_transform` de forma vetorial em SQL puro. Deriva regras de mercado calculando `GRUPO_OPERADORA` ('Unimeds' vs 'Outros').
* **`dim_localidade`**: Dimensão Completamente conformada gerando mesclagens de cidades vindas tanto das OPs (Sedes Operadoras) quanto dos Munincipio de Beneficiários pelo `UNION`, e mapeando Regiões ('Sul', 'Nordeste') via Hardcode de UFs.
* **`dim_produto`**: Valida nulos injetando 'Não Informado' genericamente via tratativas `CASE WHEN` para garantir preenchimento homogêneo no PowerBI.
* **`dim_conta_contabil`**:
   * Implementação de *Flattened Hierarchy* (Hierarquia Plana) via Left Join em série (`SUBSTRING` em string parentesco 1 a 4) poupando totalmente o processador DAX do PBI de renderizar visualizações aninhadas.
   * Filtros de apenas Contas Sintéticas da classe '3' (Receitas) e '4' (Despesas). 

---

## 4. Construção de Tabelas Fato (`dw.fact_*`)

As tabelas fatos não recebem integridade formal, o foco é modelagem OLAP, sem necessidade da integridade transacional de modelos OLTP.

* **`fact_financeiro`**:
   * **Bloqueador de Duplicação Financeira**: Implementa técnica via `NOT EXISTS` associado a correspondência pai-filho (`LIKE (f.CD_CONTA_CONTABIL || '_%')`). Garante que apenas **Contas Folha** (Accounts Leaves - nível estritamente analítico sem descendentes) cheguem à fato.
   * Deriva indicador direto Subtrativo `VL_VARIACAO` (Delta do $).
* **`fact_beneficiarios`**:
   * **Pré-agregação (Sumarização)**: A fato executa o recuo de carga sumarizando `SUM(QTD_VIDAS) ... GROUP BY` pelas SKs dimensionais, comprimindo vertiginosamente o DW e facilitando a carga no dataviz.

---

## 5. Exportação para Power BI

Utilizado a formatação Apache **Parquet**.
O script descarrega dinamicamente a cláusula local `COPY (SELECT * FROM dw...) TO ...` com otimização SNAPPY, direcionado diretamente no Path `data/dw/`. 
A ausência do formato nativo da Microsoft e do CSV justifica-se na superioridade dos binários `.parquet` no modelo de dados local.
