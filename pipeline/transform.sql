-- SCHEMAS
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;

-- STAGING
-- Otimização: Remoção de colunas desnecessárias (Telefone, Fax, etc)
CREATE OR REPLACE TABLE staging.stg_operadoras AS
SELECT 
    CAST(REGISTRO_OPERADORA AS VARCHAR) AS REG_ANS,
    CNPJ,
    Razao_Social AS NOME_RAZAO,
    Nome_Fantasia AS NOME_FANTASIA,
    Modalidade AS MODALIDADE,
    Cidade,
    UF
FROM raw.operadoras
WHERE REGISTRO_OPERADORA IS NOT NULL;

-- Otimização: Trazendo apenas os dados necessários pro dashboard
CREATE OR REPLACE TABLE staging.stg_produtos AS
SELECT
    CAST(ID_PLANO AS INTEGER) AS ID_PLANO,
    CD_PLANO,
    --NM_PLANO,
    CAST(REGISTRO_OPERADORA AS VARCHAR) AS REG_ANS,
    GR_CONTRATACAO AS TIPO_CONTRATACAO,
    COBERTURA,
    GR_SGMT_ASSISTENCIAL AS SEGMENTACAO,
    --ACOMODACAO_HOSPITALAR,
    SITUACAO_PLANO,
    ABRANGENCIA_COBERTURA AS ABRANGENCIA
FROM raw.produtos;

-- Otimização: Filtrando apenas registros com vidas ativas
CREATE OR REPLACE TABLE staging.stg_beneficiarios AS
SELECT
    -- Cria data base a partir da competência YYYY-MM
    CAST(ID_CMPT_MOVEL || '-01' AS DATE) AS DATA_COMPETENCIA,
    CD_OPERADORA AS REG_ANS,
    CD_PLANO,
    CAST(CD_MUNICIPIO AS INTEGER) AS CD_MUNICIPIO,
    NM_MUNICIPIO,
    SG_UF AS UF,
    CAST(QT_BENEFICIARIO_ATIVO AS INTEGER) AS QTD_VIDAS
FROM raw.beneficiarios
WHERE QT_BENEFICIARIO_ATIVO > 0;

-- Otimização: Ajustando tipagem numérica de saldos e descartando linhas zeradas
CREATE OR REPLACE TABLE staging.stg_financeiro AS
SELECT
    CAST(DATA AS DATE) AS DATA_COMPETENCIA,
    CAST(REG_ANS AS VARCHAR) AS REG_ANS,
    CAST(CD_CONTA_CONTABIL AS VARCHAR) AS CD_CONTA_CONTABIL,
    DESCRICAO,
    CAST(REPLACE(VL_SALDO_INICIAL, ',', '.') AS DECIMAL(18,2)) AS VL_SALDO_INICIAL,
    CAST(REPLACE(VL_SALDO_FINAL, ',', '.') AS DECIMAL(18,2)) AS VL_SALDO_FINAL
FROM raw.financeiro
WHERE CAST(REPLACE(VL_SALDO_INICIAL, ',', '.') AS DECIMAL(18,2)) != 0 
   OR CAST(REPLACE(VL_SALDO_FINAL, ',', '.') AS DECIMAL(18,2)) != 0;


-- DW DIMENSÕES

-- dw.dim_calendario
CREATE OR REPLACE TABLE dw.dim_calendario AS
SELECT
    ROW_NUMBER() OVER() AS SK_DATA,
    dt AS DATA,
    EXTRACT(YEAR FROM dt) AS ANO,
    EXTRACT(QUARTER FROM dt) AS TRIMESTRE,
    EXTRACT(MONTH FROM dt) AS MES,
    CASE EXTRACT(MONTH FROM dt)
        WHEN 1 THEN 'Janeiro'
        WHEN 2 THEN 'Fevereiro'
        WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril'
        WHEN 5 THEN 'Maio'
        WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho'
        WHEN 8 THEN 'Agosto'
        WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro'
        WHEN 11 THEN 'Novembro'
        WHEN 12 THEN 'Dezembro'
    END AS MES_NOME
FROM (
    -- Calendário estendido para pegar antes e depois de 2025/2026 (intervalo de 5 anos)
    SELECT generate_series AS dt 
    FROM generate_series(DATE '2023-01-01', DATE '2027-12-31', INTERVAL 1 DAY)
);

-- dw.dim_operadora
CREATE OR REPLACE TABLE dw.dim_operadora AS
SELECT 
    ROW_NUMBER() OVER(ORDER BY REG_ANS) AS SK_OPERADORA,
    REG_ANS,
    NOME_RAZAO,
    array_to_string(list_transform(string_split(trim(COALESCE(NOME_FANTASIA, NOME_RAZAO)), ' '), w -> upper(w[1]) || lower(w[2:])), ' ') AS NOME_FANTASIA,
    MODALIDADE,
    Cidade AS CIDADE,
    UF,
    CASE
        WHEN UPPER(NOME_RAZAO) LIKE '%UNIMED%' THEN 'Unimeds'
        ELSE 'Outros'
    END AS GRUPO_OPERADORA
FROM staging.stg_operadoras;

-- dw.dim_localidade
-- Conforme ADR-001 (Conformada), integrando Cidades reais das Sedes
CREATE OR REPLACE TABLE dw.dim_localidade AS
WITH locs AS (
    SELECT DISTINCT UF, NM_MUNICIPIO AS MUNICIPIO
    FROM staging.stg_beneficiarios
    UNION
    SELECT DISTINCT UF, array_to_string(list_transform(string_split(trim(CIDADE), ' '), w -> upper(w[1]) || lower(w[2:])), ' ') AS MUNICIPIO
    FROM staging.stg_operadoras
)
SELECT 
    ROW_NUMBER() OVER() AS SK_LOCALIDADE,
    UF,
    MUNICIPIO,
    CASE 
        WHEN UF IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') THEN 'Norte'
        WHEN UF IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste'
        WHEN UF IN ('DF', 'GO', 'MT', 'MS') THEN 'Centro-Oeste'
        WHEN UF IN ('ES', 'MG', 'RJ', 'SP') THEN 'Sudeste'
        WHEN UF IN ('PR', 'RS', 'SC') THEN 'Sul'
        ELSE 'Desconhecido'
    END AS REGIAO
FROM locs
WHERE UF IS NOT NULL;

-- dw.dim_produto
CREATE OR REPLACE TABLE dw.dim_produto AS
SELECT
    ROW_NUMBER() OVER(ORDER BY CD_PLANO) AS SK_PRODUTO,
    CD_PLANO AS ID_PRODUTO,
    CASE WHEN TIPO_CONTRATACAO IS NULL OR TRIM(TIPO_CONTRATACAO) = '' THEN 'Não Informado' ELSE TIPO_CONTRATACAO END AS TIPO_CONTRATACAO,
    CASE WHEN COBERTURA IS NULL OR TRIM(COBERTURA) = '' THEN 'Não Informado' ELSE COBERTURA END AS COBERTURA,
    CASE WHEN SEGMENTACAO IS NULL OR TRIM(SEGMENTACAO) = '' THEN 'Não Informado' ELSE SEGMENTACAO END AS SEGMENTACAO,
   -- CASE WHEN ACOMODACAO_HOSPITALAR IS NULL OR TRIM(ACOMODACAO_HOSPITALAR) = '' THEN 'Não Informado' ELSE ACOMODACAO_HOSPITALAR END AS ACOMODACAO_HOSPITALAR,
    CASE WHEN SITUACAO_PLANO IS NULL OR TRIM(SITUACAO_PLANO) = '' THEN 'Não Informado' ELSE SITUACAO_PLANO END AS SITUACAO_PLANO,
    CASE WHEN ABRANGENCIA IS NULL OR TRIM(ABRANGENCIA) = '' THEN 'Não Informado' ELSE ABRANGENCIA END AS ABRANGENCIA
FROM staging.stg_produtos;

-- dw.dim_conta_contabil
CREATE OR REPLACE TABLE dw.dim_conta_contabil AS
WITH base AS (
    SELECT DISTINCT 
        CD_CONTA_CONTABIL, 
        array_to_string(list_transform(string_split(trim(DESCRICAO), ' '), w -> upper(w[1]) || lower(w[2:])), ' ') AS DESCRICAO
    FROM staging.stg_financeiro
    WHERE CD_CONTA_CONTABIL LIKE '3%' OR CD_CONTA_CONTABIL LIKE '4%'
)
SELECT
    ROW_NUMBER() OVER(ORDER BY b.CD_CONTA_CONTABIL) AS SK_CONTA,
    b.CD_CONTA_CONTABIL,
    b.DESCRICAO,
    CASE 
        WHEN b.CD_CONTA_CONTABIL LIKE '3%' THEN 'Receita'
        WHEN b.CD_CONTA_CONTABIL LIKE '4%' THEN 'Despesa'
    END AS TIPO_CONTA,
    CASE 
        WHEN b.CD_CONTA_CONTABIL LIKE '31%' THEN 'Receita de Contraprestação'
        WHEN b.CD_CONTA_CONTABIL LIKE '41%' THEN 'Despesa Assistencial'
        ELSE 'Outras Contas'
    END AS GRUPO_CONTA,
    -- Hierarquia Plana para Drill-Down no PBI
    n1.DESCRICAO AS NIVEL_1_DESC,
    n2.DESCRICAO AS NIVEL_2_DESC,
    n3.DESCRICAO AS NIVEL_3_DESC,
    n4.DESCRICAO AS NIVEL_4_DESC
FROM base b
LEFT JOIN base n1 ON n1.CD_CONTA_CONTABIL = SUBSTRING(b.CD_CONTA_CONTABIL, 1, 1)
LEFT JOIN base n2 ON n2.CD_CONTA_CONTABIL = SUBSTRING(b.CD_CONTA_CONTABIL, 1, 2)
LEFT JOIN base n3 ON n3.CD_CONTA_CONTABIL = SUBSTRING(b.CD_CONTA_CONTABIL, 1, 3)
LEFT JOIN base n4 ON n4.CD_CONTA_CONTABIL = SUBSTRING(b.CD_CONTA_CONTABIL, 1, 4);


-- DW FATOS

-- dw.fact_financeiro
-- Otimização: Apenas Operadoras e Contas válidas, E EXCLUSIVAMENTE Contas Analíticas para evitar soma duplicada de Contas Sintéticas
CREATE OR REPLACE TABLE dw.fact_financeiro AS
WITH leaves AS (
    SELECT f.*
    FROM staging.stg_financeiro f
    WHERE NOT EXISTS (
        SELECT 1 
        FROM staging.stg_financeiro f2 
        WHERE f2.REG_ANS = f.REG_ANS 
          AND f2.DATA_COMPETENCIA = f.DATA_COMPETENCIA 
          AND f2.CD_CONTA_CONTABIL LIKE (f.CD_CONTA_CONTABIL || '_%')
    )
)
SELECT
    c.SK_DATA,
    o.SK_OPERADORA,
    ct.SK_CONTA,
    l.SK_LOCALIDADE,
    f.VL_SALDO_INICIAL,
    f.VL_SALDO_FINAL,
    (f.VL_SALDO_FINAL - f.VL_SALDO_INICIAL) AS VL_VARIACAO
FROM leaves f
INNER JOIN dw.dim_calendario c ON c.DATA = f.DATA_COMPETENCIA
INNER JOIN dw.dim_operadora o ON o.REG_ANS = f.REG_ANS
INNER JOIN dw.dim_conta_contabil ct ON ct.CD_CONTA_CONTABIL = f.CD_CONTA_CONTABIL
INNER JOIN dw.dim_localidade l ON l.UF = o.UF AND l.MUNICIPIO = array_to_string(list_transform(string_split(trim(o.CIDADE), ' '), w -> upper(w[1]) || lower(w[2:])), ' ');

-- dw.fact_beneficiarios
-- Otimização: Pré-Agroupamento para performance no PBI
CREATE OR REPLACE TABLE dw.fact_beneficiarios AS
SELECT
    c.SK_DATA,
    o.SK_OPERADORA,
    p.SK_PRODUTO,
    l.SK_LOCALIDADE,
    SUM(b.QTD_VIDAS) AS QTD_VIDAS
FROM staging.stg_beneficiarios b
INNER JOIN dw.dim_calendario c ON c.DATA = b.DATA_COMPETENCIA
INNER JOIN dw.dim_operadora o ON o.REG_ANS = b.REG_ANS
INNER JOIN dw.dim_produto p ON p.ID_PRODUTO = b.CD_PLANO
INNER JOIN dw.dim_localidade l ON l.UF = b.UF AND l.MUNICIPIO = b.NM_MUNICIPIO
GROUP BY c.SK_DATA, o.SK_OPERADORA, p.SK_PRODUTO, l.SK_LOCALIDADE;


-- EXPORTAR PRO PBI
COPY (SELECT * FROM dw.dim_calendario) TO 'data/dw/dim_calendario.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM dw.dim_operadora) TO 'data/dw/dim_operadora.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM dw.dim_localidade) TO 'data/dw/dim_localidade.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM dw.dim_produto) TO 'data/dw/dim_produto.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM dw.dim_conta_contabil) TO 'data/dw/dim_conta_contabil.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM dw.fact_financeiro) TO 'data/dw/fact_financeiro.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM dw.fact_beneficiarios) TO 'data/dw/fact_beneficiarios.parquet' (FORMAT PARQUET);
