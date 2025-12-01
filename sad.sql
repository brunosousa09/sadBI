/* =============================================================================
PROJETO: DATA MART EPIDEMIOLÓGICO (SAD/BI)
AUTORES: Bruno Antônio de Sousa Costa; Maria Eduarda Alves do Nascimento; Iago Felipe Freire Nascimento
DESCRIÇÃO: Script completo de criação (DDL) e transformação (ELT)
           para análise de óbitos no PostgreSQL.
=============================================================================
*/

-- ==========================================================================
-- 1. LIMPEZA INICIAL (DROP TABLES)
-- ==========================================================================
DROP TABLE IF EXISTS public.fato_obitos CASCADE;
DROP TABLE IF EXISTS public.dim_tempo CASCADE;
DROP TABLE IF EXISTS public.stg_obitos_raw CASCADE; -- Opcional, se quiser recriar a staging

-- ==========================================================================
-- 2. CRIAÇÃO DA TABELA DE STAGING (ÁREA DE PALCO)
-- Esta tabela recebe os dados BRUTOS do CSV via PDI
-- ==========================================================================
CREATE TABLE public.stg_obitos_raw (
    ano_uid text,
    ano_nome text,
    local_uid text,
    local_nome text,
    indicador_uid text,
    indicador_nome text,
    categoria_uid text,
    categoria_nome text,
    estatistica_uid text,
    estatistica_nome text,
    lococor_uid text,
    lococor_nome text,
    atestante_uid text,
    atestante_nome text,
    grupoetario_uid text,
    grupoetario_nome text,
    racacor_uid text,
    racacor_nome text,
    sexo_uid text,
    sexo_nome text,
    abrangencia_uid text,
    abrangencia_nome text,
    localidade_uid text,
    localidade_nome text,
    janeiro text,
    fevereiro text,
    marco text,
    abril text,
    maio text,
    junho text,
    julho text,
    agosto text,
    setembro text,
    outubro text,
    novembro text,
    dezembro text,
    total_ano text
);

-- ==========================================================================
-- 3. CRIAÇÃO E POPULAÇÃO DA DIMENSÃO TEMPO
-- Gera calendário de 2019 a 2030 automaticamente
-- ==========================================================================
CREATE TABLE public.dim_tempo (
    sk_data integer NOT NULL PRIMARY KEY,
    data_completa date,
    ano integer,
    mes integer,
    nome_mes varchar(20),
    dia integer,
    trimestre integer,
    semestre integer,
    dia_semana varchar(20)
);

INSERT INTO public.dim_tempo
SELECT 
    to_char(datum, 'yyyymmdd')::INT AS sk_data,
    datum AS data_completa,
    EXTRACT(YEAR FROM datum)::INT AS ano,
    EXTRACT(MONTH FROM datum)::INT AS mes,
    CASE EXTRACT(MONTH FROM datum)
        WHEN 1 THEN 'Janeiro' WHEN 2 THEN 'Fevereiro' WHEN 3 THEN 'Março'
        WHEN 4 THEN 'Abril' WHEN 5 THEN 'Maio' WHEN 6 THEN 'Junho'
        WHEN 7 THEN 'Julho' WHEN 8 THEN 'Agosto' WHEN 9 THEN 'Setembro'
        WHEN 10 THEN 'Outubro' WHEN 11 THEN 'Novembro' WHEN 12 THEN 'Dezembro'
    END AS nome_mes,
    EXTRACT(DAY FROM datum)::INT AS dia,
    EXTRACT(QUARTER FROM datum)::INT AS trimestre,
    CASE WHEN EXTRACT(QUARTER FROM datum) <= 2 THEN 1 ELSE 2 END AS semestre,
    CASE EXTRACT(ISODOW FROM datum)
        WHEN 1 THEN 'Segunda' WHEN 2 THEN 'Terça' WHEN 3 THEN 'Quarta'
        WHEN 4 THEN 'Quinta' WHEN 5 THEN 'Sexta' WHEN 6 THEN 'Sábado' WHEN 7 THEN 'Domingo'
    END AS dia_semana
FROM generate_series(
    '2019-01-01'::DATE, 
    '2030-12-31'::DATE, 
    '1 day'::INTERVAL
) AS datum;

-- ==========================================================================
-- 4. CRIAÇÃO DA TABELA FATO
-- Tabela final otimizada para o Power BI
-- ==========================================================================
CREATE TABLE public.fato_obitos (
    id_fato SERIAL PRIMARY KEY,
    sk_data INT NOT NULL, -- FK para Dimensão Tempo
    
    -- Dimensões Degeneradas (Descritivos mantidos na Fato)
    localidade_uid TEXT,
    localidade_nome TEXT,
    categoria_uid TEXT,
    categoria_nome TEXT,
    grupoetario_uid TEXT,
    grupoetario_nome TEXT,
    sexo_uid TEXT,
    sexo_nome TEXT,
    racacor_uid TEXT,
    racacor_nome TEXT,
    
    -- Métricas
    quantidade_obitos INT,
    
    -- Constraint (Integridade Referencial)
    CONSTRAINT fk_fato_tempo FOREIGN KEY (sk_data) REFERENCES public.dim_tempo (sk_data)
);

-- ==========================================================================
-- 5. TRANSFORMAÇÃO DE DADOS (ELT)
-- Lê da Staging, Limpa, Despivota e Carrega na Fato
-- ==========================================================================
/*
   IMPORTANTE: Execute este bloco APÓS rodar o PDI para carregar a stg_obitos_raw.
   Se a tabela staging estiver vazia, este insert não fará nada.
*/

INSERT INTO public.fato_obitos (
    sk_data, 
    localidade_uid, localidade_nome, 
    categoria_uid, categoria_nome, 
    grupoetario_uid, grupoetario_nome, 
    sexo_uid, sexo_nome, 
    racacor_uid, racacor_nome, 
    quantidade_obitos
)
WITH limpeza AS (
    SELECT 
        -- Limpa o Ano (remove asteriscos)
        NULLIF(regexp_replace(ano_nome, '[^0-9]', '', 'g'), '')::INT AS ano_limpo,
        
        -- Colunas descritivas
        localidade_uid, localidade_nome,
        categoria_uid, categoria_nome,
        grupoetario_uid, grupoetario_nome,
        sexo_uid, sexo_nome,
        racacor_uid, racacor_nome,
        
        -- Limpa os números (remove pontos de milhar, trata nulos)
        COALESCE(NULLIF(REPLACE(janeiro, '.', ''), ''), '0')::INT AS jan,
        COALESCE(NULLIF(REPLACE(fevereiro, '.', ''), ''), '0')::INT AS fev,
        COALESCE(NULLIF(REPLACE(marco, '.', ''), ''), '0')::INT AS mar,
        COALESCE(NULLIF(REPLACE(abril, '.', ''), ''), '0')::INT AS abr,
        COALESCE(NULLIF(REPLACE(maio, '.', ''), ''), '0')::INT AS mai,
        COALESCE(NULLIF(REPLACE(junho, '.', ''), ''), '0')::INT AS jun,
        COALESCE(NULLIF(REPLACE(julho, '.', ''), ''), '0')::INT AS jul,
        COALESCE(NULLIF(REPLACE(agosto, '.', ''), ''), '0')::INT AS ago,
        COALESCE(NULLIF(REPLACE(setembro, '.', ''), ''), '0')::INT AS set,
        COALESCE(NULLIF(REPLACE(outubro, '.', ''), ''), '0')::INT AS out,
        COALESCE(NULLIF(REPLACE(novembro, '.', ''), ''), '0')::INT AS nov,
        COALESCE(NULLIF(REPLACE(dezembro, '.', ''), ''), '0')::INT AS dez
    FROM public.stg_obitos_raw
    WHERE ano_uid IS NOT NULL
)
SELECT 
    -- CORREÇÃO CRÍTICA: LPAD garante chave 20240101 (8 dígitos) e não 2024101
    (ano_limpo::TEXT || LPAD(mes_num::TEXT, 2, '0') || '01')::INT AS sk_data,
    
    localidade_uid, localidade_nome,
    categoria_uid, categoria_nome,
    grupoetario_uid, grupoetario_nome,
    sexo_uid, sexo_nome,
    racacor_uid, racacor_nome,
    
    quantidade
FROM limpeza
-- Unpivot (Transforma colunas em linhas)
CROSS JOIN LATERAL (
    VALUES 
    (1, jan), (2, fev), (3, mar), (4, abr), (5, mai), (6, jun),
    (7, jul), (8, ago), (9, set), (10, out), (11, nov), (12, dez)
) AS meses(mes_num, quantidade)
WHERE quantidade > 0; -- Remove linhas zeradas

-- ==========================================================================
-- 6. VALIDAÇÃO E SIMULAÇÕES (QUERIES DE BI)
-- ==========================================================================

-- 6.1 Conferência de Carga
SELECT count(*) as total_linhas FROM public.fato_obitos;

-- 6.2 Ranking Regional com Visualização (ASCII Art)
SELECT 
    RANK() OVER (ORDER BY SUM(quantidade_obitos) DESC) || 'º' as "Rank",
    localidade_nome as "Região",
    SUM(quantidade_obitos) as "Total Óbitos",
    REPEAT('█', (SUM(quantidade_obitos)::numeric / MAX(SUM(quantidade_obitos)) OVER () * 20)::int) as "Gráfico"
FROM public.fato_obitos
WHERE localidade_nome IN ('Norte', 'Nordeste', 'Centro-Oeste', 'Sudeste', 'Sul')
GROUP BY localidade_nome
ORDER BY 3 DESC;

-- 6.3 Tendência Mensal (Valida se o JOIN de tempo funcionou)
SELECT 
    d.ano,
    d.mes, 
    d.nome_mes,
    SUM(f.quantidade_obitos) as total_geral
FROM public.fato_obitos f
JOIN public.dim_tempo d ON f.sk_data = d.sk_data
GROUP BY d.ano, d.mes, d.nome_mes
ORDER BY d.ano DESC, d.mes DESC
LIMIT 12;