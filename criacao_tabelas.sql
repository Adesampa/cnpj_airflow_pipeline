-- Esse arquivo deve ser rodado apenas uma vez para criar as tabelas auxiliares necessárias para o processo de ETL
-- Rode isso no seu postgres local

-- CNAE
CREATE TABLE IF NOT EXISTS stg_cnae (
    codigo TEXT,
    descricao TEXT
);

-- EMPRESAS
CREATE TABLE IF NOT EXISTS stg_empresas (
    cnpj_basico TEXT,
    razao_social TEXT,
    natureza_juridica TEXT,
    qualificacao_responsavel TEXT,
    capital_social TEXT,
    porte_empresa TEXT,
    ente_federativo_responsavel TEXT
);

-- ESTABELECIMENTOS
CREATE TABLE IF NOT EXISTS stg_estabelecimentos (
    cnpj_basico TEXT,
    cnpj_ordem TEXT,
    cnpj_dv TEXT,
    matriz_filial TEXT,
    nome_fantasia TEXT,
    situacao_cadastral TEXT,
    data_situacao_cadastral TEXT,
    motivo_situacao_cadastral TEXT,
    nome_cidade_exterior TEXT,
    pais TEXT,
    data_inicio_atividades TEXT,
    cnae_fiscal TEXT,
    cnae_fiscal_secundaria TEXT,
    tipo_logradouro TEXT,
    logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cep TEXT,
    uf TEXT,
    municipio TEXT,
    ddd1 TEXT,
    telefone1 TEXT,
    ddd2 TEXT,
    telefone2 TEXT,
    ddd_fax TEXT,
    fax TEXT,
    correio_eletronico TEXT,
    situacao_especial TEXT,
    data_situacao_especial TEXT
);

-- MOTIVO
CREATE TABLE IF NOT EXISTS stg_motivo (
    codigo TEXT,
    descricao TEXT
);

-- MUNICIPIO
CREATE TABLE IF NOT EXISTS stg_municipio (
    codigo TEXT,
    descricao TEXT
);

-- NATUREZA JURIDICA
CREATE TABLE IF NOT EXISTS stg_natureza_juridica (
    codigo TEXT,
    descricao TEXT
);

-- PAIS
CREATE TABLE IF NOT EXISTS stg_pais (
    codigo TEXT,
    descricao TEXT
);

-- QUALIFICACAO SOCIO
CREATE TABLE IF NOT EXISTS stg_qualificacao_socio (
    codigo TEXT,
    descricao TEXT
);

-- SIMPLES
CREATE TABLE IF NOT EXISTS stg_simples (
    cnpj_basico TEXT,
    opcao_simples TEXT,
    data_opcao_simples TEXT,
    data_exclusao_simples TEXT,
    opcao_mei TEXT,
    data_opcao_mei TEXT,
    data_exclusao_mei TEXT
);