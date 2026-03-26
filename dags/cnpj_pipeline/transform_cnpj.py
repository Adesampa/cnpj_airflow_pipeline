import os
import csv
import zipfile
import tempfile
import psycopg2
from pathlib import Path
from airflow.models.connection import Connection

DATA_DIR = Path("/opt/airflow/cnpj_data")
SQL_PATH = Path("/opt/airflow/dags/cnpj_pipeline/sql/transform.sql")

MUNICIPIO_SP = "7107"

# nomes das tabelas pequenas que serão carregadas completas
SMALL_FILE_MAP = {
    "CNAECSV": "public.stg_cnae",
    "MOTICSV": "public.stg_motivo",
    "MUNICCSV": "public.stg_municipio",
    "NATJUCSV": "public.stg_natureza_juridica",
    "PAISCSV": "public.stg_pais",
    "QUALSCSV": "public.stg_qualificacao_socio",
}

# tabela distritos fora do padrão da Receita
DISTRITOS_FILE = "distritos.csv"

CONN_ID = "banco_postgres"

def get_connection():
    os.environ["PGPASSFILE"] = "/dev/null"

    conn_cfg = Connection.get_connection_from_secrets(CONN_ID)

    conn = psycopg2.connect(
        host=conn_cfg.host,
        user=conn_cfg.login,
        password=conn_cfg.password,
        dbname=conn_cfg.schema,
        port=conn_cfg.port,
    )
    conn.autocommit = True
    return conn


def unzip_files():
    """
    Descompacta todos os ZIPs em DATA_DIR.
    """
    for file in os.listdir(DATA_DIR):
        if file.endswith(".zip"):
            zip_path = DATA_DIR / file
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(DATA_DIR)
            print(f"Descompactado: {file}", flush=True)


def clear_staging_tables(conn):
    """
    Limpa staging tables antes da nova carga.
    Faz TRUNCATE tabela por tabela para facilitar diagnóstico.
    """
    tables = [
        "public.stg_cnae",
        "public.stg_empresas",
        "public.stg_estabelecimentos",
        "public.stg_motivo",
        "public.stg_municipio",
        "public.stg_natureza_juridica",
        "public.stg_pais",
        "public.stg_qualificacao_socio",
        "public.stg_simples",
        "public.distritos",
    ]

    with conn.cursor() as cur:
        cur.execute("SET lock_timeout = '10s';")

        for table in tables:
            print(f"Limpando {table}...", flush=True)
            try:
                cur.execute(f"TRUNCATE TABLE {table};")
                print(f"OK: {table}", flush=True)
            except Exception as e:
                print(f"ERRO ao limpar {table}: {e}", flush=True)
                raise

    print("Staging tables limpas", flush=True)


def find_files_by_pattern(pattern_substring):
    """
    Retorna lista de arquivos em DATA_DIR cujo nome contém pattern_substring.
    """
    files = []
    for name in sorted(os.listdir(DATA_DIR)):
        path = DATA_DIR / name
        if path.is_file() and pattern_substring in name:
            files.append(path)
    return files


def copy_file_to_table(conn, file_path, table_name, delimiter=";", header=False):
    """
    Carrega um arquivo já limpo para o Postgres via COPY.
    """
    with conn.cursor() as cur:
        copy_sql = f"""
            COPY {table_name}
            FROM STDIN
            WITH (
                FORMAT CSV,
                DELIMITER '{delimiter}',
                HEADER {'TRUE' if header else 'FALSE'},
                NULL '',
                QUOTE '"',
                ESCAPE '"'
            )
        """
        with open(file_path, "r", encoding="utf-8", newline="") as f:
            cur.copy_expert(copy_sql, f)

    conn.commit()
    print(f"Carregado {file_path} → {table_name}", flush=True)


def normalize_text(value: str) -> str:
    """
    Remove bytes nulos e normaliza quebras de linha internas.
    """
    if value is None:
        return ""
    return value.replace("\x00", "").replace("\r", " ").replace("\n", " ").strip()


def filter_estabelecimentos_sp(estabele_files, output_file):
    """
    Lê todos os arquivos ESTABELE, filtra apenas municipio == 7107,
    grava em um CSV temporário limpo e retorna o conjunto de cnpj_basico.
    """
    cnpj_basicos_sp = set()
    total_in = 0
    total_out = 0

    with open(output_file, "w", encoding="utf-8", newline="") as out_f:
        writer = csv.writer(
            out_f,
            delimiter=";",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )

        for file_path in estabele_files:
            print(f"Filtrando estabelecimentos SP: {file_path.name}", flush=True)

            with open(file_path, "r", encoding="latin1", newline="") as in_f:
                reader = csv.reader(in_f, delimiter=";", quotechar='"')

                for row in reader:
                    total_in += 1

                    # remove bytes/linhas problemáticas
                    row = [normalize_text(col) for col in row]

                    # estabelecimentos deve ter 30 colunas
                    if len(row) != 30:
                        continue

                    municipio = row[20]
                    if municipio != MUNICIPIO_SP:
                        continue

                    cnpj_basico = row[0]
                    writer.writerow(row)

                    cnpj_basicos_sp.add(cnpj_basico)
                    total_out += 1

    print(
        f"Estabelecimentos lidos: {total_in:,} | SP gravados: {total_out:,} | "
        f"CNPJs básicos únicos: {len(cnpj_basicos_sp):,}",
        flush=True
    )

    return cnpj_basicos_sp


def filter_empresas_by_cnpj(empresas_files, cnpj_basicos, output_file):
    """
    Filtra empresas pelos cnpj_basico encontrados em SP.
    """
    total_in = 0
    total_out = 0

    with open(output_file, "w", encoding="utf-8", newline="") as out_f:
        writer = csv.writer(
            out_f,
            delimiter=";",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )

        for file_path in empresas_files:
            print(f"Filtrando empresas: {file_path.name}", flush=True)

            with open(file_path, "r", encoding="latin1", newline="") as in_f:
                reader = csv.reader(in_f, delimiter=";", quotechar='"')

                for row in reader:
                    total_in += 1
                    row = [normalize_text(col) for col in row]

                    if len(row) != 7:
                        continue

                    if row[0] in cnpj_basicos:
                        writer.writerow(row)
                        total_out += 1

    print(
        f"Empresas lidas: {total_in:,} | gravadas: {total_out:,}",
        flush=True
    )


def filter_simples_by_cnpj(simples_files, cnpj_basicos, output_file):
    """
    Filtra simples pelos cnpj_basico encontrados em SP.
    """
    total_in = 0
    total_out = 0

    with open(output_file, "w", encoding="utf-8", newline="") as out_f:
        writer = csv.writer(
            out_f,
            delimiter=";",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )

        for file_path in simples_files:
            print(f"Filtrando simples: {file_path.name}", flush=True)

            with open(file_path, "r", encoding="latin1", newline="") as in_f:
                reader = csv.reader(in_f, delimiter=";", quotechar='"')

                for row in reader:
                    total_in += 1
                    row = [normalize_text(col) for col in row]

                    if len(row) != 7:
                        continue

                    if row[0] in cnpj_basicos:
                        writer.writerow(row)
                        total_out += 1

    print(
        f"Simples lidos: {total_in:,} | gravados: {total_out:,}",
        flush=True
    )



def load_small_tables(conn):
    """
    Carrega tabelas pequenas completas.
    """
    print("\n--- Carregando tabelas pequenas ---", flush=True)

    # arquivos da Receita
    for pattern, table_name in SMALL_FILE_MAP.items():
        files = find_files_by_pattern(pattern)
        for file_path in files:
            print(f"Preparando tabela pequena: {file_path.name}", flush=True)

            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", newline="", delete=False
            ) as tmp_f:
                writer = csv.writer(
                    tmp_f,
                    delimiter=";",
                    quotechar='"',
                    quoting=csv.QUOTE_MINIMAL,
                    lineterminator="\n",
                )

                with open(file_path, "r", encoding="latin1", newline="") as in_f:
                    reader = csv.reader(in_f, delimiter=";", quotechar='"')
                    for row in reader:
                        row = [normalize_text(col) for col in row]
                        writer.writerow(row)

                tmp_path = tmp_f.name

            copy_file_to_table(conn, tmp_path, table_name)
            os.remove(tmp_path)

    # distritos.csv
    distritos_path = DATA_DIR / DISTRITOS_FILE
    if distritos_path.exists():
        print("Carregando distritos.csv", flush=True)

        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", delete=False
        ) as tmp_f:
            writer = csv.writer(
                tmp_f,
                delimiter=";",
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                lineterminator="\n",
            )

            with open(distritos_path, "r", encoding="utf-8", newline="") as in_f:
                # se seu arquivo já está com ';' e cabeçalho
                reader = csv.reader(in_f, delimiter=";", quotechar='"')
                rows = list(reader)

                # grava sem cabeçalho
                for row in rows[1:]:
                    row = [normalize_text(col) for col in row]
                    writer.writerow(row)

            tmp_path = tmp_f.name

        copy_file_to_table(conn, tmp_path, "public.distritos")
        os.remove(tmp_path)


def load_large_tables(conn):
    """
    Filtra e carrega as tabelas grandes só para SP.
    """
    print("\n--- Carregando tabelas grandes filtradas para SP ---", flush=True)

    estabele_files = find_files_by_pattern("ESTABELE")
    empresas_files = find_files_by_pattern("EMPRECSV")
    simples_files = find_files_by_pattern("SIMPLES")

    if not estabele_files:
        raise RuntimeError("Nenhum arquivo ESTABELE encontrado")
    if not empresas_files:
        raise RuntimeError("Nenhum arquivo EMPRECSV encontrado")
    if not simples_files:
        raise RuntimeError("Nenhum arquivo SIMPLES encontrado")

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir)

        estabele_sp = tmp_dir / "estabelecimentos_sp.csv" 
        empresas_sp = tmp_dir / "empresas_sp.csv"
        simples_sp = tmp_dir / "simples_sp.csv"

        cnpj_basicos_sp = filter_estabelecimentos_sp(estabele_files, estabele_sp)
        copy_file_to_table(conn, estabele_sp, "public.stg_estabelecimentos")

        filter_empresas_by_cnpj(empresas_files, cnpj_basicos_sp, empresas_sp)
        copy_file_to_table(conn, empresas_sp, "public.stg_empresas")

        filter_simples_by_cnpj(simples_files, cnpj_basicos_sp, simples_sp)
        copy_file_to_table(conn, simples_sp, "public.stg_simples")



def create_staging_indexes(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stg_estab_cnpj_basico
                ON public.stg_estabelecimentos (cnpj_basico);

            CREATE INDEX IF NOT EXISTS idx_stg_estab_cnae_fiscal
                ON public.stg_estabelecimentos (cnae_fiscal);

            CREATE INDEX IF NOT EXISTS idx_stg_estab_motivo
                ON public.stg_estabelecimentos (motivo_situacao_cadastral);

            CREATE INDEX IF NOT EXISTS idx_stg_estab_cep
                ON public.stg_estabelecimentos (cep);

            CREATE INDEX IF NOT EXISTS idx_stg_empresas_cnpj_basico
                ON public.stg_empresas (cnpj_basico);

            CREATE INDEX IF NOT EXISTS idx_stg_empresas_nat_jur
                ON public.stg_empresas (natureza_juridica);

            CREATE INDEX IF NOT EXISTS idx_stg_empresas_qual_resp
                ON public.stg_empresas (qualificacao_responsavel);

            CREATE INDEX IF NOT EXISTS idx_stg_simples_cnpj_basico
                ON public.stg_simples (cnpj_basico);

            CREATE INDEX IF NOT EXISTS idx_stg_cnae_codigo
                ON public.stg_cnae (codigo);

            CREATE INDEX IF NOT EXISTS idx_stg_motivo_codigo
                ON public.stg_motivo (codigo);

            CREATE INDEX IF NOT EXISTS idx_stg_nat_jur_codigo
                ON public.stg_natureza_juridica (codigo);

            CREATE INDEX IF NOT EXISTS idx_distritos_cep
                ON public.distritos (cep);
                    
            CREATE INDEX IF NOT EXISTS idx_stg_qual_socio_codigo
                ON public.stg_qualificacao_socio (codigo); 
        """)
    conn.commit()
    print("Índices criados", flush=True)


def analyze_staging_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            ANALYZE public.stg_estabelecimentos;
            ANALYZE public.stg_empresas;
            ANALYZE public.stg_simples;
            ANALYZE public.stg_cnae;
            ANALYZE public.stg_motivo;
            ANALYZE public.stg_natureza_juridica;
            ANALYZE public.distritos;
            ANALYZE public.stg_qualificacao_socio;
        """)
    conn.commit()
    print("ANALYZE executado", flush=True)



def run_transformation(conn):
    """
    Executa o SQL final no Postgres.
    """
    with conn.cursor() as cur:
        with open(SQL_PATH, "r", encoding="utf-8") as f:
            sql = f.read()
        cur.execute(sql)

    conn.commit()
    print("Transformação SQL executada", flush=True)


def cleanup_cnpj_data():
    """
    Remove todos os arquivos da pasta cnpj_data, exceto distritos.csv.
    """
    for path in DATA_DIR.iterdir():
        if not path.is_file():
            continue

        if path.name == "distritos.csv":
            continue

        path.unlink()

    print("Arquivos de cnpj_data removidos, mantendo apenas distritos.csv", flush=True)


def run():
    print("Iniciando pipeline CNPJ", flush=True)

    unzip_files()

    conn = get_connection()

    print(">>> clear_staging_tables", flush=True)
    clear_staging_tables(conn)

    print(">>> load_small_tables", flush=True)
    load_small_tables(conn)
    print(">>> terminou load_small_tables", flush=True)

    print(">>> load_large_tables", flush=True)
    load_large_tables(conn)
    print(">>> terminou load_large_tables", flush=True)

    print(">>> create_staging_indexes", flush=True)
    create_staging_indexes(conn)
    print(">>> terminou create_staging_indexes", flush=True)

    print(">>> analyze_staging_tables", flush=True)
    analyze_staging_tables(conn)
    print(">>> terminou analyze_staging_tables", flush=True)

    print(">>> run_transformation", flush=True)
    run_transformation(conn)
    print(">>> terminou run_transformation", flush=True)

    conn.close()

    cleanup_cnpj_data()

    print("Pipeline finalizada", flush=True)


if __name__ == "__main__":
    run()