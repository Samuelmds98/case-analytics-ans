"""
Script de ingestão — Camada RAW
Carrega os 4 arquivos CSV da ANS no schema raw do DuckDB.
Nenhuma transformação é aplicada aqui — dados brutos preservados como vieram.
"""

import duckdb
import os
import sys

# Caminho do banco e dos arquivos
DB_PATH = "db/unimed.duckdb"
DATA_DIR = "data/raw"

# Mapeamento: nome da tabela → configurações do arquivo
# delim: separador (ANS usa ";" na maioria dos arquivos)
# encoding: ISO-8859-1 é padrão nos arquivos da ANS
FILES = {
    "operadoras": {
        "arquivo": "Relatorio_cadop.csv",
        "delim": ";",
    },
    "produtos": {
        "arquivo": "pda-008-caracteristicas_produtos_saude_suplementar.csv",
        "delim": ";",
    },
    "beneficiarios": {
        "arquivo": "pda-024-icb-CE-2026_01.csv",
        "delim": ";",
    },
    "financeiro": {
        "arquivo": "3T2025.csv",
        "delim": ";",
    },
}


def create_db_dir():
    """Cria o diretório do banco se não existir."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def detect_encoding(file_path):
    """
    Tenta decodificar o arquivo INTEIRO em UTF-8. Sendo extremamente rápido em C (Python nativo),
    garante 100% de certeza se o arquivo inteiro é UTF-8 válido.
    Se falhar na decodificação de qualquer byte, aciona o fallback para o legado (Latin1 / 8859_1).
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for _ in f:
                pass
        return 'utf-8'
    except UnicodeDecodeError:
        return '8859_1'


def load_table(conn, table_name, config):
    """
    Carrega um arquivo CSV na tabela raw correspondente.
    Retorna o número de linhas carregadas ou levanta exceção em caso de erro.
    """
    file_path = os.path.join(DATA_DIR, config["arquivo"])

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    # Detecta o melhor encoding dinamicamente
    enc = detect_encoding(file_path)

    conn.execute(f"""
        CREATE OR REPLACE TABLE raw.{table_name} AS
        SELECT *
        FROM read_csv(
            '{file_path}',
            header       = true,
            delim        = '{config["delim"]}',
            encoding     = '{enc}',
            ignore_errors = true
        )
    """)

    row_count = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()[0]
    return row_count


def print_table_info(conn, table_name):
    """Exibe colunas e tipos da tabela carregada."""
    columns = conn.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'raw'
          AND table_name   = '{table_name}'
        ORDER BY ordinal_position
    """).fetchall()

    for col_name, col_type in columns:
        print(f"     • {col_name} ({col_type})")


def main():
    create_db_dir()

    print(f"\n{'='*55}")
    print(" ELT — Ingestão RAW | Unimed Fortaleza")
    print(f"{'='*55}")
    print(f" Banco: {DB_PATH}\n")

    conn = duckdb.connect(DB_PATH)
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    total_erros = 0

    for table_name, config in FILES.items():
        print(f"[*] Carregando raw.{table_name}...")
        print(f"   Arquivo : {config['arquivo']}")

        try:
            row_count = load_table(conn, table_name, config)
            print(f"   [OK] {row_count:,} linhas carregadas")
            print(f"   Colunas detectadas:")
            print_table_info(conn, table_name)

        except FileNotFoundError as e:
            print(f"   [ERRO]: {e}")
            total_erros += 1

        except Exception as e:
            print(f"   [ERRO] inesperado em raw.{table_name}: {e}")
            total_erros += 1

        print()

    conn.close()

    print(f"{'='*55}")
    if total_erros == 0:
        print(" [OK] Ingestão concluída com sucesso.")
    else:
        print(f" [AVISO] Ingestão concluída com {total_erros} erro(s).")
        print("   Verifique os arquivos ausentes antes de continuar.")
        sys.exit(1)
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()