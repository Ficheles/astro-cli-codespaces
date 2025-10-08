"""
DAG: Importar disponibilidade de usinas (horária) para Snowflake staging

Fonte: CSV (URL pública)
Destino: tabela STAGING.DISPONIBILIDADE_USINA_2025_08

Fluxo:
  1. Baixa o CSV para um arquivo temporário
  2. Cria a tabela STAGING.DISPONIBILIDADE_USINA_2025_08 se não existir
  3. Carrega os dados usando write_pandas (Snowflake connector)

Requer:
  - Connection Airflow id: `snowflake_dev` (ajuste se necessário)
  - Provider: apache-airflow-providers-snowflake
"""

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import requests
import tempfile
import os
import pandas as pd

# using explicit PUT + COPY INTO instead of write_pandas


CSV_URL_TEMPLATE = "https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/disponibilidade_usina_ho/DISPONIBILIDADE_USINA_{month}.csv"
SNOWFLAKE_CONN_ID = "snowflake_dev"
TARGET_DATABASE = None  # usa o database do connection
TARGET_SCHEMA = "STAGING"

# temporary table name used for loading (session-scoped)
TEMP_TABLE = "DISPONIBILIDADE_USINA"

MONTHS = [
    "2025_07",
    "2025_08",
    "2025_09",
]


@dag(
    dag_id="import_disponibilidade_usina",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    tags=["import", "disponibilidade", "snowflake"],
)
def import_disponibilidade():

    @task()
    def download_csv(url: str, basename: str) -> str:
        """Baixa o CSV e retorna o caminho do arquivo local."""
        resp = requests.get(url, stream=True, timeout=60)
        if resp.status_code != 200:
            raise AirflowFailException(f"Erro ao baixar CSV: {resp.status_code}")

        tmp_dir = tempfile.mkdtemp(prefix="disponibilidade_")
        # use the provided basename so parallel/iterated downloads don't overwrite
        file_path = os.path.join(tmp_dir, basename + ".csv")

        with open(file_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    fh.write(chunk)

        return file_path

    
    @task()
    def load_to_snowflake(file_path: str, target_table: str):
        """Carrega o CSV para Snowflake usando write_pandas."""
        if not os.path.exists(file_path):
            raise AirflowFailException(f"Arquivo não encontrado: {file_path}")

        # Carrega CSV em DataFrame; tenta inferir separador automaticamente
        try:
            # sep=None + engine='python' permite que pandas detecte o delimitador
            # Não usar low_memory com engine='python'. Usar encoding utf-8-sig para remover BOM se presente
            df = pd.read_csv(file_path, sep=None, engine='python', encoding='utf-8-sig')
        except Exception as e:
            raise AirflowFailException(f"Erro lendo CSV local: {e}")

        # Normaliza nomes de colunas: remove BOM, espaços, e caracteres problemáticos
        def _clean_col(c: str) -> str:
            if not isinstance(c, str):
                c = str(c)
            # remover BOM se presente
            c = c.lstrip('\ufeff')
            c = c.strip()
            # substituir espaços por underscore e remover caracteres não alfanuméricos exceto _
            import re
            c = re.sub(r"\s+", "_", c)
            c = re.sub(r"[^0-9a-zA-Z_]", "", c)
            # garantir lowercase
            return c.lower()

        df.columns = [_clean_col(c) for c in df.columns]

        # Garantir que as colunas estejam em MAIÚSCULAS para corresponder aos identificadores não-entre-aspas do Snowflake
        df.columns = [c.upper() for c in df.columns]

        # Reindexar explicitamente para as colunas esperadas na tabela STAGING (preenche ausentes com NaN)
        expected_cols = [
            'ID_SUBSISTEMA','NOM_SUBSISTEMA','ID_ESTADO','NOM_ESTADO','NOM_USINA',
            'DIN_INSTANTE','VAL_POTENCIAINSTALADA','VAL_DISPOPERACIONAL','VAL_DISPSINCRONIZADA',
            'NOM_TIPOCOMBUSTIVEL','ID_TIPOUSINA','ID_ONS','CEG'
        ]
        df = df.reindex(columns=expected_cols)

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        # Antes de dar PUT, salvar um CSV limpo com o delimitador que o arquivo usa (;) para garantir
        # que o conteúdo esteja estável. Também vamos gravar o header e primeiras linhas no log.
        # ensure cleaned filename is unique for this downloaded file (derive basename from file_path)
        _basename = os.path.splitext(os.path.basename(file_path))[0]
        cleaned_file = os.path.join(os.path.dirname(file_path), _basename + "_cleaned.csv")
        try:
            # forçar ; como delimitador (o reindex acima garante a ordem/colunas esperadas)
            df.to_csv(cleaned_file, sep=';', index=False, header=True, encoding='utf-8-sig')
        except Exception as e:
            raise AirflowFailException(f"Erro ao escrever CSV limpo: {e}")

        # log header and first rows for debugging
        try:
            with open(cleaned_file, 'r', encoding='utf-8-sig') as fh:
                first_lines = [next(fh).rstrip('\n') for _ in range(4)]
            print('cleaned csv sample lines:')
            for l in first_lines:
                print(l)
        except StopIteration:
            # arquivo menor que 4 linhas, read what exists
            pass
        except Exception:
            pass

        # Create a temporary table in this session and load into it.
        create_temp_sql = f"""
        CREATE TEMPORARY TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TEMP_TABLE} (
            ID_SUBSISTEMA varchar,
            NOM_SUBSISTEMA varchar,
            ID_ESTADO varchar,
            NOM_ESTADO varchar,
            NOM_USINA varchar,
            DIN_INSTANTE varchar,
            VAL_POTENCIAINSTALADA number,
            VAL_DISPOPERACIONAL number,
            VAL_DISPSINCRONIZADA number,
            NOM_TIPOCOMBUSTIVEL varchar,
            ID_TIPOUSINA varchar,
            ID_ONS varchar,
            CEG varchar
        );
        """

        try:
            cur.execute(create_temp_sql)
        except Exception as e:
            raise AirflowFailException(f"Erro ao criar temporary table {TARGET_SCHEMA}.{TEMP_TABLE}: {e}")

        # PUT envia o arquivo local para o stage da tabela temporária (@%<temp_table>)
        file_uri = f"file://{os.path.abspath(cleaned_file)}"
        try:
            cur.execute(f"PUT '{file_uri}' @%{TEMP_TABLE} OVERWRITE=TRUE;")
        except Exception as e:
            raise AirflowFailException(f"Erro ao dar PUT no stage da tabela temporária: {e}")

        # listar arquivos no stage (útil para debug)
        try:
            cur.execute(f"LIST @%{TEMP_TABLE};")
            staged = cur.fetchall()
            # cada row tem (name, size, md5, last_modified)
            print('staged files:', staged)
        except Exception:
            pass

        # COPY INTO sem lista de colunas: usar mapeamento por nome (case-insensitive)
        # Ajustes para tolerar discrepâncias de colunas no arquivo (arquivos de fornecedores às vezes têm colunas extras)
        # ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE permite que o COPY processe linhas mesmo quando o número de colunas
        # no arquivo difere da tabela. ON_ERROR='CONTINUE' evita falhas globais; analise os registros carregados/erros
        # via tabela de load_history/estado.
        copy_sql = f"""
        COPY INTO {TARGET_SCHEMA}.{TEMP_TABLE}
        FROM @%{TEMP_TABLE}
        FILE_FORMAT=(TYPE=CSV FIELD_DELIMITER=';' PARSE_HEADER=TRUE FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF=('','NULL') ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR='CONTINUE'
        PURGE=TRUE;
        """

        try:
            cur.execute(copy_sql)
        except Exception as e:
            raise AirflowFailException(f"Erro ao executar COPY INTO: {e}")

        # limpa arquivos temporários
        try:
            # remove cleaned file e original
            if os.path.exists(cleaned_file):
                os.remove(cleaned_file)
            if os.path.exists(file_path):
                os.remove(file_path)
            os.rmdir(os.path.dirname(file_path))
        except Exception:
            pass

    # iterate months and create a pipeline per month
    for m in MONTHS:
        tbl = f"DISPONIBILIDADE_USINA"
        url = CSV_URL_TEMPLATE.format(month=m)
        # use a month-specific basename so files don't overwrite each other
        basename = f"DISPONIBILIDADE_USINA_{m}"
        path = download_csv(url, basename)
        # create = create_table_if_not_exists(tbl)
        load = load_to_snowflake(path, tbl)
        path >> load
        # create >> load

import_disponibilidade()
