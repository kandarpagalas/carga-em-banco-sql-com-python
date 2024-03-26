import os
from time import perf_counter
from sqlalchemy import URL, create_engine, text

# Utilizado apenas para acompanhar o tempo de execução
def elapsed_time_str(p1_start):
    p1_stop = perf_counter()
    seconds = round(p1_stop - p1_start, 2)
    minutes = round(seconds / 60, 2)
    hour = round(minutes / 60, 2)

    return f"{seconds}s | {minutes}min | {hour}h"

# Cria uma engine para conexão com o banco de dados
def db_engine_factory(
    username="aluno", password="Unifor@2024", host="localhost", database="engdb"
):
    # Create postgres db connection url
    POSTGRES_CONN_URL = URL.create(
        "postgresql+psycopg2",
        username=username,
        password=password,  # plain (unescaped) text
        host=host,
        database=database,
    )

    engine = create_engine(POSTGRES_CONN_URL, echo=False)
    return engine

# Inseres valores a paetir de uma lista de tuplas
def insert_values(conn, table: str, chunk: list[tuple]):
    values = ", ".join(chunk)
    if values == "":
        return

    stmt = f"""INSERT INTO {table} VALUES {values};"""
    conn.execute(text(stmt))
    conn.commit()

# Inseres valores a paetir de uma lista de tuplas
def create_table(conn, table: str):
    stmt = f"""
                CREATE TABLE {table} (
                COD_UF 				INT4,
                COD_MUN 			INT4,
                COD_ESPECIE 		INT4,
                LATITUDE 			FLOAT,
                LONGITUDE 			FLOAT,
                NV_GEO_COORD 		INT4
            );"""
    conn.execute(text(stmt))
    conn.commit()


def main():
    # contador para conferir quantidade de insersão no db
    total_lines = 0

    VERBOSE = True
    # diretório com os arquivos csv
    INPUT_DATA_FOLDER = "dados_ibge/"
    # tamanho dos chunks para garga no BD
    CHUNK_SIZE = 5000
    # nome da tabela no banco de dados
    OUTPUT_TABLE = "ibge_data"

    # Create the engine to connect to the PostgreSQL database
    engine = db_engine_factory()

    files = os.listdir(INPUT_DATA_FOLDER)

    # Ordenei apenas para melhorar a visualização dos logs
    files.sort()

    # cria a tabelo no BD caso não exista
    with engine.connect() as conn:
        if not engine.dialect.has_table(table_name=OUTPUT_TABLE,connection=conn):
            create_table(conn, OUTPUT_TABLE)


    # Ler cada arquivo
    for file in files:
        if file in [".gitkeep", ".DS_Store"]: continue
        p1_start = perf_counter()

        with open(INPUT_DATA_FOLDER + file, "r", encoding="utf-8") as csv:
            file_stats = os.stat(csv.name)

            # Entender relação do tamanho do arquivo com tempo de carga
            if VERBOSE:
                print(
                    f"{csv.name} -> {round(file_stats.st_size / (1024 * 1024), 2)}MB",
                    end="",
                )

            rows = csv.readlines()
            total_lines += len(rows[1:])

            # headers = tuple(rows[0].replace("\n", "").split(";"))

            with engine.connect() as conn:
                commit_flag = 0
                chunk = []
                for row in rows[1:]:
                    commit_flag += 1

                    value = tuple(row.replace("\n", "").split(";"))
                    chunk.append(str(value))

                    if commit_flag % CHUNK_SIZE == 0:
                        insert_values(conn, OUTPUT_TABLE, chunk)
                        chunk.clear()
                # insere valores restantes
                insert_values(conn, OUTPUT_TABLE, chunk)

                if VERBOSE:
                    print(" -> ", end="")
        if VERBOSE:
            print(elapsed_time_str(p1_start))
    if VERBOSE:
        print(f"linhas: {total_lines}")


if __name__ == "__main__":
    t1_start = perf_counter()
    main()
    print("TEMPO TOTAL:", elapsed_time_str(t1_start))
