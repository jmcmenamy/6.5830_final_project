import csv
import sqlite3
import os

DB_FILE = "tpch.db"
DIR_PATH = f"{os.path.dirname(os.path.abspath(__file__))}"


def create_tables(schema_file: str, db_conn):
    """
    Create tables as described in `schema_file` for the database at `db_conn`.
    """
    with open(schema_file, "r") as file:
        create_statements = [
            statement
            for statement in file.read().replace("\n", "").split(";")
            if statement
        ]

    cursor = db_conn.cursor()
    for statement in create_statements:
        cursor.execute(statement)
    db_conn.commit()


def get_table_names(db_conn) -> list[str]:
    cursor = db_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [table[0] for table in cursor.fetchall()]


def get_table_columns(table_name: str, db_conn) -> list[str]:
    cursor = db_conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
    return [description[0] for description in cursor.description]


def load_tables(raw_data_dir: str, db_conn):
    """
    Load raw data from .tbl files in `raw_data_dir` to the database at
    `db_conn`.
    """
    cursor = db_conn.cursor()

    for table_name in get_table_names(db_conn):
        raw_data_file = f"{raw_data_dir}/{table_name.lower()}.tbl"
        table_columns = get_table_columns(table_name, db_conn)
        with open(raw_data_file, mode="r") as file:
            csv_reader = csv.reader(file, delimiter="|")
            for row in csv_reader:
                cursor.execute(
                    f"""
                    INSERT INTO {table_name} ({", ".join(table_columns)})
                    VALUES ({", ".join(["?"]*len(table_columns))})
                    """,
                    ([field for field in row]),
                )

            db_conn.commit()


def execute_query(sql_query: str, db_conn):
    """
    Execute the query `sql_query` on the database at `db_conn`.
    """
    cursor = db_conn.cursor()
    cursor.execute(sql_query)

    print(cursor.fetchall())


if __name__ == "__main__":
    db_conn = sqlite3.connect(DB_FILE)

    create_tables(f"{DIR_PATH}/tpch_raw_data/schema.sql", db_conn)
    load_tables(f"{DIR_PATH}/tpch_raw_data", db_conn)

    # Execute simple query to test data has been successfully loaded.
    # For original (untransformed) data, output should be [(10000,)].
    execute_query("SELECT COUNT(*) FROM SUPPLIER;", db_conn)

    db_conn.close()
