import csv
import sqlite3
import os
import time
from itertools import islice

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

def prepend_generator(val, generator):
    yield val
    yield from generator


def load_tables(raw_data_dir: str, db_conn, parsing_strategy, strategy_name):
    """
    Load raw data from .tbl files in `raw_data_dir` to the database at
    `db_conn`.
    """
    cursor = db_conn.cursor()
    print(f"loading tables")
    start_time = time.time()

    for table_name in get_table_names(db_conn):
        raw_data_file = f"{raw_data_dir}/{table_name.lower()}.tbl"
        table_columns = get_table_columns(table_name, db_conn)

        with open(raw_data_file, mode="r") as file:
            strategy_start_time = time.time()
            parsing_strategy(csv.reader(file, delimiter="|"), cursor, table_name, table_columns)
            strategy_end_time = time.time()
            print(f"{strategy_name} on {table_name} done time elapsed: {strategy_end_time - strategy_start_time:.2f}")

        db_conn.commit()

    end_time = time.time()
    print(f"done loading tables time elapsed={end_time-start_time:.2f} seconds")

def baseline_strategy(csv_reader, chunksize, cursor, table_name, table_columns):
    """
    The baseline strategy just sequentially iterates through the whole file
    """
    start_time = time.time()
    while True:
        chunk = islice(csv_reader, chunksize)

        # this lets us know when the iteration through the csv_reader should end
        try:
            chunk = prepend_generator(next(chunk), chunk)
        except StopIteration:
            break

        cursor.executemany(
            f"""
            INSERT INTO {table_name} ({", ".join(table_columns)})
            VALUES ({", ".join(["?"]*len(table_columns))})
            """,
            chunk
        )

def sequential_subset(csv_reader, chunksize, cursor, table_name, table_columns, offset, stopping_strategy):
    """
    The sequential subset strategy is the same as baseline, but reads only a subset of the data,
    defined by offset and stopping_strategy

    stopping_strategy can be used to implement a fixed size random sequential parsing, or something more
    sophisticated (e.g. stopping until a delta change of some aggregate metric is less than some epsilon)
    """
    pass

def random_subset(csv_reader, chunksize, cursor, table_name, table_columns, stopping_strategy):
    """
    The random subset strategy seeks to a random offset in the file, then parses in both directions,
    stopping according to the stopping_strategy, which takes in a row and returns whether the parsing
    should stop or not

    stopping_strategy can be used to implement a fixed size random sequential parsing, or something more
    sophisticated (e.g. stopping until a delta change of some aggregate metric is less than some epsilon)
    """
    pass

def parallel_subsets(csv_reader, chunksize, cursor, table_name, table_columns, num_parallel, stopping_strategy):
    """
    The parallel subset strategy starts with num_parallel evenly spaced offets in the file, and parses
    in both directions from all of them, alternating through each one, stopping according to the stopping_strategy,
    which takes in a row and returns whether the parsing should stop or not
    """
    pass


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
    load_tables(f"{DIR_PATH}/tpch_raw_data", db_conn, lambda file, cursor, table_name, table_columns: baseline_strategy(file, 1000, cursor, table_name, table_columns), "baseline_strategy")

    # Execute simple query to test data has been successfully loaded.
    # For original (untransformed) data, output should be [(10000,)].
    execute_query("SELECT COUNT(*) FROM SUPPLIER;", db_conn)

    db_conn.close()
