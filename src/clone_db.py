#!/usr/bin/env python3
import argparse
import contextlib
import json
from json import dump
import os
import subprocess
import tempfile
from typing import *

import mysql.connector

from utils import quote_value, run

BASE_TABLE_SQL = (
    lambda table_name: f"""SELECT TABLE_NAME FROM information_schema.tables 
                WHERE TABLE_TYPE LIKE 'BASE TABLE' AND
                TABLE_SCHEMA like '{table_name}'"""
)


def create_connection_str(
    username: str, hostname: str, port: str = "3306", password: str = ""
) -> str:
    return f"-u {username} -p'{password}' -P {port} -h {hostname}"


def open_db_connection(db_config: dict) -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        user=db_config["username"],
        host=db_config["host"],
        port=db_config["port"],
        password=db_config["password"],
    )


def get_table_names(db_config: dict, ignore_views: bool = True) -> Set[str]:
    tables = []

    with contextlib.closing(open_db_connection(db_config)) as from_connection:
        cursor = from_connection.cursor(dictionary=True)
        cursor.execute(f"USE `{db_config['database']}`;")

        if ignore_views:
            cursor.execute(BASE_TABLE_SQL(db_config["database"]))
            tables += [
                j for i in cursor.fetchall() if (j := i.get("TABLE_NAME")) is not None
            ]

    return set(tables)


def dump_out_db(db_config: dict, tables: Set[str]) -> str:
    tables_str = " ".join(map(lambda x: f"'{x}'", tables))

    connection_str = create_connection_str(
        db_config["username"],
        db_config["host"],
        db_config["port"],
        db_config["password"],
    )

    database = db_config["database"]

    dump_name = f"{next(tempfile._get_candidate_names())}.sql"

    run(
        f"mysqldump -f --single-transaction --set-gtid-purged=OFF {connection_str} '{database}' {tables_str} > {dump_name}"
    )

    print(f"Dumped database {database}.")

    return dump_name


def dump_in_db(db_config: dict, dump_name: str) -> None:
    connection_str = create_connection_str(
        db_config["username"],
        db_config["host"],
        db_config["port"],
        db_config["password"],
    )
    database = db_config["database"]

    with contextlib.closing(open_db_connection(db_config)) as to_connection:
        cursor = to_connection.cursor(dictionary=True)
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`;")
        cursor.execute(f"USE `{database}`;")
        print(f"Created database {database}")

    run(f"mysql {connection_str} {database} < {dump_name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="""""")
    parser.add_argument("--config", required=True)
    parser.add_argument("--ignore_views", action="store_true")

    args = parser.parse_args()

    config = json.load(open(args.config, "r"))
    from_db_config, to_db_config = config.get("from"), config.get("to")

    dump_name = from_db_config.get("dump_name")
    tables = set(from_db_config.get("tables", []))
    no_provided_dump = dump_name is None

    if from_db_config is not None and no_provided_dump:
        tables = (
            tables
            if len(tables) > 0
            else get_table_names(from_db_config, args.ignore_views)
        )
        dump_name = dump_out_db(from_db_config, tables)

    if to_db_config is not None:
        dump_in_db(to_db_config, dump_name)

    if no_provided_dump:
        run(f"rm {dump_name}")
        print(f"Removed temporary dump of {dump_name}")


if __name__ == "__main__":
    main()
