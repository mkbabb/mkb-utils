#!/usr/bin/env python3
import argparse
import contextlib
import json
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


def create_connection_string(
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


def main() -> None:
    parser = argparse.ArgumentParser(description="""""")
    parser.add_argument("--config", required=True)
    parser.add_argument("--ignore_views", action="store_true")

    args = parser.parse_args()

    config = json.load(open(args.config, "r"))
    from_db_config, to_db_config = config["from"], config["to"]

    dump_name = from_db_config.get("dump_name")
    no_provided_dump = dump_name is None

    from_connection_string, to_connection_string = (
        create_connection_string(
            from_db_config["username"],
            from_db_config["host"],
            from_db_config["port"],
            from_db_config["password"],
        ),
        create_connection_string(
            to_db_config["username"],
            to_db_config["host"],
            to_db_config["port"],
            to_db_config["password"],
        ),
    )

    from_db_name, to_db_name = (from_db_config["database"], to_db_config["database"])
    tables = from_db_config.get("tables", [])

    with contextlib.closing(open_db_connection(from_db_config)) as from_connection:
        cursor = from_connection.cursor(dictionary=True)
        cursor.execute(f"USE `{from_db_name}`;")
        if args.ignore_views:
            cursor.execute(BASE_TABLE_SQL(from_db_name))
            tables += [
                j for i in cursor.fetchall() if (j := i.get("TABLE_NAME")) is not None
            ]

    tables_str = " ".join(tables)

    if no_provided_dump:
        dump_name = next(tempfile._get_candidate_names())
        run(
            f"mysqldump -f --single-transaction --set-gtid-purged=OFF {from_connection_string} '{from_db_name}' {tables_str} > {dump_name}"
        )
        print(f"Dumped database {from_db_name}.")

    with contextlib.closing(open_db_connection(to_db_config)) as to_connection:
        cursor = to_connection.cursor(dictionary=True)
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{to_db_name}`")
        cursor.execute(f"USE `{to_db_name}`;")
        print(f"Created database {to_db_name}")

    run(f"mysql {to_connection_string} {to_db_name} < {dump_name}")

    if no_provided_dump:
        run(f"rm {dump_name}")
        print(f"Removed temporary dump of {dump_name}")


if __name__ == "__main__":
    main()
