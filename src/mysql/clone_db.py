import argparse
import json
import os
import subprocess
from typing import *

from utils.utils import run, quote_value


def create_connection_string(
    username: str, hostname: str, port: str = "3306", password: str = ""
) -> str:
    return f"-u {username} -p'{password}' -P {port} -h {hostname}"


def main() -> None:
    parser = argparse.ArgumentParser(description="""""")
    parser.add_argument("--config", required=True)

    args = parser.parse_args()

    config = json.load(open(args.config, "r"))

    from_db, to_db = config["from"], config["to"]

    from_connection_string, to_connection_string = (
        create_connection_string(
            from_db["username"], from_db["host"], from_db["port"], from_db["password"]
        ),
        create_connection_string(
            to_db["username"], to_db["host"], to_db["port"], to_db["password"]
        ),
    )

    from_db_name, to_db_name = (
        quote_value(from_db["database"]),
        quote_value(to_db["database"]),
    )

    tables = " ".join(map(quote_value, from_db["tables"]))

    run(
        f"mysql {to_connection_string}"
        + f'-e "CREATE DATABASE IF NOT EXISTS {to_db_name}"'
    )

    run(
        f"mysqldump --single-transaction {from_connection_string} {from_db_name} {tables} | mysql {to_connection_string} {to_db_name}"
    )


if __name__ == "__main__":
    main()
