#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
from typing import *

from utils import quote_value, run

import tempfile


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
        quote_value(from_db["database"], '"'),
        quote_value(to_db["database"], '"'),
    )

    tables = " ".join(map(quote_value, from_db.get("tables", [])))
    dump_name = from_db.get("dump_name")
    no_provided_dump = dump_name is None

    if no_provided_dump:
        dump_name = next(tempfile._get_candidate_names())
        run(
            f"mysqldump -f --single-transaction {from_connection_string} {from_db_name} {tables} > {dump_name}"
        )
        print(f"Dumped database {from_db_name}.")

    run(
        f"mysql {to_connection_string} "
        + f'-e "CREATE DATABASE IF NOT EXISTS {to_db_name}"'
    )

    print(f"Created database {to_db_name}")

    run(f"mysql {to_connection_string} {to_db_name} < {dump_name}")

    if no_provided_dump:
        run(f"rm {dump_name}")
        print(f"Removed temporary dump of {dump_name}")


if __name__ == "__main__":
    main()
