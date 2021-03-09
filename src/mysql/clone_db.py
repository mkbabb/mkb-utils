#!/usr/bin/env python3
import argparse
import contextlib
import json
import tempfile
from typing import *

from utils import open_mysql_conn, run
import sqlalchemy as sqla


def create_connection_str(
    username: str, host: str, port: str = "3306", password: str = "", **kwargs
) -> str:
    return f"-u {username} -p'{password}' -P {port} -h {host}"


def get_table_names(db_config: dict, views: bool = True) -> Set[str]:
    tables = []

    with contextlib.closing(open_mysql_conn(db_config)) as conn:
        insp = sqla.inspect(conn.engine)
        tables.extend(insp.get_table_names())
        if views:
            tables.extend(insp.get_view_names())

    return set(tables)


def dump_out_db(db_config: dict, tables: Set[str]) -> str:
    tables_str = " ".join(map(lambda x: f"'{x}'", tables))

    connection_str = create_connection_str(**db_config)
    database = db_config["database"]

    dump_name = f"{next(tempfile._get_candidate_names())}.sql"

    command = f"mysqldump -f --single-transaction --set-gtid-purged=OFF\
         {connection_str} '{database}' {tables_str} > {dump_name}"

    run(command)

    print(f"Dumped database {database}.")

    return dump_name


def dump_in_db(db_config: dict, dump_name: str) -> None:
    connection_str = create_connection_str(**db_config)

    database = db_config["database"]

    with contextlib.closing(open_mysql_conn(db_config)) as conn:
        conn.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`;")
        print(f"Created database {database}")

    run(f"mysql {connection_str} {database} < {dump_name}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--views", action="store_true")

    args = parser.parse_args()

    config = json.load(open(args.config, "r"))
    from_db_config, to_db_config = config.get("from"), config.get("to")

    dump_name = from_db_config.get("dump_name")
    no_provided_dump = dump_name is None

    tables = set(from_db_config.get("tables", []))

    if from_db_config is not None and no_provided_dump:
        tables |= get_table_names(from_db_config, args.views)
        dump_name = dump_out_db(from_db_config, tables)

    if to_db_config is not None:
        dump_in_db(to_db_config, dump_name)

    if no_provided_dump:
        run(f"rm {dump_name}")
        print(f"Removed temporary dump of {dump_name}")


if __name__ == "__main__":
    main()
