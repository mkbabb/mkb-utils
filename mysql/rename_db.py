import argparse
import json
import os
import subprocess
from typing import *


def run(command: str) -> str:
    stdout, stderr = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ).communicate()

    if stdout is not None:
        return bytes(stdout).decode("utf-8")
    else:
        return ""


def main() -> None:
    parser = argparse.ArgumentParser(description="""""")
    parser.add_argument("old_db_name")
    parser.add_argument("new_db_name")
    parser.add_argument("-u", "--user", default="root")
    parser.add_argument("-p", "--password", default="")
    parser.add_argument("--hostname", default="127.0.0.1")
    parser.add_argument("-P", "--port", default="3306")
    parser.add_argument("--delete", action="store_true")

    args = parser.parse_args()

    user, password, hostname, port = args.user, args.password, args.hostname, args.port

    old_db_name, new_db_name = (
        f"'{args.old_db_name}'",
        f"'{args.new_db_name}'",
    )

    connection_string = f" -u {user} -p'{password}' -P {port} -h {hostname} "

    run(
        f"mysql {connection_string}"
        + f'-e "CREATE DATABASE IF NOT EXISTS {new_db_name}"'
    )
    run(
        f"mysqldump {connection_string} {old_db_name} | mysql {connection_string} {new_db_name}"
    )

    if args.delete:
        run(f"mysql {connection_string}" + f'-e "DROP DATABASE {old_db_name}"')


if __name__ == "__main__":
    main()
