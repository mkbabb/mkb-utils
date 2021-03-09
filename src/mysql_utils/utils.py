import subprocess
from typing import *

import sqlalchemy as sqla


def run(command: str, debug: bool = True) -> str:
    stdout, stderr = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ).communicate()

    if debug:
        print(stdout)
        print(stderr)

    if stdout is not None:
        s = bytes(stdout).decode("utf-8")
        return s
    else:
        return ""


def create_sqla_engine_str(
    username: str, password: str, host: str, port: str, database: Optional[str] = None
) -> sqla.engine.base.Engine:
    s = f"mysql+pymysql://{username}:{password}@{host}:{port}"
    s += f"/{database}" if database is not None else ""
    return s


def open_mysql_conn(mysql_config: dict) -> sqla.engine.Connection:
    engine_str = create_sqla_engine_str(
        username=mysql_config["username"],
        password=mysql_config["password"],
        host=mysql_config["host"],
        port=mysql_config["port"],
        database=mysql_config["database"],
    )
    engine = sqla.create_engine(
        engine_str, json_serializer=lambda x: json.dumps(x, default=str)
    )
    return engine.connect()
