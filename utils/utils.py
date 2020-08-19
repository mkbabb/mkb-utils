import argparse
import json
import os
import re
import subprocess
from typing import *


def run(command: str) -> str:
    print(command)
    stdout, stderr = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ).communicate()

    print(stdout, stderr)

    if stdout is not None:
        s = bytes(stdout).decode("utf-8")
        return s
    else:
        return ""


def quote_value(value: str, quote: str = "'") -> str:
    if re.match(re.compile(quote + "(.*)" + quote), value) is not None:
        return value
    else:
        return f"{quote}{value}{quote}"
