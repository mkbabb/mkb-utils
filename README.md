# mkb-utils

A collection of utility scripts, mainly in Python 🐍.

## [`clone_db.py`](mysql/clone_db.py)

Clones a given input database to another using `mysqldump`. Takes one argument,
`config`: see the [`clone-config-example.json`](config/clone-config-example.json) for
the required form.

## [`rename_db.py`](mysql/rename_db.py)

For some reason, MySQL does not allow you to natively rename a given database. We simply
dump the contents of our input database into an identical output database of a renamed
name. See the `--help` option for more information and required parameters.
