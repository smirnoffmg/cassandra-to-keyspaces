#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import click

from sessions import (get_cassandra_cluster, get_cassandra_session,
                      get_keyspaces_cluster, get_keyspaces_session)
from tools import copy_table
from utils import (ensure_keyspace_exists, ensure_table_exists,
                   get_all_tables_from_keyspace)


@click.command()
@click.option("--keyspace", prompt="Keyspace")
@click.option("--exclude_tables")
def main(keyspace, exclude_tables):
    k_cluster, k_session, c_cluster, c_session = (
        get_keyspaces_cluster(),
        get_keyspaces_session(),
        get_cassandra_cluster(),
        get_cassandra_session(),
    )

    click.echo("Checking if keyspace exists...")
    ensure_keyspace_exists(k_cluster, k_session, keyspace)

    click.echo("Start copying tables...")

    tables = get_all_tables_from_keyspace(c_cluster, keyspace)

    excluded_tables = exclude_tables.split(",")

    tables = list(set(tables) - set(excluded_tables))

    for table in tables:
        click.echo(f"Ensuring {keyspace}.{table} exists...")
        try:
            ensure_table_exists(c_cluster, k_cluster, k_session, keyspace, table)
        except Exception as err:
            logging.error(str(f"{table}: {err}"))

    for table in tables:
        click.echo(f"Copying {table}...")
        try:
            copy_table(keyspace, table)
        except Exception as err:
            logging.error(str(f"{table}: {err}"))


if __name__ == "__main__":
    main()
