# -*- coding: utf-8 -*-
import logging

import click

from sessions import (get_cassandra_cluster, get_cassandra_session,
                      get_keyspaces_cluster, get_keyspaces_session)
from utils import (ensure_table_exists, get_prepared_insert_cql,
                   get_table_columns, get_table_iterator, get_table_size)

logging.basicConfig(filename="replication.log", level=logging.DEBUG)


def log_copying_results(results) -> None:
    pass


def log_copying_error(error) -> None:
    logging.error(error)


def copy_table(
    keyspace: str,
    table: str,
) -> None:
    c_cluster = get_cassandra_cluster()
    k_cluster = get_keyspaces_cluster()
    c_session = get_cassandra_session()
    k_session = get_keyspaces_session()

    ensure_table_exists(c_cluster, k_cluster, k_session, keyspace, table)
    size = get_table_size(c_session, keyspace, table)
    columns = get_table_columns(c_cluster, keyspace, table)
    prepared_cql = get_prepared_insert_cql(k_session, keyspace, table, columns)

    click.echo(f"There are {size} records to copy...")

    for row in get_table_iterator(c_session, keyspace, table):
        future = k_session.execute_async(prepared_cql, row)
        future.add_callbacks(log_copying_results, log_copying_error)
