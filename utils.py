# -*- coding: utf-8 -*-
import time
import typing as t
from functools import lru_cache

import click
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, Session
from cassandra.metadata import TableMetadata
from cassandra.query import PreparedStatement, SimpleStatement


def ensure_keyspace_exists(cluster: Cluster, session: Session, keyspace: str) -> None:
    if keyspace not in cluster.metadata.keyspaces.keys():
        cql = SimpleStatement(
            f"CREATE KEYSPACE \"{keyspace}\" WITH REPLICATION = {{'class': 'SingleRegionStrategy'}}"
        )
        session.execute(cql)
        time.sleep(5)


def ensure_table_exists(
    from_cluster: Cluster,
    to_cluster: Cluster,
    to_session: Session,
    keyspace: str,
    table: str,
) -> None:
    if table not in get_all_tables_from_keyspace(to_cluster, keyspace):
        click.echo(f"Table {keyspace}.{table} does not exists. Creating it...")
        table_scheme = get_table_description_as_cql(from_cluster, keyspace, table)
        to_session.execute(SimpleStatement(table_scheme))
        time.sleep(1)


def get_all_tables_from_keyspace(cluster: Cluster, keyspace: str) -> t.List[str]:
    return list(cluster.metadata.keyspaces[keyspace].tables.keys())


def get_table_metadata(cluster: Cluster, keyspace: str, table: str) -> TableMetadata:
    return cluster.metadata.keyspaces[keyspace].tables[table]


@lru_cache
def get_table_columns(cluster: Cluster, keyspace: str, table: str) -> t.List[str]:
    metadata = get_table_metadata(cluster, keyspace, table)
    return list(metadata.columns.keys())


def get_table_description_as_cql(cluster: Cluster, keyspace: str, table: str) -> str:
    metadata = get_table_metadata(cluster, keyspace, table)
    return metadata.as_cql_query()


def get_table_iterator(
    session: Session, keyspace: str, table: str, fetch_size: int = 100
) -> t.Iterator:
    cql = SimpleStatement(f"select * from {keyspace}.{table};", fetch_size=fetch_size)
    for row in session.execute(cql):
        yield row


def get_table_size(session: Session, keyspace: str, table: str) -> int:
    cql = SimpleStatement(f"select count(*) from {keyspace}.{table};")

    return session.execute(cql, timeout=30).one()["count"]


def get_prepared_insert_cql(
    session: Session, keyspace: str, table: str, keys: t.List[str]
) -> PreparedStatement:
    positions = ["?" for _ in keys]
    query = session.prepare(
        f"INSERT INTO {keyspace}.{table} ({', '.join(keys)}) VALUES ({', '.join(positions)})"
    )
    query.consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return query
