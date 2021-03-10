# -*- coding: utf-8 -*-
import configparser
from functools import lru_cache
from ssl import CERT_REQUIRED, PROTOCOL_TLSv1_2, SSLContext

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from cassandra.query import dict_factory

config = configparser.ConfigParser()
config.read("config.ini")


@lru_cache
def get_keyspaces_cluster() -> Cluster:
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.load_verify_locations(config["keyspaces"]["cert_path"])
    ssl_context.verify_mode = CERT_REQUIRED
    auth_provider = PlainTextAuthProvider(
        username=config["keyspaces"]["username"],
        password=config["keyspaces"]["password"],
    )
    cluster = Cluster(
        [config["keyspaces"]["cluster_url"]],
        ssl_context=ssl_context,
        auth_provider=auth_provider,
        port=9142,
        connect_timeout=30,
        control_connection_timeout=15,
    )
    return cluster


def get_keyspaces_session():
    cluster = get_keyspaces_cluster()
    session = cluster.connect()
    session.default_timeout = 30
    return session


@lru_cache
def get_cassandra_cluster() -> Cluster:
    cluster = Cluster(
        [config["cassandra"]["cluster_url"]],
        connect_timeout=30,
        control_connection_timeout=15,
    )
    return cluster


def get_cassandra_session() -> Session:
    cluster = get_cassandra_cluster()
    session = cluster.connect()
    session.row_factory = dict_factory
    session.default_timeout = 30
    return session
