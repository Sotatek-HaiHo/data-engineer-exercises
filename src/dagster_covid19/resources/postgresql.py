#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from contextlib import contextmanager

from dagster import resource
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool


@resource
@contextmanager
def postgresql_engine() -> Engine:
    postgres_connection_string = os.getenv("POSTGRE_CONNECTION_STRING")
    if postgres_connection_string is None:
        raise Exception("POSTGRE_CONNECTION_STRING is not set")

    engine = None
    try:
        engine = create_engine(postgres_connection_string, poolclass=NullPool)
        yield engine
    finally:
        if engine is not None:
            engine.dispose()
