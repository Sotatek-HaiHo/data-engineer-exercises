#!/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import Any

from dagster import (
    InitResourceContext,
    InputContext,
    io_manager,
    IOManager,
    OutputContext,
)
from sqlalchemy.engine import Engine

from dagster_covid19.config.sql_table import SqlTable


class SqlTableIOManager(IOManager):
    def __init__(self, engine: Engine):
        self._engine = engine

    def _write_table(self, context: OutputContext, table: SqlTable):
        with self._engine.connect() as conn:
            # Drop old table
            context.log.info(
                "Dropping table. [schema=%s, table=%s]",
                table.schema_name,
                table.table_name,
            )
            conn.execute(table.drop_ddl)

            # Create table structure
            context.log.info(
                "Recreating table. [schema=%s, table=%s]",
                table.schema_name,
                table.table_name,
            )
            conn.execute(table.create_ddl)

            # Load content
            context.log.info(
                "Loading table data. [schema=%s, table=%s]",
                table.schema_name,
                table.table_name,
            )
            for df in table.content:
                df.to_sql(
                    name=table.table_name,
                    schema=table.schema_name,
                    con=conn,
                    if_exists="append",
                    index=False,
                )

    def load_input(self, context: InputContext) -> Any:
        raise NotImplementedError

    def handle_output(self, context: OutputContext, obj: SqlTable) -> None:
        self._write_table(context, obj)


@io_manager(required_resource_keys={"postgresql"})
def sql_table_io_manager(init_context: InitResourceContext):
    return SqlTableIOManager(init_context.resources.postgresql)
