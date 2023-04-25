#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections.abc import Iterator
from typing import Any

from dagster import DagsterType, TypeCheckContext


def data_frame_iterator_checker(_: TypeCheckContext, dataframe_iterator: Any) -> bool:
    return isinstance(dataframe_iterator, Iterator)


DataFrameIterator = DagsterType(
    name="DataFrameIterator", type_check_fn=data_frame_iterator_checker
)
