#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections.abc import Iterator
from typing import Any

from dagster import DagsterType, TypeCheckContext


def iterator_checker(_: TypeCheckContext, obj: Any) -> bool:
    return isinstance(obj, Iterator)


DataFrameIterator = DagsterType(
    name="DataFrameIterator", type_check_fn=iterator_checker
)
