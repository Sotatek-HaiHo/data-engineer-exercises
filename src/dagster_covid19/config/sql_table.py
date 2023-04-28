#!/usr/bin/env python
# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Iterator

import pandas as pd


@dataclass
class SqlTable:
    schema_name: str
    table_name: str
    create_ddl: str
    drop_ddl: str
    content: Iterator[pd.DataFrame]
