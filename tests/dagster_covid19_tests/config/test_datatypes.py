#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

from dagster import check_dagster_type

from dagster_covid19.config.datatypes import DataFrameIterator


def should__pass_type_check__when__using_dataframe_generator():
    def df_generator():
        yield pd.DataFrame([1])

    generator_instance = df_generator()
    assert check_dagster_type(DataFrameIterator, generator_instance).success
