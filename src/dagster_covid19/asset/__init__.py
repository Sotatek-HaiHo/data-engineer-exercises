#!/usr/bin/env python
# -*- coding: utf-8 -*-
from dagster import Definitions

from dagster_covid19.asset.dbt import dbt_assets, dbt_sensors
from dagster_covid19.asset.kaggle import kaggle_assets, kaggle_job

from dagster_covid19.resources import get_resources

defs = Definitions(
    assets=[*kaggle_assets, *dbt_assets],
    resources=get_resources(),
    sensors=[*dbt_sensors],
    jobs=[kaggle_job],
)
