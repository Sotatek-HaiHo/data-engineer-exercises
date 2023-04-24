#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from dagster import file_relative_path

DEFAULT_PROJECT_PATH = file_relative_path(__file__, "../../../covid19")
DEFAULT_PROFILES_PATH = file_relative_path(__file__, "../../../.dbt")

DBT_PROJECT_PATH = os.getenv("DBT_PROJECT_PATH", DEFAULT_PROJECT_PATH)
DBT_PROFILES = os.getenv("DBT_PROFILES", DEFAULT_PROFILES_PATH)
