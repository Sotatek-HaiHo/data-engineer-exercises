#!/usr/bin/env python
# -*- coding: utf-8 -*-
from dagster import resource
from kaggle.api.kaggle_api_extended import KaggleApi


@resource
def kaggle_api() -> KaggleApi:
    """
    Return an authenticated Kaggle API client
    :return:
    """
    kg = KaggleApi()
    kg.authenticate()
    return kg
