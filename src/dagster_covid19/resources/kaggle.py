#!/usr/bin/env python
# -*- coding: utf-8 -*-

from kaggle.api.kaggle_api_extended import KaggleApi


def get_kaggle_api() -> KaggleApi:
    """
    Return an authenticated Kaggle API client
    :return:
    """
    kg = KaggleApi()
    kg.authenticate()
    return kg
