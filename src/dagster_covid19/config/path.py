#!/usr/bin/env python
# -*- coding: utf-8 -*-
import atexit
import os
import shutil
from pathlib import Path
from tempfile import mkdtemp


def get_tmp_dir() -> Path:
    # User-defined persistent temporary directory
    user_defined_tmp_dir = os.getenv("DAGSTER_ASSET_TMPDIR")
    if user_defined_tmp_dir is not None:
        return Path(user_defined_tmp_dir)
    # Automatic temporary directory, this will be cleaned up on program exit
    dagster_home = os.getenv("DAGSTER_HOME")
    if dagster_home is not None:
        tmp_dir_base = Path(dagster_home)
    else:
        tmp_dir_base = Path(".")
    auto_tmp_dir = mkdtemp(dir=tmp_dir_base)
    # Automatic deletion of temporary directory
    atexit.register(shutil.rmtree, path=auto_tmp_dir, ignore_errors=True, onerror=None)
    return Path(auto_tmp_dir)
