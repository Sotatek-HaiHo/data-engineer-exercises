#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
from collections.abc import Iterator
from json import JSONDecodeError
from pathlib import Path
from typing import Optional, Union

import pandas as pd
from dagster import (
    InitResourceContext,
    InputContext,
    io_manager,
    OutputContext,
    UPathIOManager,
)
from pydantic import BaseModel, parse_obj_as, ValidationError
from upath import UPath


class DataFrameIOManagerManifestV1(BaseModel):
    run_id: Optional[str]
    files_list: list[str] = []


class DataFrameIOManagerVersionManifest(BaseModel):
    version: int = 1
    content: DataFrameIOManagerManifestV1


class DataFrameIOManager(UPathIOManager):
    def __init__(self, base_path: Path):
        super().__init__(UPath(base_path))

    @staticmethod
    def _manifest_name(asset_path: UPath) -> UPath:
        return asset_path / "manifest.json"

    def dump_to_path(
        self,
        context: OutputContext,
        obj: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        path: UPath,
    ):
        """
        Write an obj to a parquet file and save it in the directory.
        :param context: context of the output asset
        :param obj: obj to write to parquet file
        :param path: directory to write to
        """
        # Every run we write the output into a different path
        run_id_path = path / context.run_id
        if not run_id_path.exists():
            run_id_path.mkdir(parents=True)
        if context.has_asset_partitions:
            context.log.info(
                "Saving output. [asset=%s, partition=%s, path=%s]",
                context.asset_key,
                context.asset_partition_key,
                run_id_path,
            )
        else:
            context.log.info(
                "Saving output. [asset=%s, path=%s]",
                context.asset_key,
                run_id_path,
            )
        new_files = []
        if isinstance(obj, pd.DataFrame):
            data_frames = [obj]
        else:
            data_frames = obj
        for count, element in enumerate(data_frames):
            parquet_file = run_id_path / f"{count}.parquet"
            element.to_parquet(parquet_file)
            new_files.append(str(parquet_file))

        self._update_manifest(context, new_files, path)

    def _read_manifest(
        self, context: Union[InputContext, OutputContext], path: UPath
    ) -> DataFrameIOManagerManifestV1:
        manifest_name = self._manifest_name(path)
        context.log.info("Reading manifest [path=%s]", manifest_name)
        with open(manifest_name, "rb") as manifest_in:
            config_dict = json.load(manifest_in)
        return parse_obj_as(DataFrameIOManagerVersionManifest, config_dict).content

    def _update_manifest(
        self, context: OutputContext, new_files: list[str], path: UPath
    ) -> None:
        # Update manifest to point to new set of files
        # Read the list of old files
        try:
            old_manifest = self._read_manifest(context, path)
        except (FileNotFoundError, JSONDecodeError, ValidationError):
            context.log.exception("Exception reading old manifest. Skipping")
            old_manifest = None

        # Write new manifest
        manifest_name = self._manifest_name(path)
        context.log.info("Updating manifest [path=%s]", manifest_name)
        manifest_data = DataFrameIOManagerVersionManifest(
            content=DataFrameIOManagerManifestV1(
                run_id=context.run_id, files_list=new_files
            )
        )
        # Write manifest to a tempfile because
        # json.dump can mangle the output in case of exception
        tmp_manifest = manifest_name.with_suffix(".tmp")
        with open(tmp_manifest, "w") as manifest_out:
            json.dump(manifest_data.dict(), manifest_out)
        # Swap tmp manifest into place
        tmp_manifest.replace(manifest_name)
        # Remove old files from previous run_id
        if old_manifest is not None:
            run_id_path = path / old_manifest.run_id
            context.log.info(
                "Removing old files from previous runs. [path=%s]", run_id_path
            )
            for f in old_manifest.files_list:
                UPath(f).unlink(missing_ok=True)
            run_id_path.rmdir()

    def load_from_path(
        self, context: InputContext, path: UPath
    ) -> Iterator[pd.DataFrame]:
        """
        Read parquet files and add the data they contain to the database table.
        :param context: context of the input asset
        :param path: directory to read from
        :return: dataframe of the parquet files
        """

        def parquet_df_gen():
            context.log.info(
                "Loading input. [asset=%s, path=%s]",
                context.asset_key,
                path,
            )
            old_manifest = self._read_manifest(context, path)
            for input_path in old_manifest.files_list:
                par_df = pd.read_parquet(input_path)
                yield par_df

        parquet_generator = parquet_df_gen()
        return parquet_generator


@io_manager(required_resource_keys={"tmp_dir"})
def dataframe_io_manager(init_context: InitResourceContext):
    df_path = Path(
        os.getenv(
            "DAGSTER_DATAFRAME_IO_PATH",
            init_context.resources.tmp_dir / "df_io_manager",
        )
    )
    return DataFrameIOManager(base_path=df_path)
