#!/usr/bin/env python
# -*- coding: utf-8 -*-

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AccumulationMode, AfterWatermark

from src.openweathermap.ingest import MyCustomUnboundedSource
from src.openweathermap.transform import (
    AverageFn,
    CustomSink,
    GlobalTempDiffFn,
    GlobalTempFn,
    parse_input,
)


def get_flink_pipeline() -> PipelineOptions:
    # Windowing doesn't seem to work with direct runner
    # Need to start Flink runner first with
    # cd flink_runner && docker-compose up
    return PipelineOptions(
        [
            "--runner=PortableRunner",
            "--job_endpoint=localhost:8099",
            "--environment_type=LOOPBACK",
            "--streaming",
        ]
    )


if __name__ == "__main__":
    pipeline_option = get_flink_pipeline()

    with beam.Pipeline(options=pipeline_option) as pipeline_1:
        weather_data = pipeline_1 | "Custom Source Name" >> MyCustomUnboundedSource()
        average_values = (
            weather_data
            | "WindowByMinute"
            >> beam.WindowInto(
                beam.window.FixedWindows(5),
                accumulation_mode=AccumulationMode.ACCUMULATING,
            )
            | "Group element" >> beam.Map(lambda x: ("location: " + str(x["name"]), x))
            | "CalculateMean" >> beam.CombinePerKey(AverageFn())
        )
        average_values | "Read timestamp" >> beam.ParDo(CustomSink())
        global_average = (
            average_values
            | "Parse Input" >> beam.Map(parse_input)
            | "Calculate Global Average"
            >> beam.CombineGlobally(GlobalTempFn()).without_defaults()
        )
        global_diff = (
            global_average
            | "Window"
            >> beam.WindowInto(
                beam.window.SlidingWindows(10, 5),
                trigger=AfterWatermark(),
                accumulation_mode=AccumulationMode.ACCUMULATING,
            )
            | "Calculate Global Temperature"
            >> beam.CombineGlobally(GlobalTempDiffFn()).without_defaults()
        )
        global_diff | beam.ParDo(CustomSink())
