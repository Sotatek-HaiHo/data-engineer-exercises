#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import time
from collections import Counter
from datetime import datetime

import apache_beam as beam
import requests
from apache_beam import RestrictionProvider
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import WalltimeWatermarkEstimator
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.ptransform import OutputT
from apache_beam.transforms.trigger import AccumulationMode, AfterWatermark


def calculate_average(element):
    print(element)
    value = element["temp"]
    return value


class UnboundedOffsetRestrictionTracker(OffsetRestrictionTracker):
    """An unbounded restriction tracker"""

    def try_claim(self, position) -> bool:
        claim_success = super().try_claim(position)
        if claim_success:
            # Increase the restriction whenever a position is successfully claimed
            # to create an unbounded restriction tracker
            self.current_restriction().stop += 1
        return claim_success

    def is_bounded(self):
        return False


class MyRestrictionProvider(RestrictionProvider):
    def create_tracker(self, restriction):
        return UnboundedOffsetRestrictionTracker(restriction)

    def initial_restriction(self, element):
        return OffsetRange(0, 1)

    def restriction_size(self, element, restriction):
        return restriction.size()


class MyCustomUnboundedSourceDoFn(beam.DoFn):
    """A splittable DoFn that generates an unbounded amount of data"""

    def __init__(self):
        super().__init__()
        self._initial_data = None
        self._sleep_time = None

    def setup(self):
        super().setup()
        self._initial_data = 0
        self._sleep_time = 1

    def start_bundle(self):
        super().start_bundle()

    def finish_bundle(self):
        super().finish_bundle()

    def teardown(self):
        super().teardown()

    def process(
        self,
        element,
        restriction_tracker=beam.DoFn.RestrictionParam(MyRestrictionProvider()),
        watermark_estimator=beam.DoFn.WatermarkEstimatorParam(
            WalltimeWatermarkEstimator.default_provider()
        ),
    ):
        cities = [
            {"name": "Moscow", "lat": 55.7504461, "lon": 37.6174943},
            {"name": "Hà Nội", "lat": 21.0245, "lon": 105.8412},
            {"name": "New York", "lat": 40.7127281, "lon": -74.0060152},
        ]
        api_key = "11043aa0a7b7d4475fbbb7af6543b390"

        start = restriction_tracker.current_restriction().start
        json_list = []
        for city in cities:
            weather = self.fetch_data(api_key=api_key, lat=city["lat"], lon=city["lon"])

            message = {}
            message["name"] = weather["name"]
            message["timestamp"] = datetime.utcfromtimestamp(time.time()).isoformat()
            message["sunrise"] = datetime.utcfromtimestamp(
                weather["sys"]["sunrise"]
            ).isoformat()
            message["sunset"] = datetime.utcfromtimestamp(
                weather["sys"]["sunset"]
            ).isoformat()
            message["id"] = weather["weather"][0]["id"]
            message["main"] = weather["weather"][0]["main"]
            message["icon"] = weather["weather"][0]["icon"]
            message["temp"] = weather["main"]["temp"]
            message["feels_like"] = weather["main"]["feels_like"]
            message["humidity"] = weather["main"]["humidity"]
            message["visibility"] = weather["visibility"]
            message["w_speed"] = weather["wind"]["speed"]

            data = json.dumps(message).encode("utf-8")
            data_dict = json.loads(data)
            json_list.append(data_dict)
        if restriction_tracker.try_claim(start):
            for item in json_list:
                timestamp = time.time()
                yield beam.window.TimestampedValue(item, timestamp)
        # the documents said that we can use
        # defer_time = timestamp.Duration(seconds=self._sleep_time)
        # restriction_tracker.defer_remainder(defer_time)
        # but it doesn't seem to work,
        # so we just sleep inside the function before deferring
        time.sleep(self._sleep_time)
        restriction_tracker.defer_remainder()
        return

    def fetch_data(self, api_key: str, lat: float, lon: float):
        url = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&units=metric&appid={}".format(
            lat, lon, api_key
        )
        logging.info("Getting the weather...")
        r = requests.get(url)
        ret_data = r.json()
        self._initial_data = r.json()
        return ret_data


class CustomSink(beam.DoFn):
    def process(self, data, timestamp=beam.DoFn.TimestampParam):
        timestamp = timestamp
        print(data, ",", timestamp)


class GlobalCustomSink(beam.DoFn):
    def process(self, data, timestamp=beam.DoFn.TimestampParam):
        timestamp = timestamp
        print(data, timestamp)


class MyCustomUnboundedSource(beam.PTransform):
    """A root PTransform that use MyCustomUnboundedSourceDoFn
    to perform actual data generation"""

    def expand(self, pbegin) -> OutputT:
        return pbegin | beam.Impulse() | beam.ParDo(MyCustomUnboundedSourceDoFn())


def get_flink_pipeline():
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


class AverageFn(beam.CombineFn):
    def create_accumulator(self):
        sum = 0.0
        count = 0
        temp = []
        des = []
        accumulator = sum, count, temp, des
        return accumulator

    def add_input(self, accumulator, input):
        sum, count, temp, des = accumulator
        des.append(input["main"])
        temp.append(input["temp"])
        return sum + input["temp"], count + 1, temp, des

    def merge_accumulators(self, accumulators):
        def find_most_frequent(lst):
            counter = Counter(lst)
            most_common = counter.most_common()

            # Find the item with the highest count and occurred first
            max_count = most_common[0][1]
            result = next(
                (item for item, count in most_common if count == max_count), None
            )
            return result

        if len(accumulators) > 0:
            sums, counts, temp, des = zip(*accumulators)
            desc = [item for sublist in des for item in sublist]
            temps = [item for sublist in temp for item in sublist]
            temp_diff = temps[-1] - temps[0]
            main_desc = find_most_frequent(desc)
            return sum(sums), sum(counts), temp_diff, main_desc
        else:
            return 0.0, 0, [], []

    def extract_output(self, accumulator):
        sum, count, temp, des = accumulator
        if count == 0:
            return float("NaN")
        return (
            "avg_temperature: " + str(sum / count),
            "temperature_diff: " + str(temp),
            "description: " + str(des),
        )


def parse_input(line):
    # Remove the outer brackets from the tuple
    data_str = str(line).replace("(", "").replace(")", "").replace("'", "")
    # Split the string into key-value pairs

    pairs = [pair.strip() for pair in data_str.split(",")]

    result_dict = {
        pair.split(":")[0].strip(): float(pair.split(":")[1].strip())
        if "temperature" in pair.lower() or "diff" in pair.lower()
        else pair.split(":")[1].strip()
        for pair in pairs
    }
    return result_dict


class GlobalTempFn(beam.CombineFn):
    def create_accumulator(self):
        sum = 0.0
        count = 0
        avg_temp = []
        accumulator = sum, count, avg_temp
        return accumulator

    def add_input(self, accumulator, input):
        sum, count, avg_temp = accumulator
        return sum + input["avg_temperature"], count + 1, avg_temp

    def merge_accumulators(self, accumulators):
        if len(accumulators) > 0:
            sums, counts, avg_temp = zip(*accumulators)
            return sum(sums), sum(counts), avg_temp
        else:
            return 0.0, 0, []

    def extract_output(self, accumulator):
        sum, count, avg_temp = accumulator
        if count == 0:
            return float("NaN")

        return sum / count


class GlobalTempDiffFn(beam.CombineFn):
    def create_accumulator(self):
        accumulator = []
        return accumulator

    def add_input(self, accumulator, input):
        accumulator.append(input)
        return accumulator

    def merge_accumulators(self, accumulators):
        tmp = []
        for a in accumulators:
            tmp.extend(a)
        return tmp

    def extract_output(self, accumulator):
        if len(accumulator) == 1:
            return "avg_global_temperature: " + str(
                accumulator[0]
            ), "temperature_change: " + str(0)
        return "avg_global_temperature: " + str(
            accumulator[1]
        ), "temperature_change: " + str(accumulator[1] - accumulator[0])


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
        global_diff | beam.ParDo(GlobalCustomSink())
