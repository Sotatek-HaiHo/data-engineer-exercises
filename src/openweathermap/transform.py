#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import Counter

import apache_beam as beam


class AverageFn(beam.CombineFn):
    """
    Calculating average temperature for each city
    """

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
        def _find_most_frequent(lst):
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
            main_desc = _find_most_frequent(desc)
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


def parse_input(line: tuple) -> dict:
    """
    Parse the input line and return a dictionary
    """

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
    """
    Calculate the global average temperature
    """

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
    """
    Calculates the global temp difference between the avg_global_temperature from
    the current minute minus the avg_global_temperature from the previous minute
    """

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


class CustomSink(beam.DoFn):
    """
    Format the output of a pipeline
    """

    def process(self, data, timestamp=beam.DoFn.TimestampParam):
        timestamp = timestamp
        print(data, ",", timestamp)
