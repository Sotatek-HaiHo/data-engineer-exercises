#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import Counter
from dataclasses import dataclass

import apache_beam as beam


@dataclass
class _CityData:
    """
    Temperature data class for city
    """

    sum: float
    count: int
    temp: list[float]
    des: list[str]
    location: str

    def _merge(self, other: "_CityData") -> "_CityData":
        return _CityData(
            self.sum + other.sum,
            self.count + other.count,
            self.temp + other.temp,
            self.des + other.des,
            self.location,
        )


def _dict_to_city_data(input_dict) -> _CityData:
    return _CityData(
        input_dict["temp"],
        1,
        [input_dict["temp"]],
        [input_dict["main"]],
        location=input_dict["name"],
    )


class AverageFn(beam.CombineFn):
    """
    Calculating average temperature for each city
    """

    def __init__(self):
        super().__init__()

    def create_accumulator(self):
        return dict()

    def add_input(self, accumulator, input_dict):
        new_data = _dict_to_city_data(input_dict)
        location = input_dict["name"]
        if location not in accumulator:
            accumulator[location] = new_data
        else:
            accumulator[location] = accumulator[location]._merge(new_data)
        return accumulator

    def merge_accumulators(self, accumulators):
        output = dict()
        for accumulator in accumulators:
            for location, city_data in accumulator.items():
                if location not in output:
                    output[location] = city_data
                else:
                    output[location]._merge(city_data)
        return output

    def extract_output(self, accumulator):
        def _find_most_frequent(lst):
            counter = Counter(lst)
            most_common = counter.most_common()

            # Find the item with the highest count and occurred first
            max_count = most_common[0][1]
            result = next(
                (item for item, count in most_common if count == max_count), None
            )
            return result

        results = []
        for value in accumulator.values():
            res = {
                "location": value.location,
                "avg_temperature": value.sum / value.count,
                "temperature_diff": value.temp[-1] - value.temp[0],
                "description": _find_most_frequent(value.des),
            }
            results.append(res)
        return results


class GlobalTempFn(beam.CombineFn):
    """
    Calculate the global average temperature
    """

    def create_accumulator(self):
        sum = 0.0
        count = 0
        accumulator = sum, count
        return accumulator

    def add_input(self, accumulator, input):
        sum, count = accumulator
        for i in input:
            sum += float(i["avg_temperature"])
        return sum, count + len(input)

    def merge_accumulators(self, accumulators):
        if len(accumulators) > 0:
            sums, counts = zip(*accumulators)
            return sum(sums), sum(counts)
        else:
            return 0.0, 0

    def extract_output(self, accumulator):
        sum, count = accumulator
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
