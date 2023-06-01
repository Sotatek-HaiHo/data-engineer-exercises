#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import os
import time
from datetime import datetime

import apache_beam as beam
import requests
from apache_beam import RestrictionProvider
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import WalltimeWatermarkEstimator
from apache_beam.transforms.ptransform import OutputT


class _RestrictionProviderTracker(OffsetRestrictionTracker):
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


class _RestrictionProvider(RestrictionProvider):
    def create_tracker(self, restriction):
        return _RestrictionProviderTracker(restriction)

    def initial_restriction(self, element):
        return OffsetRange(0, 1)

    def restriction_size(self, element, restriction):
        return restriction.size()


class _GetDataFromSource(beam.DoFn):
    """A splittable DoFn that generates an unbounded amount of data"""

    def __init__(self):
        super().__init__()
        self._initial_data = 0
        self._sleep_time = 1
        self._api_key = os.getenv("API_KEY")
        if not self._api_key:
            raise ValueError("API_KEY not found.")
        self._cities = [
            {"name": "Moscow", "lat": 55.7504461, "lon": 37.6174943},
            {"name": "Hà Nội", "lat": 21.0245, "lon": 105.8412},
            {"name": "New York", "lat": 40.7127281, "lon": -74.0060152},
        ]

    def setup(self):
        super().setup()

    def process(
        self,
        element,
        restriction_tracker=beam.DoFn.RestrictionParam(_RestrictionProvider()),
        watermark_estimator=beam.DoFn.WatermarkEstimatorParam(
            WalltimeWatermarkEstimator.default_provider()
        ),
    ):
        start = restriction_tracker.current_restriction().start
        json_list = []
        for city in self._cities:
            try:
                weather = self.fetch_data(
                    api_key=self._api_key, lat=city["lat"], lon=city["lon"]
                )
                message = {}
                message["name"] = weather["name"]
                message["timestamp"] = datetime.utcfromtimestamp(
                    time.time()
                ).isoformat()
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
            except Exception as e:
                print(
                    "Error while getting data for {}: {}".format(city["name"], str(e))
                )

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
        """
        Get the data from the given API
        """
        url = "https://api.openweathermap.org/data/2.5/weather?lat={}&lon={}&units=metric&appid={}".format(
            lat, lon, api_key
        )
        logging.info("Getting the weather...")
        r = requests.get(url)
        ret_data = r.json()
        self._initial_data = r.json()
        return ret_data


class OpenWeatherMapSource(beam.PTransform):
    """A root PTransform that use _GetDataFromSource
    to perform actual data generation"""

    def expand(self, pbegin) -> OutputT:
        return pbegin | beam.Impulse() | beam.ParDo(_GetDataFromSource())
