import apache_beam as beam


class WeatherSink(beam.DoFn):
    """
    Format the output of a pipeline
    """

    def process(self, data: list, timestamp=beam.DoFn.TimestampParam):
        print(data, ",", timestamp)
