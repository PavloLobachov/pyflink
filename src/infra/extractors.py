from datetime import datetime
from pyflink.common.watermark_strategy import TimestampAssigner


class TweetTimestampAssigner(TimestampAssigner):
    def __init__(self):
        self.epoch = datetime.utcfromtimestamp(0)

    def extract_timestamp(self, value, record_timestamp) -> int:
        milliseconds = self._datetime_string_to_datetime(value[2])
        return int((milliseconds - self.epoch).total_seconds() * 1000)

    def _datetime_string_to_datetime(self, datetime_str) -> datetime:
        format_str = "%Y-%m-%d %H:%M:%S.%f"
        return datetime.strptime(datetime_str, format_str)
