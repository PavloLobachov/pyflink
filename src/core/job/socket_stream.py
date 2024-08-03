import re
from datetime import datetime
from typing import Callable

from pyflink.common import Types, Row, Configuration
from pyflink.datastream import MapFunction

from core.sink import ValueSink
from core.source import ValueSource
from core.streaming import Stream


class SocketStream(Stream):

    def __init__(self,
                 tweet_source_generator: Callable[[], ValueSource[str]],
                 tweet_sink_generator: Callable[[], ValueSink[Row]],
                 job_name: str,
                 jar_files: list[str] = None,
                 configuration: Configuration = None):
        super().__init__(job_name, jar_files, configuration)
        self.tweet_source = tweet_source_generator()
        self.tweet_sink = tweet_sink_generator()

    def process(self) -> None:
        ds = self.read(self.tweet_source)
        cleared_ds = (ds
                      .map(func=CleanTweetFunction(),
                           output_type=Types.STRING())
                      .uid("clean-tweets")
                      .map(func=MapTweetToRowFunction(),
                           output_type=Types.ROW([Types.STRING(), Types.STRING(), Types.STRING()]))
                      .uid("format-tweets"))
        self.write(self.tweet_sink, cleared_ds)


class CleanTweetFunction(MapFunction):
    def map(self, tweet: str) -> str:
        cleaned_string = re.sub(r'\bRT:', '', tweet)
        cleaned_string = re.sub(r'#', '', cleaned_string)
        cleaned_string = re.sub(r'http\S+|www.\S+', '', cleaned_string)
        cleaned_string = re.sub(r'"', '', cleaned_string)
        cleaned_string = re.sub(r'\n', '', cleaned_string)
        cleaned_string = cleaned_string.strip()
        return cleaned_string


class MapTweetToRowFunction(MapFunction):
    def map(self, tweet: str) -> Row:
        event_time = datetime.now()
        hash_c = tweet.__hash__()
        return Row(str(hash_c), tweet, str(event_time))
