from typing import Tuple, List

from pyflink.common import Types, Time, Row
from pyflink.datastream import StreamExecutionEnvironment, AggregateFunction, MapFunction
from pyflink.datastream.window import TumblingEventTimeWindows

from core.sink import ValueSink
from core.source import ValueSource
from core.streaming import Stream


class AggregatorStream(Stream):

    def __init__(self,
                 env: 'StreamExecutionEnvironment',
                 tweet_source: ValueSource,
                 tweet_sink: ValueSink):
        self.env = env
        self.tweet_source = tweet_source
        self.tweet_sink = tweet_sink
        self.agg_schema = Types.TUPLE(
            [Types.STRING(), Types.LIST(Types.STRING()), Types.STRING()])
        self.res_schema = Types.ROW_NAMED(
            ["tweet_hash", "tweets", "event_time"],
            [Types.STRING(), Types.LIST(Types.STRING()), Types.STRING()])

    def process(self) -> None:
        ds = self.read(self.tweet_source)

        cleared_ds = (ds
                      .key_by(lambda row: row[0])
                      .window(TumblingEventTimeWindows.of(Time.seconds(20)))
                      .aggregate(TweetAggregateFunction(),
                                 accumulator_type=self.agg_schema,
                                 output_type=self.agg_schema)
                      .uid("aggregated-tweets")
                      .map(func=MapAggregatedToRowFunction(),
                           output_type=self.res_schema)
                      .uid("result-tweets"))

        self.write(self.tweet_sink, cleared_ds)


class TweetAggregateFunction(AggregateFunction):

    def create_accumulator(self) -> Tuple[str, List[str], str]:
        return "", [], ""

    def add(self, value: Tuple[str, str, str], accumulator: Tuple[str, List[str], str]) -> Tuple[str, List[str], str]:
        if accumulator[0] == "":
            return value[0], [value[1]], value[2]
        accumulator[1].append(value[1])
        return accumulator

    def get_result(self, accumulator: Tuple[str, List[str], str]) -> Tuple[str, List[str], str]:
        return accumulator

    def merge(self, a: Tuple[str, List[str], str], b: Tuple[str, List[str], str]) -> Tuple[str, List[str], str]:
        a[1].extend(b[1])
        return a


class MapAggregatedToRowFunction(MapFunction):
    def map(self, tweets: Tuple[str, List[str], str]) -> Row:
        print("MapAggregatedToRowFunction", tweets)
        return Row(tweets[0], tweets[1], tweets[2])
