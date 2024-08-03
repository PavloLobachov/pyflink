import abc
import logging
import sys

from pyflink.common import JobClient, JobExecutionResult, Configuration
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway

from core.sink import ValueSink
from core.source import ValueSource

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")


class Stream(StreamExecutionEnvironment, metaclass=abc.ABCMeta):
    def __init__(self,
                 job_name: str,
                 jar_files: list[str] = None,
                 configuration: Configuration = None):
        gateway = get_gateway()
        JStreamExecutionEnvironment = gateway.jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
        if configuration:
            j_stream_exection_environment = JStreamExecutionEnvironment.getExecutionEnvironment(
                configuration._j_configuration)
        else:
            j_stream_exection_environment = JStreamExecutionEnvironment.getExecutionEnvironment()
        super().__init__(j_stream_exection_environment)
        self.add_jars(*jar_files)
        self.job_name = job_name

    def read(self, source: ValueSource, **kwargs) -> 'DataStream':
        return source.read(env=self, **kwargs)

    def write(self, sink: ValueSink, ds: 'DataStream', **kwargs) -> None:
        sink.write(ds=ds, env=self, **kwargs)

    @abc.abstractmethod
    def process(self) -> None:
        pass

    def execute_async(self, job_name: str = 'Flink Streaming Job') -> JobClient:
        if job_name:
            jn = job_name
        else:
            jn = self.job_name
        logging.info(f"Submitting stream: {jn}")
        self.process()
        return super().execute_async(jn)

    def execute(self, job_name: str = None) -> JobExecutionResult:
        if job_name:
            jn = job_name
        else:
            jn = self.job_name
        logging.info(f"Submitting stream: {jn}")
        self.process()
        return super().execute(jn)
