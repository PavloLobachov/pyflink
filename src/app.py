import logging
import sys

from infra.factory import Factory

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

if __name__ == "__main__":
    argv_ = sys.argv[1:]
    print("Arguments passed to script:", argv_)
    factory = Factory(argv_[0])

    env1 = factory.get_env("TwitterCollectionStreamingJob")
    stream1 = factory.create_local_socket_stream(env1)
    stream1.process()
    env1.execute_async(job_name="TwitterCollectionStreamingJob")

    env2 = factory.get_env("TwitterAggregationStreamingJob")
    stream2 = factory.create_local_aggregator_stream(env2)
    stream2.process()
    env2.execute(job_name="TwitterAggregationStreamingJob")
