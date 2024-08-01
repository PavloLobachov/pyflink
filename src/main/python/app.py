import logging
import sys

from infra.factory import Factory

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

if __name__ == "__main__":
    topic = "tweets"
    group = "g12"

    jars_path = "/Users/pavlolobachov/Downloads/flink_jars/"

    factory = Factory()

    # checkpoint_path_1 = "/Users/pavlolobachov/PycharmProjects/pyflink/checkpoints/TwitterCollectionStreamingJob"
    # env1 = factory.get_local_env(checkpoint_path_1, jars_path)
    # stream1 = factory.create_local_socket_stream(env1, topic)
    # stream1.process()
    # env1.execute_async(job_name="TwitterCollectionStreamingJob")

    checkpoint_path_2 = "/Users/pavlolobachov/PycharmProjects/pyflink/checkpoints/TwitterAggregationStreamingJob"
    env2 = factory.get_local_env(checkpoint_path_2, jars_path)
    stream2 = factory.create_local_aggregator_stream(env2, topic, group)
    stream2.process()
    env2.execute(job_name="TwitterAggregationStreamingJob")

