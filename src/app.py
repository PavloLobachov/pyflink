import logging
import sys
import argparse
from typing import Optional
from infra.factory import Factory

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")


def parse_args() -> Optional[argparse.Namespace]:
    """Parse input arguments."""
    desc = ('Read raw tweets from input topic '
            'perform event translation end enrichment with data from https://www.worldometers.info/coronavirus/ '
            'and write result to mongodb')
    parser = argparse.ArgumentParser(description=desc)
    if '-h' in sys.argv or '--help' in sys.argv:
        parser.print_help(sys.stderr)
        sys.exit(0)
    parser.add_argument(
        'Environment',
        metavar='env',
        type=str,
        nargs='?',
        default='local',
        help='Environment to run the job in. Default is "local". <local, dev>'
    )
    parser.add_argument(
        'Job',
        metavar='job',
        type=str,
        nargs='?',
        default='all',
        help='Type of job to run. Default is "collect". <collect, aggregate>'
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logging.info(f"Environment: {args.Environment}")
    logging.info(f"Job: {args.Job}")

    if args.Environment not in ["local", "dev"]:
        raise ValueError(
            f"Invalid environment parameter: {args.Environment}, "
            f"expected: env type <local, dev>")
    if args.Job not in ["collect", "aggregate", "all"]:
        raise ValueError(
            f"Invalid job parameter: {args.Job}, "
            f"expected: job type <collect, aggregate, all>")

    factory = Factory(args.Environment)
    job = args.Job

    if job == "collect":
        stream = factory.create_socket_stream("TwitterCollectionStreamingJob")
        stream.execute_async()
    elif job == "aggregate":
        pass
        stream = factory.create_aggregator_stream("TwitterAggregationStreamingJob")
        stream.execute_async()
    else:
        stream1 = factory.create_socket_stream("TwitterCollectionStreamingJob")
        stream1.execute_async()
        stream2 = factory.create_aggregator_stream("TwitterAggregationStreamingJob")
        stream2.execute()
