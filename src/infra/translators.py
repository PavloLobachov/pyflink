from pyflink.common import Row, Types

_event_schema = (Types.ROW_NAMED(
    ["tweet_hash", "tweet", "event_time"],
    [Types.STRING(), Types.STRING(), Types.STRING()]))


def _socket_kafka_record_to_row(data: str) -> Row:
    return Row(data)


def _socket_data_to_kafka_record(row: Row) -> Row:
    return row


def _aggregated_data_to_mongo_record(row: Row) -> Row:
    return Row(hash=str(row[0]),
               content=row[1],
               timestamp=str(row[2]),
               total_cases_count=int(row[3]),
               total_deaths_count=int(row[4]),
               total_recovered_count=int(row[5]),
               new_cases_count=int(row[6]),
               new_deaths_count=int(row[7]),
               new_recovered_count=int(row[8]),
               active_cases_count=int(row[9]),
               critical_cases_count=int(row[10]))
