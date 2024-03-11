from datetime import datetime


def iso_8601_to_timestamp(iso_8601_date: str) -> int:
    return int(
        datetime.strptime(
            iso_8601_date,
            "%Y-%m-%dT%H:%M:%S%z",
        ).timestamp()
        * 1000,
    )
