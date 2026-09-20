import logging
import sys
from typing import Any

from eventiq.logging import KeyValueFormatter

FORMAT = "%(levelname)s:%(name)s:%(message)s"


def raise_boom() -> None:
    msg = "boom"
    raise ValueError(msg)


def make_record(**extra: Any):
    record = logging.LogRecord(
        "eventiq.test", logging.INFO, __file__, 1, "Starting consumer task", None, None
    )
    record.__dict__.update(extra)
    return record


def test_extras_are_appended_as_key_value_pairs():
    formatted = KeyValueFormatter(FORMAT).format(
        make_record(consumer_name="orders", task_index=0)
    )
    assert formatted == (
        "INFO:eventiq.test:Starting consumer task consumer_name='orders' task_index=0"
    )


def test_record_without_extras_is_unchanged():
    formatted = KeyValueFormatter(FORMAT).format(make_record())
    assert formatted == "INFO:eventiq.test:Starting consumer task"


def test_standard_attributes_are_not_repeated():
    formatted = KeyValueFormatter("%(levelname)s %(name)s %(message)s").format(
        make_record()
    )
    assert "levelname" not in formatted
    assert "lineno" not in formatted


def test_exception_traceback_stays_after_the_extras():
    record = make_record(consumer_name="orders")
    try:
        raise_boom()
    except ValueError:
        record.exc_info = sys.exc_info()

    formatted = KeyValueFormatter(FORMAT).format(record)
    first_line, _, traceback = formatted.partition("\n")
    assert first_line.endswith("consumer_name='orders'")
    assert traceback.startswith("Traceback (most recent call last):")


def test_formatter_survives_records_without_the_expected_keys():
    """The reason extras are appended instead of named in the format string."""
    formatter = KeyValueFormatter(FORMAT)
    assert formatter.format(make_record(consumer_name="orders"))
    assert formatter.format(make_record())
