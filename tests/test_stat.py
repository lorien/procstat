import time
from threading import Event

from procstat.stat import Stat


def test_inc_default_count() -> None:
    stat = Stat()
    stat.inc("foo")
    assert stat.counters["foo"] == 1


def test_inc_specific_count() -> None:
    stat = Stat()
    stat.inc("foo", 2)
    assert stat.counters["foo"] == 2


def test_inc_multiple() -> None:
    stat = Stat()
    total = 0
    for num in range(5):
        stat.inc("foo", num)
        total += num
        assert stat.counters["foo"] == total


def test_render_moment() -> None:
    stat = Stat()
    stat.inc("foo")
    assert stat.render_moment() == "EPS: | TOTAL: foo=1"


def test_external_event_shutdown_logging_thread() -> None:
    evt_shutdown = Event()
    stat = Stat(evt_shutdown=evt_shutdown, logging_interval=0.1)
    time.sleep(0.1)
    assert stat.th_logging and stat.th_logging.is_alive()
    evt_shutdown.set()
    time.sleep(0.2)
    assert not stat.th_logging.is_alive()


def test_internal_event_shutdown_logging_thread() -> None:
    stat = Stat(logging_interval=0.1)
    time.sleep(0.1)
    assert stat.th_logging and stat.th_logging.is_alive()
    stat.shutdown(join_threads=False)
    time.sleep(0.2)
    assert not stat.th_logging.is_alive()


def test_internal_event_shutdown_logging_thread_join_threads() -> None:
    stat = Stat(logging_interval=0.1)
    time.sleep(0.1)
    assert stat.th_logging and stat.th_logging.is_alive()
    stat.shutdown(join_threads=True)
    assert not stat.th_logging.is_alive()
