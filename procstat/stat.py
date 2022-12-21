from __future__ import annotations

import json
import logging
import sys
import time
from collections.abc import Mapping, MutableMapping, Sequence
from contextlib import suppress
from copy import deepcopy
from pprint import pprint  # pylint: disable=unused-import
from queue import Queue
from threading import Event, Lock, Thread
from typing import Any

from .base import BaseExportDriver

logger = logging.getLogger("procstat")


__all__ = ["Stat"]


class Stat:  # pylint: disable=too-many-instance-attributes
    default_key_aliases: Mapping[str, str] = {}
    ignore_prefixes: list[str] = []

    def __init__(  # pylint: disable=too-many-arguments
        self,
        eps_keys: None | str | Sequence[str] = None,
        logging_enabled: bool = True,
        logging_interval: int = 3,
        logging_format: str = "text",
        logging_level: int = logging.ERROR,
        key_aliases: None | Mapping[str, str] = None,
        export_driver: None | BaseExportDriver = None,
        export_interval: int = 5,
        fatalq: None | Queue[Any] = None,
        evt_shutdown: None | Event = None,
    ):
        if eps_keys is None:
            eps_keys = []
        elif isinstance(eps_keys, str):
            eps_keys = [eps_keys]
        self.eps_keys = eps_keys
        self.logging_enabled = logging_enabled
        self.logging_interval = logging_interval
        self.logging_format = logging_format
        self.logging_level = logging_level
        self.key_aliases = dict(self.default_key_aliases)
        if key_aliases:
            self.key_aliases.update(key_aliases)

        self.fatalq = fatalq
        # If shutdown event is not defined
        # then just use own event which can never be set
        self.evt_shutdown = evt_shutdown or Event()

        # Logging
        self.counters: MutableMapping[str, int] = {}
        self.moment_counters: MutableMapping[int, MutableMapping[str, int]] = {}
        self.logging_time = 0
        self.th_logging: None | Thread = None
        if self.logging_enabled:
            self.th_logging = Thread(target=self.thread_logging)
            self.th_logging.daemon = True
            self.th_logging.start()

        # Setup exporting in last case
        self.th_export: None | Thread = None
        self.export_driver = export_driver
        self.export_prev_counters: None | Mapping[str, int] = None
        self.th_export_lock = Lock()

        self.export_interval = export_interval

        # self.shard_counters = {}
        if self.export_driver:
            self.start_export_thread()

    def start_export_thread(self) -> None:
        if self.th_export:
            logging.error("Export thread is already started.")
        if not self.export_driver:
            raise Exception("Export drivers is not set")
        self.th_export = Thread(target=self.thread_export)
        self.th_export.daemon = True
        self.th_export.start()

        # Internal
        self.service_time = 0
        self.service_interval = 1

    def build_eps_data(self, now: float, interval: int) -> Mapping[str, int]:
        """Build string with event per seconds statistics.

        Args:
            interval - number of recent seconds for
            mean value calculation
        """
        now_int = int(now)
        eps: MutableMapping[str, int] = {}
        for ts in range(now_int - interval, now_int):
            for key in sorted(self.eps_keys):
                eps.setdefault(key, 0)
                with suppress(KeyError):
                    eps[key] += self.moment_counters[ts][key]
        return eps

    def build_eps_string(self, now: float) -> str:
        interval = 30
        eps = self.build_eps_data(now, interval)
        ret = []
        for key, val in eps.items():
            label = self.key_aliases.get(key, key)
            val_str = "%.1f" % (val / interval)
            if val_str == "0.0" and val > 0:
                val_str = "0.0+"
            ret.append("%s: %s" % (label, val_str))
        ret = sorted(ret, key=lambda x: x[0])
        return ", ".join(ret)

    def build_counter_data(self) -> Mapping[str, int]:
        return {
            key: val
            for key, val in self.counters.items()
            if not key.startswith(tuple(self.ignore_prefixes))
        }

    def build_counter_string(self) -> str:
        data = self.build_counter_data()
        ret = []
        for key in sorted(data.keys()):
            label = self.key_aliases.get(key, key)
            val = data[key]
            ret.append("%s=%d" % (label, val))
        return ", ".join(ret)

    def render_moment_json(self, now: float) -> str:
        interval = 30
        return json.dumps(
            {
                "eps": self.build_eps_data(now, interval),
                "counter": self.build_counter_data(),
            }
        )

    def render_moment(self, now: None | float = None) -> str:
        if now is None:
            now = time.time()
        if self.logging_format == "json":
            return self.render_moment_json(now)
        eps_str = self.build_eps_string(now)
        counter_str = self.build_counter_string()
        delim = " " if eps_str else ""
        return "EPS: %s%s| TOTAL: %s" % (eps_str, delim, counter_str)

    def thread_logging(self) -> None:
        try:
            while not self.evt_shutdown.is_set():
                now = time.time()
                logger.log(self.logging_level, self.render_moment(now))
                # Sleep `self.logging_interval` seconds minus time spent on logging
                sleep_time = self.logging_interval + (time.time() - now)
                time.sleep(sleep_time)
        except (KeyboardInterrupt, Exception):
            if self.fatalq:
                self.fatalq.put((sys.exc_info(), None))
            else:
                raise

    def calc_diff(
        self, counters: Mapping[str, int], prev_counters: Mapping[str, int]
    ) -> Mapping[str, int]:
        return {x: (counters[x] - prev_counters.get(x, 0)) for x in counters}

    def th_export_dump_stat(self) -> None:
        assert self.export_driver is not None
        with self.th_export_lock:
            counters = deepcopy(self.counters)
            delta_counters = (
                self.calc_diff(counters, self.export_prev_counters)
                if self.export_prev_counters
                else counters
            )
            self.export_prev_counters = counters
        self.export_driver.write_events(delta_counters)

    def thread_export(self) -> None:
        try:
            while True:
                ts = time.time()
                self.th_export_dump_stat()
                sleep_time = self.export_interval - (time.time() - ts)
                if sleep_time > 0:
                    time.sleep(sleep_time)
        except (KeyboardInterrupt, Exception):
            if self.fatalq:
                self.fatalq.put((sys.exc_info(), None))
            else:
                raise
        finally:
            self.th_export_dump_stat()

    def update_moment_slot(self, key: str, count: int) -> None:
        # FIXME: delete old moment_counters items
        moment_slot = self.moment_counters.setdefault(int(time.time()), {})
        moment_slot.setdefault(key, 0)
        moment_slot[key] += count

    def inc(self, key: str, count: int = 1) -> None:
        self.update_moment_slot(key, count)
        self.counters.setdefault(key, 0)
        self.counters[key] += count

    def shutdown(self, join_threads: bool = True) -> None:
        self.evt_shutdown.set()
        if join_threads and self.th_logging:
            self.th_logging.join()
