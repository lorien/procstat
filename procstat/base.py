from __future__ import annotations

import logging
import re
import time
from abc import abstractmethod
from collections.abc import Mapping
from copy import deepcopy
from typing import Any

LOG = logging.getLogger()


class BaseExportDriver:
    reconnect_interval_sec: float = 1
    retry_exceptions: tuple[type[Exception], ...] = ()
    re_safe_metric_name = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
    re_nonsafe_metric_name_char = re.compile(r"[^a-zA-Z0-9_:]")

    def __init__(
        self,
        tags: None | Mapping[str, Any] = None,
        raise_on_error: bool = False,
        fix_metric_names: bool = True,
    ) -> None:
        self.client = None
        self.tags = deepcopy(tags or {})
        self.database_created = False
        self.raise_on_error = raise_on_error
        self.fix_metric_names = fix_metric_names
        self.connect()

    @abstractmethod
    def driver_connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def driver_write_events(
        self,
        snapshot: Mapping[str, int | float | str],
        tags: Mapping[str, str | int | float],
    ) -> None:
        raise NotImplementedError

    def connect(self) -> None:
        self.driver_connect()

    def repair_connection(self) -> None:
        while True:
            time.sleep(self.reconnect_interval_sec)
            try:
                self.connect()
            except self.retry_exceptions:
                LOG.exception("Failed to reconnect to database")
            else:
                return

    def fix_metric_name(self, name: str) -> str:
        return self.re_nonsafe_metric_name_char.sub("_", name)

    def write_events(
        self,
        snapshot: Mapping[str, int | float | str],
        tags: None | Mapping[str, str | int | float] = None,
    ) -> bool:
        proc_tags = {**self.tags, **tags} if tags else self.tags
        for metric_name in snapshot:
            if not self.re_safe_metric_name.match(metric_name):
                LOG.warning("Non-safe metric name: %s", metric_name)
        if self.fix_metric_names:
            snapshot = {self.fix_metric_name(x): y for x, y in snapshot.items()}
        try:
            # while True:
            #    try:
            self.driver_write_events(
                snapshot=snapshot,
                tags=proc_tags,
            )
            return True
            #    except self.retry_exceptions:
            #        LOG.exception("Failed to write metrics")
            #        self.repair_connection()
            #    else:
            #        break
        except Exception:
            LOG.exception("Failed to export stat data")
            if self.raise_on_error:
                raise
            return False
