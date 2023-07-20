from __future__ import annotations

import logging
import time
from abc import abstractmethod
from collections.abc import Mapping
from copy import deepcopy
from typing import Any

LOG = logging.getLogger()


class BaseExportDriver:
    reconnect_interval_sec: float = 1
    retry_exceptions: tuple[type[Exception], ...] = ()

    def __init__(
        self,
        tags: None | Mapping[str, Any] = None,
        raise_on_error: bool = False,
    ) -> None:
        self.client = None
        self.tags = deepcopy(tags or {})
        self.database_created = False
        self.raise_on_error = raise_on_error
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

    def write_events(
        self,
        snapshot: Mapping[str, int | float | str],
        tags: None | Mapping[str, str | int | float] = None,
    ) -> None:
        if not snapshot:
            return
        proc_tags = {**self.tags, **tags} if tags else self.tags
        try:
            while True:
                try:
                    self.driver_write_events(
                        snapshot=snapshot,
                        tags=proc_tags,
                    )
                except self.retry_exceptions:
                    LOG.exception("Failed to write metrics")
                    self.repair_connection()
                else:
                    break
        except Exception:
            LOG.exception("ERROR IN STAT WRITE EVENTS")
            if self.raise_on_error:
                raise
