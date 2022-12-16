from __future__ import annotations

import logging
import time
from abc import abstractmethod
from collections.abc import Mapping
from copy import deepcopy
from typing import Any

LOG = logging.getLogger()


class BaseExportDriver:
    default_measurement_name = "event"

    @abstractmethod
    def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def write_events(
        self,
        snapshot: Mapping[str, int | float | str],
        measurement: None | str = None,
        tags: None | Mapping[str, str | int | float] = None,
    ) -> None:
        raise NotImplementedError


class BaseInfluxdbExportDriver(BaseExportDriver):
    reconnect_interval_sec: float = 1
    retry_exceptions: tuple[type[Exception], ...] = ()

    @abstractmethod
    def driver_connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def driver_write_events(
        self,
        snapshot: Mapping[str, int | float | str],
        measurement: str,
        tags: Mapping[str, str | int | float],
    ) -> None:
        raise NotImplementedError

    def __init__(
        self,
        connect_options: Mapping[str, Any],
        tags: None | Mapping[str, Any] = None,
        measurement: None | str = None,
    ) -> None:
        self.connect_options = deepcopy(connect_options)
        self.client = None
        self.default_measurement = (
            measurement if measurement else self.default_measurement_name
        )
        self.tags = deepcopy(tags or {})
        self.database_created = False
        self.connect()

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
        measurement: None | str = None,
        tags: None | Mapping[str, str | int | float] = None,
    ) -> None:
        if not snapshot:
            return
        proc_tags = {**self.tags, **tags} if tags else self.tags
        proc_measurement = measurement or self.default_measurement_name
        try:
            while True:
                try:
                    self.driver_write_events(
                        snapshot=snapshot,
                        measurement=proc_measurement,
                        tags=proc_tags,
                    )
                except self.retry_exceptions:
                    LOG.exception("Failed to write metrics")
                    self.repair_connection()
                else:
                    break
        except Exception:
            LOG.exception("ERROR IN STAT WRITE EVENTS")
            raise
