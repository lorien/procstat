from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
from requests import RequestException

from .base import BaseExportDriver

LOG = logging.getLogger(__file__)


class InfluxdbV1ExportDriver(BaseExportDriver):
    retry_exceptions = (
        OSError,
        RequestException,
        InfluxDBClientError,
        InfluxDBServerError,
    )

    def __init__(
        self,
        connect_options: Mapping[str, Any],
        tags: None | Mapping[str, Any] = None,
    ) -> None:
        super().__init__(tags=tags)
        self.connect_options = connect_options

    def driver_connect(self) -> None:
        self.client = InfluxDBClient(**self.connect_options)

    def driver_write_events(
        self,
        snapshot: Mapping[str, int | float | str],
        tags: Mapping[str, str | int | float],
    ) -> None:
        assert self.client is not None
        if not self.database_created:
            self.client.create_database(self.connect_options["database"])
            self.database_created = True
        items = []
        for field_key, field_val in snapshot.items():
            items.append(
                {
                    "measurement": field_key,
                    "tags": tags,
                    "time": datetime.utcnow().isoformat(),
                    "fields": {"value": field_val},
                }
            )
        self.client.write_points([items])
