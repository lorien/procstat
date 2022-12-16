from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
from requests import RequestException

from .base import BaseInfluxdbExportDriver

LOG = logging.getLogger(__file__)


class InfluxdbV1ExportDriver(BaseInfluxdbExportDriver):
    retry_exceptions = (
        OSError,
        RequestException,
        InfluxDBClientError,
        InfluxDBServerError,
    )

    def driver_connect(self) -> None:
        self.client = InfluxDBClient(**self.connect_options)

    def driver_write_events(
        self,
        snapshot: Mapping[str, int | float | str],
        measurement: str,
        tags: Mapping[str, str | int | float],
    ) -> None:
        assert self.client is not None
        if not self.database_created:
            self.client.create_database(self.connect_options["database"])
            self.database_created = True
        data = {
            "measurement": measurement,
            "tags": tags,
            "time": datetime.utcnow().isoformat(),
            "fields": snapshot,
        }
        self.client.write_points([data])
