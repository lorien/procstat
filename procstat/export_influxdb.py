from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from influxdb_client import InfluxDBClient, Point, WriteApi
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS
from requests import RequestException

from .base import BaseExportDriver

LOG = logging.getLogger(__file__)


class InfluxdbExportDriver(BaseExportDriver):
    retry_exceptions = (
        OSError,
        RequestException,
        InfluxDBError,
    )

    def __init__(
        self,
        connect_options: Mapping[str, Any],
        tags: None | Mapping[str, Any] = None,
    ) -> None:
        super().__init__(tags=tags)
        self.write_api: None | WriteApi = None
        self.connect_options = connect_options

    def driver_connect(self) -> None:
        self.client = InfluxDBClient(**self.connect_options)
        assert self.client is not None
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def driver_write_events(
        self,
        snapshot: Mapping[str, int | float | str],
        tags: Mapping[str, str | int | float],
    ) -> None:
        for field_key, field_val in snapshot.items():
            point = Point(field_key).time(datetime.utcnow())
            point.field("value", field_val)
            for key, val in tags.items():
                point.tag(key, val)
            assert self.write_api is not None
            self.write_api.write(bucket=self.connect_options["bucket"], record=point)
