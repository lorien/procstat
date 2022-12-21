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

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.write_api: None | WriteApi = None

    def driver_connect(self) -> None:
        self.client = InfluxDBClient(**self.connect_options)
        assert self.client is not None
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def driver_write_events(
        self,
        snapshot: Mapping[str, int | float | str],
        measurement: str,
        tags: Mapping[str, str | int | float],
    ) -> None:
        point = Point(measurement).time(datetime.utcnow())
        for key, val in tags.items():
            point.tag(key, val)
        for key, val in snapshot.items():
            point.field(key, val)
        assert self.write_api is not None
        self.write_api.write(bucket=self.connect_options["bucket"], record=point)
