from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from typing import Any

from rich.pretty import pprint  # pylint: disable=unused-import # noqa: F401
from urllib3 import PoolManager, make_headers

from .base import BaseExportDriver

DEFAULT_NETWORK_TIMEOUT = 3
LOG = logging.getLogger(__file__)


class PrometheusExportDriver(BaseExportDriver):
    retry_exceptions = ()

    def __init__(
        self,
        connect_options: Mapping[str, Any],
        tags: None | Mapping[str, Any] = None,
    ) -> None:
        super().__init__(tags=tags)
        self.connect_options = connect_options
        self.network = PoolManager()
        assert connect_options["address"].startswith(("http://", "https://"))
        self.export_url = "{}/api/v1/import/prometheus".format(
            connect_options["address"],
        )
        if connect_options.get("username") and connect_options.get("password"):
            auth = "{}:{}".format(
                connect_options["username"], connect_options["password"]
            )
            self.headers = make_headers(basic_auth=auth)
        else:
            self.headers = {}

    def driver_connect(self) -> None:
        pass

    def driver_write_events(
        self,
        snapshot: Mapping[str, int | float | str],
        tags: Mapping[str, str | int | float],
    ) -> None:
        items = []
        for field_key, field_val in snapshot.items():
            data = "{}{{{}}} {} {}".format(
                field_key,
                ",".join('{}="{}"'.format(x[0], x[1]) for x in tags.items()),
                field_val,
                str(int(time.time() * 1000)),
            )
            items.append(data.encode())
        res = self.network.request(
            "POST",
            self.export_url,
            headers=self.headers,
            body=b"\n".join(items),
            timeout=DEFAULT_NETWORK_TIMEOUT,
        )
        if res.status != 204:
            LOG.error(
                "Failed to write data to %s, response status is %s",
                self.export_url,
                res.status,
            )
