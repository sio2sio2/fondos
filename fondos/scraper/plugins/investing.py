# vim: set fileencoding=utf-8

"Scraper para INVESTING.COM"

from __future__ import annotations
from typing import Iterator, Tuple
from .. import ScraperBase
from ..errors import ParseError
from datetime import date, datetime


class Scraper(ScraperBase):
    "Scraper que obtiene datos de investing.com"

    URL: str = ("https://in.investing.com/funds/{data}?end_date={ff}&"
                "st_date={fi}")
    NAME: str = "investing"
    HEADERS: dict = {"res-scheme": "1", "User-Agent": "Mozilla/5.0"}

    def _make_url(self, fecha, delta=None) -> str:
        from datetime import timedelta

        fecha = fecha or date.today()
        delta = delta or 7

        fi = f'{(fecha + timedelta(days=-delta)):%s}'
        ff = f'{(fecha + timedelta(days=1)):%s}'

        return self.URL.format(data=self.data, fi=fi, ff=ff)

    def _parse(self) -> Iterator[Tuple[date, float]]:
        import json

        if self._content_type != "application/json":
            raise ParseError("Error en la escarga. Esperado "
                             "'application/json)'; encontrado "
                             f"{self._content_type}")
        body = json.loads(self._page)["body"]
        data = body["content"]["_list"][-2]["nested"]["body"]["vars"]["data"]
        for d in data:
            yield (date.fromtimestamp(d["rowDateRaw"]),
                   float(d["last_closeRaw"]))
