# vim: set fileencoding=utf-8

"Scraper para VIMEA.ES"

from __future__ import annotations
from typing import Iterator, Tuple
from .. import ScraperBase
from ..errors import ParseError
from datetime import date, datetime


class Scraper(ScraperBase):
    "Scraper que obtiene datos de broker,vinea.es (Unicaja)"

    URL: str = ("https://broker.vinea.es/Broker2_2105/csv/ServiceCsv.action?"
                "service=historicoFondosLiquidativosCsv&fechaInicio={fi}"
                "&fechaFin={ff}&periodo={peo}&isin={isin}")
    NAME: str = "vinea"

    def _make_url(self, fecha, delta=None) -> str:
        """Construye la URL que permite obtener la cotización.

           Dependiendo de cuáles sean los argumentos se obtendrá lo siguiente:

           =========== =============== =======================================
             fecha     delta (en días) Valor de retorno
           =========== =============== =======================================
           :obj:`None` :obj:`None`     La última cotización
           :obj:`None` *N*             Las cotizaciones de los N últimos días.
           *FECHA*     :obj:`None`     La cotización en *FECHA* o la anterior.
           *FECHA*     *N*             Las cotizaciones entre FECHA-N y FECHA.
           =========== =============== =======================================

           :param fecha: Fecha en que se quiere obtener la cotización.
           :param delta: El periodo de tiempo en días del que se quieren
             obtener todas las cotizaciones.
        """
        from datetime import timedelta

        if delta and not fecha:
            from datetime import date
            fecha = date.today()

        peo = "DIA"
        if not delta:
            peo = "v"
            # En el periodo de una semana seguro que
            # ha habido alguna cotización
            delta = 7

        if fecha:
            fi = f"{fecha + timedelta(days=-delta):%d%m%Y}"
            ff = f"{fecha + timedelta(days=1):%d%m%Y}"
        else:
            fi = ff = ""

        url = self.URL.format(isin=self.data, peo=peo, fi=fi, ff=ff)
        return url

    def _parse(self) -> Iterator[Tuple[date, float]]:
        if self._content_type != "text/csv":
            msg = ("Error en la descarga. "
                   f"Esperado 'text/csv'; encontrado '{self._content_type}'")
            raise ParseError(msg)

        lineas = self._page.split('\n')[1:-1]
        for cotizacion in lineas:
            fecha, vl = cotizacion.split(';')
            fecha = datetime.strptime(fecha, "%Y-%m-%d %H:%M:%S.%f").date()
            yield fecha, float(vl.replace(",", "."))
