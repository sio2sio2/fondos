# vim: set fileencoding=utf-8

"""
.. module:: fondos.scraper
   :platform: Unix; Windows
   :synopsis: Obtención de datos

Obtención de datos.

,, moduleauthor:: José Miguel Sánchez Alés

.. version: |version|
   :date: |today|

Paquete que reúne los scrapers posibles para obtener la cotización
de los fondos de inversión.
"""

from __future__ import annotations
from typing import Iterator, Tuple, List
from importlib import import_module
from .errors import ConnectionError, AlreadyConnectedError, \
        ScraperNotFoundError, ParseError
from abc import ABCMeta, abstractmethod, abstractproperty
from datetime import datetime, date
from ..utils.config import Logger

__all__ = ['scraper', 'list']

#: Registro de información
logger = Logger().get_logger(__name__)


def scraper(plugin: str,
            data: str) -> ScraperBase:
    """Devuelve el scraper apropiado para obtener la cotización del fondo.

      :param plugin: Nombre del plugin que requiere el fondo.
      :param data: Datos que utiliza el scraper para localizar la cotización
            del fondo. Normalmente el ISIN, pero puede ser otro.

      Uso básico::

        >>> s = scraper(fondo.scraper,  # Fondo es un objeto Fondo.
                        fondo.scraper_data or fondo.isin)
        >>> tuple(s.cotizacion)  # Devuelve la última cotización.
        >>> s.disconnect()
        >>> s.connect(None, delta=30)
        >>> tuple(s.cotizacion)  # Devuelve las cotizaciones del último mes.
        >>> s.disconnect()
        >>> s.connect(date(2016, 12, 31), delta=30)
        >>> tuple(s.cotizacion)  # Devuelve las cotizaciones de dic. de 2016.
    """

    try:
        scraper = import_module(".plugins." + plugin, package=__package__)
    except ImportError:
        msg = f"{plugin}: El scraper no existe."
        logger.error(msg)
        raise ScraperNotFoundError(msg)
    return scraper.Scraper(data)


def list() -> List:
    """Lista los scrapers disponibles"""
    from pkgutil import iter_modules
    import os.path

    return [m[1] for m in iter_modules(
        path=[os.path.join(os.path.dirname(__file__), "plugins")]
    )]


class ScraperBase(metaclass=ABCMeta):
    """Base para definir los scrapers.

       :cvar str URL: Base de la URL de la que se obtendrán los datos.
       :cvar str NAME: Nombre del plugin.
       :ivar fondo: Fondo del que se quiere obtener la cotización.
       :ivar _fecha: Fecha en la que se desea obtener la cotización.
       :ivar _delta: Periodo de tiempo del que se quieren obtener cotizaciones.
            Si es :obj:`None`, sólo se quiere obtener la cotización expresada
            :attr:`~fondos.scraper.ScraperBase._fecha`.
       :ivar str _page: Página descargada
       :ivar str _content_type: Tipo MIME de la respuesta.
       :ivar _cotizacion: Cotizaciones obtenidas de la web.
       :vartype fondo: class:`~fondos.backend.model.Fondo`
       :vartype _fecha: class:`~datetime.date`
       :vartype _cotizacion: Iterator
    """

    URL = abstractproperty()
    NAME = abstractproperty()
    HEADERS = {"User-Agent": "Mozilla/5.0"}

    @abstractmethod
    def _make_url(self, fecha: date, delta: int = None) -> str:
        """Construye la URL que permite obtener la cotización.

           :param fecha: Fecha en que se quiere obtener la cotización.
           :param delta: Número de días de los que se quiere obtener
             contización. Su obtendrán, pues, las cotizaciones entre
             ``fecha - delta`` y ``fecha``. Si es ``None``, se obtiene
             sólo la cotización de la fecha pedida.

           :returns: La URL construida
        """
        pass

    @abstractmethod
    def _parse(self) -> Iterator[Tuple[date, float]]:
        """Procesa la página en busca de los valores de liquidación.

           rtype: Iterator
        """
        pass

    def __init__(self, data: str):
        self.data = data
        self.disconnect()

    @property
    def connected(self) -> bool:
        """Informa de si ya hubo conexión a la página de datos"""
        return self._page is not None

    @property
    def fecha(self) -> date:
        """Fecha de obtención del valor de liquidación"""
        return self._fecha

    @property
    def cotizacion(self) -> Iterator[Tuple[date, float]]:
        """Valores liquidativos y fecha correspondiente.

           Si se pide la cotización sin una conexión, previa; debe obtenerse
           la última cotización. Si se quiere obtener una serie de cotizaciones
           es necesario usar previamente el método
           :meth:`~fondos.scraper._scraper.ScraperBase.connect` con los
           argumentos apropiados.

           rtype: Iterator
        """
        if not self._cotizacion:
            # Si se pide la cotización sin haber hecho
            # una extracción manual, se obtiene la última cotización.
            self.extract(reparse=True)
        return self._cotizacion

    def connect(self, fecha=None, delta=None):
        """Descarga la página

           :param fecha: Fecha de la que se quiere obtener la cotización.
                :obj:`None`, implica que se quiere obtener la última
                disponible en la web.
           :param delta: Si no es :obj:`None`, se entiende que se quieren
             obtener las cotizaciones entre *fecha - delta* y *fecha*
        """
        import urllib.request as request

        url = self._make_url(fecha, delta)
        logger.debug(f"Intentando conexión a {url}")
        req = request.Request(url, headers=self.HEADERS)
        try:
            response = request.urlopen(req)
        except request.URLError:
            raise ConnectionError("Imposible conectar")
        else:
            msg = f"Fallo de conexión. Código HTTP {response.getcode()}"
            if response.getcode() != 200:
                logger.error(msg)
                raise ConnectionError(msg)
            else:
                fp = fecha and f'{fecha:%Y-%m-%d}'
                logger.debug(f'Cotización en {fp}[{delta or 1}] '
                             f'para {self.data}.')
                charset = response.info().get_content_charset() or "utf-8"
                self._page = response.read().decode(charset)
                self._content_type = response.info().get_content_type()
                self._delta = delta
                self._fecha = fecha

    def extract(self, reparse=False):
        """Extrae los datos de cotización de la web.

           :param reparse: Si se quiere reprocesar la página,
                aún habiendo ya hecho la conexión.
        """
        if self.connected and not reparse:
            logger.error("Conexión existente: debería desconectar.")
            raise AlreadyConnectedError("Desconéctese primero")
        elif not self.connected:
            self.connect(self._fecha, self._delta)
        try:
            self._cotizacion = self._parse()
        except ParseError as e:
            logger.error(e)
            raise e


    def disconnect(self):
        """Desconecta de la págian web.

           En realidad, resetea los atributos que almacenan los datos
           obntenidos por el scraper.
        """
        self._fecha = self._cotizacion = self._delta = None
        self._page = self._content_type = None
