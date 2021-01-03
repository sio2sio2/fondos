# vim: set fileencoding=utf-8

"""Clases que modelan las tablas de la base de datos"""

from __future__ import annotations
from datetime import date, datetime
from typing import Iterator, Optional, Union, Tuple, List
from ..scraper.errors import ScraperError
from ..scraper import scraper
from ..utils.backend.model import Register
from ..utils.backend.errors import DataError
from ..utils.config import Logger

#: Registro de sucesos
logger = Logger().get_logger(__name__)


class Fondo(Register):
    """Modela un fondo de inversión"""
    _fields = ("isin* nombre alias gestora moneda riesgo "
               "scraper scraper_data activo")

    @property
    def alias(self) -> str:
        """Nombre corto del fondo"""
        return self._alias or self.nombre

    @property
    def scraper_data(self) -> str:
        """Dato que debe pasarse al scraper para caracterizar al fondo"""
        return self._scraper_data or self.isin

    def insert(self) -> int:
        """Registra el fondo en la base de datos.

           :returns: Devuelve el ISIN del fondo.
        """
        return self.db.registrar(self)

    @classmethod
    def get(cls, *, isin: str = None, activo: bool = None) -> Iterator[Fondo]:
        return cls.db.get_fondo(isin=isin, activo=activo)

    @property
    def crecimiento(self) -> Optional[bool]:
        """Indica si ha subido o bajado el fondo utilizando
           sus dos últimas cotizaciones disponibles."""
        cotizaciones = tuple(self.db.get_cotizacion(self.isin, limit=2))
        try:
            return cotizaciones[0][2] - cotizaciones[1][2] > 0.
        except IndexError:  # Ni hay datos suficientes.
            return None


class Cuenta(Register):
    """Modela una cuenta partícipe"""
    _fields = "id* isin comercializadora"

    _fondo: Fondo

    def insert(self) -> str:
        """Registra la cuenta partícipe.

           :returns: El número la cuenta.
        """
        return self.db.registrar_cuenta(self)

    @classmethod
    def get(cls, *, num: str = None) -> Iterator[Cuenta]:
        return cls.db.get_cuenta(num)

    @property
    def fondo(self) -> Fondo:
        """Fondo que se ha suscrito"""
        try:
            return self._fondo
        except AttributeError:
            try:
                self._fondo = next(self.db.Fondo.get(isin=self.isin))
            except AttributeError:
                raise AttributeError("Registro no asociado a la base de datos")

            return self._fondo


class Suscripcion(Register):
    """Modula las suscripciones hechas a fondos"""
    _fields = "id* cuentaID fecha participaciones coste origen"

    _cuenta: Cuenta
    _comision: float
    _coste: float
    _vl: float

    def insert(self) -> int:
        """Registra la suscripción en la base de datos.

           :returns: El identificador de la suscripción.
        """
        if self.coste is None:
            logger.info("No puede registrarse el coste de la suscripción")

        return self.db.suscribir(self)

    @classmethod
    def get(cls, *,
            id: int = None,
            cuenta: Union[str, Cuenta] = None,
            vivo: bool = True,
            inversion: bool = None) -> Iterator[Suscripcion]:
        if isinstance(cuenta, Cuenta):
            cuenta = cuenta.cuentaID
        return cls.db.get_suscripcion(id=id, cuenta=cuenta,
                                      vivo=vivo, inversion=inversion)

    @property
    def coste(self) -> Optional[float]:
        """Coste de la suscripcion"""
        if not self._coste and self.vl:
            self._coste = self.vl*self.participaciones

        return self._coste

    @property
    def vl(self) -> Optional[float]:
        """VL del fondo el día de las suscripción"""
        try:
            return self._vl
        except AttributeError:
            pass

        try:
            self._vl = self.db.Cotizacion.force_get(self.cuenta.fondo,
                                                    self.fecha).vl
        except AttributeError as e:
            return None

        return self._vl

    @property
    def activa(self) -> bool:
        """Informa de si la suscripción está activa"""
        return self.participaciones > 0

    @property
    def comision(self):
        """Comisión de suscripción"""
        if not hasattr(self, "_comision"):
            if self.coste is None:
                return None

            self._comision = self.coste - self.vl*self.participaciones

        return self._comision

    @property
    def cuenta(self) -> Cuenta:
        """Cuenta partícipe asociada"""
        try:
            return self._cuenta
        except AttributeError:
            self._cuenta = next(self.db.Cuenta.get(num=self.cuentaID), None)

        return self._cuenta

    @property
    def venta(self):
        "Venta que originó la suscripción"
        if self.origen is None:
            return None

        try:
            return self._venta
        except AttributeError:
            self._venta = next(self.db.Venta.get(id=self.origen), None)

        return self._venta


class Cotizacion(Register):
    """Modela la cotización de un fondo.

       Si la cotización se crea un con VL nulo, se intentará obtener
       usando el scraper.
    """
    _fields = "isin* fecha* vl"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.vl is None:
            if self.isin is None:
                raise DataError("Fondo indeterminado")

            fondo = next(type(self).db.Fondo.get(isin=self.isin), None)
            if not fondo:
                raise DataError(f'No existe el fondo {self.isin}')

            s = scraper(fondo.scraper, fondo.scraper_data or fondo.isin)
            try:
                s.connect(self.fecha)
            except ScraperError:
                logger.warn('No puede obtenerse la cotización')
            else:
                try:
                    self._vl = next(s.cotizacion, None)[1]
                except TypeError:
                    logger.warn("No puede obtenerse la cotización")

                s.disconnect()

    def insert(self) -> int:
        return self.db.apuntar(self)

    @classmethod
    def get(cls,
            fondo: Union[str, Fondo], *,
            fi: date = None,
            ff: date = None,
            limit: int = 0) -> Iterator[Cotizacion]:

        if isinstance(fondo, Fondo):
            fondo = fondo.isin
        return cls.db.get_cotizacion(fondo, fi=fi, ff=ff, limit=limit)

    @classmethod
    def get_last(cls,
                 fondo: Union[str, Fondo], *,
                 limite: date = None) -> Optional[Cotizacion]:
        """Obtiene la última cotización registrada del fondo.

           :param fondo: Fondo del que se quiere obtener la cotización.
           :param limite: Fecha límite que se considerará como última.
        """
        return next(cls.get(fondo, ff=limite, limit=1), None)

    @classmethod
    def force_get(cls,
                  fondo: Union[str, Fondo],
                  fecha: date) -> Optional[Cotizacion]:
        """Obtiene la cotización de un fondo en una fecha, incluso si
           no existe aún en la base de datos, en cuyo caso utliza el
           el scraper y registra el valor permanentemente.

           :param fondo: Fondo del que se quiere obtener la cotización.
           :param fecha: Fecha en la que se quiere obtener el valor.

           :returns: La cotización o ``None``, si no es posible obtenerla.
        """
        c = next(cls.get(fondo, fi=fecha, ff=fecha), None)
        if c:
            return c

        fondo = getattr(fondo, "isin", fondo)
        logger.debug(f'Sin registro del VL de {fondo} en {fecha:%d/%m/%Y}')
        c = cls(fondo, fecha, None)
        try:
            c.insert()
        except Exception:  # TODO: Cambiar este ERROR.
            return None

        return c

    @property
    def fondo(self) -> Fondo:
        """Fondo que se ha suscrito"""
        try:
            return self._fondo
        except AttributeError:
            try:
                self._fondo = next(self.db.Fondo.get(isin=self.isin))
            except AttributeError:
                raise AttributeError("Registro no asociado a la base de datos")

            return self._fondo


class Venta(Register):
    """Modela una venta agregada"""
    _fields = "orden* cuentaID fecha participaciones reintegro comentario"

    _cuenta: Cuenta
    _vl: float
    _reintegro: float

    def insert(self) -> int:
        """Registra la venta en la base de datos.

           :returns: El identificador de la venta.
        """
        return self.db.vender(self)

    @classmethod
    def get(self, *,
            orden: int = None,
            cuenta: Union[str, Cuenta] = None,
            rembolso: bool = None) -> Iterator[Venta]:

        if isinstance(cuenta, Cuenta):
            cuenta = cuenta.cuentaID

        return self.db.get_venta(orden=orden, cuenta=cuenta, rembolso=rembolso)

    @property
    def reintegro(self) -> float:
        "Rembolso de la venta"
        if not self._reintegro and self.vl:
            self._reintegro = self.vl*self.participaciones

        return self._reintegro

    @property
    def cuenta(self):
        """Cuenta de la que se venden participaciones"""
        try:
            return self._cuenta
        except AttributeError:
            try:
                cuenta = self.db.Cuenta
                self._cuenta = next(cuenta.get(id=self.cuentaID))
            except StopIteration:
                raise AttributeError("Registro no asociado a la base de datos")

            return self._cuenta

    @property
    def vl(self) -> Optional[float]:
        """VL del fondo vendido el día de la venta"""
        try:
            return self._vl
        except AttributeError:
            pass

        try:
            isin = self.cuenta.isin
            self._vl = self.db.Cotizacion.force_get(isin, self.fecha).vl
        except AttributeError:
            return None

        return self._vl


class Traspaso(Register):
    """Modela un traspaso de fondos"""
    _fields = ("orden* origenID fecha_v part_v monto "
               "destinoID fecha_c part_c comentario")

    _venta: Venta
    _origen: Cuenta
    _destino: Cuenta

    def insert(self) -> int:
        """Registra el traspaso en la base de datos.

           :returns: El identificador de la venta.
        """
        return self.db.traspasar(self)

    @classmethod
    def get(cls, *,
            orden: int = None,
            origen: str = None,
            destino: str = None) -> Iterator[Traspaso]:
        """Obtiene órdenes de traspaso.

           :param orden: Sólo obtiene las relativas a la orden de venta
                especificada.
           :param traspaso: Sólo devuelve traspasos reales, no ventas.
        """
        return cls.db.get_traspaso(orden, origen, destino)

    def venta(self):
        """Venta que origina el traspaso"""
        try:
            self._venta
        except AttributeError:
            try:
                self._venta = next(self.db.get_venta(orden=self.orden))
            except StopIteration:
                raise AttributeError("Registro no asociado a la base de datos")

        return self._venta

    @property
    def origen(self):
        try:
            return self._origen
        except AttributeError:
            try:
                cuenta = self.db.Cuenta
                self._origen = next(cuenta.get(id=self.origenID))
            except StopIteration:
                raise AttributeError("Registro no asociado a la base de datos")

        return self._origen

    @property
    def destino(self):
        try:
            return self._destino
        except AttributeError:
            try:
                cuenta = self.db.Cuenta
                self._destino = next(cuenta.get(id=self.destinoID))
            except StopIteration:
                raise AttributeError("Registro no asociado a la base de datos")

        return self._destino


class Cartera(Register):
    """Modela la visión de una cartera"""
    _fields = "isin cuentaID comercializadora capital " \
              "fecha vl participaciones valoracion plusvalia"

    _cuenta: Cuenta

    @classmethod
    def get(cls, *,
            fondo: Union[str, Fondo] = None,
            comercializadora: str = None,
            viva: bool = None) -> Iterator[Cartera]:
        if isinstance(fondo, Fondo):
            fondo = fondo.isin
        return cls.db.get_cartera(fondo=fondo,
                                  comercializadora=comercializadora, viva=viva)

    @property
    def cuenta(self) -> Cuenta:
        try:
            return self._cuenta
        except AttributeError:
            self._cuenta = next(self.db.Cuenta.get(num=self.cuentaID), None)

        return self._cuenta

    @property
    def fondo(self) -> Fondo:
        """Fondo que se ha suscrito"""
        try:
            return self._fondo
        except AttributeError:
            try:
                self._fondo = next(self.db.Fondo.get(isin=self.isin))
            except AttributeError:
                raise AttributeError("Registro no asociado a la base de datos")

            return self._fondo

class Historial(Register):
    """Historial de una inversion"""
    _fields = "desinversion suscripcionID cuentaID orden fecha_i " \
              "fecha fecha_v coste participaciones reintegro"

    _cuenta: Optional[Cuenta]
    _suscripcion: Optional[Suscripcion]

    @classmethod
    def get(cls,
            orden: int = None,
            inversion: int = None,
            rembolso: bool = None) -> Iterator[Tuple]:

        return cls.db.get_historial(orden=orden, inversion=inversion,
                                    rembolso=rembolso)

    @property
    def cuenta(self):
        try:
            return self._cuenta
        except AttributeError:
            self._cuenta = next(self.db.Cuenta.get(num=self.cuentaID), None)

        return self._cuenta

    @property
    def suscripcion(self) -> Optional[Suscripcion]:
        try:
            return self._suscripcion
        except AttributeError:
            suscr = self.db.Suscripcion
            self._suscripcion = next(suscr.get(id=self.suscripcionID), None)

            return self._suscripcion

    @property
    def rembolsada(self) -> bool:
        """Indica si la inversión ya se rembolsó"""
        return self.orden != 0


class Plusvalia(Register):
    """Modela la información fiscal"""
    _fields = ("desinversion fecha_i capital origenID fecha_v orden "
               "suscripcionID cuentaID participaciones rembolso")

    _origem: Suscripcion
    _suscripcion: Suscripcion
    _cuenta: Cuenta

    @classmethod
    def get(cls, *,
            origen: Union[int, Suscripcion] = None,
            orden: int = None,
            cuenta: Union[str, Cuenta] = None) -> Iterator[Plusvalia]:
        """Obtiene las plusvalías de en una inversión.

           :param origen: Identificador de la suscripción original. Si se
                especifica no se atiende a los dos restantes parámetros.
           :param orden: Orden de venta. Si es 0, se obtienen previsibles
                plusvalías de participaciones aún no vendidas.
           :param cuenta: Cuenta de la que se venden participaciones.
        """
        origen = getattr(origen, "id", origen)
        cuenta = getattr(cuenta, "id", cuenta)

        return cls.db.get_plusvalia(origen=origen, orden=orden, cuenta=cuenta)

    @property
    def cuenta(self):
        try:
            return self._cuenta
        except AttributeError:
            self._cuenta = next(self.db.Cuenta.get(num=self.cuentaID), None)

        return self._cuenta

    @property
    def suscripcion(self) -> Optional[Suscripcion]:
        try:
            return self._suscripcion
        except AttributeError:
            suscr = self.db.Suscripcion
            self._suscripcion = next(suscr.get(id=self.suscripcionID), None)

            return self._suscripcion

    @property
    def origen(self) -> Optional[Suscripcion]:
        try:
            return self._origen
        except AttributeError:
            suscr = self.db.Suscripcion
            self._origen = next(suscr.get(id=self.origenID), None)

            return self._origen

    @property
    def rembolsada(self):
        """Indica si la plusvalía es real, o sea, si ya se desinvirtió"""
        return orden != 0
