# vim: set fileencoding=utf-8

"""
.. module:: fondos.backend.connect
   :platform: Unix; Windows
   :synopsis: Conexión directa con la base de datos.

Conexión directa con la base de datos.

.. moduleauthor:: José Miguel Sánchez Alés

.. version: |version|
   :date: |today|

Define una clase conectora para el acceso a la base de datos. Tal clase no usa
los objetos definidos en el modelo (véase :mod:`fondos.backend.model`), sino
que se limita a obtener los datos de la base de datos y devolverlos en forma de
tuplas crudas.
"""

from __future__ import annotations
import sqlite3
from typing import Iterable, Iterator, List, Tuple, Any, Union
from ..utils.config import Logger
from ..utils.backend.connect import ConnectorWithCursor, cursor
from ..utils.backend.errors import DatabaseError, DataError, IntegrityError
from contextlib import suppress
from datetime import date

#: Registro de sucesos
logger = Logger().get_logger(__name__)


class SQLiteConector(ConnectorWithCursor):
    """Clase conectora con la base de datos.

       Este conector devuelve en crudo los resultados en forma de tuplas o
       iteradores sobre tuplas.

       :cvar str BACKEND: Sistema gestor de bases de datos al que accede la
            clase.
    """

    BACKEND = sqlite3

    def __init__(self, database: str, ref_int: bool = False, dump=print,
                 schema: str = None):
        """Iniciliazador del objeto.

           :param database: Nombre del fichero que alberga la base de datos.
           :param bool ref_int: Si se habilita la integridad referencial o no.
           :param schema: Nombre del fichero que contiene el esquema de la
                base de datos.

           :raises DatabaseError: La base de datos no existe y no hay esquema
                para crearla.
        """
        super(SQLiteConector, self).__init__(
                database,
                dump=dump,
                detect_types=sqlite3.PARSE_DECLTYPES  # Convierte fechas a date
        )
        logger.info("Abierta conexión con la base de datos.")
        with self.session:
            self._init(database, ref_int, schema)

    @cursor
    def _init(self, database, ref_int, schema):
        """Comprueba si la base de datos posee el esquema.

           :returns: :obj:`True` si se creó el esquema.
           :rtype: bool
        """

        self.execute("""SELECT count(name)
                     FROM sqlite_master WHERE type = 'table'""")
        num = self.fetchone()[0]

        if num == 0:  # La base de datos está vacía
            msg = None
            if not schema:
                logger.debug(f'Leyendo el fichero de esquema {schema}')
                msg = "No hay esquema para la base de datos"
            else:
                try:
                    schema = open(schema, "r").read()
                except Exception as e:
                    msg = f'Error con el esquema: {str(e)}'

            if msg:
                import os
                self.close()
                logger.critical(msg)
                if database != ":memory:":
                    with suppress(OSError):
                        os.remove(database)
                raise DatabaseError(msg)

            logger.info("Dotando a la base de datos de un esquema")
            with self.log:
                self.executescript(schema)
        else:
            # Se supone que el esquema está bien.
            # aunque no se ha comprobado cuáles son las tablas.
            pass

        fk = "ON" if ref_int else "OFF"
        self.execute("PRAGMA foreign_keys={}".format(fk))

        return not num

    @cursor
    def registrar(self, reg: Iterable) -> int:
        """Registra un fondo en la base de datos.

           :param reg: El registro que representa el fondo a insertar.

           :returns: El ISIN del propio fondo.
        """
        reg = tuple(reg)
        with self.log:
            self.execute("INSERT INTO Fondo VALUES "
                         "(?, ?, ?, ?, ?, ?, ?, ?, ?)", reg)
        logger.info(f"Creado el fondo '{reg[0]}'")
        return self.lastrowid

    @cursor
    def get_fondo(self, *,
                  isin: str = None,
                  activo: bool = None) -> Iterator[Tuple]:
        """Devuelve los datos de los fondos de inversión.

           :param isin: ISIN del fondo. Si no se especifica se devuelven todos.
           :param activo:  Si ``True``, sólo devuelve fondos marcados como
                activos.

           :returns: Los registros que representan esos fondos.
        """
        sql, cond = "SELECT * FROM Fondo", []
        params: List[Any] = []

        if isin:
            cond.append("isin = ?")
            params.append(isin)

        if activo is not None:
            cond.append("activo = ?")
            params.append(activo)

        scond = "" if not cond else f'WHERE {" AND ".join(cond)}'

        self.execute(f'{sql} {scond}', params)
        logger.debug(f'Extraído fondo {isin if isin else "*"}')
        yield from self

    @cursor
    def registrar_cuenta(self, reg: Iterable) -> int:
        """Registra una cuenta partícipe.

           :param reg: El registro que representa la cuenta.
           :return: Un número identificador para la cuenta.
        """
        with self.log:
            self.execute("INSERT INTO tCuenta VALUES (?, ?, ?)", tuple(reg))
        logger.info("Registrada la cuenta {self.lastrowid}")
        return self.lastrowid

    @cursor
    def get_cuenta(self, num: str = None) -> Iterator[Tuple]:
        """Devuelve cuentas partícipes de fondos de inversión.

           :param num: Número de la cuenta. Si no se especifica se devuelven
                todas.

           :returns: Las cuentas pertinentes
        """
        sql, params = "SELECT * FROM tCuenta", []
        if num:
            sql += " WHERE cuentaID = ?"
            params.append(num)

        self.execute(sql, params)
        logger.debug(f'Extraída cuenta {num if num else "*"}')
        yield from self

    @cursor
    def apuntar(self, reg):
        """Apunta la cotización de un fondo.

           :param reg: Los datos del registro.

           :returns: Un número identificador para el registro.
        """
        apunte = list(reg)
        # TODO: Esto debería estar resuelto en la base de datos no aquí
        apunte[2] = round(apunte[2], 4)
        with self.log:
            self.execute("INSERT INTO tCotizacion VALUES (?, ?, ?)", apunte)
        logger.info("Registrada la cotización del día "
                    f"{apunte[1]} para el fondo {apunte[0]}.")

    @cursor
    def get_cotizacion(self, fondo: str, *, fi: date = None, ff: date = None,
                       limit: int = 0) -> Iterator[Tuple]:
        """Obtiene las cotizaciones registradas de un fondo.

           :param fondo: ISIN del fondo.
           :param fi: Fecha antes de la cual se desecharán cotizaciones.
           :param ff: Fecha tras la cual se desecharán cotizaciones.
           :param limit: Obtiene los n primeros resultados.
        """
        sql = ["SELECT * FROM tCotizacion WHERE isin = ?"]
        params: List[Any] = [fondo]

        if fi == ff and fi:  # Las fechas son iguales
            sql.append("AND fecha = ?")
            params.append(fi)
        else:
            if fi:
                sql.append("AND fecha >= ?")
                params.append(fi)
            if ff:
                sql.append("AND fecha <= ?")
                params.append(ff)

        sql.append("ORDER BY fecha DESC")
        if limit:
            sql.append(f"LIMIT {limit}")

        self.execute(" ".join(sql), params)
        logger.debug("Extraída las cotizaciones del fondo "
                     f'{fondo if fondo else "*"}')
        yield from self

    @cursor
    def suscribir(self, reg) -> int:
        """Realiza una suscripción sobre un fondo.

           :param reg: Los datos del registro.

           :returns: La identificación de la suscripción.
        """
        with self.log:
            self.execute("INSERT INTO Suscripcion "
                         "VALUES (?, ?, ?, ?, ?, ?)", tuple(reg))
        logger.info(f"Registrada la suscripción {self.lastrowid}")
        return self.lastrowid

    @cursor
    def get_suscripcion(self, *, id: int = None,
                        cuenta: str = None,
                        vivo: bool = True,
                        inversion: bool = None) -> Iterator[Tuple]:
        """Obtiene las suscripciones.

           En caso de obtener varias suscripciones se ordenan por fecha
           de inversión, no de suscripción (véase el SQL del esquema).

           :param id: Identificador de la suscripción. Si se especifica,
                no se atiende al resto de parámetros.
           :param cuenta: El número de cuenta partícipe de la que se
                quieren obtener las suscripciones.
           :param vivo: Si verdadero, sólo devuelve suscripciones cuyas
                participaciones no se han vendido todas.
           :param inversion: Si verdadero sólo se obtienen las suscripciones
                hechas con dinero nuevo, esto es, con dinero que no procede
                de la venta de otra suscripción. Si falso, todo lo contrario.
                Si no se especifica, no se atiende la procedencia del dinero.
        """
        sql = "SELECT * FROM Suscripcion"
        cond = []
        params: List = []

        if id:
            cond.append("suscripcionID = ?")
            params.append(id)
            msg = f'Extraída suscripción {id}'
        else:
            # Ordenamos por fecha de inversión, porque así es
            # como es como se desinvierte.
            if cuenta:
                cond.append("cuentaID = ? ")
                params.append(cuenta)

            if vivo:
                cond.append("(participaciones IS NULL OR participaciones > 0)")
            elif vivo is False:
                cond.append("participaciones = 0")

            if inversion:
                cond.append("origen IS NULL")
            elif inversion is False:
                cond.append("origen IS NOT NULL")

            msg = "Extraídas suscripciones de la cuenta " \
                  f'{cuenta if cuenta else "*"}.'

        scond = f'WHERE {" AND ".join(cond)}' if cond else ""

        self.execute(f'{sql} {scond}', params)
        logger.debug(msg)
        yield from self

    @cursor
    def get_ultima_orden(self) -> int:
        """Devuelve el número de la última orden ejecutada. Es útil si se
           quiere generar una nueva orden de compra.

           :returns: Tal número. 0, si aún no se hizo ninguna.
        """
        self.execute("SELECT MAX(orden) FROM tOrdenVenta")
        return next(self)[0] or 0

    @cursor
    def vender(self, reg) -> int:
        """Registra la venta de particionaciones.

           :param reg: El registro de venta.

           :returns: El identificador de la venta.
        """
        reg = tuple(reg)
        with self.log:
            self.execute("INSERT INTO VentaAggr VALUES (?, ?, ?, ?, ?, ?)",
                         reg)
        logger.info(f"Registra la venta con orden {reg[0]}")
        return reg[0]

    @cursor
    def get_venta(self, *,
                  orden: int = None,
                  cuenta: str = None,
                  rembolso: bool = None) -> Iterator[Tuple]:
        """Obtiene ventas de la base de datos.

           :param orden: Número de orden de la venta.
           :param cuenta: Cuenta partícipe de la que se venden participaciones.
           :param rembolso: Si ``True``, la venta no se invirtió en la compra
                de otro fondo. Si no se especifica, no se tiene en cuenta
                este parámetro.
        """
        sql, cond = "SELECT V.* FROM VentaAggr V", []
        params: List[Any] = []

        if orden:
            cond.append("V.orden = ?")
            params.append(orden)
            msg = f'Extraída la venta con orden {orden}'
        else:
            if cuenta:
                cond.append("V.cuentaID = ?")
                params.append(cuenta)

            if rembolso is not None:
                cond.append("IN (SELECT DISTINCT origen FROM Suscripcion)")
                if rembolso:
                    cond[-1] = f'orden IN {cond[-1]}'
                else:
                    cond[-1] = f'orden NOT IN {cond[-1]}'

            msg = "Extraídas ventas según las condiciones impuestas"

        scond = f'WHERE {" AND ".join(cond)}' if cond else ""

        self.execute(f'{sql} {scond}', params)
        logger.debug(msg)
        yield from self

    @cursor
    def traspasar(self, reg) -> int:
        """Inserta un traspaso en la base de datos.

           :param reg: El registro de traspaso.

           :returns: Un entero que no es el identificador, porque la tabla
                es en realidad una vista.
        """
        reg = tuple(reg)
        with self.log:
            self.execute("INSERT INTO TraspasoAggr VALUES "
                         "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", reg)
        logger.info(f'Registrado traspaso de {reg[1]} a {reg[6]}')
        return reg[0]

    @cursor
    def get_traspaso(self,
                     orden: int = None,
                     origen: str = None,
                     destino: str = None) -> Iterator[Tuple]:
        """Devuelve órdenes de traspaso.

           :param orden: Orden de venta relacionada con el traspaso.
           :param origen: Cuenta partícipe de la que se venden participaciones.
           :param destino: Cuenta de la que se compran participaciones.
        """
        sql, cond = "SELECT * FROM TraspasoAggr", []
        params: List[Any] = []

        if orden:
            cond.append("orden = ?")
            params.append(orden)
            msg = f'Extraído el traspaso con orden {orden}'
        else:
            if origen:
                cond.append("origen = ?")
                params.append(origen)
            if destino:
                cond.append("destino = ?")
                params.append(destino)

            msg = "Extraídas ventas según las condiciones impuestas"

        scond = f'WHERE {" AND ".join(cond)}' if cond else ""

        self.execute(f'{sql} {scond}', params)
        logger.debug(msg)
        yield from self

    @cursor
    def get_cartera(self, *,
                    fondo: str = None,
                    comercializadora: str = None,
                    fecha_i: date = None,
                    fecha_f: date = None,
                    viva: bool = None) -> Iterator[Tuple]:
        """Devuelve las suscripciones agrupadas por cuenta y con expresión
           del VL más reciente.

           :param fondo: ISIN del fondo del que se quiere conocer la inversión.
                Si no se especifica se obtiene la cartera completa, aunque
                se excluyen los fondos en los que ya no haya inversión.
           :param comercializadora: Numbre de la comercializadora de la cuenta.
           :param viva: Si `True``, la cuenta tiene participaciones.
        """
        cond = []
        params: List[Any] = []

        sql = """
            WITH Tiempo(inicial, final) AS (SELECT ?, ?)
            SELECT * FROM CarteraHistorica
        """
        params.extend((fecha_i, fecha_f))

        # TODO: Eliminar cuando se solucione el problema con CarteraHistorica
        if (fecha_i, fecha_f) == (None, None):
            sql = "SELECT * FROM Cartera"
            params.clear()

        if fondo:
            cond.append("isin = ?")
            params.append(fondo)

        if viva is not None:
            if viva:
                cond.append("(participaciones IS NULL OR participaciones > 0)")
            else:
                cond.append("participaciones = 0")

        if comercializadora:
            cond.append("comercializadora = ?")
            params.append(comercializadora)

        scond = f'WHERE {" AND ".join(cond)}' if cond else ""

        self.execute(f'{sql} {scond} ORDER BY cuentaID', params)
        logger.debug(f'Obtenida cartera del fondo {fondo if fondo else "*"}')
        yield from self

    @cursor
    def get_historial(self, *,
                      orden: int = None,
                      desinversion: int = None,
                      terminal: bool = False,
                      rembolso: bool = None) -> Iterator[Tuple]:
        """Obtiene el historial completo de cada inversión terminal.
           Por inversión terminal se entiende una inversión de la que se
           rembolsó ya el dinero (venta terminal) o un grupo de
           participaciones con un historial común que aún se conservan
           (suscripción terminal).

           :param orden: Orden de venta. Se devuelven los historiales de
                todas los registros de venta correspondientes a la orden
                referida. Téngase presente que se si venden participaciones
                con, por ejemplo, dos hitorias de inversión diferentes, se
                generarán dos registros de venta distintos con la misma orden
                de venta. Si la orden es 0, se devuelven inversiones terminales
                que aún son suscripciones.
           :param desinversion: Número de la suscripción de la que se
                desinvirtió dinero o se predente desinvertir.
           :param terminal: Si ``True``, se obtienen sólo las líneas de historial
                terminales; si ``False``, se obtienen todas.
           :param rembolso: Si ``True``, se obtienen ventas terminales; si
                ``False``, suscripciones terminales.
        """
        sql, cond = "SELECT * FROM Historial", []
        params: List[Any] = []

        if orden is not None:
            cond.append("orden = ?")
            params.append(orden)

        if desinversion:
            if rembolso is None:
                logger.warn("Pide el historial de una inversión "
                            "sin especificar el tipo")
            cond.append("desinversion = ?")
            params.append(desinversion)

        if rembolso is not None:
            if rembolso:
                cond.append("orden <> 0")
            else:
                cond.append("orden = 0")

        if terminal:
            cond.append("desinversion = suscripcionID")

        scond = f'WHERE {" AND ".join(cond)}' if cond else ""

        self.execute(f'{sql} {scond}')
        logger.debug('Extraídos los historiales requeridos')
        yield from self

    @cursor
    def get_plusvalia(self, *,
                      origen: int = None,
                      orden: int = None,
                      cuenta: str = None) -> Iterator[Tuple]:
        """Devuelve la plusvalía obtenida en una inversión.

           :param origen: Identificador de la suscripción original. Si se
                especifica no se atiende a los dos restantes parámetros.
           :param orden: Sólo devuelve la información fiscal de las
                inversiones que se venden con éste número de orden. Si
                el número de orden es 0, se devuelve las plusvalías de las
                suscripciones aún invertidas en caso de que se vendieran
                el último día del que se tienen datos sobre sus valores
                liquidativos.
           :param cuenta: Devuelve las plusvalías de las suscripciones
                asociadas a una cuenta.
        """
        sql, cond = "SELECT * FROM Plusvalia", []
        params: List[Any] = []

        if origen:
            cond.append("origen = ?")
            params.append(origen)
        else:
            if orden is not None:
                cond.append("orden = ?")
                params.append(orden)

            if cuenta is not None:
                cond.append("cuentaID = ?")
                params.append(cuenta)

        scond = f'WHERE {" AND ".join(cond)}' if cond else ""

        self.execute(f'{sql} {scond}', params)
        logger.debug('Extraída la información fiscal requerida')
        yield from self

    @cursor
    def get_ult_cotizaciones(self, num: int):
        """Obtiene las últimas cotizaciones de los fondos que tienen
           alguna suscripción viva.

            :param num: Número de cotizaciones que quieren obtenerse.
        """

        sql = "SELECT * FROM VarCotizacion WHERE numvar <= ?"
        self.execute(sql, [num])
        logger.debug(f'Extraídas las {num} últimas cotizaciones')
        yield from self

    @cursor
    def get_evolucion(self, periodo: str, *,
                      fi: date = None,
                      ff: date = None,
                      abcisas: bool = True,
                      desinversion: Union[None, bool, int] = None):
        """
        Devuelve la evolución temporal de cada inversión individual desde que
        se suscribieron con dinero nuevo.
        :param periodo: Periodo temporal de separación entre los distintos
            valores.
        :param fi: Fecha de comienzo del gráfico. Si no se especifica, se toma
            la fecha de la primera inversión.
        :param ff: Fecha de final del gráfico. Si no se especifica, se toma
            la fecha registrada más reciente.
        :param abcisas: Define si se quieren obtener valores para
            las ordenadas.
        :param desinversion: Desinversión de la que se quiere obtener
            la evolución. Si es :kbd:`False`, no se obtendrá ninguna, pero
            aún podrán obtenerse los valores para el eje de abcisas.
        """

        sql = """WITH Tiempo(inicial, final, periodo) AS (SELECT ?, ?, ?)
                 SELECT * FROM Evolucion"""

        params, cond = [fi, ff, periodo], []

        ret = True

        if abcisas:
            if desinversion is not None:
                if desinversion:
                    cond.append( "(desinversion IS NULL OR desinversion = ?)")
                    params.append(desinversion)
                else:
                    cond.append( "desinversion IS NULL")
        elif desinversion:
            cond.append("desinversion = ?")
            params.append(desinversion)
        elif desinversion is False:
            logger.warning('Sin datos de abcisas ni selección '
                           ' de desinversión la consulta no devuelve nada')
            ret = False
        else:
            cond.append('desinversion is not NULL')

        if not ret:
            yield from ()
        else:
            scond = f'WHERE {" AND ".join(cond)}' if cond else ""
            self.execute(f'{sql} {scond}', params)
            logger.debug('Extraída la evolución requirida')
            yield from self
