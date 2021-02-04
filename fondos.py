#!/usr/bin/env python3

"Gestiona mis fondos de inversión"

from __future__ import annotations

import sys
from os import path
from datetime import date, datetime, timedelta
from fondos.scraper import scraper
from fondos.utils.config import config, Logger
from fondos.utils.backend.errors import Error
from fondos.utils.iniargparse import IniArgumentParser
from argparse import Action
from fondos.backend import SQLiteConector, Fondo, Cuenta, Cotizacion, \
        Suscripcion, Venta, Cartera, Historial, Traspaso, Plusvalia, Evolucion
import matplotlib.pyplot as plt

logger = Logger().get_logger(__name__)

def agrega_path(nombre: str) -> str:
    return path.join(path.dirname(sys.argv[0]), nombre)


class VerboseAction(Action):
    def __call__(self, parser, args, values, option_string=None):
        if option_string in ("--quiet", "-q"):
            setattr(args, self.dest, 0)

        if getattr(args, self.dest) != 0:
            verbose = getattr(args, self.dest)
            if verbose < 4:
                setattr(args, self.dest, verbose + 1)


def registrar():
    if not config.dump:
        return print

    def printf(statement):
        with open(agrega_path(config.dump), "a") as f:
            print(statement, file=f)

    return printf


def parse_args():
    """Analiza los parámetros facilitados"""
    parser = IniArgumentParser(description=__doc__)
    parser.register("action", "verbose", VerboseAction)

    vgroup = parser.add_mutually_exclusive_group()
    vgroup.add_argument("-v", nargs=0, action="verbose", dest="verbose",
                        default=1, help="Muestra información adicional del " +
                        "proceso. Aumente la información añadiendo más " +
                        "veces la opción.")
    vgroup.add_argument("-q", "--quiet", nargs=0,
                        action="verbose", dest="verbose",
                        help="No muestra mensajes de información")

    parser.add_argument("-d", "--dump", action="store", default="/dev/null",
                         help=("Fichero donde se apuntan las operaciones "
                               "de registro"))

    parser.add_argument("-D", "--database", action="store",
                        default="fondos.db", help="Base de datps")
    parser.add_argument("-e", "--extract", action="store_true",
                        help="Extrae la última cotización de la web")
    parser.add_argument("-E", "--force-extract", action="store_true",
                        help="Extrae las cotizaciones necesarias para mostrar "
                             "la información requerida")
    parser.add_argument("-C", "--config-file", action="config",
                        default=agrega_path("fondos.ini"),
                        help="Fichero de configuración")

    vgroup = parser.add_mutually_exclusive_group()
    vgroup.add_argument("-i", "--inversiones", action="store_true",
                        help="Muestra las cuentas actuales de inversiones "
                        "con expresión de todas sus participaciones y la "
                        "la ganancia obtenida con ellas. Es la información "
                        "que, de primeras, suelen proporcionar las "
                        "comercializadoras")
    vgroup.add_argument("-p", "--plusvalia", action="store_true",
                        help="Muestra las plusvalias obtenidas")
    vgroup.add_argument("-g", "--grafico", action="store_true",
                        help="Muestra gráficamente la evolución de las "
                        "inversiones")
    vgroup.add_argument("-H", "--history", action="store",
                        help="Muestra las últimas cotizaciones de un fondo "
                        "(ISIN:DIAS). Si no se especifica DÍAS, se "
                        "sobreentienden 10")
    vgroup.add_argument("-I", "--inversiones_H", action="store",
                        help="Muestra la composición de la cartera desde o "
                             "hasta la fecha indicada: AAAA-MM-DDi, o bien "
                             "AAAA-MM-DD.")

    vgroup = parser.add_mutually_exclusive_group()
    vgroup.add_argument("-f", "--fondo", action="store_true",
                        help="Agrega fondos desde stdin: "
                             "ISIN|NOMBRE;ALIAS|GESTORA|SCRAPER|ACTIVO|"
                             "MONEDA|SCRAPER_DATA")
    vgroup.add_argument("-r", "--rembolso", action="store_true",
                        help="Agrega venta a la base de datos (stdin)")
    vgroup.add_argument("-c", "--cuenta", action="store_true",
                        help="Agrega una cuenta partícipe a la base de datos "
                        "desde stdin: IDENTIFICADOR|ISIN|COMERCIALIZADORA")
    vgroup.add_argument("-s", "--suscripcion", action="store_true",
                        help="Agrega suscripción a la base de datos (stdin)")
    vgroup.add_argument("-t", "--traspaso", action="store_true",
                        help="Agrega traspaso a la base de datos (stdin)")
    vgroup.add_argument("-l", "--valor-liquidativo", action="store_true",
                        help="Agrega manualmente valor liquidativo: "
                             "ISIN|fecha|VL")
    vgroup.add_argument("-L", "--cotizaciones", action="store_true",
                        help="Extrae con el scraper cotizaciones entre una "
                             "fecha inicial y una final. Si la final no se "
                             "expresa, se obtiene una sóla cotización: "
                             "ISIN|fecha_i|fecha_")

    parser.parse_args(namespace=config)


def extraer_cotizaciones(fecha=None):
    "Extrae las cotizaciones de los fondos activos"

    fecha = fecha and datetime(fecha, "%Y-%m-%d").date()

    logger.debug("Extrayendo cotizaciones de la web")
    db = config.db

    with db.session:

        def actualizado(fondo, fecha):
            if(not fecha):
                fecha = datetime.today()
                # Hasta las diez de la noche, se intenta
                # obtener cotizaciones del día anterior.
                if datetime.today().hour < 22:
                    fecha -= timedelta(days=1)

            # Si la fecha cae en fin de semana, el último día es el viernes.
            dia = max(fecha.weekday() - 4, 0)
            fecha -= timedelta(days=dia)

            try:
                fecha = fecha.date()  # Era una fecha con hora.
            except AttributeError:
                pass

            return len(tuple(db.Cotizacion.get(fondo, fi=fecha, ff=fecha))) > 0

        vistos = []

        for suscr in db.Suscripcion.get(vivo=True):
            fondo = suscr.cuenta.fondo

            if fondo.isin in vistos:
                continue
            vistos.append(fondo.isin)

            if not fondo.activo:
                logger.info(f"{fondo.alias} inactivo. No se obtiene "
                            "cotización.")
                continue

            if(actualizado(fondo, fecha)):
                logger.info(f'{fondo.alias} ya está actualizado')
                continue

            s = scraper(fondo.scraper, fondo.scraper_data)
            s.connect(fecha)
            uc, vl = next(s.cotizacion, (None, None))
            if vl is None:
                logger.error("Imposible obtener cotizaciones del fondo"
                             f"{fondo.alias}")
                continue

            cot = db.Cotizacion(fondo.isin, uc, round(vl, 4))
            try:
                cot.insert()
            except Error as e:
                logger.warning(f"{fondo.alias} ya tiene la última "
                               "cotización disponible")

    logger.debug("Extracción de la web finalizada")


def parse_line(line, tipo, sep="|"):
    "Procesa una línea proporcionada al programa"
    from distutils.util import strtobool

    def convert(value):
        """Convierte el valor a su tipo más probable"""
        value = value.strip()

        if not value:
            return None

        try:
            return int(value)
        except ValueError:
            pass

        try:
            return float(value)
        except ValueError:
            pass

        try:
            return strtobool(value)
        except ValueError:
            pass

        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return value

    def fondo(isin, nombre, alias, gestora, scraper, riesgo,
              activo=0, moneda="EUR", scraper_data=None):

        return (isin, nombre, alias, gestora, moneda, riesgo,
                scraper, scraper_data, activo)

    def cuenta(numero, isin, comercializadora):
        return (numero, isin, comercializadora)

    def suscripcion(cuenta, fecha, coste=None, participaciones=None):
        return (None, cuenta, fecha, participaciones, coste, None)

    def rembolso(cuenta, fecha, participaciones,
                 reintegro=None, comentario=None):
        return (None, cuenta, fecha, participaciones, reintegro, comentario)

    def traspaso(cuenta, fecha, destino, fecha_c=None, participaciones=None,
                 rembolso=None, part_c=None, comentario=None):
        return (None, cuenta, fecha, participaciones, rembolso,
                destino, fecha_c, part_c, comentario)

    def cotizacion(cuenta, fecha, vl=None):
        return (cuenta, fecha, vl)

    def cotizaciones(fondo, fecha_i, fecha_f=None):
        if not isinstance(fecha_i, date):
            raise TypeError('Fecha inicial incorrecta')

        try:
            delta = fecha_f and (fecha_f - fecha_i).days
        except (AttributeError, TypeError):
            raise TypeError('Fecha final incorrecta')

        if (delta or 0) < 0:
            raise ValueError('Fecha final anterior a la inicial')

        return (fondo, fecha_f or fecha_i, delta)

    ftipo = {
        "fondo": fondo,
        "cuenta": cuenta,
        "suscripcion": suscripcion,
        "rembolso": rembolso,
        "traspaso": traspaso,
        "valor_liquidativo": cotizacion,
        "cotizaciones": cotizaciones
    }[tipo]

    return ftipo(*map(convert, line.split(sep)))


class Interfaz:
    """Implementa la interfaz de texto del programa"""

    def __init__(self, color=True):
        self.color = color and self.test_ansi()

    @staticmethod
    def test_ansi():
        """Comprueba si hay soporte para colores en la terminal."""
        import sys
        import os
        import platform

        return 'TERM' in os.environ and os.environ['TERM'] == 'ANSI' or \
            hasattr(sys.stdout, "isatty") and sys.stdout.isatty() and \
            platform.system() != 'Windows'

    @staticmethod
    def crear_tabla(cabeceras, anchuras, datos):

        def colorear(texto: str, color: bool):
            """Colorea el texto de una celda.

               * En las suscripciones desactivas todas las columnas aparecen en
                 gris.
               * En las activas, las tres últimas columnas aparecen en rojo o
               verde dependiendo de si hay perdida o ganancia.

               :param texto: Texto de la celda, ya formateado con su ancho y
                    alineación.
               :param activo: Si la fila representa una suscripción activa.
               :param color: Si el dato debe colorearse y en qué color.
            """
            if color is False:
                return texto
            elif color:
                return ("\033[91m"
                        if color == "-" else "\033[92m") + texto + "\033[0m"
            else:  # Las suscripciones desactivas se escriben en gris.
                return "\033[1;30m" + texto + "\033[0m"

        def formatear(dato, anchura, color, centrar=False):
            if isinstance(dato, str):
                centrar = "^" if centrar else "<"
                p = f'{{:{centrar}{anchura}.{anchura}s}}'
            elif isinstance(dato, (int, float)):
                centrar = "^" if centrar else ">"
                signo = "+" if color else ""
                if isinstance(dato, int):
                    p = f'{{:{centrar}{signo}{anchura}d}}'
                else:
                    p = f'{{:{centrar}{signo}{anchura}.{2}f}}'
            elif isinstance(dato, date):
                p = '{:%d/%m/%Y}'
            elif dato is None:
                p = ' ' * anchura
            else:
                raise NotImplemented("Tipo de dato sin soporte")

            return p.format(dato)

        def linea(fila, colores, cabecera=False):
            campos = [colorear(formatear(t, w, c, cabecera), c)
                      for t, w, c in zip(fila, anchuras, colores)]
            return '| ' + ' | '.join(campos) + ' |'

        sep = "+" + "+".join("-"*(w+2) for w in anchuras) + "+"

        print(sep)
        print(linea(cabeceras, len(cabeceras)*[False], True))
        print(sep)

        for activa, fila in datos:
            campos, colores = tuple(zip(*fila))
            if not activa:
                colores = (None, )*len(colores)
            print(linea(campos, colores))
            print(sep)

    def mostrar_cartera(self, arg=None):
        if not arg:
            fi = ff = None
        elif arg[-1] == "i":
            fi, ff = arg[0:-1], None
        else:
            ff, fi = arg[0:-1], None

        db = config.db
        with db.session:
            inv = []
            for cartera in db.Cartera.get(fi=fi, ff=ff, viva=True):
                if config.force_extract and cartera.valoracion is None:
                    cot = db.Cotizacion(cartera.fondo.isin,
                                        cartera.fecha, None)
                    cot.insert()
                    if not cot.vl:  # No se puede obtener.
                        logger.warn("Imposible obtener el capital "
                                    "de {cartera.fondo.alias}"
                                    "[{cartera.comercializadora}]")
                    else:
                        cartera.vl = cot.vl
                        part = cartera.participaciones
                        cartera.valoracion = part * cartera.vl

                if cartera.valoracion is not None and \
                        cartera.capital is not None:
                    ganancia = cartera.valoracion - cartera.capital
                else:
                    ganancia = None

                if self.color:
                    color_vl = cartera.fondo.crecimiento
                    if color_vl:
                        color_vl = "+"
                    elif color_vl is False:
                        color_vl = "-"
                    color_ganancia = None
                    if ganancia is not None:
                        if ganancia > 0:
                            color_ganancia = "+"
                        elif ganancia < 0:
                            color_ganancia = "-"
                        else:
                            color_ganancia = False

                    color_ant = False
                    if cartera.anterior is not None:
                        if cartera.anterior < 0:
                            color_ant = "-"
                        elif cartera.anterior == 0:
                            color_ant = False
                        else:
                            color_ant = "+"

                else:
                    color_vl = color_ganancia = color_ant = False

                inv.append((True,  # Sólo salen inversiones activas.
                            [(cartera.fondo.alias, False),
                             (cartera.fondo.isin, False),
                             (cartera.fondo.riesgo, False),
                             (cartera.comercializadora, False),
                             (cartera.anterior, color_ant),
                             (cartera.capital, False),
                             (cartera.fecha, False),
                             (cartera.participaciones, False),
                             (cartera.vl, color_vl),
                             (ganancia, color_ganancia)]))

        # Calculamos qué porcentaje del total representa cada inversión
        total = sum(i[5][0] + i[9][0] for _, i in inv)  # capital + ganancia
        for _, i in inv:
            i.insert(6, (round((i[5][0] + i[9][0])/total*100, 2), False))

        inv.sort(key=lambda e: e[1][2][0] or 0)  # Ordenamos por riesgo

        self.crear_tabla(["Fondo", "ISIN", "R", "Banco", "Anterior",
                          "Inversión", "%Cartera", "Fecha", "Part.",
                          "VL", "Ganancia"],
                         [15, 12, 1, 10, 9, 9, 8, 10, 9, 9, 9], inv)

    def mostrar_plusvalias(self):
        db = config.db
        with db.session:
            inv, totales = [], {}
            for p in db.Plusvalia.get():

                try:
                    plusvalia = p.rembolso - p.capital
                except TypeError:
                    logger.warning('No puede calcularse el rembolso de '
                                   f'{p.suscripcion.id}')
                    plusvalia = None

                if self.color and plusvalia is not None:
                    color_ganancia = "+" if plusvalia > 0 else "-"
                else:
                    color_ganancia = False

                dias = None

                if p.fecha_v:
                    totales.setdefault(p.fecha_v.year, [0, 0, 0])
                    totales[p.fecha_v.year][0] += p.capital
                    totales[p.fecha_v.year][1] += p.rembolso
                    totales[p.fecha_v.year][2] += plusvalia
                    dias = (p.fecha_v - p.fecha_i).days

                inv.append((p.orden == 0,  # True si no se ha vendido la inv.
                            [(f'{p.desinversion}/{p.orden}', False),
                             (p.suscripcion.cuenta.fondo.alias, False),
                             (p.suscripcion.cuenta.comercializadora, False),
                             (p.capital, False),
                             (p.fecha_i, False),
                             (p.fecha_v, False),
                             (p.participaciones, False),
                             (p.rembolso, color_ganancia),
                             (plusvalia, color_ganancia),
                             (plusvalia and
                                plusvalia/p.capital*100, color_ganancia),
                             (plusvalia and dias and
                                ((1+plusvalia/p.capital)**(365/dias) - 1)*100,
                                color_ganancia)]))

        for year, (capital, remb, plus) in totales.items():
            if self.color and plusvalia is not None:
                color_ganancia = "+" if plus > 0 else "-"
            else:
                color_ganancia = False

            inv.append((True,
                        [("", False),
                         ("Total", False),
                         (str(year), False),
                         (capital, False),
                         ("", False),
                         ("", False),
                         ("", False),
                         (remb, color_ganancia),
                         (plus, color_ganancia),
                         (plus/capital*100, color_ganancia),
                         ("", False)]))

        self.crear_tabla(["ID", "Fondo", "Banco", "Inversión", "F. compra",
                          "F: venta", "Partic.", "Rembolso", "Plusvalia",
                          "Plu (%)", "TAE (%)"],
                         [4, 13, 9, 10, 10, 10, 8, 10, 10, 7, 10], inv)

    def mostrar_evolucion(self):
        db = config.db
        with db.session:
            puntos = tuple(db.Evolucion.get("semanas", abcisas=False))

        inversiones, minimo, maximo = {}, datetime.now()\
            .strftime('%Y-%m-%d'), '1900-01-01'

        graficos = []

        for p in puntos:
            if p.desinversionID is None:
                continue

            minimo, maximo = min(minimo, p.fecha), max(maximo, p.fecha)

            inversiones.setdefault(f'{p.desinversionID}/{p.orden}', [])\
                .append((datetime.strptime(p.fecha, '%Y-%m-%d').timestamp(),
                         p.rembolso/p.coste - 1))

        for tag, curva in inversiones.items():
            graficos.append({
                "puntos": curva,
                "titulo":  f'{tag}'
            })

        minimo = datetime.strptime(minimo, '%Y-%m-%d').year
        maximo = datetime.strptime(maximo, '%Y-%m-%d').year

        aa = tuple(range(minimo + 1, maximo + 1))

        xticks = [datetime.strptime(f'{y}-01-01', '%Y-%m-%d').timestamp()
                  for y in aa]

        for g in graficos:
            plt.plot(*tuple(zip(*g["puntos"])))
            plt.text(*g["puntos"][-1], g["titulo"])

        plt.xticks(xticks, labels=aa)
        plt.grid(True)
        plt.show()

    def mostrar_cotizaciones(self, arg):
        try:
            isin, dias = map(str.strip, arg.split(':'))
        except ValueError:
            isin, dias = arg.strip(), 10
        else:
            try:
                dias = int(dias)
            except ValueError:
                logger.error(f'Los días de "{arg}" no son un entero')
                exit(1)

        db = config.db

        with db.session:
            try:
                fondo = next(db.Fondo.get(isin=isin))
            except StopIteration:
                logger.error(f'{isin}: Fondo desconocido')
                exit(1)

            # +1 para que podamos colorear la primera cotización
            cotizaciones = list(db.Cotizacion.get(isin, limit=dias + 1))

        if not cotizaciones:
            logger.warn(f"No hay cotizaciones de '{fondo.alias}'")
            return

        if len(cotizaciones) <= dias:
            cotizaciones.append(db.Cotizacion(isin, '1900-01-01',
                                              cotizaciones[-1].vl))

        cotizaciones = [(True, ((d.fecha, False),
                         (d.vl, "-" if round(d.vl/c.vl, 5) < 1 else "+"),
                         (f'{(d.vl/c.vl - 1)*100:.2f}%',
                          "-" if round(d.vl/c.vl, 5) < 1 else "+")))
                        for d, c in zip(cotizaciones, cotizaciones[1:])]

        print(f"Fondo: {fondo.alias} -- {isin}")
        self.crear_tabla(["Fecha", "Valor", "Var."],
                         [10, 7, 6], cotizaciones)


def main():
    "Programa principal"
    parse_args()
    Logger().set_output(getattr(config, "logfile", None))
    Logger().set_verbose(getattr(config, "verbose", 1))

    config.db = db = SQLiteConector(database=agrega_path(config.database),
                                    ref_int=True,
                                    dump=registrar(),
                                    schema=agrega_path("SQL/esquema.sql"))
    db.attach((Fondo, Cuenta, Cotizacion, Suscripcion,
               Venta, Cartera, Traspaso, Historial, Plusvalia, Evolucion))

    if config.fondo:
        with db.session:
            for line in sys.stdin:
                if line.startswith("#") or not line.strip():
                    continue
                reg = parse_line(line, tipo="fondo")
                fondo = db.Fondo(*reg)
                try:
                    fondo.insert()
                except Error as e:
                    logger.error(f'Inserción de {fondo.isin} :{e}')
    elif config.cuenta:
        with db.session:
            for line in sys.stdin:
                if line.startswith("#") or not line.strip():
                    continue
                reg = parse_line(line, tipo="cuenta")
                db.Cuenta(*reg).insert()
    elif config.suscripcion:
        with db.session:
            for line in sys.stdin:
                if line.startswith("#") or not line.strip():
                    continue
                reg = parse_line(line, tipo="suscripcion")
                suscr = db.Suscripcion(*reg)
                suscr.insert()
                #print(f'El VL de {suscr.cuenta.fondo.isin} en '
                #      f'{suscr.fecha:%d/%m/%Y} fue de {suscr.vl} '
                #      f'{suscr.cuenta.fondo.moneda}')
    elif config.rembolso:
        with db.session:
            for line in sys.stdin:
                if line.startswith("#") or not line.strip():
                    continue
                reg = parse_line(line, tipo="rembolso")
                db.Venta(*reg).insert()
    elif config.traspaso:
        with db.session:
            for line in sys.stdin:
                if line.startswith("#") or not line.strip():
                    continue
                reg = parse_line(line, tipo="traspaso")
                db.Traspaso(*reg).insert()
    elif config.valor_liquidativo:
        with db.session:
            for line in sys.stdin:
                if line.startswith("#") or not line.strip():
                    continue
                reg = parse_line(line, tipo="valor_liquidativo")
                db.Cotizacion(*reg).insert()
    elif config.cotizaciones:
        with db.session:
            for line in sys.stdin:
                if line.startswith("#") or not line.strip():
                    continue
                try:
                    reg = parse_line(line, tipo="cotizaciones")
                except Exception as err:
                    logger.error(f'{err}: {line}')
                    continue
                try:
                    fondo = next(db.Fondo.get(isin=reg[0]))
                except StopIteration:
                    logger.error(f'{reg[0]}: Fondo desconocido')
                    continue
                s = scraper(fondo.scraper, fondo.scraper_data)
                s.connect(reg[1], reg[2])
                logger.info(f'Inscribiendo cotizaciones de {fondo.alias}')
                for uc, vl in s.cotizacion:
                    cot = db.Cotizacion(fondo.isin, uc, round(vl, 4))
                    try:
                        cot.insert()
                    except Error:
                        logger.warning(f"'{fondo.alias}' [{fondo.isin}] ya "
                                       f"tiene la cotización de {uc}")

    if config.extract:
        extraer_cotizaciones()

    interfaz = Interfaz(color=True)

    if config.inversiones:
        interfaz.mostrar_cartera()
    elif config.plusvalia:
        interfaz.mostrar_plusvalias()
    elif config.grafico:
        interfaz.mostrar_evolucion()
    elif config.history:
        interfaz.mostrar_cotizaciones(config.history)
    elif config.inversiones_H:
        interfaz.mostrar_cartera(config.inversiones_H)


if __name__ == '__main__':
    main()

