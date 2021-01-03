# vim: set fileencoding=utf-8

"""
.. module:: fondos.utils.config
   :platform: Unix; Windows
   :synopsis: Gestión de la configuración del programa.

Gestión de la configuración del programa.

.. moduleauthor:: José Miguel Sánchez Alés

.. version: |version|
   :date: |today|

"""

import logging
from argparse import Namespace


#: Objeto que almacena la configuración del programa
config = Namespace()


class Logger:
    """Facilita que todos los objetos de registro compartan salida,
       formateo y nivel de depuración, aunque se definan dentro de distintos
       ficheros (módulos).

       >>> logger = Logger().get_logger(__name__)
       >>> logger.set_output("registro.log")  # None para que sea stderr.
       >>> logger.set_verbose(2)  # 0: crit. 1: error, 2: warn; 3: info; 4: d.
       >>> logger,warn("Mensaje de atención")

    """

    __object = None

    # Siempre se devuelve el mismo objeto.
    def __new__(cls):
        if cls.__object is None:
            cls.__object = super().__new__(cls)

        return cls.__object

    def __init__(self):
        self.root = logging.getLogger("root")
        self.formatter = logging.Formatter(
            fmt='{asctime:s} {name:<12s} [{levelname:·^8s}]: {message:s}',
            style='{'
        )

    def set_output(self, logfile=None):
        """Fija cómo será la salida y su formato.

           :param logfile: Nombre del fichero de salida. ``None`` para stderr.
        """
        if logfile:
            handler = logging.FileHandler(logfile)
        else:  # Si no se define fichero, stderr.
            handler = logging.StreamHandler()

        handler.setFormatter(self.formatter)
        self.root.addHandler(handler)

    def set_verbose(self, verbose):
        """Nivel de explicaciones.

           :param verbose: Un número entre 0 (crítico) y 4 (debug) que
                determina el nivel de los mensajes.
        """
        self.root.setLevel((5-verbose)*10)

    def get_logger(self, name):
        """Genera un objeto de registro que hereda las características
           fijadas.

           :param name: Nombre del objeto.
        """
        return self.root.getChild(name)
