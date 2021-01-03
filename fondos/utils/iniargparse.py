#!/usr/bin/env python3

from __future__ import annotations
from typing import Callable, Dict, List, Iterable, Tuple
from itertools import chain
import argparse


class _IniAction(argparse.Action):
    """Acción asociada a ficheros de configuración INI"""

    preserve: bool
    first: bool = True

    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        default = kwargs.get("default", None)
        self.preserve = kwargs.pop("preserve", False) and default is not None
        if isinstance(default, str):
            kwargs["default"] = [default]
        super().__init__(option_strings, dest, nargs, **kwargs)

    def __call__(self, parser, namespace, values, options_string=None):
        v = getattr(namespace, self.dest, [])
        if self.first:
            self.first = False
            if not self.preserve:
                v = []
        v.append(values)
        setattr(namespace, self.dest, v)


class IniArgumentParser(argparse.ArgumentParser):
    """Redefinición de :class:`~python:argparse.ArgumentParser` para que, de
       menor a mayor precedencia, se proporcionen valores a los argumentos
       del programa desde:

       - El propio código a través de los predefinidos dentro de él.
       - Variables de ambiente que comienzan con un determinado prefijo.
       - Definiciones hechas en ficheros de configuración en formato INI.
       - Definiciones pasadas en la línea de órdenes.

       El uso es el mismo que el de su superclase, con los añadidos ilustrados
       en este código:

       >>> parser = IniArgumentParser(prefix="XXX")
       >>> parser.add_argument("-c", "--config-file", action="config",
                               default="config.ini", preserve=True)
       >>> # Más definiciones de argumentos...

       Es decir, al crear el objeto se puede pasar un prefijo que identifica
       a las variuables de ambiente significativas para el programa. Si el
       prefijo es XXX, entonces "XXX_PARAM" será la variable de ambiente que
       defina el valor para el argumento "param". Las variables de ambiente,
       siempre deben estar en mayúsculas.

       Por otra parte, existe la posibilidad de definir un argumento de tipo
       "config" que contendrá el nombre de un fichero INI en el que también
       se pueden definir argumentos. En la línea de órdenes el argumento
       puede repetirse varias veces (como las acciones *append*) lo cual
       generará una lista de ficheros que se leerán en el orden en que aparecen
       en la línea de órdenes. El valor predeterminado también debe ser una
       lista de ficheros, pero si sólo es uno, puede directamente darse su
       nombre, sin necesidad de crear la lista, tal como se hace en el ejemplo.
       Además, la opción *preserve* indica si se desea conservar el valor de
       *default*, a pesar de indicar más valores en la línea de órdenes.

       Dada la opción :kbd:`--foo-bar` en la línea de órdenes y supuesto
       que no se haya usado *dest* para alterar el nombre de la opción:

       - Supuesto que el prefijo sea "XXX", la variable de ambiente
         correspondiente es XXX_FOO_BAR, donde los guiones se convierten en
         subrayados y todos los caracteres se convierten a mayúsculas.

       - En un fichero ini se puede usar el nombre foo_bar dentro de la sección
         ``[main]`` o el nombre *bar* dentro de la sección ``[foo]``.
    """

    def __init__(self, *args, prefix: str = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.prefix = prefix
        self.register("action", "config", _IniAction)

    def parse_known_args(self, args=None, namespace=None):
        import os

        defaults = {}

        # Primero se obtienen los valores de las variables de ambiente
        if self.prefix:
            for k, v in os.environ.items():
                if not k.startswith(f'{self.prefix.upper()}_'):
                    continue

                name = k[len(self.prefix)+1:].lower()
                defaults[name] = v

        # En segundo lugar, se obtienen los valores de los ficheros INI.
        # Para ello, se preprocesan sólo los argumentos que definen ficheros
        # de configuración.

        iniactions = [a for a in self._actions if isinstance(a, _IniAction)]

        # Preprocesa las acciones que sean config
        preparser = argparse.ArgumentParser(add_help=False)
        for a in iniactions:
            preparser.add_argument(*a.option_strings, dest=a.dest,
                                   nargs=a.nargs, default=a.default,
                                   required=a.required, preserve=a.preserve,
                                   action=_IniAction)

        args, unparsed = preparser.parse_known_args(args, namespace)

        # Añadimos las definiciones contenidas en los ficheros.
        inis = chain.from_iterable(vars(args).values())
        defaults.update(self._fileparser(inis))

        # Si un argumento obligatorio se define mediante
        # entorno o ficheros de configuración, entonces deja de serlo.
        for action in self._actions:
            if not action.required:
                continue
            if action.dest in defaults:
                action.required = False

        # Añade como valores predefinidos lo declarado como variable
        # de ambiente o en ficheros de configuración.
        self.set_defaults(**{k: self._convert(k)(v)
                             for k, v in defaults.items()})

        # Procesa el resto de argumentos
        return super().parse_known_args(unparsed, namespace=args)

    @staticmethod
    def _fileparser(files: Iterable) -> Dict[str, str]:
        """Obtiene los argumentos definidos en el fichero INI.

           :param files: Interable con los nombres de los ficheros INI.

           :returns: Diccionario que contiene las definiciones.
        """
        from configparser import ConfigParser
        from os.path import expandvars, expanduser
        from glob import glob

        ini = ConfigParser()
        ini.read(chain.from_iterable(glob(expandvars(expanduser(x)))
                                     for x in files))

        args = {}

        for section in ini.sections():
            prefix = "" if section == "main" else f'{section}_'
            for name, value in ini.items(section):
                name = f'{prefix}{name}'
                args[name] = value

        return args

    def _convert(self, name: str) -> Callable:
        """Convierte los valores del fichero INI en el tipo adecuado.

           :param name: Nombre del argumento.

           :returns: La función que convierte el valor en el tipo adecuado.
        """
        from distutils.util import strtobool

        def parse_list(line: str) -> List[str]:
            import csv
            return tuple(csv.reader([line], skipinitialspace=True))[0]

        try:
            action = next(a for a in self._actions if a.dest == name)
        except StopIteration:
            return str

        if isinstance(action, (argparse._StoreTrueAction,
                               argparse._StoreFalseAction,
                               argparse._StoreConstAction)):
            func = type(action.const)
            if issubclass(func, bool):
                return lambda v: bool(strtobool(v))
            else:
                return func
        elif isinstance(action, argparse._StoreAction):
            return action.type or str
        elif isinstance(action, (argparse._AppendAction,
                                 argparse._AppendConstAction)):
            return lambda v: [(action.type or str)(x)
                              for x in parse_list(v)]
        elif isinstance(action, (argparse._HelpAction,
                                 argparse._VersionAction)):
            return str
        else:
            raise NotImplementedError("f{name}: Tipo no implementado")
