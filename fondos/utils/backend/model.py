# vim: set fileencoding=utf-8

"""Define la base :class:`~utils.backend.register.Register` para definir
   por herencia todas las clases que representan registros en la base de datos
   y sirven para generar el modelo de objetos.

   Estas clases se comportan de la siguiente manera:

   * Tienen un atributo ``_fields`` que es una cadena compuesta por palabras
     separadas por espacio que representan los nombres de los campos del
     registro. Las palabras aparecen en el orden en que las columnas están
     definidas en la tabla: la primera palabra es la primera columna, etc.

   * Los atributos que representan claves primarias se notan acabándolas con
     un asterisco.

   * Al crear un objeto se pasan en orden los valores (el primer argumento
     es el valor de la primera columna, etc.) y se añade al final un argumento
     nominal *db* que contiene el objeto conector a la base de datos de la que
     proceden los datos (el objeto se crea como consecuencia de un ``SELECT``).
     Si el  objeto se rellenó con datos generados por el programa el valor será
     :obj:`None` y sólo cuando se haga su inserción en la base de datos se
     deberá asociar el objeto con el conector.

   * Opcionalmente, se pueden pasar los valores para crear el objeto como
     argumentos nominales.

   * La metaclase :class:`~fondos.morm.RegisterMeta` se encarga de crear
     automáticamente para cada columna un atributo privado.. Por ejemplo,
     una columna *nombre* se traduce en el objeto con el atributo *_nombre*.
     Además, se crea para cada campo una propiedad *nombre*.

   * La propiedad se define en la metaclase para que devuelva el valor del
     atributo privado correspondiente. Sin embargo, en una clase concreta
     determinada puede redefinirse para que devuelva otro valor diferente. Por
     ejemplo, en una clase particular puede interesar que en caso de que el
     atributo sea nulo, el valor de la propiedad se calcule mediante algún
     algoritmo.

   * Como los tipos de datos que soporta un base de datos y los que soporta
     *python* son distintos (p.e. *Python* tiene el tipo *Enum* sin traducción
     directa en la base de datos), es posible definir funciones que
     decodifiquen el dato al leerlo en la base de datos y pasarlo al objeto y
     que lo codifiquen para hacer el proceso inverso.
"""

from abc import ABCMeta, abstractproperty
from enum import Enum
from functools import wraps
from .errors import NotStandardError, TransactionError


class RegisterMeta(ABCMeta):
    """Metaclase para lograr la definición automática de las propiedades
       que representan las columnas de la tabla.
    """

    @staticmethod
    def dbassoc(finsert):
        "Asocia al conector el objeto insertado"
        @wraps(finsert)
        def inner(self):
            if not type(self).attached:
                raise NotStandardError("Clase no registrada en el conector.")
            try:
                self._attached = True
                res = finsert(self)
            except Exception as e:
                self._attached = False
                raise e
            return self._set_id(res)
        return inner

    @staticmethod
    def dbdissoc(fremove):
        "Disocia del conector un objeto borrado"
        @wraps(fremove)
        def inner(self):
            if not self.stored:
                return False
            fremove(self)
            self._attached = False
            return True
        return inner

    @staticmethod
    def makeobj(fget, cls):
        "Crea el objeto a partir del registro de la base de datos"
        fget = fget.__func__

        @wraps(fget)
        def inner(*args, **kwargs):
            if not cls.attached:
                raise NotStandardError("Clase no registrada en el conector.")
            for reg in fget(cls, *args, **kwargs):
                yield cls(*reg, attached=True)
        return inner

    def __new__(mcls, name, bases, namespace):
        fields = namespace.get("_fields")
        if fields and not isinstance(fields, abstractproperty):
            fields = fields.split()
            fds, pk = [], []
            for attr in fields:
                if attr.endswith("*"):
                    attr = attr[:-1]
                    pk.append(attr)
                fds.append(attr)
                # Definimos la propiedad "x" que devuelve el atributo "_x",
                # a menos que ya la tenga definida la clase.
                namespace.setdefault(attr, property(
                    lambda self, _attr="_" + attr: getattr(self, _attr)
                ))
            # Desdoblamos la información de _fields en estas dos propiedades
            namespace["_pkfields"] = tuple(pk)
            namespace["_fields"] = tuple(fds)
        # _Register se añadirá como antecesora de la clase,
        # sólo en caso de que no lo sea ya.
        for base in bases:
            if isinstance(base, RegisterMeta):
                break
        else:
            bases += (RegisterMeta._Register, )

        return super(RegisterMeta, mcls).__new__(mcls, name, bases, namespace)

    def __init__(cls, name, bases, namespace):
        if "insert" in vars(cls):
            cls.insert = RegisterMeta.dbassoc(cls.insert)

        if "remove" in vars(cls):
            cls.remove = RegisterMeta.dbdissoc(cls.remove)

        if "get" in vars(cls):
            cls.__fget = cls.get
            cls.get = RegisterMeta.makeobj(cls.get, cls)
        elif "_db" in vars(cls) and hasattr(cls, "_RegisterMeta__fget"):
            # Para clases asociadas en un conector
            cls.get = RegisterMeta.makeobj(cls.__fget, cls)
        return type.__init__(cls, name, bases, namespace)

    @property
    def db(cls):
        try:
            return cls._db
        except AttributeError:
            pass

    @property
    def attached(cls):
        return cls._db is not None

    class _Register:
        """Clase antecesora de :class:`~utils.backend.model.Register` que:

           * Realiza la definición de los atributos privados.
           * Convierte el objeto en iterable, de manera que al iterar
             en él se devuelven los campos en el orden en que está
             definidos en la tabla.
           * Define como se presenta el objeto.
        """
        _fields = abstractproperty()
        _fdeco = {}
        _fco = {}

        def __init__(self, *args, **kwargs):
            fields, args = list(self._fields), list(args)
            # Se asocian argumentos posicionales
            # a atributos en el orden en que está definidos
            while fields:
                try:
                    value = args.pop(0)
                except IndexError:
                    break

                name = fields.pop(0)

                setattr(self, "_" + name, self._fdeco.get(name,
                                                          lambda n: n)(value))

            # Se intentan obtener el resto de valores
            # de los atributos de los argumentos nominales.
            while fields:
                name = fields.pop(0)

                try:
                    value = kwargs.pop(name)
                except KeyError:
                    raise TypeError("'{}' necesita valor.".format(name))

                setattr(self, "_" + name, self._fdeco.get(name,
                                                          lambda n: n)(value))

            if kwargs:
                raise TypeError(
                    "{}: parámetro desconocido".format(kwargs.popitem()[0])
                )

            if args:
                raise TypeError("Demasiados argumentos")

        def __iter__(self):
            if isinstance(self._fields, abstractproperty):
                return iter(())

            return (self._fco.get(name, lambda n: n)(getattr(self, "_" + name))
                    for name in self._fields)

        def __repr__(self):
            if isinstance(self._fields, abstractproperty):
                return "{}(Registro abstracto)".format(type(self).__name__)

            return "{}(".format(type(self).__name__) + ", ".join(
                "{}={!r}".format(k, v) for k, v in zip(self._fields, self)
            ) + ")"


class Register(metaclass=RegisterMeta):
    """Superclase para las clases que representan tablas de una
       base de datos relacional.

       A las características que le confiere la metaclase, añade
       la capacidad de asociación a un conector.

       Uso básico::

          >>> class Cliente(Register):
          ...     _fields = "id* nif nombre fecha_nac sexo"
          ...
          >>> cliente = Cliente(None, '0T', "Pepe", "2000-01-01", "V")
          >>> cliente
          Cliente(id=None, nif='0T', nombre='Pepe', fecha_nac='2000-01-01', sexo='V')
          >>> cliente.stored  # Como no se ha obtenido de la base de datos
          False
          >>> cliente.sexo = "V"

       Si queremos que alguno de los campos tenga sea de un tipo que no tiene
       traducción directa en la base de datos podemos definir una función
       decodificadora y otra codificadora::

          >>> from enum import Enum
          >>> class Sexo(Enum):
          ...     VARON = "V"
          ...     HEMBRA = "H"
          ...
          >>> class Cliente(Register):
          ...     _fields = "id nif nombre fecha_nac sexo"
          ...
          ...     _fdeco = { # Base de datos ---> objeto
          ...          "sexo": lambda n: n if isinstance(n, Sexo) else Sexo[n]
          ...     }
          ...     _fco = {  # Objeto --> base de datos.
          ...          "sexo"  lambda v: v.name
          ...     }

       En el ejemplo, almacenamos en la base de datos los :class:`~enum.Enum`
       a través de su nombre.

       Si la clase quiere usarse para manipular una base de datos, hay que
       asociarla a un objeto conector::

          >>> db = SQLiteConector(":memory:", schema="banco.sql")
          >>> db.attach((Cliente, ))
          >>> cliente = db.Cliente(None, '0T', "Pepe", "2000-01-01", "V")
          >>> with db.session:  # Para usar la base hay que abrir una sesión.
          ...     cliente.insert()
    """

    def __init__(self, *args, attached=False, **kwargs):
        super(Register, self).__init__(*args, **kwargs)
        self._attached = attached

    @property
    def stored(self):
        "Informa de si el objeto se encuentra almacenado en la base de datos"
        return self._attached and type(self).db

    def __getattr__(self, name):
        if name == "db" and self._attached:
            return type(self).db
        else:
            return super().__getattribute__(name)

    def _set_id(self, id):
        """Fija el ID del objeto y devuelve las claves primarias.

           Esta circunstancia se produce cuando el ID es generado por la
           base de datos y al guardarse el objeto en la base de datos esta
           le asigna uno.
        """
        if len(self._pkfields) == 1:
            name_id =self._pkfields[0]
            if getattr(self, name_id) is None:
                setattr(self, f'_{name_id}', id)

            return getattr(self, name_id)
        else:
            return tuple(getattr(self, name) for name in self._pkfields)

    def insert(self, db):
        raise NotImplementedError("Inserción no implementada")

    def remove(self):
        raise NotImplemented("Borrado no implementado")

    @classmethod
    def get(cls, id_=None):
        raise NotImplemented("¿Ha olvidado definir cómo obtener objetos?")
