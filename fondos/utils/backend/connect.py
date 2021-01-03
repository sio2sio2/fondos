# vim: set fileencoding=utf-8

"""
Conector de acceso.

.. module:: utils.backend.connect
   :platform: Unix, Windos
   :synopsys: Conector de acceso a bases de datos.

Coector de acceso a bases de datos.

El módulo implementa una clase conectora con las siguientes características:

* Evita la creación y el cierre de cursores gracias al uso del decorador
  :kbd:`@cursor`. Con el las operaciones con *execute* se pueden hacer
  directamente sobre la clase::

    @cursor
    def listar_personas(self):
        self.execute("SELECT * FROM Persona")
        yield from self

  en vez de::

    def listar_personas(self):
        c = self.cursor()
        c.execute("SELECT * FROM Persona")
        try:
            yield from c
        finally:
            c.close()

* Para realizar una operación (o varias operaciones) sobre la base de datos
  es necesario abrir una sesión::

    with db.session:
       for p in db.listar_personas():
           print(p)

  Suponiendo que *db* sea un objeto creado con la clase.

  La sesión es conceptualmente una transacción, es decir, que encierra una
  serie de operaciones simples que juntas deben cumplir el principio de
  atomicidad.
"""

from abc import ABCMeta, abstractproperty
from functools import wraps
from inspect import isgeneratorfunction
from contextlib import contextmanager, ContextDecorator
from copy import copy
from . import errors


@contextmanager
def _manage_cursor(db):
    """Gestiona de forma transparente el cursor:

       * Hace una copia del objeto conector que se le pasa como argumento.
       * Al nuevo conector le asocia un nuevo cursor.
       * Transforma los errores propios del driver en errores del módulo.
       * Cierra el cursor.

       :param db: Objeto conector.
       :type db: :class:`~utils.backend.connect.ConnectorWithCursor`
    """
    if not db.session_opened():
        raise errors.TransactionError("No hay sesión abierta")

    db._cursor.append(db.connection.cursor())

    try:
        yield db  # Se devuelve la copia de db.
    except db.BACKEND.Error as e:
        tb = e.__traceback__
        e = vars(errors).get(type(e).__name__, errors.NotStandardError)(e)
        e.__cause__ = None
        raise e.with_traceback(tb)
    finally:
        db._cursor.pop().close()


def cursor(func):
    """Decorador que posibilita que la creación y destrucción
       transparente de un cursor"""

    @wraps(func)
    def _generator_cursor(*args, **kwargs):
        with _manage_cursor(args[0]) as db:
            yield from func(db, *args[1:], **kwargs)

    @wraps(func)
    def _normal_cursor(*args, **kwargs):
        with _manage_cursor(args[0]) as db:
            return func(db, *args[1:], **kwargs)

    return _generator_cursor if isgeneratorfunction(func) else _normal_cursor


class _Transaction(ContextDecorator):
    """Para abrir y cerrar transacción mediante la creación de un contexto
       o la decoración de una función.
    """

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.db._transaction += 1

    def __exit__(self, exc, value, tb):
        self.db._transaction -= 1
        if self.db._transaction == 0:
            if not exc:
                self.db.connection.commit()
                if self.db._buffer:
                    self.db.logger(self.db._buffer.getvalue())
            else:
                self.db.connection.rollback()

            if self.db._buffer:
                # Nos deshacemos de lo escrito
                # en el buffer.
                self.db._buffer.truncate(0)
                self.db._buffer.seek(0)
        return False


class _StatementLog(ContextDecorator):
    """Contextualizador para habilitar y deshabilitar el registro
       de las sentencias SQL que se ejecutan en la base de datos.
    """

    incontext = 0

    def __init__(self, db):
        from io import StringIO
        self.db = db
        db._buffer = StringIO()  # Para almacenar las sentencias SQL.

    def __enter__(self):
        # Antes habilitar el registro nos aseguramos de que
        # la transacción ha empezado para evitar que se registre
        # el comienzo de la propia transacción.
        try:
            self.db.execute("BEGIN TRANSACTION")
        except Exception:
            pass

        if not self.db.logger or self.incontext > 0:
            return

        self.incontext += 1
        buffer = self.db._buffer
        self.db.connection.set_trace_callback(lambda s:
                                              buffer.write(f'{s};\n'))

    def __exit__(self, exc, value, tb):
        self.incontext -= 1

        if not exc and not self.incontext and self.db.logger:
            self.db.connection.set_trace_callback(None)


class ConnectorWithCursor(metaclass=ABCMeta):
    """Clase conectora a la base de datos preparada para:

       * Manipular cursores de forma transparente (mediante un decorador).
         Esto implica que los métodos *.execute* y *.executemany* que facilitan
         los objetos *cursor* pueden usarse directamente sobre el objeto
         conector, siempre que la operación se haga en un método decorado
         con :func:`~utils.backend.connect.cursor`. El atributo *_cursor* se
         encarga de guardar el cursor propietario de estos métodos. Ahora
         bien, como existir cursores simultáneos, al crearse un nuevo cursor,
         se crea una copia del objeto a la que se le asocia el nuevo cursor y
         es este objeto el que realmente está accesible dentro del método
         decorado, es decir:

            @cursor
            def operacion_sobre_bd(self, arg):
                self.execute("SELECT ...")

         el objeto *self* que se usa dentro del método *operacion_sobre_bd* no
         es el conector original, sino una copia cuyo atributo *_cursor* es el
         cursor creado a la sazón.

       * Gestionar transacciones de forma sencilla con
         :meth:`~utils.backend.connect.ConnectorWithCursor.session` para crear
         contextos o como decorador de funciones. Sólo si se abierto una
         sesión (transacción) se pueden usar métodos del conector decorados
         mediante :func:`~utils.backend.connect.cursor`. Por ejemplo, si
         quisiéramos usar la función de arriba::

            with db.session:
                db.operacion_sobre_db("lo que sea")

       * Permitir la asociación de clases :class:`utils.backend.model.Register`
         a una conexión, a fin de que puedan usarse para manipular la base
         de datos correspondiente en vez de hacerlo directamente con el
         conector.

       La clase está pensada para que los métodos que escriben y leen datos
       en la base de datos, los reciban o devuelvan en forma de tupla (u otro
       iterable), de manera que el orden de cada elemento coincida con el orden
       de las columnas de la tabla que se manipula.

       :cvar BACKEND: Driver de conexión a la base de datos.
       :ivar _cursor: Lista de cursores abiertos que realizan operaciones
            sobre la base de datos. Las operaciones las lleva a cabo el
            último.
       :ivar int _transaction: Informa de si hay abierta una transacción.
            Es un contador. En principio, vale cero y cada vez que abrimos
            una transacción suma 1. La transacción acaba cuando el contador
            vuelve a 0. Esto permite "anidar" transacciones.
       :ivar set _attached: Contiene todas las clases del modelo asociadas
            al conector.
    """

    BACKEND = abstractproperty()

    @property
    def connection(self):
        "Conexión a la base de datos"
        return self._connection

    def __init__(self, *args, **kwargs):
        self._transaction = 0
        self._cursor = []
        self.logger = kwargs.pop("dump", None)
        self.log = _StatementLog(self)
        self._connection = self.BACKEND.connect(*args, **kwargs)
        self._attached = set()

    def close(self):
        "Cierra manualmente la conexión"
        return self.connection.close()

    @property
    def id(self):
        """Identificador de la conexión. Todas las copias del objeto
           tienen la misma identificación, ya que todas se refieren a la
           misma conexión.
        """
        return id(self._connection)

    @property
    def session(self):
        "Modela una transacción en la base de datos"
        return _Transaction(self)

    def session_opened(self):
        """Informa de si hay abierta una transacción.
           rtype: bool
        """
        return self._transaction > 0

    def attach(self, classes):
        """Registra las clases definidas en el modelo de objetos.

           La consecuencia de registrar una clase es que aparece disponible
           como atributo del objeto una copia que añade el atributo de clase
           *_db* con valor el objeto de conexión.  Así, los métodos *get* y
           *insert* de las clases pueden operar sobre la base de datos, sin
           tener que pasar el objeto de conexión como argumento.

           :param classes: Las clases a registrar.
        """
        for class_ in classes:
            class_ = type(class_)(class_.__name__, (class_,), {"_db": self})
            self._attached.add(class_)
            if hasattr(self, class_.__name__):
                raise AttributeError("No puede ajuntarse {} al objeto. "
                                     "Ya existe un atributo con ese nombre."
                                     .format(class_.__name__))
            setattr(self, class_.__name__, class_)

    def __getattr__(self, name):
        # Se busca el método/atributo en el atributo cursor y, si no, en
        # en el propio objeto (esto permite usar directamente execute, etc.)
        try:
            return getattr(self._cursor[-1], name)
        except (IndexError, AttributeError):
            return super().__getattribute__(name)

    # Necesario debido a que las invocaciones implícitas
    # de métodos no evalúan ni __getattr__ ni __getattribute__
    # https://docs.python.org/3/reference/datamodel.html#object.__getattr__
    def __iter__(self):
        return iter(self._cursor[-1])

    def __next__(self):
        return next(self._cursor[-1])
