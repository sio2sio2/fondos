# vim: set fileencoding=utf-8

"""
Errores del acceso a datos.

.. module:: utils.backend.errors
   :platform: Unix, Windows
   :synopsis: Errores de acceso a bases de datos.

Errores de acceso a bases de datos.

Tienen el mismo nombre que los expuestos en la DB-APIv2, a fin de que los
errores del driver correspondiente (sqlite3, MySQLdb, etc) se conviertan al
homónimo definido aquí. De este modo, se independiza el modelo y el código
de la aplicación de las aprticularidades del driver.
"""


class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


# Adicionales
class NotStandardError(DatabaseError):
    "Error implementado por el driver que no forma parte de la DB-API"
    pass


class TransactionError(NotStandardError):
    "Error en la transacción"
    pass
