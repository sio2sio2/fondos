class ScraperError(Exception):
    "Error base del scraper"
    pass


class ConnectionError(ScraperError):
    "Error de conexión a la página web"
    pass


class AlreadyConnectedError(ConnectionError):
    "Error producido al intentar reconectar sin desconectar antes"
    pass


class ParseError(ScraperError):
    "Error de procesamiento de la página"
    pass


class ScraperNotFoundError(ScraperError):
    "No se encuentra el scraper solicitado"
    pass
