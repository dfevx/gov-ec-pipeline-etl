# etl/log.py
"""
Logging JSON-Lines para el pipeline ETL.

Diseño:
- Emite a STDOUT en formato JSON por línea (ideal para Railway/Logs de contenedor).
- Opcionalmente, escribe a archivo local con rotación (seguro para dev/ejecuciones largas).
- Evita handlers duplicados si `get_logger()` se llama varias veces.

Uso típico:
    from .log import get_logger
    logger = get_logger(__name__, logfile="etl.log")
    logger.info("Extraction completed", {"rows": 1234, "file": "foo.csv"})

Convenciones:
- Estructura base del evento: {time, level, name, message, ...extras}
- Extras: si pasas un dict como segundo argumento (logger.info("msg", {"k": "v"})),
  se fusiona en el JSON de salida.
"""

from __future__ import annotations

import json
import logging
import sys
from logging.handlers import RotatingFileHandler


class JsonFormatter(logging.Formatter):
    """
    Formatea cada registro como una línea JSON con campos estándar.

    Campos:
        time   : timestamp según `self.datefmt` o por defecto del Formatter
        level  : nivel textual (INFO, WARNING, ERROR, ...)
        name   : nombre del logger (suele ser el módulo)
        message: mensaje ya renderizado
        ...extras (si se entregó un dict como args del logger)
    """
    def format(self, record: logging.LogRecord) -> str:
        event = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
        }
        # Si el logger fue llamado con un dict como args → adjuntar como extras
        if isinstance(record.args, dict):
            event.update(record.args)
        return json.dumps(event, ensure_ascii=False)


def get_logger(name: str = "etl", logfile: str | None = None) -> logging.Logger:
    """
    Crea (o devuelve) un logger configurado para emitir JSON-Lines.

    Args:
        name   : nombre del logger (por módulo suele ser __name__).
        logfile: ruta a archivo local para logging rotado (opcional).

    Returns:
        logging.Logger ya configurado. Si ya tenía handlers, lo reutiliza.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Evitar configurar dos veces si el caller invoca get_logger repetidamente
    if logger.handlers:
        return logger

    # Handler a STDOUT (Railway/containers capturan este stream)
    h_stdout = logging.StreamHandler(sys.stdout)
    h_stdout.setFormatter(JsonFormatter())
    logger.addHandler(h_stdout)

    # Handler opcional a archivo local con rotación (≈2 MB x 3 backups)
    if logfile:
        h_file = RotatingFileHandler(
            logfile,
            maxBytes=2_000_000,
            backupCount=3,
            encoding="utf-8"
        )
        h_file.setFormatter(JsonFormatter())
        logger.addHandler(h_file)

    return logger
