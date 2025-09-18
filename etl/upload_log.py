# etl/upload_log.py
"""
Módulo para procesar y subir logs del pipeline a Supabase.

Responsabilidades:
- Leer un archivo de log local (JSON-Lines o texto simple) y convertirlo en DataFrame.
- Subir el DataFrame a una tabla de Supabase (PostgREST), en lotes para evitar payloads grandes.

Convenciones:
- Se espera que los logs se emitan en formato JSON por línea (ver `etl/log.py`).
- Si una línea no es JSON válido, se guarda como {"raw": "..."}.
- La tabla de destino en Supabase debe ser creada previamente (DDL SQL).

Uso típico:
    from .upload_log import read_log_to_df, upload_df_to_supabase

    df = read_log_to_df("etl.log")
    upload_df_to_supabase(df, table="etl_logs", supabase_url=URL, supabase_key=KEY)
"""

from __future__ import annotations
from pathlib import Path
from typing import Iterable, List, Dict, Any
import json

import pandas as pd
from supabase import create_client


# -----------------------------------------------------------------------------
# Utilidades
# -----------------------------------------------------------------------------
def _chunk(it: Iterable[Dict[str, Any]], size: int = 500):
    """
    Divide un iterable en chunks de tamaño fijo.

    Args:
        it: iterable de diccionarios (ej. registros del DataFrame).
        size: número máximo de elementos por chunk.

    Yields:
        List[dict] de longitud <= size.
    """
    buf = []
    for x in it:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


# -----------------------------------------------------------------------------
# Lectura de logs
# -----------------------------------------------------------------------------
def read_log_to_df(path: str) -> pd.DataFrame:
    """
    Lee un archivo de log en formato JSON-Lines y devuelve un DataFrame.

    - Si una línea es JSON válido, se convierte en dict.
    - Si no lo es, se almacena en una columna 'raw' (compatibilidad con logs antiguos).

    Args:
        path: ruta al archivo de log.

    Returns:
        pd.DataFrame con registros parseados.
    """
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))    # JSON-Lines
            except Exception:
                rows.append({"raw": line})       # fallback texto plano
    return pd.DataFrame(rows)


# -----------------------------------------------------------------------------
# Subida a Supabase
# -----------------------------------------------------------------------------
def upload_df_to_supabase(df: pd.DataFrame, table: str, supabase_url: str, supabase_key: str) -> None:
    """
    Sube un DataFrame a una tabla en Supabase (PostgREST) en chunks.

    Args:
        df: DataFrame con registros a insertar.
        table: nombre de la tabla en Supabase (debe existir en Postgres).
        supabase_url: URL de Supabase (del proyecto).
        supabase_key: clave de servicio (service_role) o anon key con permisos.

    Notas:
        - Convierte NaNs → None antes de subir.
        - Divide en chunks de 500 filas para evitar payloads demasiado grandes.
    """
    if df.empty:
        return

    df = df.where(pd.notnull(df), None)  # NaN → None
    client = create_client(supabase_url, supabase_key)

    for batch in _chunk(df.to_dict(orient="records"), size=500):
        _ = client.table(table).insert(batch).execute()
