# etl/__init__.py
"""
Paquete ETL – interfaz pública y puntos de entrada

Este archivo expone, de forma ordenada, las funciones y clases principales del
pipeline para facilitar su uso desde `pipeline.py`, notebooks o tests.

Componentes expuestos
---------------------
Configuración
- load_settings:      Carga estricta de settings (Supabase/artefactos) desde el entorno.
- load_ckan_settings: Carga de parámetros CKAN (package_show, prefix, state, etc.).

Estado (Storage)
- download_object:       Descarga objetos desde Supabase Storage (con reintentos).
- ensure_state_locally:  Garantiza que el `state.json` exista en local (o entra en modo “sin estado”).

Extracción
- extract_updated_dfs:   Consulta CKAN, detecta cambios vs state y devuelve DataFrames por recurso.

Transformación
- run_transform:         Normaliza/valida y enriquece DataFrames según el YAML.
- TransformHardFail:     Excepción para fallos críticos de transformación.

Carga
- load_to_supabase:      Upsert a Postgres (vía Supabase), auditoría y subida de artefactos/logs.

Logging
- get_logger:            Logger JSON-Lines (stdout + archivo opcional rotado).

Logs a tabla
- read_log_to_df:        Convierte un log JSON-Lines a DataFrame.
- upload_df_to_supabase: Inserta el DataFrame de logs en una tabla de Supabase.

Configuración YAML/JSON
- load_config:           Carga un archivo de configuración (YAML/JSON) a dict.

Uso rápido
----------
>>> from etl import (
...     load_settings, load_ckan_settings, get_logger,
...     extract_updated_dfs, run_transform, load_to_supabase
... )
>>> st = load_settings()
>>> ckan = load_ckan_settings()
>>> logger = get_logger(__name__, logfile="etl.log")
>>> dfs = extract_updated_dfs(ckan.package_id, ckan.prefix, ckan.state_path)
>>> tdfs, report = run_transform(dfs, "configs/detenidos_aprehendidos.yaml", run_id="run_2025_09_16", logger=logger)
>>> # `client` debe ser un Supabase client inicializado por la app (service role si corresponde)
>>> # load_to_supabase(tdfs, "configs/detenidos_aprehendidos.yaml", client, run_id="run_2025_09_16", logger=logger, settings=st)

Notas
-----
- Esta interfaz pública está pensada para que `pipeline.py` sea minimalista y
  declarativo. Mantén `__all__` sincronizado si agregas nuevos puntos de entrada.
"""

from .config import load_settings, load_ckan_settings
from .get_status import download_object, ensure_state_locally
from .extract import extract_updated_dfs
from .transform import run_transform, TransformHardFail
from .load import load_to_supabase
from .log import get_logger
from .upload_log import read_log_to_df, upload_df_to_supabase
from .yaml_config_loader import load_config

__all__ = [
    "load_settings", "load_ckan_settings",
    "download_object", "ensure_state_locally",
    "extract_updated_dfs",
    "run_transform", "TransformHardFail",
    "load_to_supabase",
    "get_logger",
    "read_log_to_df", "upload_df_to_supabase",
    "load_config",
]
