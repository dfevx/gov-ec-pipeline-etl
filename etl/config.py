# etl/config.py
"""
Configuración central del proyecto ETL.

Este módulo define:
- Carga de variables de entorno (local vs. Railway).
- Dataclasses inmutables con la configuración de Supabase y CKAN.
- Helpers para leer y validar variables de entorno.

Principios:
- En Railway **no** se lee `.env`; las variables llegan por entorno.
- En local/dev, si existe `.env`, se carga automáticamente con `python-dotenv`.
- Falla temprano (errores claros) cuando faltan variables requeridas.

Variables de entorno esperadas
------------------------------
# Supabase / artefactos
SUPABASE_URL            (requerido)
SUPABASE_KEY            (requerido)
SUPABASE_SB_BUCKET      (requerido)  -> bucket principal en Storage
ARTIFACTS_BUCKET        (opcional)   -> bucket para artefactos (default: SUPABASE_SB_BUCKET)
ENV_SLUG                (requerido)  -> etiqueta de entorno (e.g., 'dev', 'prod')
DATASET_SLUG            (requerido)  -> etiqueta del dataset (e.g., 'detenidos_aprehendidos')
STATE_SB_OBJECT         (requerido)  -> key/objeto del state.json en Storage

# Parámetros de artefactos (opcionales)
ARTIFACTS_MODE          (default: 'manifest_on_oversize')
ARTIFACTS_MAX_MB        (default: '50')        -> entero
UPLOAD_RAW              (default: 'true')      -> bool
UPLOAD_SNAPSHOTS        (default: 'true')      -> bool
UPLOAD_REPORTS          (default: 'true')      -> bool
UPLOAD_LOG              (default: 'true')      -> bool

# CKAN (extractor)
API_PKG_SHOW            (default: 'https://www.datosabiertos.gob.ec/api/3/action/package_show')
PACKAGE_ID              (requerido)
PREFIX                  (opcional)
SHEET_BLACKLIST         (default: 'Contenido')
STATE_DIR               (default: 'state')
STATE_PATH              (opcional)   -> ruta absoluta o relativa al JSON de estado

Notas:
- Si estás en Railway, se detecta por RAILWAY_PROJECT_ID o RAILWAY_ENVIRONMENT.
- En local, `.env` no sobre-escribe variables ya presentes en el entorno (override=False).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv, find_dotenv

__all__ = [
    "Settings",
    "CkanSettings",
    "load_settings",
    "load_ckan_settings",
]

# -----------------------------------------------------------------------------
# Utilidades comunes
# -----------------------------------------------------------------------------

_RAILWAY_FLAGS = ("RAILWAY_PROJECT_ID", "RAILWAY_ENVIRONMENT")
_URL_RE = re.compile(r"^https?://", re.IGNORECASE)


def _running_in_railway() -> bool:
    """Devuelve True si el proceso parece ejecutarse en Railway."""
    return any(os.getenv(k) for k in _RAILWAY_FLAGS)


def _load_env(env_file: Optional[str] = None) -> None:
    """
    Carga variables de entorno desde `.env` solo en local.

    - En Railway: retorna sin cargar `.env`.
    - En local: busca `.env` (o usa `env_file` si se pasa) y lo carga con override=False.
    """
    if _running_in_railway():
        return  # usar exclusivamente variables del entorno
    # Local/dev:
    if env_file:
        load_dotenv(env_file, override=False)
    else:
        load_dotenv(find_dotenv(), override=False)


def _env(name: str, default: Optional[str] = None, *, required: bool = False) -> str:
    """
    Lee una variable de entorno.

    :param name: Nombre de la variable.
    :param default: Valor por defecto si no está presente.
    :param required: Si True y no existe valor, se lanza RuntimeError.
    :return: Valor leído (str).
    """
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        raise RuntimeError(f"Falta la variable de entorno: {name}")
    return val  # type: ignore[return-value]


def _env_bool(name: str, default: bool) -> bool:
    """
    Parsea una variable booleana del entorno con defaults.

    Acepta: '1','true','t','yes','y' como True; '0','false','f','no','n' como False (case-insensitive).
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    val = raw.strip().lower()
    if val in {"1", "true", "t", "yes", "y"}:
        return True
    if val in {"0", "false", "f", "no", "n"}:
        return False
    # Si el valor es inválido, conserva el default para evitar fallos sorpresivos
    return default


def _env_int(name: str, default: int) -> int:
    """Parsea un entero desde el entorno; si falla, devuelve el default."""
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _ensure_url(name: str, value: str) -> str:
    """Valida mínimamente que un valor parezca URL http(s)."""
    if not _URL_RE.match(value or ""):
        raise RuntimeError(f"{name} debe ser una URL http(s) válida. Valor recibido: {value!r}")
    return value


# -----------------------------------------------------------------------------
# Supabase / artefactos
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Settings:
    """
    Configuración de artefactos y credenciales para trabajar con Supabase Storage.

    Atributos:
        supabase_url: URL base de Supabase (http/https).
        supabase_key: Clave de servicio o anon key.
        sb_bucket: Bucket principal (descargas/subidas generales).
        artifacts_bucket: Bucket para artefactos (por defecto, el mismo que sb_bucket).
        env_slug: Etiqueta del entorno (ej. 'dev', 'prod').
        dataset_slug: Identificador corto del dataset.
        state_key: Key/objeto del state.json en Storage.
        artifacts_mode: Modo de manejo de artefactos grandes.
        artifacts_max_mb: Límite de tamaño (MB) para partir/manifestar artefactos.
        upload_raw, upload_snapshots, upload_reports, upload_log: Flags de subida.
    """
    supabase_url: str
    supabase_key: str
    sb_bucket: str
    artifacts_bucket: str
    env_slug: str
    dataset_slug: str
    state_key: str

    # Parámetros opcionales (se rellenan en load_settings)
    artifacts_mode: str = "manifest_on_oversize"
    artifacts_max_mb: int = 50
    upload_raw: bool = True
    upload_snapshots: bool = True
    upload_reports: bool = True
    upload_log: bool = True


def load_settings(env_file: Optional[str] = None) -> Settings:
    """
    Construye `Settings` leyendo el entorno (y `.env` en local).

    Retorna una instancia inmutable con valores validados.
    """
    _load_env(env_file)

    supabase_url = _ensure_url("SUPABASE_URL", _env("SUPABASE_URL", required=True))
    supabase_key = _env("SUPABASE_KEY", required=True)
    sb_bucket = _env("SUPABASE_SB_BUCKET", required=True)

    artifacts_bucket = _env("ARTIFACTS_BUCKET", default=sb_bucket)
    env_slug = _env("ENV_SLUG", required=True)
    dataset_slug = _env("DATASET_SLUG", required=True)
    state_key = _env("STATE_SB_OBJECT", required=True)

    # Opcionales y flags
    artifacts_mode = _env("ARTIFACTS_MODE", default="manifest_on_oversize")
    artifacts_max_mb = _env_int("ARTIFACTS_MAX_MB", default=50)
    upload_raw = _env_bool("UPLOAD_RAW", default=True)
    upload_snapshots = _env_bool("UPLOAD_SNAPSHOTS", default=True)
    upload_reports = _env_bool("UPLOAD_REPORTS", default=True)
    upload_log = _env_bool("UPLOAD_LOG", default=True)

    return Settings(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        sb_bucket=sb_bucket,
        artifacts_bucket=artifacts_bucket,
        env_slug=env_slug,
        dataset_slug=dataset_slug,
        state_key=state_key,
        artifacts_mode=artifacts_mode,
        artifacts_max_mb=artifacts_max_mb,
        upload_raw=upload_raw,
        upload_snapshots=upload_snapshots,
        upload_reports=upload_reports,
        upload_log=upload_log,
    )


# -----------------------------------------------------------------------------
# CKAN (extracción por package_show)
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class CkanSettings:
    """
    Configuración para el extractor CKAN (package_show).

    Atributos:
        api_pkg_show: URL del endpoint `package_show`.
        package_id: ID del package a consultar.
        prefix: Prefijo opcional para filtrar resources.
        sheet_blacklist: Nombres de hojas (CSV) a excluir en Excel.
        state_dir: Carpeta base para estado local.
        state_path_override: Ruta explícita del archivo de estado (si se provee).

    Propiedades:
        state_path: Ruta efectiva del archivo de estado local (override > state_dir).
    """
    api_pkg_show: str
    package_id: str
    prefix: Optional[str]
    sheet_blacklist: str
    state_dir: Path
    state_path_override: Optional[Path]

    @property
    def state_path(self) -> Path:
        """Ruta efectiva del archivo de estado local."""
        if self.state_path_override is not None:
            return self.state_path_override
        return self.state_dir / f"state_{self.package_id}.json"


def load_ckan_settings(env_file: Optional[str] = None) -> CkanSettings:
    """
    Construye `CkanSettings` leyendo el entorno (y `.env` en local).

    Variables relevantes:
        API_PKG_SHOW, PACKAGE_ID, PREFIX, SHEET_BLACKLIST, STATE_DIR, STATE_PATH.
    """
    _load_env(env_file)

    api_pkg_show = _ensure_url(
        "API_PKG_SHOW",
        _env("API_PKG_SHOW", default="https://www.datosabiertos.gob.ec/api/3/action/package_show"),
    )
    package_id = _env("PACKAGE_ID", required=True)
    prefix = _env("PREFIX", default=None)
    sheet_blacklist = _env("SHEET_BLACKLIST", default="Contenido")
    state_dir = Path(_env("STATE_DIR", default="state"))

    _state_override = os.getenv("STATE_PATH")
    state_path_override = Path(_state_override) if _state_override else None

    return CkanSettings(
        api_pkg_show=api_pkg_show,
        package_id=package_id,
        prefix=prefix,
        sheet_blacklist=sheet_blacklist,
        state_dir=state_dir,
        state_path_override=state_path_override,
    )
