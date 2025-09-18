# etl/extract.py
"""
Módulo de extracción de datos desde CKAN.

Este componente:
- Consulta el endpoint CKAN `package_show` para obtener recursos.
- Detecta cambios comparando contra un archivo de estado (state.json).
- Descarga los recursos nuevos/modificados a `data/raw`.
- Convierte a CSV en `data/staging`.
- Devuelve un diccionario de DataFrames listos para transformación.

Arquitectura:
- Configuración centralizada se carga desde `etl/config.py`.
- Usa requests con reintentos exponenciales.
- Almacena/lee estado en JSON local para detectar cambios incrementales.
- Soporta múltiples formatos (XLSX, XLS, CSV).
"""

from __future__ import annotations

import os
import re
import json
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from dateutil import parser as dtparse, tz as dttz

from .config import load_ckan_settings
from .log import get_logger

logger = get_logger(__name__)

# -----------------------------------------------------------------------------
# Configuración CKAN (desde .env / entorno)
# -----------------------------------------------------------------------------
_cfg = load_ckan_settings()
API_PKG_SHOW     = _cfg.api_pkg_show
PACKAGE_ID       = _cfg.package_id
PREFIX           = _cfg.prefix
SHEET_BLACKLIST  = _cfg.sheet_blacklist
STATE_PATH       = _cfg.state_path  # Ruta efectiva del archivo de estado local

# -----------------------------------------------------------------------------
# Sesión HTTP robusta
# -----------------------------------------------------------------------------
def _session_with_retries() -> requests.Session:
    """
    Crea una sesión requests con política de reintentos.

    - Reintenta en 429/5xx con backoff exponencial.
    - Establece User-Agent identificable para debugging.
    """
    s = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "etl-proto/1.0 (+requests)"})
    return s

# -----------------------------------------------------------------------------
# Estado local (state.json)
# -----------------------------------------------------------------------------
def _load_state(path: Path) -> dict:
    """Carga el JSON de estado local. Si no existe, retorna plantilla vacía."""
    if not path.exists():
        return {"resources": {}}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def _save_state(path: Path, state: dict) -> None:
    """Guarda el estado local en disco, con indentación y sort_keys para legibilidad."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)

def _normalize_dt(dt_str: str | None) -> str:
    """Normaliza fechas a ISO UTC 'YYYY-MM-DDTHH:MM:SSZ'."""
    if not dt_str:
        return ""
    dt = dtparse.parse(dt_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=dttz.UTC)
    else:
        dt = dt.astimezone(dttz.UTC)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _minimal_fp_payload(res: dict) -> dict:
    """Extrae fingerprint mínimo de un recurso CKAN para detectar cambios."""
    lm = res.get("last_modified") or res.get("metadata_modified")
    return {
        "id": res.get("id"),
        "last_modified": _normalize_dt(lm),
        "size": int(res.get("size") or 0),
        "url": res.get("url") or "",
        "format": (res.get("format") or "").upper(),
    }

# -----------------------------------------------------------------------------
# CKAN: package_show
# -----------------------------------------------------------------------------
def extract_package(package_id: str, prefix: str | None = None) -> dict:
    """
    Consulta package_show de CKAN y filtra recursos opcionalmente por prefijo.

    Args:
        package_id: ID del package CKAN.
        prefix: Si se define, filtra recursos por nombre/id que empiecen con ese prefijo.

    Returns:
        dict con metadata de package (incluye lista 'resources').
    """
    s = _session_with_retries()
    r = s.get(API_PKG_SHOW, params={"id": package_id}, timeout=(10, 60))
    r.raise_for_status()

    payload = r.json()
    if payload.get("success") is False:
        raise RuntimeError(f"CKAN devolvió success=false para package_id={package_id}")

    data = payload.get("result", {})
    resources = data.get("resources", [])

    # Normaliza y aplica prefijo
    if prefix:
        p = prefix.strip().strip('"').strip("'")
        if p:
            p_norm = _normalize(p)
            filtered = [
                res for res in resources
                if _normalize(res.get("name", "")).startswith(p_norm)
                or _normalize(res.get("id", "")).startswith(p_norm)
            ]
            if not filtered:  # fallback contains
                filtered = [
                    res for res in resources
                    if p_norm in _normalize(res.get("name", ""))
                    or p_norm in _normalize(res.get("id", ""))
                ]
            data["resources"] = filtered
        else:
            data["resources"] = resources
    else:
        data["resources"] = resources

    return data

# -----------------------------------------------------------------------------
# Diferencias contra estado previo
# -----------------------------------------------------------------------------
def diff_resources(resources: List[dict], prev_state: dict) -> Tuple[List[dict], List[dict], List[Dict[str, str]]]:
    """
    Compara recursos actuales con el estado previo.

    Returns:
        (to_download, unchanged, reasons)
    """
    to_download, unchanged, reasons = [], [], []
    prev = prev_state.get("resources", {})
    for res in resources:
        rid = res.get("id")
        cur = _minimal_fp_payload(res)
        old = prev.get(rid)
        if not old:
            to_download.append(res)
            reasons.append({"id": rid, "reason": "new"})
        else:
            fields = ["last_modified", "size", "url", "format"]
            changed = [f for f in fields if cur[f] != old.get(f)]
            if changed:
                to_download.append(res)
                reasons.append({"id": rid, "reason": ",".join(changed)})
            else:
                unchanged.append(res)
    return to_download, unchanged, reasons

# -----------------------------------------------------------------------------
# Utilidades de archivo
# -----------------------------------------------------------------------------
def _infer_ext(res: dict) -> str:
    """Infere extensión esperada (.xlsx/.xls/.csv) a partir de formato o URL."""
    fmt = (res.get("format") or "").upper()
    if fmt == "XLSX": return ".xlsx"
    if fmt == "XLS":  return ".xls"
    if fmt == "CSV":  return ".csv"
    url = (res.get("url") or "").lower()
    for ext in (".xlsx", ".xls", ".csv"):
        if url.endswith(ext):
            return ext
    return ".bin"

def _strip_known_ext(basename: str) -> str:
    """Quita extensiones conocidas del final del nombre de archivo."""
    if not basename:
        return basename
    name, lower = basename.strip(), basename.strip().lower()
    for ext in (".xlsx", ".xls", ".csv"):
        if lower.endswith(ext):
            return name[:-len(ext)]
    return name

def _safe_stem(name: str, fallback: str) -> str:
    """Genera nombre de archivo seguro (solo A-Za-z0-9._-)."""
    name = (name or "").strip() or fallback
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    return stem or fallback

def _resource_paths(res: dict, raw_dir: Path, staging_dir: Path) -> tuple[Path, Path]:
    """Devuelve rutas locales (raw_path, staging_path) para un recurso CKAN."""
    ext = _infer_ext(res)
    base_name = _strip_known_ext(res.get("name", ""))
    stem = _safe_stem(base_name, res.get("id", "resource"))
    raw_path = raw_dir / f"{stem}{ext}"
    staging_path = staging_dir / f"{stem}.csv"
    return raw_path, staging_path

# -----------------------------------------------------------------------------
# Excel: selección de hojas
# -----------------------------------------------------------------------------
def _normalize(s: str) -> str:
    """Normaliza texto: sin tildes, lowercase, sin espacios extra."""
    s = (s or "").strip()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return s.lower()

def _blacklist_from_cfg() -> set[str]:
    """Construye set de hojas a excluir según config."""
    raw = SHEET_BLACKLIST or ""
    items = [_normalize(x) for x in raw.split(",") if x.strip()]
    return set(items)

def _pick_excel_sheets(xl: pd.ExcelFile, blacklist: set[str]) -> list[str]:
    """
    Selecciona hojas de Excel a importar:
    - Numéricas puras ("1","2").
    - Excluye blacklist.
    - Fallback: primera hoja válida o primera de todas.
    """
    numeric = [s for s in xl.sheet_names if s.strip().isdigit() and _normalize(s) not in blacklist]
    if numeric:
        return numeric
    for s in xl.sheet_names:
        if _normalize(s) not in blacklist:
            return [s]
    return [xl.sheet_names[0]]

# -----------------------------------------------------------------------------
# Descarga RAW + parseo DataFrame
# -----------------------------------------------------------------------------
def _download_raw(session: requests.Session, url: str, dest: Path, progress: bool = False, read_timeout: int = 180) -> None:
    """Descarga un archivo remoto a disco, mostrando progreso opcional."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with session.get(url, stream=True, timeout=(10, read_timeout), allow_redirects=True) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("Content-Length") or 0)
        downloaded = 0
        with dest.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1 MB
                if chunk:
                    f.write(chunk)
                    if progress and total:
                        downloaded += len(chunk)
                        pct = (downloaded / total) * 100
                        print(f"\rDescargando {dest.name}: {downloaded/1_048_576:.1f}/{total/1_048_576:.1f} MB ({pct:.1f}%)", end="")
        if progress and total:
            print()

def _read_df_from_raw(raw_path: Path, res: dict) -> pd.DataFrame:
    """Lee archivo local (CSV/XLS/XLSX) a DataFrame, aplicando reglas de selección de hojas."""
    ext = raw_path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(raw_path)
    if ext in (".xlsx", ".xls"):
        xl = pd.ExcelFile(raw_path)
        sheets = _pick_excel_sheets(xl, _blacklist_from_cfg())
        dfs = [xl.parse(s) for s in sheets]
        return pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
    raise ValueError(f"Formato no soportado para parseo: {ext} ({raw_path.name})")

# -----------------------------------------------------------------------------
# Semillado (opcional)
# -----------------------------------------------------------------------------
def seed_state_from_metadata(package_id: str, prefix: str | None, state_path: Path) -> None:
    """Inicializa el state.json con metadata (sin descargas)."""
    state_path = Path(state_path)
    prev_state = _load_state(state_path)
    pkg = extract_package(package_id, prefix=prefix)
    for res in pkg.get("resources", []):
        prev_state.setdefault("resources", {})[res["id"]] = _minimal_fp_payload(res)
    _save_state(state_path, prev_state)

# -----------------------------------------------------------------------------
# API principal de extracción
# -----------------------------------------------------------------------------
def extract_updated_dfs(
    package_id: str,
    prefix: str | None,
    state_path: Path,
    raw_dir: Path = Path("../data/raw"),
    staging_dir: Path = Path("../data/staging"),
    progress: bool = True,
    seed_if_missing: bool = False,
    read_timeout: int = 180,
) -> Dict[str, pd.DataFrame]:
    """
    Orquesta la extracción de recursos CKAN actualizados.

    Flujo:
      - Lee estado previo (o lo inicializa).
      - Detecta recursos nuevos/modificados.
      - Descarga archivos RAW.
      - Convierte a CSV en staging.
      - Devuelve dict {resource_id: DataFrame}.
    """
    logger.info("Starting extraction")
    state_path = Path(state_path)

    # Seed inicial
    if seed_if_missing and not state_path.exists():
        seed_state_from_metadata(package_id, prefix, state_path)
        print(f"Estado inicial creado en {state_path} (sin descargas).")
        return {}

    prev_state = _load_state(state_path)
    pkg = extract_package(package_id, prefix=prefix)
    resources = pkg.get("resources", [])

    to_download, _, _ = diff_resources(resources, prev_state)
    if not to_download:
        print("No hay recursos nuevos o modificados.")
        return {}

    session = _session_with_retries()
    dfs: Dict[str, pd.DataFrame] = {}

    for res in to_download:
        rid = res["id"]
        name = res.get("name", rid)
        size_mb = (res.get("size") or 0) / (1024 * 1024)
        raw_path, staging_path = _resource_paths(res, Path(raw_dir), Path(staging_dir))

        print(f"→ Bajando: {name} (~{size_mb:.1f} MB) -> {raw_path}")
        _download_raw(session, res["url"], raw_path, progress=progress, read_timeout=read_timeout)

        df = _read_df_from_raw(raw_path, res)
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(staging_path, index=False, encoding="utf-8")
        dfs[rid] = df

        prev_state.setdefault("resources", {})[rid] = _minimal_fp_payload(res)

    _save_state(state_path, prev_state)
    logger.info("Extraction completed")
    return dfs