# etl/load.py
"""
Carga y auditoría del pipeline hacia Supabase (PostgREST + Storage).

Este módulo implementa la fase **LOAD** del pipeline:
1) Convierte DataFrames transformados en registros listos para upsert en Postgres (vía Supabase).
2) Sube artefactos (raw, snapshots de config, reports, logs) a Supabase Storage
   con manejo seguro de archivos demasiado grandes (oversize).
3) Inserta auditoría del run en tablas `etl_runs` (padre) y `etl_run_resources` (hijas),
   garantizando integridad referencial (la fila padre se inserta primero).
4) Promueve el `state.json` local a Storage **solo** si la BD terminó OK
   y hubo recursos (o si se fuerza con `promote_state_if_no_resources=True`).

Convenciones:
- Tiempos se registran en hora local de Ecuador (UTC-05) en ISO-8601 con offset.
- El comportamiento de “oversize” se controla con `settings.artifacts_mode`:
  - "manifest_on_oversize": escribe `<key>.oversize.json` con metadata del archivo.
  - "skip": omite la subida.
  - "strict": lanza excepción (falla dura).
- La lista de columnas esperadas se toma desde el YAML (tipos, orden, críticas) y
  se complementa con claves conocidas (`surrogate_id`, `business_key`, `ano`).
"""

from __future__ import annotations

import os
import io
import json
import time
import hashlib
import mimetypes
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Iterable, Union, Tuple

import pandas as pd

# El SDK de Storage lanza StorageApiError (código 413 en archivos grandes)
try:
    from storage3.exceptions import StorageApiError  # para detectar 413
except Exception:
    class StorageApiError(Exception):
        def __init__(self, *args, code: int | None = None, **kwargs):
            super().__init__(*args, **kwargs)
            self.code = code


# =========================
# Helpers básicos
# =========================
def _env(key: str, default: str | None = None) -> str | None:
    """Lee variable de entorno con default ‘seguro’ (None si vacío)."""
    v = os.getenv(key)
    return v if v is not None and v != "" else default

def _now_iso_ec() -> str:
    """Devuelve hora de Ecuador (UTC-05) en ISO-8601 con offset (YYYY-MM-DDTHH:MM:SS-05:00)."""
    ts_utc = pd.Timestamp.utcnow()
    ts_ec = ts_utc - pd.Timedelta(hours=5)
    return ts_ec.strftime("%Y-%m-%dT%H:%M:%S-05:00")

def _to_iso_local(x):
    """
    Normaliza timestamps de datos para el destino:
    - Datetime → 'YYYY-MM-DD HH:MM:SS'
    - Fecha (YYYY-MM-DD) → agrega ' 00:00:00'
    - NaN → None
    """
    if pd.isna(x):
        return None
    if isinstance(x, pd.Timestamp):
        return x.strftime("%Y-%m-%d %H:%M:%S")
    s = str(x).strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s + " 00:00:00"
    return s

def _norm(s: str) -> str:
    """Normaliza nombres de columnas a minúsculas sin espacios extra (para matching)."""
    return str(s).strip().lower()


# =========================
# YAML / columnas esperadas
# =========================
from .yaml_config_loader import load_config

def _expected_columns(cfg: Dict[str, Any]) -> set:
    """
    Construye el conjunto de columnas esperadas (canonical) a partir del YAML:
    - tipos (datetime/numeric/category/string_codes/time)
    - orden base (order)
    - critical (all / any_of)
    - ids conocidas y (opcional) ‘extras_json_column’
    """
    exp: set[str] = set()

    t = cfg.get("types", {}) or {}
    for k in ("datetime", "numeric", "category", "string_codes", "time"):
        for c in (t.get(k, []) or []):
            exp.add(_norm(c))

    for c in (cfg.get("order", []) or []):
        exp.add(_norm(c))

    crit = cfg.get("critical", {}) or {}
    for c in (crit.get("all", []) or []):
        exp.add(_norm(c))
    for any_group in (crit.get("any_of", []) or []):
        for c in any_group or []:
            exp.add(_norm(c))

    # IDs conocidas
    exp.update({"surrogate_id", "business_key", "ano"})
    # si el YAML define extras_json_column, consérvala como columna física
    extras_col = (cfg.get("load", {}) or {}).get("extras_json_column")
    if extras_col:
        exp.add(_norm(extras_col))

    return {c for c in exp if c}


# =========================
# Batch / reintentos
# =========================
def _split_batches(records: List[dict], batch_size: int) -> Iterable[List[dict]]:
    """Genera lotes de tamaño `batch_size` (último puede ser menor)."""
    if batch_size <= 0:
        batch_size = 1000
    for i in range(0, len(records), batch_size):
        yield records[i:i+batch_size]

def _shrink_sequence(initial: int) -> List[int]:
    """
    Secuencia decreciente de tamaños para reintentos:
    Ej.: 1000 → [1000, 250, 100, 50]
    """
    seq = [max(1, initial)]
    if initial > 500:
        seq.append(250)
    if initial > 100:
        seq.append(100)
    if seq[-1] != 50:
        seq.append(50)
    return seq

def _parse_backoff(env_val: str | None, defaults: List[int]) -> List[int]:
    """Convierte '2,5,10' → [2,5,10]; si falla, usa `defaults`."""
    if not env_val:
        return defaults
    try:
        return [int(x.strip()) for x in str(env_val).split(",") if x.strip()]
    except Exception:
        return defaults


# =========================
# Storage (con oversize handling)
# =========================
def _guess_content_type(path: Union[str, Path], explicit: str | None = None) -> str:
    """Resuelve content-type por extensión; permite override explícito."""
    if explicit:
        return explicit
    ct, _ = mimetypes.guess_type(str(path))
    return ct or "application/octet-stream"

def _sha256_file(path: Union[str, Path]) -> str:
    """SHA-256 por streaming (constante memoria)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _upload_file(
    client,
    bucket: str,
    local_path: Union[str, Path],
    remote_key: str,
    *,
    logger=None,
    mode: str = "manifest_on_oversize",
    max_mb: float = 50.0,
    content_type: str | None = None,
) -> Tuple[bool, bool]:
    """
    Sube un archivo a Storage con manejo de oversize.

    Returns:
        (ok_subida_o_registrado, fue_oversize)

    Comportamiento por `mode`:
      - "manifest_on_oversize": escribe `<remote_key>.oversize.json` con {size_mb, sha256, ...}.
      - "skip": no sube (retorna ok=True para no frenar el run).
      - "strict": lanza si excede `max_mb` o si el SDK devuelve 413.
    """
    local_path = Path(local_path)
    if not local_path.exists():
        if logger:
            logger.warning("UPLOAD skip: file not found", {"path": str(local_path)})
        return (False, False)

    size_mb = local_path.stat().st_size / (1024 * 1024)
    ct = _guess_content_type(local_path, explicit=content_type)
    opts = {"upsert": "true"}
    if ct:
        opts["contentType"] = ct

    def _write_manifest():
        """Guarda un JSON con metadata del archivo demasiado grande."""
        manifest = {
            "type": "oversize",
            "file": local_path.name,
            "remote_key": remote_key,
            "size_mb": round(size_mb, 2),
            "limit_mb": max_mb,
            "sha256": _sha256_file(local_path),
            "generated_at": _now_iso_ec(),
        }
        mk = remote_key + ".oversize.json"
        payload = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")

        # Subir desde archivo temporal (el SDK maneja mejor file-like reales)
        with tempfile.NamedTemporaryFile("wb", suffix=".json", delete=False) as tmp:
            tmp.write(payload)
            tmp.flush()
            tmp_path = tmp.name
        try:
            with open(tmp_path, "rb") as f:
                client.storage.from_(bucket).upload(
                    path=mk,
                    file=f,
                    file_options={"upsert": "true", "contentType": "application/json"},
                )
        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

        if logger:
            logger.warning("Storage oversize → manifest written", {"manifest_key": mk})
        return (True, True)

    # Pre-chequeo local de tamaño
    if size_mb > max_mb:
        if mode == "manifest_on_oversize":
            return _write_manifest()
        elif mode == "skip":
            if logger:
                logger.warning("Storage oversize → skipped", {"key": remote_key, "size_mb": round(size_mb, 2)})
            return (True, True)
        else:  # strict
            raise RuntimeError(f"Artifact too large: {remote_key} ({round(size_mb, 2)}MB > {max_mb}MB)")

    # Intento de subida normal
    try:
        with local_path.open("rb") as f:
            client.storage.from_(bucket).upload(path=remote_key, file=f, file_options=opts)
        if logger:
            logger.info("UPLOAD ok", {"key": remote_key, "size_mb": round(size_mb, 2)})
        return (True, False)
    except StorageApiError as e:
        # Si Storage responde 413, aplicar política de oversize.
        if getattr(e, "code", None) == 413:
            if mode == "manifest_on_oversize":
                return _write_manifest()
            elif mode == "skip":
                if logger:
                    logger.warning("Storage oversize (413) → skipped", {"key": remote_key})
                return (True, True)
            else:
                raise
        raise

def _upload_tree(
    client,
    bucket: str,
    local_dir: Union[str, Path],
    remote_prefix: str,
    *,
    logger=None,
    mode: str = "manifest_on_oversize",
    max_mb: float = 50.0,
) -> Tuple[int, int]:
    """
    Sube recursivamente todos los archivos bajo `local_dir` a `<remote_prefix>/...`.

    Returns:
        (files_ok, oversize_count)
    """
    local_dir = Path(local_dir)
    if not local_dir.exists():
        if logger:
            logger.warning("UPLOAD tree skip: dir not found", {"local_dir": str(local_dir)})
        return (0, 0)

    files_ok = 0
    oversize_count = 0
    for p in local_dir.rglob("*"):
        if p.is_file():
            rel = p.relative_to(local_dir).as_posix()
            remote_key = f"{remote_prefix.rstrip('/')}/{rel}"
            try:
                ok, over = _upload_file(
                    client, bucket, p, remote_key, logger=logger, mode=mode, max_mb=max_mb
                )
                files_ok += 1 if ok else 0
                oversize_count += 1 if over else 0
            except Exception as e:
                # No-fatal: seguimos con el resto de archivos
                if logger:
                    logger.warning("UPLOAD file failed (non-fatal)", {"key": remote_key, "error": str(e)})
    return (files_ok, oversize_count)


# =========================
# Registros para destino
# =========================
def _records_from_df(
    df: pd.DataFrame,
    canonical: set,
    extras_colname: str | None,
    run_id: str,
) -> List[dict]:
    """
    Convierte un DataFrame a una lista de registros compatibles con la tabla destino.

    - Incluye solo columnas canónicas (respeta `extras_json_column` si está configurada).
    - Deriva `ano` desde `fecha_detencion_aprehension` si no existe pero la fecha sí.
    - Añade `load_run_id` como trazabilidad del run.
    - Serializa ‘extras’ (columnas no canónicas) si `extras_colname` está configurado.
    """
    df = df.copy()

    # Normaliza formato de fecha al estándar local (cadena simple sin tz)
    if "fecha_detencion_aprehension" in df.columns:
        df["fecha_detencion_aprehension"] = df["fecha_detencion_aprehension"].map(_to_iso_local)

    # Deriva 'ano' si está previsto en el esquema canónico y no existe aún
    if "ano" in canonical and "ano" not in df.columns and "fecha_detencion_aprehension" in df.columns:
        def _year_from_date(s):
            if s is None or pd.isna(s):
                return None
            try:
                return int(str(s)[:4])
            except Exception:
                return None
        df["ano"] = df["fecha_detencion_aprehension"].map(_year_from_date)

    present = set(c.lower() for c in df.columns)
    extras_cols = sorted(list(present - canonical))

    out: List[dict] = []
    for _, row in df.iterrows():
        rec = {}
        # Campos canónicos
        for c in df.columns:
            c_norm = c.lower()
            val = row[c]
            if pd.isna(val):
                val = None
            if c_norm in canonical and (extras_colname is None or c_norm != _norm(extras_colname)):
                rec[c_norm] = val

        # Trazabilidad del run
        rec["load_run_id"] = run_id  # existe en tu tabla de destino

        # Extras serializados (si aplica)
        if extras_colname:
            extras = {}
            for c in extras_cols:
                src = df.columns[df.columns.str.lower() == c][0]
                val = row[src]
                if pd.isna(val):
                    continue
                extras[c] = val
            rec[_norm(extras_colname)] = extras if extras else None

        out.append(rec)
    return out


# =========================
# Upsert destino
# =========================
def _upsert_batch(client, table: str, rows: List[dict], on_conflict: str, logger=None) -> int:
    """
    Ejecuta un upsert de `rows` en la `table` con la clave `on_conflict`.

    Nota: el SDK de Supabase realiza el upsert vía PostgREST.
    """
    client.table(table).upsert(rows, on_conflict=on_conflict).execute()
    if logger:
        logger.info("UPSERT batch ok", {"table": table, "rows": len(rows)})
    return len(rows)

def _upsert_with_retries(
    client,
    table: str,
    rows: List[dict],
    on_conflict: str,
    batch_size: int,
    max_retries: int,
    backoff_seq: List[int],
    logger=None
) -> Tuple[int, int]:
    """
    Upsert robusto con:
      - Lotes decrecientes (batch_size → 250 → 100 → 50).
      - Reintentos por lote con backoff (2,5,10...).

    Returns:
        (total_insertados, total_failed_batches)
    """
    total_inserted = 0
    total_failed_batches = 0
    if not rows:
        return 0, 0

    sizes = _shrink_sequence(batch_size)
    for size in sizes:
        for batch in _split_batches(rows, size):
            attempt = 0
            while True:
                try:
                    total_inserted += _upsert_batch(client, table, batch, on_conflict, logger=logger)
                    break
                except Exception as e:
                    attempt += 1
                    if logger:
                        logger.warning("UPSERT batch failed", {"size": size, "attempt": attempt, "error": str(e)})
                    if attempt > max_retries:
                        total_failed_batches += 1
                        if logger:
                            logger.error("UPSERT batch giving up", {"size": size, "rows": len(batch)})
                        break
                    idx = min(attempt - 1, len(backoff_seq) - 1) if backoff_seq else 0
                    wait = backoff_seq[idx] if backoff_seq else 2
                    time.sleep(wait)
    return total_inserted, total_failed_batches


# =========================
# Auditoría (con allowlist segura)
# =========================
# Columnas que EXISTEN en tus tablas (según DDL enviado)
_ETL_RUNS_ALLOWED_DEFAULT = {
    "run_id", "started_at", "ended_at", "status",
    "resources", "rows_in_total", "rows_out_total",
    "schema_missing_total", "schema_extra_total", "error_message",
}
_ETL_RUN_RES_ALLOWED_DEFAULT = {
    "run_id", "rid", "rows_in", "rows_out",
    "schema_missing", "schema_extra", "duplicates_business_key", "dedup_rows_dropped",
}

def _filter_fields(row: Dict[str, Any], allowed: List[str] | set | tuple, default_allowed: set) -> Dict[str, Any]:
    """
    Aplica una allowlist de columnas de destino:
      - Si el YAML no define nada, usa el set ‘seguro’ por defecto.
      - Si define pero trae extras que no existen en la tabla, se recortan.
    """
    allow = { _norm(c) for c in (allowed or []) }
    if not allow:
        allow = { _norm(c) for c in default_allowed }
    else:
        # incluso si YAML trae extras, recortamos a lo que existe en la tabla
        allow &= { _norm(c) for c in default_allowed }
    return {k: v for k, v in row.items() if _norm(k) in allow}

def _insert_run_started(client, run_id: str, runs_fields: List[str] | set | tuple, logger=None):
    """
    Registra la fila padre en `etl_runs` ANTES que las hijas (FK).
    Usa `status='error'` como placeholder válido (CHECK), se actualizará al final.
    """
    base_row = {
        "run_id": run_id,
        "started_at": _now_iso_ec(),
        "status": "error",  # placeholder conservador válido para el CHECK
    }
    row = _filter_fields(base_row, runs_fields, _ETL_RUNS_ALLOWED_DEFAULT) or base_row
    client.table("etl_runs").upsert([row], on_conflict="run_id").execute()
    if logger:
        logger.info("AUDIT etl_runs start ok", {"run_id": run_id})

def _insert_run_audit(client, run_id: str, status: str, runs_meta: Dict[str, Any], runs_fields: List[str] | set | tuple, logger=None):
    """Upsert final en `etl_runs` con métricas agregadas del run."""
    row = {
        "run_id": run_id,
        "status": status,  # solo 'ok' o 'error'
        "resources": runs_meta.get("resources_json", {}),
        "rows_in_total": runs_meta.get("rows_in_total", 0),
        "rows_out_total": runs_meta.get("rows_out_total", 0),
        "schema_missing_total": runs_meta.get("schema_missing_total", 0),
        "schema_extra_total": runs_meta.get("schema_extra_total", 0),
        "error_message": runs_meta.get("error_message"),
        "started_at": runs_meta.get("started_at"),
        "ended_at": runs_meta.get("ended_at"),
    }
    row = _filter_fields(row, runs_fields, _ETL_RUNS_ALLOWED_DEFAULT)
    client.table("etl_runs").upsert([row], on_conflict="run_id").execute()
    if logger:
        logger.info("AUDIT etl_runs upsert ok", {"run_id": run_id, "status": status})

def _insert_run_resource_audit(client, run_id: str, per_resource: Dict[str, Any], runres_fields: List[str] | set | tuple, logger=None):
    """
    Inserta/actualiza auditoría por recurso en `etl_run_resources`.
    Mantiene solo columnas que existen realmente en tu esquema.
    """
    rows = []
    for rid, meta in per_resource.items():
        rows.append({
            "run_id": run_id,
            "rid": rid,
            "rows_in": meta.get("rows_in", 0),
            "rows_out": meta.get("rows_out", 0),
            "schema_missing": meta.get("schema_missing", []),
            "schema_extra": meta.get("schema_extra", []),
            "duplicates_business_key": meta.get("duplicates_business_key", 0),
            "dedup_rows_dropped": meta.get("dedup_rows_dropped", 0),
        })
    rows = [ _filter_fields(r, runres_fields, _ETL_RUN_RES_ALLOWED_DEFAULT) for r in rows ]
    rows = [ r for r in rows if r ]
    if rows:
        client.table("etl_run_resources").upsert(rows, on_conflict="run_id,rid").execute()
        if logger:
            logger.info("AUDIT etl_run_resources upsert ok", {"run_id": run_id, "count": len(rows)})


# =========================
# Estado (promoción atómica)
# =========================
def _promote_state_atomic(client, bucket: str, local_state_path, final_key: str,
                          tmp_prefix: str, run_id: str, logger=None):
    """
    Sube el `state.json` local con estrategia segura:
      1) Subir a `tmp_prefix/<run_id>` (upsert).
      2) Copiar a `final_key`. Si existe (409), borrar y reintentar.
      3) Si COPY vuelve a fallar, subir directo con upsert=True.
      4) Borrar el temporal (best-effort).
    """
    from pathlib import Path
    local_state_path = Path(local_state_path)

    if not local_state_path.exists():
        if logger:
            logger.warning("STATE local path not found; skipping upload", {"path": str(local_state_path)})
        return

    tmp_key = f"{tmp_prefix.rstrip('/')}/{run_id}"

    # === IMPORTANTE: usar las MISMAS opciones que _upload_file ===
    # - camelCase en contentType
    # - upsert como string "true" (no bool)
    opts = {"upsert": "true", "contentType": "application/json"}

    # 1) Subir temporal con upsert
    with open(local_state_path, "rb") as f:
        client.storage.from_(bucket).upload(path=tmp_key, file=f, file_options=opts)

    # 2) Intentar COPY → final_key
    def _copy_tmp_to_final():
        client.storage.from_(bucket).copy(tmp_key, final_key)

    try:
        _copy_tmp_to_final()
        if logger:
            logger.info("STATE promoted via COPY", {"tmp_key": tmp_key, "final_key": final_key})
    except Exception as e_copy:
        msg = str(e_copy)
        if "Duplicate" in msg or "409" in msg:
            try:
                client.storage.from_(bucket).remove([final_key])
                _copy_tmp_to_final()
                if logger:
                    logger.info("STATE promoted after remove+COPY", {"final_key": final_key})
            except Exception as e_copy2:
                # Último recurso: upload directo con upsert (igual que _upload_file)
                try:
                    with open(local_state_path, "rb") as f2:
                        client.storage.from_(bucket).upload(path=final_key, file=f2, file_options=opts)
                    if logger:
                        logger.info("STATE promoted via direct upload (upsert)", {"final_key": final_key})
                except Exception as e_up:
                    if logger:
                        logger.warning("STATE upload failed (non-fatal)", {"error": str(e_up)})
        else:
            # Error distinto → subir directo con upsert
            try:
                with open(local_state_path, "rb") as f2:
                    client.storage.from_(bucket).upload(path=final_key, file=f2, file_options=opts)
                if logger:
                    logger.info("STATE promoted via direct upload (fallback)", {"final_key": final_key})
            except Exception as e_up:
                if logger:
                    logger.warning("STATE upload failed (non-fatal)", {"error": str(e_up)})

    # 4) Limpieza del temporal (best-effort)
    try:
        client.storage.from_(bucket).remove([tmp_key])
    except Exception:
        pass

# =========================
# API principal (STRICT settings)
# =========================
class ConfigError(Exception):
    """Errores de configuración estricta para `load_to_supabase`."""
    pass

_ALLOWED_ARTIFACTS_MODES = {"manifest_on_oversize", "skip", "strict"}

def _require_setting(settings: Any, name: str):
    """Obtiene `settings.name` o lanza ConfigError si está ausente/vacío."""
    if settings is None or not hasattr(settings, name):
        raise ConfigError(f"Missing required settings.{name}")
    v = getattr(settings, name)
    if v is None or (isinstance(v, str) and v.strip() == ""):
        raise ConfigError(f"Missing required settings.{name}")
    return v

def _coerce_mode(x):
    """Valida y normaliza el modo de artefactos."""
    m = str(x).strip().lower()
    if m not in _ALLOWED_ARTIFACTS_MODES:
        raise ConfigError(f"settings.artifacts_mode must be one of {_ALLOWED_ARTIFACTS_MODES}")
    return m

def _coerce_float_pos(x):
    """Convierte a float y exige valor positivo (>0)."""
    fx = float(x)
    if fx <= 0:
        raise ConfigError("settings.artifacts_max_mb must be > 0")
    return fx


def load_to_supabase(
    tdfs: Dict[str, pd.DataFrame],
    config_path: Union[str, Path],
    client,
    run_id: str,
    logger=None,
    settings: Any | None = None,   # pipeline.py pasa st
    promote_state_if_no_resources: bool = False,
) -> Dict[str, Any]:
    """
    Carga idempotente por lotes a Supabase/Postgres, sube artefactos a Storage y
    registra auditoría en BD. Devuelve métricas del run.

    Flujo:
      1) Upsert por recurso a tabla destino (`table`, `on_conflict`).
      2) Auditoría por recurso en `etl_run_resources` (si hubo recursos).
      3) Subida de artefactos (raw, snapshots, reports, log) con política de oversize.
      4) Cierre del run en `etl_runs` y, si corresponde, promoción de `state.json` (canónico, overwrite).
    """
    # === Config / YAML
    cfg = load_config(config_path)
    load_cfg = cfg.get("load", {}) or {}
    table = load_cfg.get("table", "detenidos_aprehendidos")
    on_conflict = load_cfg.get("upsert_key", "surrogate_id")
    extras_colname = load_cfg.get("extras_json_column")

    batch_size = int(load_cfg.get("batch_size", 1000))
    max_retries = int(load_cfg.get("max_retries", 3))
    backoff_seq = list(load_cfg.get("retry_backoff_seconds", [2, 5, 10]))

    audit_cfg = load_cfg.get("audit", {}) or {}
    runs_fields = audit_cfg.get("etl_runs_fields") or []
    runres_fields = audit_cfg.get("etl_run_resources_fields") or []

    # === Settings estrictos (sin fallback silencioso)
    bucket = _require_setting(settings, "sb_bucket")
    env_slug = _require_setting(settings, "env_slug")
    dataset_slug = _require_setting(settings, "dataset_slug")
    base_prefix = f"{env_slug}/{dataset_slug}/runs/{run_id}"

    # --- STATE (canónico) ---
    # Clave final canónica (sobrescribible SIEMPRE) y prefijo temporal.
    # Si no viene desde settings.state_key, fabricamos una por convención.
    state_final_key = getattr(settings, "state_key", None)
    if not state_final_key:
        state_final_key = f"{env_slug}/{dataset_slug}/states/{dataset_slug}.json"

    # Prefijo temporal por dataset (evita colisiones entre datasets)
    state_tmp_prefix = f"{env_slug}/{dataset_slug}/states/tmp"

    # Ruta local donde extract dejó el state (o donde lo sincronizaste antes del run).
    # No es crítica: si no existe, simplemente no promovemos.
    state_local_path = Path(_env("STATE_PATH", "state.json"))

    mode = _coerce_mode(_require_setting(settings, "artifacts_mode"))
    max_mb = _coerce_float_pos(_require_setting(settings, "artifacts_max_mb"))

    do_raw = bool(_require_setting(settings, "upload_raw"))
    do_snaps = bool(_require_setting(settings, "upload_snapshots"))
    do_reports = bool(_require_setting(settings, "upload_reports"))
    do_log = bool(_require_setting(settings, "upload_log"))

    # Canonical esperado (contrato de columnas)
    canonical = _expected_columns(cfg)

    # Tiempos de auditoría (Ecuador fijo)
    started_at = _now_iso_ec()

    if logger:
        if not tdfs:
            logger.info("No hay recursos para cargar (tdfs vacío); se registrará el run y artefactos si aplica.")
        logger.info("LOAD start", {
            "table": table, "batch_size": batch_size,
            "bucket": bucket, "base_prefix": base_prefix,
            "mode": mode, "max_mb": max_mb,
            "upload_raw": do_raw, "upload_snapshots": do_snaps,
            "upload_reports": do_reports, "upload_log": do_log,
            "state_key_set": bool(state_final_key),
            "state_final_key": state_final_key,
            "state_tmp_prefix": state_tmp_prefix,
        })

    # === Asegura fila padre al inicio (FK)
    _insert_run_started(client, run_id, runs_fields, logger=logger)

    # Estructura común de métricas para cerrar el run
    run_report = {
        "rows_in_total": 0,
        "rows_out_total": 0,
        "schema_missing_total": 0,
        "schema_extra_total": 0,
        "resources_json": {},
        "started_at": started_at,
        "ended_at": None,
        "error_message": None,
    }

    failed_batches_total = 0
    oversize_count = 0

    try:
        # 1) UPSERT destino (por recurso)
        for rid, df in (tdfs or {}).items():
            rows_in = int(getattr(df, "_rows_in", len(df)))
            records = _records_from_df(df, canonical, extras_colname, run_id)
            rows_out = len(records)

            _, failed_batches = _upsert_with_retries(
                client, table, records, on_conflict=on_conflict,
                batch_size=batch_size, max_retries=max_retries,
                backoff_seq=backoff_seq, logger=logger
            )
            failed_batches_total += failed_batches

            run_report["rows_in_total"] += rows_in
            run_report["rows_out_total"] += rows_out
            run_report["resources_json"][rid] = {
                "rows_in": rows_in,
                "rows_out": rows_out,
                "schema_missing": [],
                "schema_extra": [],
                "duplicates_business_key": 0,
                "dedup_rows_dropped": 0,
            }

        # 1b) Auditoría por recurso (si hay recursos)
        _insert_run_resource_audit(client, run_id, run_report["resources_json"], runres_fields, logger=logger)

        # Indicador: hubo recursos en este run
        has_resources = bool(run_report["resources_json"])

        # 2) Artefactos (no fatales)
        try:
            if do_raw:
                _, over = _upload_tree(client, bucket, "data/raw", f"{base_prefix}/raw",
                                       logger=logger, mode=mode, max_mb=max_mb)
                oversize_count += over
            if do_reports:
                _, over = _upload_tree(client, bucket, "data/artifacts/reports", f"{base_prefix}/reports",
                                       logger=logger, mode=mode, max_mb=max_mb)
                oversize_count += over
            if do_snaps:
                _, over = _upload_tree(client, bucket, "data/artifacts/config_snapshots", f"{base_prefix}/config_snapshots",
                                       logger=logger, mode=mode, max_mb=max_mb)
                oversize_count += over
            if do_log:
                log_file = Path(_env("LOG_FILE", "etl.log"))
                if log_file.exists():
                    ok, over = _upload_file(client, bucket, log_file,
                                            f"{base_prefix}/logs/{log_file.name}",
                                            logger=logger, mode=mode, max_mb=max_mb,
                                            content_type="application/json")
                    oversize_count += 1 if over else 0
        except Exception as e_art:
            # No-fatal: la carga principal no depende de artefactos
            if logger:
                logger.warning("ARTIFACTS upload failed (non-fatal)", {"error": str(e_art)})

        # 3) Cierre del run
        ended_at = _now_iso_ec()
        run_report["ended_at"] = ended_at

        db_ok = (failed_batches_total == 0)
        status = "ok" if db_ok else "error"   # compatible con CHECK en tu DDL

        _insert_run_audit(client, run_id, status=status,
                          runs_meta=run_report, runs_fields=runs_fields, logger=logger)

        # 4) Promoción de state (canónico, overwrite garantizado)
        if db_ok and state_final_key and (has_resources or promote_state_if_no_resources):
            try:
                # Intento normal (sube a tmp y hace COPY → destino)
                _promote_state_atomic(
                    client=client,
                    bucket=bucket,
                    local_state_path=state_local_path,
                    final_key=state_final_key,
                    tmp_prefix=state_tmp_prefix,
                    run_id=run_id,
                    logger=logger,
                )
            except Exception as e_state:
                # Fallback fuerte: upload directo con upsert al canónico (SDK espera camelCase y strings)
                try:
                    if state_local_path.exists():
                        opts = {"upsert": "true", "contentType": "application/json"}
                        with open(state_local_path, "rb") as f2:
                            client.storage.from_(bucket).upload(
                                path=state_final_key,
                                file=f2,
                                file_options=opts,
                            )
                        if logger:
                            logger.info("STATE promoted via direct upload (upsert)", {
                                "final_key": state_final_key
                            })
                    else:
                        if logger:
                            logger.warning("STATE local path not found; skipping upload", {
                                "path": str(state_local_path)
                            })
                except Exception as e_up:
                    if logger:
                        logger.warning("STATE upload failed (non-fatal)", {"error": str(e_up)})
        elif not db_ok and logger:
            logger.warning("State NOT promoted (DB errors present)")
        elif not has_resources and not promote_state_if_no_resources and logger:
            logger.info("State NOT promoted (no resources and promote_state_if_no_resources=False)")

        if logger:
            logger.info("LOAD completed", {
                "table": table,
                "status": status,
                "resources": len(run_report["resources_json"]),
                "rows_in_total": run_report["rows_in_total"],
                "rows_out_total": run_report["rows_out_total"],
            })

        return {
            "rows_in_total": run_report["rows_in_total"],
            "rows_out_total": run_report["rows_out_total"],
            "schema_missing_total": run_report["schema_missing_total"],
            "schema_extra_total": run_report["schema_extra_total"],
            "resources_json": run_report["resources_json"],
            "started_at": started_at,
            "ended_at": ended_at,
            "status": status,
        }

    except Exception as e:
        # === Camino de error duro: cerramos el run, registramos mensaje y subimos log si se puede
        run_report["error_message"] = str(e)
        run_report["ended_at"] = _now_iso_ec()

        try:
            _insert_run_audit(client, run_id, status="error",
                              runs_meta=run_report, runs_fields=runs_fields, logger=logger)
        finally:
            if do_log:
                try:
                    log_file = Path(_env("LOG_FILE", "etl.log"))
                    if log_file.exists():
                        _upload_file(client, bucket, log_file,
                                     f"{base_prefix}/logs/{log_file.name}",
                                     logger=logger, mode=mode, max_mb=max_mb,
                                     content_type="application/json")
                except Exception as e2:
                    if logger:
                        logger.warning("LOG upload failed in error path (non-fatal)", {"error": str(e2)})

        if logger:
            logger.error("LOAD failed hard", {"error": str(e)})
        raise

