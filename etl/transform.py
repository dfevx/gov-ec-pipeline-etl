# etl/transform.py
"""
Transformación de DataFrames extraídos (staging → canonical).

Responsabilidades:
- Normalizar encabezados y nombres de columnas (sin tildes, snake_case, etc.).
- Aplicar reglas declarativas desde YAML (rename, normalize, types, categories, numeric_rules).
- Validar esquema esperado (expected vs. missing/extra) con política configurable.
- Generar identificadores: business_key (compuesta o simple) y surrogate_id (uuid5/sha256).
- Deduplicar según reglas de integridad (mantener último por fecha, etc.).
- Reordenar columnas y producir un **reporte por recurso** (métricas, diffs, advertencias).

Entradas:
- `dfs`: dict {resource_id: DataFrame} producido por la etapa de extract.
- `config_path`: ruta a YAML/JSON con la configuración del dataset (ver `yaml_config_loader.load_config`).
- `run_id`: id de corrida (para snapshot de config y trazabilidad).
- `logger`: logger opcional (JSON-Lines) para auditoría.
- `snapshot_dir`: carpeta donde se guardan snapshots de la config usada.

Salidas:
- `(out_dfs, report)`:
  - `out_dfs`: dict {resource_id: DataFrame transformado}
  - `report`: dict con métricas y verificaciones por recurso.

Nota: Esta etapa **no** persiste nada; devuelve datos y reporte para que `load.py` procese la inserción.
"""

from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List, Tuple, Union
import json
import uuid
import hashlib
import unicodedata
import re
import pandas as pd

from .yaml_config_loader import load_config


class TransformHardFail(RuntimeError):
    """Error crítico en transformación (severidad controlada por YAML.promote_policy)."""
    pass


# =========================
# Normalización de texto/columnas
# =========================
def _strip_accents(s: str) -> str:
    """Elimina tildes/diacríticos en la cadena `s`."""
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))

def _normalize_colname(name: str) -> str:
    """Normaliza nombres de columna → snake_case sin tildes ni caracteres no alfanuméricos."""
    s = str(name).strip()
    s = _strip_accents(s).lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def _norm_text(x):
    """Normaliza texto libre (minúsculas, sin tildes, espacios comprimidos)."""
    if pd.isna(x):
        return x
    s = str(x).strip()
    s = _strip_accents(s).lower()
    s = ' '.join(s.split())
    return s

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica `_normalize_colname` a todas las columnas del DataFrame."""
    df = df.copy()
    df.columns = [_normalize_colname(c) for c in df.columns.astype(str)]
    return df

def _fix_header(df: pd.DataFrame, lookahead: int = 5) -> pd.DataFrame:
    """
    Detecta la fila más probable de encabezados (en las primeras `lookahead` filas)
    y la promueve a columnas. Limpia columnas totalmente nulas y 'unnamed_*'.
    """
    if df.empty:
        return df

    cols = [str(c).lower() for c in df.columns]
    prop_unnamed = sum(c.startswith("unnamed") or c == "" for c in cols) / max(len(cols), 1)

    k = min(lookahead, len(df))
    best_idx, best_score = 0, -1.0
    for i in range(k):
        row = df.iloc[i]
        nn = int(row.notna().sum())
        str_nonempty = sum(1 for v in row.tolist() if isinstance(v, str) and v.strip())
        numeric_like = int(pd.to_numeric(row, errors="coerce").notna().sum())
        score = str_nonempty * 2 + nn - numeric_like * 0.5
        if score > best_score:
            best_idx, best_score = i, score

    # Si muchas columnas son 'unnamed' o la mejor fila no es la primera → promover fila i como header
    if prop_unnamed >= 0.5 or best_idx > 0:
        raw = df.iloc[best_idx].astype("string").fillna("").tolist()
        new_cols = [_normalize_colname(c) if c else f"col_{i+1}" for i, c in enumerate(raw)]
        df = df.iloc[best_idx + 1 :].copy()
        df.columns = new_cols

    # Drop columnas completamente nulas
    all_null = [c for c in df.columns if df[c].isna().all()]
    if all_null:
        df = df.drop(columns=all_null)

    # Limpia unnamed residuales vacías
    unnamed_residual = [c for c in df.columns if str(c).startswith("unnamed") or str(c)==""]
    to_drop = [c for c in unnamed_residual if df[c].notna().sum() == 0]
    if to_drop:
        df = df.drop(columns=to_drop)

    # Homogeneiza nombres
    df = df.rename(columns=lambda c: _normalize_colname(c))
    return df


# =========================
# Config / rename / normalize
# =========================
def _apply_rename(df: pd.DataFrame, rename: Dict[str, str] | None) -> pd.DataFrame:
    """Renombra columnas conforme al mapeo del YAML (normalizado)."""
    if not rename:
        return df
    mapping = { _normalize_colname(k): _normalize_colname(v) for k, v in rename.items() }
    return df.rename(columns=mapping)

def _apply_normalize(df: pd.DataFrame, ncfg: Dict[str, Any] | None) -> pd.DataFrame:
    """
    Aplica normalizaciones ligeras:
    - Remplazo de valores NA declarados.
    - Strip y cast a string en columnas de texto.
    - Drops opcionales de filas/columnas totalmente nulas.
    """
    if not ncfg:
        return df
    df = df.copy()

    for token in ncfg.get("na_values", []):
        df.replace(to_replace=token, value=pd.NA, inplace=True)

    for c in df.columns:
        if pd.api.types.is_string_dtype(df[c]) or df[c].dtype == "object":
            df[c] = df[c].astype("string").str.strip()

    if ncfg.get("drop_all_null_rows", False):
        df.dropna(axis=0, how="all", inplace=True)
    if ncfg.get("drop_all_null_cols", False):
        df.dropna(axis=1, how="all", inplace=True)

    return df


# =========================
# Tipificación defensiva
# =========================
def _coerce_types(df: pd.DataFrame, tcfg: Dict[str, List[str]] | None) -> pd.DataFrame:
    """
    Ajusta tipos con tolerancia:
    - datetime → `to_datetime(errors="coerce")`
    - numeric  → `to_numeric(errors="coerce")`
    - category/string_codes → string
    """
    if not tcfg:
        return df
    df = df.copy()
    for c in tcfg.get("datetime", []) or []:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=False)
    for c in tcfg.get("numeric", []) or []:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in tcfg.get("category", []) or []:
        if c in df.columns:
            df[c] = df[c].astype("string")
    for c in tcfg.get("string_codes", []) or []:
        if c in df.columns:
            df[c] = df[c].astype("string")
    return df


# =========================
# Validaciones de esquema
# =========================
def _expected_columns(cfg: Dict[str, Any]) -> set:
    """
    Construye el conjunto de columnas esperadas combinando:
    - tipos (datetime/numeric/category/string_codes/time)
    - orden base
    - críticas (all, any_of)
    - recomendadas (lista simple o grupos)
    - evolución/rename (para contemplar columnas renombradas)
    """
    exp: set[str] = set()

    t = cfg.get("types", {}) or {}
    for k in ("datetime","numeric","category","string_codes","time"):
        for c in (t.get(k, []) or []):
            exp.add(_normalize_colname(c))

    for c in (cfg.get("order", []) or []):
        exp.add(_normalize_colname(c))

    crit = cfg.get("critical", {}) or {}
    for c in (crit.get("all", []) or []):
        exp.add(_normalize_colname(c))
    for any_group in (crit.get("any_of", []) or []):
        for c in any_group or []:
            exp.add(_normalize_colname(c))

    rec = cfg.get("recommended", []) or []
    for r in rec:
        if isinstance(r, list):
            for c in r:
                exp.add(_normalize_colname(c))
        else:
            exp.add(_normalize_colname(r))

    evo = cfg.get("evolution", {}) or {}
    for v in evo.values():
        for c in (v.get("optional_only_here", []) or []):
            exp.add(_normalize_colname(c))

    rename_map = cfg.get("rename", {}) or {}
    for v in rename_map.values():
        exp.add(_normalize_colname(v))

    return {c for c in exp if c}

def _schema_diffs(cols: set, expected: set) -> Tuple[List[str], List[str]]:
    """Devuelve (missing, extra) en relación a `expected`."""
    missing = sorted(list(expected - cols))
    extra   = sorted(list(cols - expected))
    return missing, extra


# =========================
# Reglas de calidad
# =========================
def _check_critical(df: pd.DataFrame, cc) -> Dict[str, Any]:
    """Evalúa columnas críticas: faltantes ‘all’ y grupos ‘any_of’ sin presencia."""
    issues = {"missing_all": [], "missing_any_of": []}
    all_cols = cc.get("all", []) or []
    for c in all_cols:
        if c not in df.columns or df[c].isna().all():
            issues["missing_all"].append(c)
    for grp in (cc.get("any_of", []) or []):
        present_nonnull = any((c in df.columns) and df[c].notna().any() for c in grp)
        if not present_nonnull:
            issues["missing_any_of"].append(grp)
    return issues

def _warn_recommended(df: pd.DataFrame, rec) -> List[str]:
    """Retorna advertencias por columnas recomendadas ausentes o vacías."""
    warns = []
    for item in (rec or []):
        if isinstance(item, list):
            if not any((c in df.columns) and df[c].notna().any() for c in item):
                warns.append(f"any_of_missing:{'|'.join(item)}")
        else:
            if (item not in df.columns) or (df[item].isna().all()):
                warns.append(f"missing:{item}")
    return warns


# =========================
# Orden de columnas
# =========================
def _reorder(df: pd.DataFrame, order: List[str] | None) -> pd.DataFrame:
    """Coloca `order` al frente y conserva el resto al final en orden original."""
    if not order:
        return df
    front = [c for c in order if c in df.columns]
    rest  = [c for c in df.columns if c not in front]
    return df[front + rest]


# =========================
# Reglas categorías / numéricas
# =========================
def _apply_category_rules(df: pd.DataFrame, cat_cfg: Dict[str, Any] | None, report: Dict[str, Any]) -> pd.DataFrame:
    """
    Normaliza/castea columnas categóricas:
    - `map`: diccionario de equivalencias con normalización de texto (case/acentos).
    - `allowed`: dominio permitido (valores fuera de dominio → opcionalmente `coerce_to`).
    """
    if not cat_cfg:
        return df
    df = df.copy()
    cat_report = {}
    for col, rules in cat_cfg.items():
        if col not in df.columns:
            continue
        series = df[col].astype("string")
        norm_series = series.map(_norm_text)
        mapping = rules.get("map", {}) or {}
        norm_map = { _norm_text(k): v for k, v in mapping.items() }
        mapped = norm_series.map(norm_map).fillna(series)
        allowed = set(rules.get("allowed", []))
        out_of_domain = []
        if allowed:
            mask_oob = ~mapped.isna() & ~mapped.isin(list(allowed))
            out_of_domain = sorted(set(mapped[mask_oob].dropna().tolist()))
            coerce_to = rules.get("coerce_to")
            if coerce_to is not None:
                mapped.loc[mask_oob] = coerce_to
        if mapping or rules.get("coerce_to") is not None:
            df[col] = mapped
        cat_report[col] = {
            "mapped_distinct": int(mapped.dropna().nunique()),
            "out_of_domain_values": out_of_domain,
            "out_of_domain_count": int(len(out_of_domain)),
        }
    report["category_checks"] = cat_report
    return df

def _apply_numeric_rules(df: pd.DataFrame, nr_cfg: Dict[str, Any] | None, report: Dict[str, Any]) -> pd.DataFrame:
    """
    Aplica reglas numéricas (bounds):
    - min/max → valores fuera de rango pasan a NA y se contabilizan en el reporte.
    """
    if not nr_cfg:
        return df
    df = df.copy()
    num_report = {}
    for col, rules in nr_cfg.items():
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        before_na = int(s.isna().sum())
        minv = rules.get("min")
        maxv = rules.get("max")
        mask_oob = pd.Series(False, index=s.index)
        if minv is not None:
            mask_oob = mask_oob | (s < minv)
        if maxv is not None:
            mask_oob = mask_oob | (s > maxv)
        oob_count = int(mask_oob.sum())
        s.loc[mask_oob] = pd.NA
        df[col] = s
        num_report[col] = {
            "oob_count": oob_count,
            "na_count_after": int(df[col].isna().sum()),
            "na_count_before": before_na
        }
    report["numeric_checks"] = num_report
    return df


# =========================
# IDs: business_key + surrogate
# =========================
def _build_business_key(df: pd.DataFrame, fields: List[str]) -> pd.Series:
    """Concatena campos (string/‘’) con pipe para formar la semilla de la business_key."""
    vals = []
    for c in fields:
        if c in df.columns:
            vals.append(df[c].astype("string").fillna(""))
        else:
            vals.append(pd.Series([""] * len(df), index=df.index, dtype="string"))
    seed = pd.concat(vals, axis=1).apply(lambda r: "|".join(list(r.values)), axis=1)
    return seed

def _uuid5_series(namespace: str, seed_series: pd.Series) -> pd.Series:
    """Genera UUIDv5 deterministas a partir de `namespace` y semillas por fila."""
    ns = uuid.uuid5(uuid.NAMESPACE_URL, namespace)
    return seed_series.map(lambda s: str(uuid.uuid5(ns, s)))


# =========================
# API principal
# =========================
def run_transform(
    dfs: Dict[str, pd.DataFrame],
    config_path: Union[str, Path],
    run_id: str | None = None,
    logger=None,
    snapshot_dir: Union[str, Path] = "data/artifacts/config_snapshots",
):
    """
    Aplica todas las transformaciones declarativas y de integridad a `dfs`.

    Retorna:
        out_dfs, report
    """
    cfg = load_config(config_path)
    if logger:
        logger.info("Transform config loaded", {"config_path": str(config_path)})

    # Snapshot de config (para trazabilidad/reproducibilidad del run)
    try:
        snap_dir = Path(snapshot_dir)
        snap_dir.mkdir(parents=True, exist_ok=True)
        snap = snap_dir / f"{cfg.get('dataset','dataset')}_{(run_id or 'norun')}.json"
        snap.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        if logger:
            logger.info("Transform config snapshot saved", {"snapshot_path": str(snap)})
    except Exception as e:
        if logger:
            logger.warning("Config snapshot failed", {"error": str(e)})

    # Config principal
    allowed_diffs = int(cfg.get("allowed_schema_diffs", 0))
    tcfg = cfg.get("types", {}) or {}
    rename = cfg.get("rename", {}) or {}
    base_order  = cfg.get("order", []) or []
    critical = cfg.get("critical", {}) or {}
    recommended = cfg.get("recommended", []) or []
    categories_cfg = cfg.get("categories", {}) or {}
    numeric_rules = cfg.get("numeric_rules", {}) or {}

    ids_cfg = cfg.get("id_strategy", {}) or {}
    integ = cfg.get("integrity", {}) or {}
    id_mode = ids_cfg.get("mode", "composite_first")
    single_key = [*ids_cfg.get("single_key", ids_cfg.get("primary_key", []))]
    comp_key = [*ids_cfg.get("composite_key", ids_cfg.get("fallback_composite", []))]
    allow_null_parts = bool(ids_cfg.get("composite_allow_nulls", True))
    surr_cfg = ids_cfg.get("surrogate", {}) or {}

    expected = _expected_columns(cfg)

    out_dfs: Dict[str, pd.DataFrame] = {}
    report: Dict[str, Any] = {"resources": {}, "allowed_schema_diffs": allowed_diffs}

    for rid, df in dfs.items():
        if logger:
            logger.info("[Transform] start", {"rid": rid, "rows": int(len(df)), "cols": int(len(df.columns))})

        # 1) normalizar nombres, arreglar header y renombrar
        df = _norm_cols(df)
        df = _fix_header(df)
        df = _apply_rename(df, rename)

        # 2) normalización base
        df = _apply_normalize(df, cfg.get("normalize"))

        # 3) tipificación
        df = _coerce_types(df, tcfg)

        # 3.1) derivar 'ano' desde 'fecha_detencion_aprehension' si se habilita en YAML
        if (cfg.get("derive", {}) or {}).get("ano_from_fecha", False):
            if "fecha_detencion_aprehension" in df.columns:
                # si 'ano' existe pero está vacío → derivar
                if "ano" in df.columns and df["ano"].isna().all():
                    if pd.api.types.is_datetime64_any_dtype(df["fecha_detencion_aprehension"]):
                        df["ano"] = df["fecha_detencion_aprehension"].dt.year
                # si 'ano' no existe → crearlo derivado
                elif "ano" not in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df["fecha_detencion_aprehension"]):
                        df["ano"] = df["fecha_detencion_aprehension"].dt.year

        # 4) reglas opcionales
        per_res_report: Dict[str, Any] = {}
        df = _apply_category_rules(df, categories_cfg, per_res_report)
        df = _apply_numeric_rules(df, numeric_rules, per_res_report)

        # 5) IDs (business y surrogate)
        if id_mode == "composite_first" and comp_key:
            seed = _build_business_key(df, comp_key)
        elif single_key:
            seed = _build_business_key(df, single_key)
        elif comp_key:
            seed = _build_business_key(df, comp_key)
        else:
            seed = pd.Series([""] * len(df), index=df.index, dtype="string")

        if (not allow_null_parts) and (id_mode == "composite_first") and comp_key:
            null_part_mask = pd.Series(False, index=df.index)
            for c in comp_key:
                if c in df.columns:
                    null_part_mask |= df[c].isna() | (df[c].astype("string").fillna("") == "")
                else:
                    null_part_mask |= True
            per_res_report["business_key_null_components"] = int(null_part_mask.sum())
            if int(null_part_mask.sum()) > 0:
                per_res_report["business_key_quarantined_due_to_nulls"] = True

        df["business_key"] = seed

        if surr_cfg.get("enabled", False):
            method = (surr_cfg.get("method") or "uuid5").lower()
            ns = surr_cfg.get("namespace", "https://tu-org.ec/mdi/detenidos_aprehendidos")
            fields = surr_cfg.get("fields")
            s_seed = _build_business_key(df, fields) if fields else seed
            if method == "uuid5":
                df["surrogate_id"] = _uuid5_series(ns, s_seed)
            elif method == "sha256":
                df["surrogate_id"] = s_seed.map(lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest())
            else:
                df["surrogate_id"] = _uuid5_series(ns, s_seed)

        # 6) deduplicación por business_key (opcional)
        dup_keys = int((df["business_key"].value_counts() > 1).sum())
        per_res_report["duplicates_business_key"] = dup_keys
        dedup_rows = 0
        if integ.get("enforce_unique_business_key", False) and dup_keys > 0:
            how = integ.get("on_duplicate", "keep_first")
            before = len(df)
            if how == "keep_latest_by_fecha" and "fecha_detencion_aprehension" in df.columns:
                df = df.sort_values("fecha_detencion_aprehension").drop_duplicates(subset=["business_key"], keep="last")
            else:
                df = df.drop_duplicates(subset=["business_key"], keep="first")
            dedup_rows = before - len(df)
        per_res_report["dedup_rows_dropped"] = int(dedup_rows)

        # 7) validación de esquema (ignorar columnas auto-generadas)
        auto_cols = {"surrogate_id", "business_key"}
        cols = set(c for c in df.columns if c not in auto_cols)
        missing, extra = _schema_diffs(cols, expected)

        # padding opcional de columnas faltantes
        if (cfg.get("schema", {}) or {}).get("pad_missing", False):
            for c in missing:
                df[c] = pd.NA
            cols = set(c for c in df.columns if c not in auto_cols)
            missing, extra = _schema_diffs(cols, expected)

        diffs = len(missing) + len(extra)

        if logger and diffs:
            logger.warning("[Transform] schema diff samples", {
                "rid": rid,
                "missing_sample": list(missing)[:10],
                "extra_sample": list(extra)[:10],
            })

        # 8) críticas / recomendadas
        crit_issues = _check_critical(df, critical)
        rec_warns   = _warn_recommended(df, recommended)

        # 9) Fail-fast según YAML (severa con faltantes, tolerante con extras por defecto)
        promote_policy = cfg.get("promote_policy", {}) or {}
        on_fail = promote_policy.get("on_critical_fail", "error").lower()

        fail_on_missing = bool(promote_policy.get("fail_on_missing_expected", True))
        fail_on_extra = bool(promote_policy.get("fail_on_extra_columns", False))
        fail_on_drift = bool(promote_policy.get("fail_on_schema_drift", False))  # apagado por defecto

        has_crit_fail = bool(
            crit_issues["missing_all"] or
            crit_issues["missing_any_of"] or
            per_res_report.get("business_key_quarantined_due_to_nulls")
        )

        if on_fail == "error" and has_crit_fail:
            raise TransformHardFail(
                f"[{rid}] Critical failure: missing_all={crit_issues['missing_all']}, "
                f"missing_any_of_groups={len(crit_issues['missing_any_of'])}"
            )

        missing_count = len(missing)
        extra_count = len(extra)

        if fail_on_missing and missing_count > 0:
            raise TransformHardFail(
                f"[{rid}] Missing expected cols: {list(missing)[:10]}"
            )

        if fail_on_extra and extra_count > 0:
            raise TransformHardFail(
                f"[{rid}] Extra cols present: {list(extra)[:10]}"
            )

        if fail_on_drift and (missing_count + extra_count) > allowed_diffs:
            raise TransformHardFail(
                f"[{rid}] Schema drift {(missing_count + extra_count)} > allowed {allowed_diffs}; "
                f"missing={list(missing)[:10]} extra={list(extra)[:10]}"
            )

        # 10) orden final (IDs al frente + orden base)
        final_order = []
        for id_col in ("surrogate_id", "business_key"):
            if id_col in df.columns:
                final_order.append(id_col)
        final_order += [c for c in base_order if c not in final_order]
        df = _reorder(df, final_order)

        # 11) reporte por recurso
        rep = {
            "rows_in": int(len(dfs[rid])),
            "rows_out": int(len(df)),
            "cols": list(df.columns),
            "schema_missing": missing,
            "schema_extra": extra,
            "schema_diffs": diffs,
            "critical_issues": crit_issues,
            "recommended_warnings": rec_warns,
            "id_strategy": {
                "mode": id_mode,
                "single_key": single_key,
                "composite_key": comp_key,
                "composite_allow_nulls": allow_null_parts,
                "surrogate_enabled": bool(surr_cfg.get("enabled", False)),
                "surrogate_method": (surr_cfg.get("method") or "uuid5"),
            },
            "duplicates_business_key": int(dup_keys),
            "dedup_rows_dropped": int(dedup_rows),
        }
        report["resources"][rid] = rep

        out_dfs[rid] = df
        if logger:
            logger.info("[Transform] ok", {"rid": rid, "rows_out": int(len(df))})

    return out_dfs, report
