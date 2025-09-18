# pipeline.py
"""
Orquestador del pipeline ETL (Extract → Transform → Load).

Responsabilidades:
- Inicializar settings y cliente de Supabase.
- Asegurar el `state.json` local desde Storage (si existe).
- Ejecutar EXTRACT contra CKAN (incremental usando state).
- Ejecutar TRANSFORM según la configuración YAML.
- Persistir reporte de transform localmente (artefacto).
- Ejecutar LOAD (upsert a BD, subir artefactos, auditoría).
- Registrar logs en formato JSON-Lines (stdout y archivo).

Notas de ejecución:
- Requiere variables de entorno definidas (ver etl/config.py).
- El logger escribe a STDOUT (containers) y opcionalmente a `etl.log`.
- Compatible con ejecución local, GitHub Actions y Railway.
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
import json

from supabase import create_client

from etl.config import load_settings, load_ckan_settings
from etl.get_status import ensure_state_locally
from etl.extract import extract_updated_dfs
from etl.transform import run_transform
from etl.log import get_logger

# Logger central del pipeline:
# - JSON-Lines a STDOUT (ideal para contenedores/CI)
# - Archivo rotado opcional (útil en desarrollo o para subir como artefacto)
logger = get_logger("etl.pipeline", logfile="etl.log")


def etl_process() -> None:
    """
    Ejecuta el flujo completo del pipeline ETL.

    Etapas:
      0) SETTINGS & CLIENTS  → carga configuración y crea cliente Supabase.
      1) STATE               → asegura state.json local (descarga si existe en Storage).
      2) EXTRACT             → descarga recursos nuevos/modificados desde CKAN.
      3) TRANSFORM           → normaliza/valida dataframes según YAML y guarda reporte.
      4) LOAD                → upsert a BD, sube artefactos y registra auditoría.

    Lanza:
        Cualquier excepción no controlada. Se loguea y se propaga (fail-fast).
    """
    try:
        # ------------------------------------------------------------------------------------
        # 0) SETTINGS & CLIENTS
        # ------------------------------------------------------------------------------------
        st = load_settings()            # credenciales + flags de artefactos (Storage)
        ck = load_ckan_settings()       # parámetros CKAN (package_show/prefix/state)
        client = create_client(st.supabase_url, st.supabase_key)
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")  # trazabilidad por corrida

        # ------------------------------------------------------------------------------------
        # 1) STATE
        # ------------------------------------------------------------------------------------
        # Si el objeto `state` existe en Storage, lo descarga a la ruta local esperada;
        # si no existe (404), el pipeline correrá en modo “sin estado” (full download).
        state_missing = ensure_state_locally(
            client=client,
            bucket=st.sb_bucket,
            key=st.state_key,
            out_path=ck.state_path,
            logger=logger,
        )
        if state_missing:
            logger.info(
                "No se encontró state en Storage. Se forzará descarga completa en EXTRACT."
            )

        # ------------------------------------------------------------------------------------
        # 2) EXTRACT
        # ------------------------------------------------------------------------------------
        # Descarga solo recursos nuevos/modificados (según state.json) y produce
        # DataFrames en memoria; además guarda CSVs en data/staging para trazabilidad.
        dfs = extract_updated_dfs(
            package_id=ck.package_id,
            prefix=ck.prefix,
            state_path=ck.state_path,
            raw_dir=Path("data/raw"),
            staging_dir=Path("data/staging"),
            progress=False,
            read_timeout=180,
            seed_if_missing=False,  # full download si no hay estado
        )

        # ------------------------------------------------------------------------------------
        # 3) TRANSFORM
        # ------------------------------------------------------------------------------------
        # Aplica reglas declarativas desde el YAML (tipos, renombres, integridad, IDs, etc.)
        cfg_path = Path("configs") / "detenidos_aprehendidos.yaml"

        tdfs, treport = run_transform(
            dfs=dfs,
            config_path=cfg_path,
            run_id=run_id,
            logger=logger,
        )

        # Persistimos reporte de transform como artefacto local (se sube en LOAD)
        rep_dir = Path("data/artifacts/reports")
        rep_dir.mkdir(parents=True, exist_ok=True)
        (rep_dir / f"transform_report_{run_id}.json").write_text(
            json.dumps(treport, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("TRANSFORM completed", {"resources": list(tdfs.keys())})

        # ------------------------------------------------------------------------------------
        # 4) LOAD
        # ------------------------------------------------------------------------------------
        # Carga idempotente por lotes, auditoría (etl_runs / etl_run_resources)
        # y subida de artefactos (raw/reports/snapshots/logs) con política de oversize.
        from etl.load import load_to_supabase

        load_to_supabase(
            tdfs=tdfs,                     # puede venir vacío {}
            config_path=cfg_path,
            client=client,
            run_id=run_id,
            logger=logger,
            settings=st,
            promote_state_if_no_resources=False,  # ver detalle en etl/load.py
        )

        logger.info("ETL process completed successfully")

    except Exception:
        # Log estructurado con stack trace (exc_info=True lo incluye en el JSON)
        logger.error("ETL process failed", exc_info=True)
        raise


if __name__ == "__main__":
    etl_process()
