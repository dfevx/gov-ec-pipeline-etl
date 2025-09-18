# etl/get_status.py
"""
Módulo para gestionar y descargar objetos de estado desde Supabase Storage.

Funciones principales:
- Descarga objetos (p. ej. state.json) desde Supabase con reintentos.
- Expone una CLI simple para probar descargas (subcomandos state, object, compose).
- Garantiza manejo seguro de errores (404 vs errores duros).

Usos:
- Permite inicializar o sincronizar el state.json en local antes de ejecutar el pipeline.
- Se integra en pipeline.py a través de `ensure_state_locally`.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from supabase import create_client
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError

# Import opcional de excepción específica del SDK
try:
    from storage3.exceptions import StorageApiError
except Exception:
    class StorageApiError(Exception):
        """Fallback si storage3.exceptions no está disponible."""
        pass

# -----------------------------------------------------------------------------
# Descarga con reintentos
# -----------------------------------------------------------------------------
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.6, min=0.5, max=4),
    reraise=True
)
def download_object(client, bucket: str, key: str, out_path: Path) -> Path:
    """
    Descarga un objeto de Supabase Storage y lo guarda en `out_path` de forma atómica.

    Args:
        client: Cliente Supabase inicializado.
        bucket: Nombre del bucket de Supabase Storage.
        key: Key completa dentro del bucket.
        out_path: Ruta local donde guardar el archivo.

    Returns:
        Path al archivo descargado.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    data = client.storage.from_(bucket).download(key)  # bytes
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(out_path)  # escritura atómica

    return out_path

# -----------------------------------------------------------------------------
# CLI: argparse
# -----------------------------------------------------------------------------
def parse_args(argv: list[str]) -> argparse.Namespace:
    """
    Construye CLI con subcomandos:

    - state  : descarga el objeto STATE_SB_OBJECT definido en el entorno.
    - object : descarga un objeto por key completa.
    - compose: construye key con patrón ENV_SLUG/DATASET_SLUG/PREFIX/FILENAME.

    Args:
        argv: Lista de argumentos (normalmente sys.argv[1:]).

    Returns:
        argparse.Namespace con parámetros parseados.
    """
    p = argparse.ArgumentParser(description="Descarga un archivo desde Supabase Storage.")
    sub = p.add_subparsers(dest="cmd", required=True)

    # etl-download state [--out ./state/state.json] [--bucket ...] [--env-file ...]
    p_state = sub.add_parser("state", help="Descargar el STATE_SB_OBJECT del .env")
    p_state.add_argument("--out", type=Path, help="Ruta local destino (por defecto ./state/<basename>)")
    p_state.add_argument("--bucket", type=str, help="Bucket alterno (por defecto SUPABASE_SB_BUCKET)")
    p_state.add_argument("--env-file", type=str, help="Ruta a .env alternativa")

    # etl-download object --key <FULL_KEY> --out <file>
    p_obj = sub.add_parser("object", help="Descargar por key completa")
    p_obj.add_argument("--key", type=str, required=True, help="Key completa (ej. dev/personas-detenidas/state.json)")
    p_obj.add_argument("--out", type=Path, required=True, help="Ruta local de salida")
    p_obj.add_argument("--bucket", type=str, help="Bucket alterno (por defecto SUPABASE_SB_BUCKET)")
    p_obj.add_argument("--env-file", type=str)

    # etl-download compose --prefix runs/latest --filename metrics.json --out <file>
    p_comp = sub.add_parser("compose", help="Construir key como ENV_SLUG/DATASET_SLUG/PREFIX/FILENAME")
    p_comp.add_argument("--prefix", type=str, required=True)
    p_comp.add_argument("--filename", type=str, required=True)
    p_comp.add_argument("--out", type=Path, required=True)
    p_comp.add_argument("--bucket", type=str, help="Bucket alterno (por defecto SUPABASE_SB_BUCKET)")
    p_comp.add_argument("--env-file", type=str)

    return p.parse_args(argv)

# -----------------------------------------------------------------------------
# API: asegurar state.json en local
# -----------------------------------------------------------------------------
def ensure_state_locally(client, bucket: str, key: str, out_path: Path, logger) -> bool:
    """
    Intenta descargar state.json desde Supabase y manejar escenarios típicos.

    Args:
        client: Cliente Supabase.
        bucket: Bucket en Supabase Storage.
        key: Key del objeto state.json.
        out_path: Ruta local de destino.
        logger: Logger para registrar advertencias/errores.

    Returns:
        bool:
            - False si el archivo se descargó correctamente.
            - True si no existe en Storage (404/not found), lo que implica correr en modo "sin estado".
    """
    out_path = Path(out_path)
    try:
        downloaded_path = download_object(client, bucket, key, out_path)
        if not downloaded_path:
            logger.warning("state.json no encontrado (retorno vacío). Activando modo 'sin estado'.")
            return True
        logger.info(f"State descargado correctamente: {downloaded_path}")
        return False

    except StorageApiError as e:
        msg = str(e).lower()
        not_found = any(s in msg for s in ("404", "object not found", "not_found", "no such key", "object does not exist"))
        if not_found:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            logger.warning("state.json no encontrado (404/Not Found). Modo 'sin estado' activado.")
            return True
        logger.error(f"No se pudo descargar state.json por otro error: {e}")
        raise

    except RetryError as re:
        # Manejo especial de RetryError de tenacity
        inner = re.last_attempt.exception()
        msg = str(inner).lower()
        not_found = any(s in msg for s in ("404", "object not found", "not_found", "no such key", "object does not exist"))
        if not_found:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            logger.warning("state.json no encontrado (404/Not Found). Modo 'sin estado' activado.")
            return True
        logger.error(f"No se pudo descargar state.json por otro error: {inner}")
        raise inner

    except Exception as e:
        msg = str(e).lower()
        not_found = any(s in msg for s in ("404", "object not found", "not_found", "no such key", "object does not exist"))
        if not_found:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            logger.warning("state.json no encontrado (404/Not Found). Modo 'sin estado' activado.")
            return True
        logger.error(f"No se pudo descargar state.json por otro error: {e}")
        raise