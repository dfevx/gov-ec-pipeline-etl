# etl/yaml_config_loader.py
"""
Cargador de configuración (YAML/JSON) para el pipeline.

Objetivo
--------
Unificar la carga de archivos de configuración del dataset en formato
YAML (.yaml/.yml) o JSON (.json), devolviendo siempre un `dict`.
Incluye validaciones mínimas y mensajes de error claros para facilitar
el debugging en CI/CD (GitHub Actions, Railway).

Comportamiento
--------------
- Si la extensión es .yaml/.yml → se usa PyYAML (yaml.safe_load).
- Si la extensión es .json      → se usa `json.loads`.
- Si la extensión es otra       → intenta YAML y luego JSON.
- Exige que el tope sea un mapeo/objeto (dict). Si no, lanza ValueError.
- Si falta PyYAML cuando es necesario, lanza RuntimeError con hint de instalación.

Uso
---
    from .yaml_config_loader import load_config
    cfg = load_config("configs/detenidos_aprehendidos.yaml")

    # ejemplo de acceso:
    table = (cfg.get("load", {}) or {}).get("table", "detenidos_aprehendidos")
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Union
import json


def load_config(path: Union[str, Path]) -> Dict[str, Any]:
    """
    Carga un archivo de configuración YAML o JSON y lo devuelve como dict.

    Args:
        path: Ruta al archivo de configuración (.yaml/.yml/.json o sin extensión conocida).

    Returns:
        Dict[str, Any]: configuración ya parseada.

    Raises:
        ValueError: si el archivo está vacío, el tope no es un dict, o el formato no es soportado.
        RuntimeError: si se requiere PyYAML y no está instalado.
    """
    p = Path(path)
    text = p.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"Config file is empty: {p}")

    ext = p.suffix.lower()

    # --- YAML explícito (.yaml/.yml) ---
    if ext in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except Exception as e:
            raise RuntimeError(
                f"PyYAML is required to read '{p.name}'. Install with: pip install PyYAML"
            ) from e
        cfg = yaml.safe_load(text)
        if not isinstance(cfg, dict):
            raise ValueError(f"YAML config must be a mapping at top-level: {p}")
        return cfg

    # --- JSON explícito (.json) ---
    if ext == ".json":
        cfg = json.loads(text)
        if not isinstance(cfg, dict):
            raise ValueError(f"JSON config must be an object at top-level: {p}")
        return cfg

    # --- Extensión desconocida: intentar YAML primero, luego JSON ---
    try:
        import yaml  # type: ignore
        cfg = yaml.safe_load(text)
        if isinstance(cfg, dict):
            return cfg
    except Exception:
        # Ignorar y probar JSON después
        pass

    try:
        cfg = json.loads(text)
        if isinstance(cfg, dict):
            return cfg
    except Exception:
        pass

    # Si nada funcionó, detallar el error:
    raise ValueError(
        f"Unsupported or invalid config format for {p}; "
        f"use .yaml/.yml or .json with a mapping/object at the top level."
    )
