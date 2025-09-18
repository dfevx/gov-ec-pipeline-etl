# 🚀 ETL – Observatorio Ciudadano Estado Abierto (Ecuador)

Este proyecto implementa un **pipeline ETL** para procesar datos de la API CKAN del Gobierno de Ecuador disponible desde https://www.datosabiertos.gob.ec/.  
Este proyecto extrae los datos de detenciones/aprehensiones, los normaliza y audita, y los sube a una base en **Supabase**, junto con metadatos que permiten seguimiento de *drift* en esquemas y auditoría de corridas. Si bien aquí nos enfocamos en un solo dataset de los disponibles en https://www.datosabiertos.gob.ec/ este proyecto esta diseñado para que con ajustes de esquemas y variables, se pueda aprovechar otros recursos de la misma plataforma. 

---

## 🎯 Propósito

Parte del proyecto **Infraestructura de datos semántica para la co-decisión ciudadana.**  
Busca transformar datos públicos en recursos reutilizables, trazables y auditables que fortalezcan la participación ciudadana en el marco del Observatorio Ciudadano para Estado Abierto.

---

## 📂 Estructura del proyecto

```
.
├── configs/                  # Configuración declarativa (YAML)
│   └── detenidos_aprehendidos.yaml
├── data/                     # Artefactos locales (raw, staging, reports, state)
├── etl/                      # Paquete Python con etapas ETL
│   ├── config.py
│   ├── extract.py
│   ├── get_status.py
│   ├── load.py
│   ├── log.py
│   ├── transform.py
│   ├── upload_log.py
│   ├── yaml_config_loader.py
│   └── __init__.py
├── sql/                      # Definiciones SQL de la BD destino
│   ├── 001_extensions.sql
│   ├── 002_fn_set_updated_at.sql
│   ├── 010_table_detenidos_aprehendidos.sql
│   └── 020_etl_audit.sql
├── pipeline.py               # Orquestador del flujo ETL
├── requirements.txt          # Dependencias Python
├── .env.example              # Variables de entorno (plantilla)
├── Procfile                  # Configuración de Railway Worker
└── .gitignore
```

---

## ⚙️ Configuración

### 1) Variables de entorno

Crea un archivo `.env` en la raíz (nunca lo subas a GitHub). Toma como guía `.env.example`:

```dotenv
SUPABASE_URL=...
SUPABASE_KEY=...
SUPABASE_SB_BUCKET=etl-artifacts

ENV_SLUG=dev
DATASET_SLUG=personas-detenidas
STATE_SB_OBJECT=dev/personas-detenidas/state.json

API_PKG_SHOW=https://www.datosabiertos.gob.ec/api/3/action/package_show
PACKAGE_ID=<uuid>
PREFIX=MDI_DETENIDOSAPREHENDIDOS_PM
SHEET_BLACKLIST=Contenido

STATE_DIR=state
STATE_PATH=./state_<uuid>.json
LOG_FILE=etl.log

ARTIFACTS_MODE=manifest_on_oversize
ARTIFACTS_MAX_MB=50
UPLOAD_RAW=true
UPLOAD_SNAPSHOTS=true
UPLOAD_REPORTS=true
UPLOAD_LOG=true
```

---

### 2) Instalar dependencias

```bash
# Crear y activar entorno virtual
python -m venv .venv
# macOS / Linux
source .venv/bin/activate
# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# Instalar
pip install -r requirements.txt
```

---

### 3) Supabase Bucket Setup

El pipeline espera un bucket en Supabase con la siguiente convención:

- **Nombre del bucket**: `etl-artifacts`  
- **Directorio base**: `{ENV_SLUG}/{DATASET_SLUG}`  
  - Ejemplo (desarrollo): `dev/personas-detenidas`

Dentro de ese directorio:

- `state_<uuid>.json` → archivo de estado global (se crea en la primera corrida).  
- `runs/{run_id}/...` → artefactos por corrida:
  - `raw/` → archivos originales descargados.  
  - `reports/` → reportes de transformación (`transform_report_<run_id>.json`).  
  - `logs/` → logs de ejecución.  
  - `snapshots/` → snapshots de datasets intermedios.  

⚠️ **Importante**: ajusta `ENV_SLUG` y `DATASET_SLUG` en tu `.env` para separar ambientes (ej. `dev`, `prod`) y datasets.


---

### 4) Crear tablas en Supabase

Ejecuta los scripts SQL en orden (usa tu `DATABASE_URL` o la consola de SQL de Supabase):

```bash
psql $DATABASE_URL -f sql/001_extensions.sql
psql $DATABASE_URL -f sql/002_fn_set_updated_at.sql
psql $DATABASE_URL -f sql/010_table_detenidos_aprehendidos.sql
psql $DATABASE_URL -f sql/020_etl_audit.sql
```

---

## ▶️ Ejecución

### Local

```bash
python pipeline.py
```

El pipeline:

- Descarga **datos nuevos** desde CKAN (según `state.json` en Storage; si no existe, hace descarga completa).
- **Transforma** y valida contra el esquema definido en YAML.
- Realiza **upsert** en Supabase (`detenidos_aprehendidos`).
- Registra **auditoría** en `etl_runs` y `etl_run_resources`.
- Sube **artefactos** a Storage (raw, reports, logs, snapshots).
- Emite **logs** en JSON-Lines a consola y archivo `etl.log`.

### Railway (Worker + Schedule)

1. Crea un proyecto en **Railway** y conecta el repo.  
2. Define las **Variables de Entorno** (usa `.env.example` como guía).  
3. Asegúrate de tener un **Procfile** en la raíz con:

   ```Procfile
   worker: python pipeline.py
   ```

   Esto permite ejecutar el pipeline como un worker en Railway.  

4. Configura un **Schedule** en Railway para correr automáticamente:  
   - Ve a **Settings → Schedules → New schedule**  
   - **Command**:  
     ```
     python pipeline.py
     ```
   - **Cron expression (UTC)**: Ecuador = UTC-5, domingo 3 AM → `0 8 * * 0`  
   - **Timezone**: UTC  
   - Guarda. Railway ejecutará el pipeline cada domingo a las 03:00 hora Ecuador.  

📌 Snippet de referencia (Railway Schedule):

```yaml
schedules:
  - name: weekly-etl
    command: "python pipeline.py"
    cron: "0 8 * * 0"
    timezone: "UTC"
```

---

## 📊 Auditoría

El pipeline registra cada corrida en Supabase:

- **etl_runs**: resumen por corrida (`run_id`, filas in/out, estado, errores).  
- **etl_run_resources**: detalle por recurso (`rid`, filas procesadas, duplicados, *drift*).  

Esto permite **reproducir**, **depurar** y **explicar** cada paso del flujo.

---

## 🧩 Referencias

- UN-Habitat (2021). *Managing Smart City Governance*.  
- Bailur & Gigler (2014). *Closing the Feedback Loop*.  
- Peixoto & Fox (2016). *When Does ICT-Enabled Citizen Voice Lead to Government Responsiveness?*  
- Pandey & Schoof (2023). *Building ETL Pipelines with Python*.  
- Pérez Paredes & Orbea (2024). *De la participación a la co-decisión*.  
- Grohmann (2025). *Latin American Critical Data Studies*.  
- Juárez-Merino (2025). *AI and Citizenship in Latin American Governments*.  
- Hoefsloot et al. (2022). *Data justice framework for participatory governance*.  

---

## 📄 Licencia

Este proyecto está licenciado bajo los términos de la [MIT License](LICENSE).
