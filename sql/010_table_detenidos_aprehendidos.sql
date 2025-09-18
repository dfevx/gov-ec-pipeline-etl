create table if not exists public.detenidos_aprehendidos (
  surrogate_id text primary key,
  business_key text unique,

  -- claves/campos principales
  codigo_iccs text,
  fecha_detencion_aprehension timestamp without time zone,
  hora_detencion_aprehension text,

  -- atributos
  tipo text,
  presunta_infraccion text,
  estado_civil text,
  estatus_migratorio text,
  edad numeric,
  sexo text,
  genero text,
  nacionalidad text,
  autoidentificacion_etnica text,
  nivel_de_instruccion text,
  condicion text,
  movilizacion text,
  tipo_arma text,
  arma text,
  lugar text,
  tipo_lugar text,
  nombre_zona text,
  nombre_subzona text,
  codigo_distrito text,
  codigo_circuito text,
  codigo_subcircuito text,
  codigo_provincia text,
  codigo_canton text,
  codigo_parroquia text,
  nombre_distrito text,
  nombre_circuito text,
  nombre_subcircuito text,
  nombre_provincia text,
  nombre_canton text,
  nombre_parroquia text,
  latitud numeric,
  longitud numeric,
  grupo_edad text,
  ano numeric,

  -- extras y auditoría
  extras jsonb,
  load_run_id text,                     -- último run que tocó la fila
  inserted_at timestamptz default now(),
  updated_at timestamptz default now()
);

-- Trigger de updated_at
drop trigger if exists trg_da_updated_at on public.detenidos_aprehendidos;
create trigger trg_da_updated_at
before update on public.detenidos_aprehendidos
for each row execute function public.set_updated_at();

-- Índices recomendados (consultas típicas)
create index if not exists idx_da_fecha
  on public.detenidos_aprehendidos (fecha_detencion_aprehension);

create index if not exists idx_da_prov_fecha
  on public.detenidos_aprehendidos (codigo_provincia, fecha_detencion_aprehension);

create index if not exists idx_da_canton_fecha
  on public.detenidos_aprehendidos (codigo_canton, fecha_detencion_aprehension);

create index if not exists idx_da_infraccion_fecha
  on public.detenidos_aprehendidos (presunta_infraccion, fecha_detencion_aprehension);
