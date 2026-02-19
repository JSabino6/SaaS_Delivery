create table if not exists public.metricas_gastos_restaurante (
  id bigserial primary key,
  restaurante_id bigint not null references public.restaurantes(id),
  periodo date not null default current_date,
  pedidos_total bigint not null default 0,
  ia_calls bigint not null default 0,
  ia_prompt_tokens bigint not null default 0,
  ia_completion_tokens bigint not null default 0,
  ia_audio_calls bigint not null default 0,
  redis_ops bigint not null default 0,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint ux_metricas_gastos_rest_per unique (restaurante_id, periodo)
);

create index if not exists ix_metricas_gastos_periodo on public.metricas_gastos_restaurante (periodo);
create index if not exists ix_metricas_gastos_restaurante on public.metricas_gastos_restaurante (restaurante_id);
