-- Entregas / motoboys: histórico de encaminhamento e entregas
-- Rode no Supabase (SQL Editor) após criar a tabela public.motoboys.

-- 1) Motoboy precisa de PIN/senha para ter uma tela própria
alter table public.motoboys
  add column if not exists senha text;

alter table public.motoboys
  add column if not exists placa text,
  add column if not exists chave_pix text,
  add column if not exists cpf text,
  add column if not exists modelo text;

-- 2) Tabela de entregas (uma linha por encaminhamento do pedido)
create table if not exists public.entregas (
  id bigint generated always as identity primary key,
  restaurante_id bigint not null references public.restaurantes(id) on delete cascade,
  pedido_id bigint not null references public.pedidos(id) on delete cascade,
  motoboy_id bigint not null references public.motoboys(id) on delete cascade,
  motoboy_nome text not null,
  motoboy_telefone text not null,
  endereco text,
  maps_url text,
  waze_url text,
  status text not null default 'encaminhado', -- encaminhado | entregue
  encaminhado_em timestamptz not null default timezone('utc'::text, now()),
  entregue_em timestamptz
);

create index if not exists entregas_restaurante_id_idx on public.entregas(restaurante_id);
create index if not exists entregas_motoboy_id_idx on public.entregas(motoboy_id);
create index if not exists entregas_pedido_id_idx on public.entregas(pedido_id);
create index if not exists entregas_status_idx on public.entregas(status);
