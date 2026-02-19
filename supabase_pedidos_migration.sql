-- Migração mínima para suportar:
-- - Pedido visível no Dashboard desde a 1ª mensagem (status=novo)
-- - Trava de aceite até o bot finalizar (bot_finalizado)
-- - Badge de abandono (last_cliente_msg_at)
-- - Notificação de atendimento humano (needs_human)
--
-- Rode no Supabase SQL Editor.

alter table public.pedidos
  add column if not exists bot_finalizado boolean not null default false,
  add column if not exists bot_finalizado_em timestamptz,
  add column if not exists last_cliente_msg_at timestamptz,
  add column if not exists needs_human boolean not null default false,
  add column if not exists needs_human_reason text,
  add column if not exists needs_human_at timestamptz,
  add column if not exists needs_human_resolved_at timestamptz;

create index if not exists idx_pedidos_bot_finalizado on public.pedidos(bot_finalizado);
create index if not exists idx_pedidos_last_cliente_msg_at on public.pedidos(last_cliente_msg_at);
create index if not exists idx_pedidos_needs_human on public.pedidos(needs_human);
