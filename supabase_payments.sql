
alter table public.restaurantes
  add column if not exists pix_whatsapp_enabled boolean not null default false,
  add column if not exists pix_provider text not null default 'mercadopago',
  add column if not exists mp_access_token_enc text;

update public.restaurantes
set pix_whatsapp_enabled = coalesce(pix_whatsapp_enabled, false) or coalesce(pix_enabled, false)
where true;


alter table public.pedidos
  add column if not exists payment_provider text,
  add column if not exists payment_id text,
  add column if not exists payment_status text,
  add column if not exists payment_amount numeric,
  add column if not exists payment_qr_code text,
  add column if not exists payment_ticket_url text,
  add column if not exists payment_created_at timestamptz,
  add column if not exists payment_paid_at timestamptz;

-- Se seu banco antigo tinha `valor_pago`, pode mapear para `payment_amount` (opcional).
update public.pedidos
set payment_amount = coalesce(payment_amount, valor_pago)
where payment_amount is null and valor_pago is not null;

-- Se `payment_status` veio com default 'pending', limpe para NULL em pedidos sem payment_id.
update public.pedidos
set payment_status = null
where payment_id is null and payment_status is not null;

-- 3) Índices úteis
create index if not exists idx_pedidos_payment_id on public.pedidos(payment_id);
create index if not exists idx_pedidos_payment_status on public.pedidos(payment_status);

-- Observações:
-- - mp_access_token_enc deve guardar o token criptografado (Fernet) com a mesma chave no API e no Dashboard.
-- - Para multi-tenant de verdade, ative RLS/policies (ver SECURITY_NEXT_STEPS.md).
