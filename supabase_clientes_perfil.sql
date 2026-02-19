create table if not exists public.clientes_perfil (
  id bigserial primary key,
  restaurante_id bigint not null,
  cliente_zap text not null,
  tipo_entrega_favorita text,
  endereco_favorito text,
  bairro_favorito text,
  forma_pagamento_favorita text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint clientes_perfil_tipo_entrega_chk check (tipo_entrega_favorita in ('entrega','retirada') or tipo_entrega_favorita is null),
  constraint clientes_perfil_forma_pgto_chk check (forma_pagamento_favorita in ('pix','dinheiro','cartao') or forma_pagamento_favorita is null)
);

create unique index if not exists ux_clientes_perfil_rest_cliente
  on public.clientes_perfil (restaurante_id, cliente_zap);

create index if not exists ix_clientes_perfil_restaurante
  on public.clientes_perfil (restaurante_id);

create index if not exists ix_clientes_perfil_cliente
  on public.clientes_perfil (cliente_zap);
