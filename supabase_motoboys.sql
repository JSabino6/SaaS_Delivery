-- Crie a tabela de motoboys no Supabase (SQL Editor)
-- Ela é usada pelo Dashboard na aba "Motoboys".

create table if not exists public.motoboys (
  id bigint generated always as identity primary key,
  restaurante_id bigint not null references public.restaurantes(id),
  nome text not null,
  telefone text not null,
  placa text,
  chave_pix text,
  cpf text,
  modelo text,
  senha text,
  ativo boolean default true,
  created_at timestamptz default timezone('utc'::text, now())
);

create index if not exists motoboys_restaurante_id_idx on public.motoboys(restaurante_id);
