alter table public.restaurantes
  add column if not exists endereco_loja text,
  add column if not exists telefone_loja text,
  add column if not exists horario_loja text;

-- Opcional: valores padrão vazios para evitar null em consultas antigas
update public.restaurantes
set endereco_loja = coalesce(endereco_loja, ''),
    telefone_loja = coalesce(telefone_loja, ''),
    horario_loja = coalesce(horario_loja, '')
where true;
