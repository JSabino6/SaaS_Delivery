# Próximos passos de segurança (SaaS WhatsApp)

Este arquivo lista melhorias recomendadas que **não** dá para “consertar só no código” sem mexer em Supabase/infra, mas que são importantes para um SaaS multi-tenant.

## 1) Multi-tenant no banco (RLS)

**Objetivo:** garantir que um restaurante (tenant) só leia/escreva seus próprios dados.

### Recomendações
- Adicionar uma coluna `tenant_id` (ou usar `restaurante_id` como tenant) em **todas** as tabelas que guardam dados do tenant (`pedidos`, `conversas`, `clientes_estado`, `produtos`, `bairros`, etc.).
- Ativar Row Level Security (RLS) e criar policies para `select/insert/update/delete`.
- Evitar usar `SUPABASE_KEY` de alto privilégio no Dashboard. Preferir autenticar usuários e usar a key pública + RLS.

### Exemplo (esqueleto) de policies
> Ajuste nomes de colunas conforme seu schema. A ideia é mostrar o padrão.

```sql
-- Exemplo: pedidos( id, restaurante_id, ... )
alter table public.pedidos enable row level security;

-- Para usuários autenticados, permitir apenas o tenant do usuário
-- Supondo que você guarda restaurante_id em app_metadata/claims.
create policy "pedidos_select_tenant" on public.pedidos
for select
to authenticated
using (
  restaurante_id = (auth.jwt() ->> 'restaurante_id')::bigint
);

create policy "pedidos_write_tenant" on public.pedidos
for insert
to authenticated
with check (
  restaurante_id = (auth.jwt() ->> 'restaurante_id')::bigint
);

create policy "pedidos_update_tenant" on public.pedidos
for update
to authenticated
using (
  restaurante_id = (auth.jwt() ->> 'restaurante_id')::bigint
)
with check (
  restaurante_id = (auth.jwt() ->> 'restaurante_id')::bigint
);
```

## 2) Autenticação do Dashboard

**Hoje:** login por `usuario/senha` em tabela, e admin hardcoded (já movido para `.env`).

**Recomendado:**
- Migrar para **Supabase Auth** (email+senha, magic link, etc.).
- Guardar `restaurante_id` como claim no JWT (ou resolver via tabela de vínculo `usuarios_restaurantes`).
- Aplicar rate limit / lockout de login (especialmente para admin).

Se quiser manter tabela `restaurantes` por enquanto:
- Trocar senha em texto puro por hash (`bcrypt`/`argon2`).
- Criar script de migração para hashear senhas existentes.

## 3) Webhook: assinatura/validação forte

**Hoje:** o backend valida um secret simples via `x-webhook-secret`.

**Recomendado (ideal):**
- Validar assinatura HMAC do provedor (se disponível) com timestamp.
- IP allowlist (quando aplicável).
- Log/auditoria dos eventos (event_id, instance, chatid, received_at).

## 4) Jobs/cron confiáveis

**Hoje:** `/cron/avaliar` foi protegido e ficou idempotente via update condicional.

**Próximo passo (mais robusto):**
- Transformar em job interno (Celery/RQ/APS, ou Supabase scheduled functions).
- Fazer o envio “claim-and-send” via função SQL/RPC (atomiza seleção+claim e reduz roundtrips).

## 5) Segredos

- Rotacionar tokens/keys periodicamente.
- Nunca logar tokens inteiros.
- Garantir que `.env` não vá para git.
