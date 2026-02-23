# AI Atendimento

Este projeto nasceu da necessidade de automatizar o atendimento em pizzarias locais, solucionando dores reais do comércio. O desenvolvimento uniu tecnologias que eu já dominava a novas stacks que integrei durante meus estudos. Atualmente, o fluxo principal de atendimento está estável e funcional. O projeto encontra-se em fase de lapidação em ambiente local, com foco total no refinamento da arquitetura multi-tenant.

>>>>>>> cbb02c39c9b6974d5c7bc8f1492bd7869b69b790
Projeto com dois componentes principais:
Este projeto nasceu da necessidade de automatizar o atendimento em pizzarias locais, solucionando dores reais do comércio. O desenvolvimento uniu tecnologias que eu já dominava a novas stacks que integrei durante meus estudos. Atualmente, o fluxo principal de atendimento está estável e funcional. O projeto encontra-se em fase de lapidação em ambiente local, com foco total no refinamento da arquitetura multi-tenant.
Projeto com dois componentes principais:

- **API**: backend (FastAPI) responsável por receber webhooks do WhatsApp/provider, orquestrar atendimento via IA, gerenciar carrinho/pedidos no Supabase, estoque e rotinas (cron).
- **Dashboard**: interface de administração (cadastros, configurações do bot, produtos, bairros/taxas, etc).

---

## Estrutura

- `API/`
  - `main.py`: aplicação FastAPI (webhook, integrações, rotinas e fluxo do atendimento)
  - `health_startup.py`: checagens/diagnóstico no startup (referenciado no `main.py`)
  - Outros módulos auxiliares (logging, integrações, etc)
- `Dashboard/`
  - Frontend/admin do sistema (configuração de restaurantes, cardápio, bairros, etc)

---

## Requisitos

- Python 3.10+ (recomendado)
- Conta/instância Supabase (tabelas como `restaurantes`, `produtos`, `bairros`, `pedidos`, `clientes_estado`, `conversas`)
- Chave de IA (Groq)
- Redis (opcional, porém recomendado para deduplicação/locks/rate-limit/buffer)

---

## Migrações (Supabase)

Algumas features recentes usam colunas adicionais na tabela `pedidos` (ex.: trava de aceite até o bot finalizar, badge de abandono e sinalização de atendimento humano).



## Migrações (Supabase)

Algumas features recentes usam colunas adicionais na tabela `pedidos` (ex.: trava de aceite até o bot finalizar, badge de abandono e sinalização de atendimento humano).

- Rode o SQL de [supabase_pedidos_migration.sql](supabase_pedidos_migration.sql) no Supabase SQL Editor.
- É seguro rodar mais de uma vez (`if not exists`).

---
## Configuração (.env)

A API lê variáveis via `.env` (ex.: `API/.env`) ou variáveis de ambiente do deploy.

Obrigatórias:
- `SUPABASE_URL`
- `SUPABASE_KEY`
- `GROQ_API_KEY`

Recomendadas:
- `REDIS_URL` (habilita deduplicação, lock distribuído, rate-limit e buffer de mensagens)
- `PUBLIC_BASE_URL` (para gerar link público do QR do Pix, ex.: `https://seu-dominio.com`)

Segurança/segredos:
- `WEBHOOK_SECRET` (se você validar webhook do provedor; depende do provedor)
- `CRON_SECRET` (protege endpoints `/cron/*`)
- `MP_WEBHOOK_TOKEN` (protege `/webhook/mercadopago`)
- `CRED_ENCRYPTION_KEY` (Fernet key para decriptar credenciais como `mp_access_token_enc`)

Tuning/limites (defaults no código):
- `MAX_WEBHOOK_BODY_BYTES` (default 262144)
- `MAX_INCOMING_TEXT_CHARS` (default 4000)

# AI Atendimento SaaS

Projeto completo para automatizar pedidos de restaurantes, pizzarias e marmitarias via WhatsApp, com painel de gestão intuitivo, regras flexíveis de cardápio, gestão de motoboys e integração Pix. Ideal para portfólio, pensado para operação real e fácil adaptação.

---

## Visão Geral

O sistema conecta clientes ao restaurante pelo WhatsApp, reconhece pedidos automaticamente, aplica regras de mistura (quentinha, pizza, combos), permite editar produtos e aliases, e gerencia motoboys e entregas. Tudo é configurável pelo painel, sem necessidade de mexer no código.

**Principais recursos:**
- Atendimento automatizado via WhatsApp (Uazapi)
- Integração Mercado Pago (Pix)
- Painel de gestão (Streamlit)
- Cadastro e edição de produtos, aliases, regras de exceção (JSON)
- Borda grátis configurável para pizzarias
- Validação de regras de mistura (quentinha, pizza, combos)
- Gestão de motoboys e entregas
- Cache e logs para performance
- Multi-restaurante (cada restaurante tem suas configurações)

---

## Arquitetura

**Frontend:**
- Dashboard (Streamlit/app.py): painel para gestão de cardápio, produtos, aliases, regras, motoboys, configurações e métricas.

**Backend:**
- API (FastAPI): webhooks, validação de pedidos, integração Pix, regras de exceção, cache, logs.
- Banco de dados (Supabase/Postgres): restaurantes, produtos, pedidos, aliases, regras, motoboys.
- Redis (opcional): rate-limit, buffer, locks.

---

## Fluxo Operacional

1. Cliente envia mensagem no WhatsApp.
2. API recebe via webhook, valida cardápio, aplica aliases e regras.
3. Painel permite editar produtos, aliases, regras de exceção (JSON), borda grátis, motoboys.
4. Pedido é processado, Pix gerado, status atualizado.
5. Motoboy recebe entrega, painel mostra métricas e histórico.

---

## Diferenciais

- Regras de exceção estruturadas (JSON) para validar misturas, limites, combos.
- Aliases automáticos e editáveis para produtos (facilita reconhecimento de pedidos).
- Borda grátis configurável (backend e painel).
- Multi-tenant: cada restaurante tem seu cardápio, regras e configurações.
- Painel intuitivo para não técnicos.
- Logs detalhados e cache para performance.

---

## Tecnologias

- FastAPI (backend)
- Streamlit (dashboard)
- Supabase/Postgres (banco de dados)
- Redis (opcional)
- Mercado Pago (Pix)
- Uazapi (WhatsApp)

---

## Estrutura do Projeto

```
API/
  main.py           # FastAPI endpoints
  cerebro.py        # Lógica de validação, aliases, regras, borda
  banco.py          # Supabase/Postgres, Pix, cache
  zap.py            # Webhook WhatsApp, envio de mensagens
  health_startup.py # Diagnóstico inicial
  requirements.txt  # Dependências backend
Dashboard/
  app.py            # Streamlit dashboard (frontend)
  requirements.txt  # Dependências frontend
logs/               # Logs de API e dashboard
```

---

## Configuração e Uso

1. Configure o .env com as chaves do Supabase, Mercado Pago, Uazapi, etc.
2. Rode o backend (FastAPI) e o frontend (Streamlit).
3. Acesse o painel para cadastrar produtos, aliases, regras, motoboys.
4. O bot já reconhece aliases, aplica borda grátis (se configurado), valida regras de mistura.
5. Pedidos são processados automaticamente, Pix gerado, motoboy recebe entrega.

---

## Exemplo de Regra de Exceção (JSON)

```json
{
  "quentinha": {
    "max_misturas": 2,
    "permitidos": ["carne", "frango", "peixe", "ovo"]
  },
  "pizza": {
    "borda_gratis": true,
    "sabores_max": 2
  }
}
```

---

## Observações

- O painel permite editar todas as regras e aliases sem necessidade de código.
- O backend valida tudo antes de fechar o pedido, evitando erros.
- Logs e cache garantem performance e rastreabilidade.

---

## Portfolio

Projeto pensado para ser flexível, robusto e fácil de operar, ideal para restaurantes que querem automatizar pedidos sem complicação. O código está organizado, documentado e pronto para ser usado ou adaptado.

Qualquer dúvida ou sugestão, entre em contato!
