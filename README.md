# AI Atendimento

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
- `MAX_BUFFER_TEXT_CHARS` (default 8000)
- `MAX_QTD_ITEM` (default 10)
- `WEBHOOK_RATE_LIMIT_PER_MIN` (default 60)
- `MESSAGE_DEBOUNCE_SECONDS` (default 5)
- `CART_ABANDONED_REMINDER_MIN` (default 10)
- `CART_ABANDONED_CANCEL_MIN` (default 15)
- `STATE_STALE_RESET_MIN` (default 120)
- `REPEAT_ORDER_LOOKBACK_DAYS` (default 30)
- `AVALIACAO_DELAY_MIN` (default 30)

HTTP/TLS:
- `HTTP_VERIFY_TLS` (default true; em DEV pode setar `false`)

Integração WhatsApp/provedor:
- `UAZAPI_BASE_URL` ( `https://free.uazapi.com`)
- `UAZAPI_TIMEOUT` (default 15)
- `UAZAPI_SEND_RETRIES` (default 2)

---

## Como rodar (API)

A partir da pasta `API/`:

1. Criar e ativar um venv
2. Instalar dependências
3. Subir o servidor

Exemplo (PowerShell):

- Criar venv:
  - `python -m venv .venv`
  - `.\.venv\Scripts\Activate.ps1`

- Instalar:
  - `pip install -r requirements.txt`

- Rodar:
  - `uvicorn main:app --host 0.0.0.0 --port 8000 --reload`

Docs:
- `http://localhost:8000/docs`
- `http://localhost:8000/health`

---

## Endpoints principais (API)

- `GET /health`
  - Health check simples

- `POST /webhook`
  - Webhook de entrada do provedor/WhatsApp
  - Faz agregação de mensagens (buffer + debounce) e processa o fluxo do atendimento

- `POST /webhook/mercadopago`
  - Webhook de pagamento Pix (Mercado Pago)
  - Recomenda-se proteger com `MP_WEBHOOK_TOKEN`

- `GET /payments/qr/{payment_id}.png`
  - Gera PNG do QR Code (Pix copia-e-cola) associado ao `payment_id`

Rotinas (cron) protegidas por `CRON_SECRET`:
- `GET /cron/abandoned-carts`
  - Lembrete/cancelamento de carrinho abandonado + devolução de estoque (idempotente com Redis)
- `GET /cron/reset-states`
  - Reseta estados travados por inatividade (volta para `INICIO`)
- `GET /cron/avaliar`
  - Envia avaliação pós-venda (1..5) após X minutos do pedido finalizado

---

## Notas operacionais

- Redis é recomendado para:
  - deduplicar eventos do webhook
  - controlar rate-limit por conversa
  - lock distribuído (evitar dupla devolução de estoque em múltiplos workers)
  - buffer de mensagens por conversa

- Supabase:
  - O backend assume tabelas e colunas usadas no código (ex.: `pix_whatsapp_enabled`, `mp_access_token_enc`, `taxa_entrega_padrao`, etc).
  - A atualização de estoque usa RPC `movimentar_estoque_seguro`.

---

## Dashboard

O `Dashboard/` é o módulo de administração do projeto. Em geral ele:
- cadastra restaurantes/instâncias (ex.: `phone_id`, `instance_name`, `instance_token`)
- gerencia produtos/estoque/categorias
- gerencia bairros e taxas de entrega
- configura prompts e flags do bot (ex.: `bot_ativo`, Pix, etc)

Para detalhes de build/run do Dashboard, consulte a documentação do próprio `Dashboard/` (package manager, scripts e variáveis do frontend).
