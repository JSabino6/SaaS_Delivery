# AI Atendimento SaaS

Eu desenvolvi este projeto para automatizar atendimento e pedidos via WhatsApp em operações de delivery (pizzaria, marmitaria e restaurante), mantendo controle real de cardápio, estoque, pagamento e entrega.

A proposta é simples: reduzir gargalo de atendimento nos horários de pico sem transformar a conversa em um bot engessado. O cliente conversa naturalmente, e o sistema organiza tudo em fluxo operacional.

---

## O que o projeto faz

- Recebe mensagens do WhatsApp via webhook
- Interpreta intenção e itens com IA
- Valida pedido com regras do cardápio (sem deixar a IA inventar preço)
- Gerencia estado de conversa por cliente
- Fecha pedido com entrega, taxa de bairro e pagamento
- Gera Pix no WhatsApp (Mercado Pago) quando habilitado
- Atualiza dashboard em tempo real para operação
- Executa rotinas automáticas (carrinho abandonado, reset de estado, avaliação)

---

## Arquitetura atual

Eu separei o sistema em dois blocos:

### 1) Frontend/Admin
- **Dashboard Streamlit**: `Dashboard/app.py`
- É o painel operacional: pedidos live, cardápio, produtos, aliases, regras de exceção, motoboys, configurações e métricas.

### 2) Backend
- **API FastAPI**: pasta `API/`
- Responsável por webhook, motor conversacional, persistência, pagamentos, validações, cron jobs e integrações.

Serviços de apoio:
- Supabase/Postgres (dados)
- Redis (opcional, mas recomendado para buffer/dedup/lock/cache)
- Uazapi (mensageria WhatsApp)
- Groq (LLM e transcrição)
- Mercado Pago (Pix)

---

## Estrutura real do repositório

```text
.
├─ API/
│  ├─ main.py                  # Entrypoint FastAPI, middleware e rotas HTTP
│  ├─ cerebro.py               # Núcleo de decisão conversacional e validação de pedido
│  ├─ banco.py                 # Camada de acesso a dados, cron, Pix e utilitários de persistência
│  ├─ zap.py                   # Webhook WhatsApp, debounce/buffer e envio de mensagens
│  ├─ utils.py                 # Config, helpers, normalização de texto, Redis e logging base
│  ├─ health_startup.py        # Diagnóstico de inicialização
│  ├─ logging_setup.py         # Configuração de logger da API
│  ├─ testaraudio.py           # Teste utilitário de áudio/transcrição
│  ├─ teste_zap.py             # Teste utilitário de integração WhatsApp
│  ├─ requirements.txt
│  └─ Dockerfile
│
├─ Dashboard/
│  ├─ app.py                   # Frontend Streamlit (painel de gestão)
│  ├─ requirements.txt
│  └─ Dockerfile
│
├─ docker-compose.yml
├─ logging_setup.py            # Setup de logs compartilhado na raiz
├─ Simulador_zap.py            # Simulador/apoio de testes de mensagens
├─ README.md
├─ CONFIGURACAO_DO_BOT.txt
├─ DOCUMENTACAO_COMPLETA_DO_CODIGO.txt
├─ SECURITY_NEXT_STEPS.md
├─ Fases.txt
├─ tools/
├─ logs/                       # Logs rotacionados da API e do Dashboard
│
├─ restaurantes_rows.sql
├─ supabase_entregas.sql
├─ supabase_loja_faq.sql
├─ supabase_motoboys.sql
├─ supabase_payments.sql
├─ supabase_pedidos_itens.sql
└─ supabase_pedidos_migration.sql
```

---

## Fluxo resumido de ponta a ponta

1. O cliente manda mensagem no WhatsApp.
2. `API/zap.py` recebe o webhook e agrega mensagens curtas com debounce.
3. `API/cerebro.py` interpreta intenção e extrai slots do pedido.
4. `API/banco.py` valida no cardápio oficial, aplica regras, persiste pedido e itens.
5. Se Pix estiver ativo, o sistema cria cobrança e aguarda confirmação do webhook de pagamento.
6. O Dashboard acompanha status e execução operacional em tempo real.

---

## Endpoints principais da API

- `POST /webhook`
- `POST /webhook/mercadopago`
- `GET /payments/qr/{payment_id}.png`
- `GET /health`
- `POST /admin/cache/invalidate`
- `POST /admin/chat/toggle_pause`
- `GET /cron/abandoned-carts`
- `GET /cron/reset-states`
- `GET /cron/avaliar`

---

## Diferenciais de implementação

- **Multi-tenant de verdade**: cada restaurante tem configuração, cardápio e operação isolados.
- **Regras de exceção por JSON**: adapto facilmente cenários como quentinha, meio a meio, adicionais e borda.
- **Aliases de produto**: melhora reconhecimento de linguagem natural sem depender de nome exato.
- **Borda grátis configurável**: regra operacional editável por restaurante.
- **IA com guardrails**: a IA sugere; o backend valida preço/estoque/consistência.
- **Resiliência operacional**: debounce, deduplicação, lock e cache via Redis.

---

## Como rodar

### Pré-requisitos
- Python 3.10+
- Projeto Supabase configurado
- Chave da Groq
- Instância Uazapi
- (Recomendado) Redis

### Variáveis mínimas
- `SUPABASE_URL`
- `SUPABASE_KEY`
- `GROQ_API_KEY`
- `WEBHOOK_SECRET`
- `PUBLIC_BASE_URL`

### Subir com Docker
```bash
docker-compose up --build -d
```

### Rodar local (sem Docker)
```bash
# API
cd API
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Dashboard (novo terminal)
cd Dashboard
pip install -r requirements.txt
streamlit run app.py
```

---

## Status do projeto

O projeto está funcional e em uso de lapidação contínua: estou refinando regras de negócio, observabilidade e experiência operacional do painel.

---

## Autor

Projeto pessoal desenvolvido por mim, com foco em arquitetura aplicada, integração de serviços reais e operação de delivery no mundo real.
