# Delivery AI Ops Engine

Plataforma SaaS para automacao de atendimento de delivery via WhatsApp, com foco em operacao multi-tenant, checkout robusto, regras de negocio deterministicas e observabilidade de ponta a ponta.

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](#)
[![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?logo=fastapi&logoColor=white)](#)
[![Supabase](https://img.shields.io/badge/Supabase-Postgres-3ECF8E?logo=supabase&logoColor=white)](#)
[![WhatsApp](https://img.shields.io/badge/WhatsApp-Automation-25D366?logo=whatsapp&logoColor=white)](#)
[![Status](https://img.shields.io/badge/QA%20Bateria%20Obrigatoria-PASSOU-brightgreen)](#)

---

## Sumario

1. Visao geral
2. Problema que o projeto resolve
3. Solucao proposta
4. Principais resultados
5. Galeria (placeholders para voce inserir prints)
6. Resumo das tecnologias (curto e objetivo)
7. Stack detalhada
8. Arquitetura do sistema
9. Fluxo funcional (cliente ate finalizacao)
10. Estrutura do repositorio
11. Modulos principais
12. Regras de negocio e guardrails
13. Banco de dados e SQLs do projeto
14. Integracoes externas
15. Setup local (passo a passo)
16. Variaveis de ambiente
17. Como rodar API, painel e webhook
18. Testes e validacao de regressao
19. Observabilidade, logs e operacao
20. Seguranca e hardening
21. Deploy e ambiente produtivo
22. Roadmap tecnico
23. Licenca e uso

---

## Visao geral

O Delivery AI Ops Engine foi criado para operar atendimento de delivery em escala sem perder consistencia de negocio.

Principio de engenharia central:

- IA interpreta linguagem natural.
- Backend deterministico valida e decide.
- Persistencia garante rastreabilidade e continuidade de estado.

Isso evita que o sistema dependa apenas da IA para decidir preco, taxa, fechamento, validacoes e finalizacao de pedido.

---

## Problema que o projeto resolve

Times de atendimento de delivery normalmente enfrentam:

- fila de mensagens em horario de pico;
- erros de fechamento de pedido;
- perdas por abandono de checkout;
- inconsistencias na aplicacao de regras;
- dificuldade para manter qualidade sob carga.

Esse projeto resolve esses pontos com um fluxo conversacional orientado a estado e validacoes deterministicas no backend.

---

## Solucao proposta

A solucao combina:

- atendimento conversacional via WhatsApp;
- API transacional com validacoes de negocio;
- memoria de checkout e estado de conversa por cliente;
- painel web para operacao e administracao;
- camada de pagamentos e webhooks;
- rotinas de cron e monitoramento.

Diferencial:

- suporte a multi-intencao no mesmo turno;
- robustez para pedidos incompletos (ex.: endereco sem numero);
- confirmacao final unificada de checkout;
- bateria de QA conversacional com cenarios reais.

---

## Principais resultados

- Fluxo de checkout unificado com confirmacao final unica.
- Guardrails para evitar saida indevida do checkout.
- Slot filling incremental para endereco (rua -> numero).
- Melhor comportamento para pedidos multi-item no mesmo texto.
- Bateria obrigatoria de QA executada com status PASS.

---

## Galeria (placeholders para voce inserir prints)

- <img width="2559" height="1331" alt="image" src="https://github.com/user-attachments/assets/f47aa765-25c5-4bb9-8384-94630c2dc37d" />

- <img width="1441" height="1028" alt="image" src="https://github.com/user-attachments/assets/5026505e-7469-45bd-88ff-130ae267ccd4" />

<img width="602" height="1127" alt="image" src="https://github.com/user-attachments/assets/c636ad3d-1707-476d-b622-2fc6f4f745c3" />

<img width="599" height="1117" alt="image" src="https://github.com/user-attachments/assets/a9338c25-b371-47c5-92fd-a85983534fff" />

<img width="2192" height="1163" alt="image" src="https://github.com/user-attachments/assets/6ea5d2e1-6640-4153-8989-5fcd63fbfdec" />

<img width="1975" height="1203" alt="image" src="https://github.com/user-attachments/assets/0f7b8f18-a0b9-425a-87af-80655c5b931a" />


---

## Resumo das tecnologias

- Backend: Python + FastAPI + Uvicorn.
- IA: Groq (tool-calling com guardrails no backend).
- Banco: Supabase (Postgres) com scripts SQL de evolucao.
- Cache/estado: Redis.
- Integracoes: WhatsApp provider (Uazapi), Mercado Pago, Google Maps/Distance.
- Frontend: Painel web modular (HTML + JavaScript).
- Infra local: Docker Compose + .env.
- Observabilidade: logs estruturados + request-id + health checks.

---

## Stack detalhada

### Backend

- FastAPI
- Uvicorn
- Jinja2
- python-dotenv
- requests / urllib3
- python-multipart

### IA e orquestracao

- Groq SDK
- Tool calling com validacao deterministica de argumentos

### Persistencia e estado

- Supabase (Postgres)
- Redis

### Seguranca e auth

- PyJWT
- cryptography

### Pagamentos e geolocalizacao

- Mercado Pago (webhook + OAuth + transacoes)
- Google APIs (validacao/geocoding/distancia)

### Frontend operacional

- Painel Web em (HTML + JS modular)

---

## Arquitetura do sistema

```mermaid
flowchart TD
    A[Cliente no WhatsApp] --> B[Webhook FastAPI]
    B --> C[Orquestrador Conversacional]
    C --> D[Guardrails e Regras de Negocio]
    D --> E[Supabase/Postgres]
    D --> F[Redis]
    C --> G[Groq LLM]
    D --> H[Mercado Pago]
    D --> I[Google APIs]
    E --> J[Painel Web]
    B --> J
```

### Camadas

1. Entrada de eventos: webhook e endpoints HTTP.
2. Orquestracao: classifica intencao e conduz estado.
3. Regras deterministicas: valida contrato de tool e negocio.
4. Persistencia: pedidos, perfil, estado, metricas.
5. Operacao: painel web para acompanhamento e administracao.

---

## Fluxo funcional (cliente ate finalizacao)

1. Cliente envia mensagem no WhatsApp.
2. API recebe webhook e identifica tenant/restaurante.
3. Estado atual e carregado (pedido, checkout, pendencias).
4. Agente interpreta a mensagem.
5. Backend valida argumentos e regras.
6. Itens/endereco/pagamento sao normalizados.
7. Quando pronto, sistema envia resumo final unico.
8. Cliente confirma.
9. Pedido e finalizado e persistido.
10. Painel e logs refletem toda a trilha.

---

## Estrutura do repositorio

text
.
├─ API/
│  ├─ main.py
│  ├─ banco.py
│  ├─ cerebro_v2_agente.py
│  ├─ routes/
│  ├─ services/
│  ├─ templates/
│  └─ requirements.txt
├─ Painel_Web/
│  ├─ index.html
│  ├─ app.js
│  ├─ core/
│  ├─ pedidos/
│  ├─ cardapio/
│  ├─ metricas/
│  ├─ motoboys/
│  └─ admin/
├─ docker-compose.yml


## Modulos principais

### API

- `API/main.py`: composicao da API, routers, middlewares, health e endpoints principais.
- `API/banco.py`: regras de negocio, persistencia, checkout, pagamentos, cron e integracoes.
- `API/cerebro_v2_agente.py`: nucleo conversacional com guardrails e maquina de estado.

### Painel web

- `Painel_Web/index.html`: shell principal da interface.
- `Painel_Web/app.js`: bootstrap e orquestracao frontend.
- `Painel_Web/core/`: configuracao e camada base.
- Demais pastas: dominios funcionais por area operacional.

---

## Regras de negocio e guardrails

O projeto aplica validacoes robustas antes de qualquer acao critica.

Exemplos:

- bloqueio de finalizacao sem checkout completo;
- endereco de entrega validado e normalizado;
- tratamento incremental de endereco (rua e depois numero);
- confirmacao final unica para fechamento;
- protecao contra respostas neutras desviando fluxo;
- conduta para pedidos multi-item no mesmo turno.

---

## Banco de dados e SQLs do projeto

O repositorio inclui scripts SQL para:

- criacao/evolucao de tabelas;
- visoes analiticas;
- hardening de RLS e multi-tenant;
- recursos de pagamento e metricas;
- tabelas de pedidos, itens, entregas, clientes e operacao.

Arquivos SQL relevantes estao na raiz com prefixo `supabase_`.

---

## Integracoes externas

### WhatsApp provider

- Entrada e saida de mensagens
- Fluxo de webhook

### Groq

- Interpretacao de linguagem natural
- Sem delegar decisao transacional critica para o modelo

### Supabase

- Banco principal (dados transacionais e operacionais)

### Redis

- Estado auxiliar, lock e performance operacional

### Mercado Pago

- Checkout online, webhook de pagamento e OAuth connect

### Google APIs

- Validacao/geocoding de endereco
- Distancia para suporte a regras de entrega

---

## Setup local (passo a passo)

### 1) Pre-requisitos

- Python 3.10+
- pip
- conta/projeto Supabase
- credenciais de integracoes

### 2) Clone do repositorio

```bash
git clone <URL_DO_REPOSITORIO>
cd "Saas - Delivery - HTML"
```

### 3) Ambiente virtual

```bash
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
```

### 4) Dependencias da API

```bash
cd API
pip install -r requirements.txt
cd ..
```

### 5) Arquivo de ambiente

Crie/ajuste `.env` com as variaveis necessarias (veja secao abaixo).

### 6) Subir API

```bash
cd API
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 7) Servir painel web estatico (opcional local)

Em outro terminal:

```bash
cd Painel_Web
python -m http.server 5500
```

Abra no navegador:

- API: `http://localhost:8000/health`
- Painel: `http://localhost:5500`

---

## Variaveis de ambiente

> Importante: nunca versione segredos reais no GitHub.

### Exemplo minimo (modelo)

```dotenv
SUPABASE_URL=https://SEU_PROJETO.supabase.co
SUPABASE_KEY=SEU_SUPABASE_SERVICE_KEY
GROQ_API_KEY=SUA_CHAVE_GROQ
REDIS_URL=redis://USER:PASS@HOST:PORT
WEBHOOK_SECRET=SEU_SEGREDO_WEBHOOK
PUBLIC_BASE_URL=https://SEU_DOMINIO_PUBLICO
CORS_ALLOW_ORIGINS=http://localhost:5500,http://localhost:3000
```

### Variaveis comuns no projeto

- `EVOLUTION_API_URL`
- `EVOLUTION_API_KEY`
- `GROQ_API_KEY`
- `SUPABASE_URL`
- `SUPABASE_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `REDIS_URL`
- `WEBHOOK_SECRET`
- `MP_WEBHOOK_TOKEN`
- `MERCADOPAGO_CLIENT_ID`
- `MERCADOPAGO_CLIENT_SECRET`
- `MERCADOPAGO_REDIRECT_URI`
- `GOOGLE_DISTANCE_MATRIX_API_KEY`
- `MAPS_PUBLIC_KEY` ou `Maps_PUBLIC_KEY`

---

##rodar webhook com URL publica (ngrok)

### 1) Expor API local

```bash
ngrok http 8000
```

### 2) URL de webhook

Use o formato:

```text
https://SEU_SUBDOMINIO.ngrok-free.app/webhook?token=SEU_WEBHOOK_SECRET
```

### 3) Configurar no provedor WhatsApp

- Defina a URL publica do webhook.
- Garanta que o token confere com `WEBHOOK_SECRET`.

---

## Testes e validacao de regressao

O projeto possui scripts para checklists e QA conversacional.

Exemplos:

- `API/checklist_agent_v2_regressions.py`
- `API/qa_bateria_obrigatoria_whatsapp.py`

### Execucao da bateria obrigatoria

```bash
python API/qa_bateria_obrigatoria_whatsapp.py
```

Saida esperada:

- gera `API/qa_bateria_obrigatoria_whatsapp_report.json`
- traz resultado por cenario
- campo final `evaluation.passed`

---

## Observabilidade, logs e operacao

### Logs

- API usa logging estruturado e request-id.
- Arquivos de log ficam em `logs/`.

### Health checks

- Endpoint de saude: `/health`.

### Operacao diaria recomendada

- monitorar falhas de webhook;
- acompanhar latencia e erro por rota;
- validar periodicamente fluxo de checkout;
- revisar metricas de abandono.

---

## Seguranca e hardening

Praticas aplicadas:

- isolamento multi-tenant por restaurante;
- regras deterministicas para acoes criticas;
- validacao de argumentos de tools;
- hardening de RLS no Supabase;
- segregacao entre inferencia e decisao transacional.


## Deploy e ambiente produtivo

Checklist de producao:

1. HTTPS habilitado em todos os endpoints.
2. `PUBLIC_BASE_URL` correto.
3. Webhooks configurados com token.
4. CORS restrito a origens reais.
5. Logs e alertas ativos.
