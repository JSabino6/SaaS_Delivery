# Delivery AI Ops Engine

### Atendimento de WhatsApp para Delivery em escala SaaS, com IA conversacional e backend deterministico para operacao real.

---

## Problema vs. Solucao

### O caos
Na sexta-feira a noite, o atendimento humano entra em colapso:
- filas de mensagens no WhatsApp;
- erros de pedido por pressa;
- atrasos em checkout e pagamento;
- perda de venda por falta de padrao;
- dependencia total do suporte tecnico para incidentes operacionais.

### A solucao
Este projeto implementa um motor de atendimento com IA que escala operacao sem perder controle:
- interpreta intencao do cliente em linguagem natural;
- valida regras de negocio no backend (nao no modelo);
- fecha checkout com fluxo sem friccao;
- integra pagamento Pix e operacao em tempo real;
- permite auto-recuperacao de sessao WhatsApp via QR Code.

Resultado: mais throughput, menos erro humano e uma operacao previsivel mesmo em pico de demanda.

---

## Arquitetura Tecnica

Stack principal:
- Python: FastAPI no backend e Streamlit no dashboard operacional.
- Supabase (Postgres): persistencia multi-tenant, pedidos, estados de conversa e configuracoes.
- Groq: inferencia LLM para roteamento de intencao e condução conversacional.
- WhatsApp API (uazapi): entrada e saida de mensagens, status de instancia e reconexao por QR.

Componentes:
- API: processamento de webhook, agente conversacional v2, validacao de checkout, pagamentos, cron jobs e endpoints administrativos.
- Dashboard: operacao de pedidos, cardapio, regras de negocio, metricas e monitoramento de conexao WhatsApp.
- Redis (recomendado): deduplicacao, debounce, locks e cache operacional.

Principio de engenharia:
**A IA interpreta. O backend decide.**

---

## Engineering Highlights

### 1) Defensive AI
- Blindagem contra prompt injection e tentativas de exfiltracao de regras internas.
- Guardrails para evitar alucinacoes de desconto, cupom, brinde e promessas nao autorizadas.
- Enforcement de argumentos de tools com sanitizacao e validacao de limites.

### 2) Fuzzy Matching / Fallback
- Reconhecimento tolerante de itens (aliases, variacoes de escrita e contexto).
- Quando item nao existe ou esta indisponivel, o fluxo sugere alternativa valida sem quebrar a jornada.
- Mantem consistencia com cardapio oficial e estoque real.

### 3) Checkout Sem Friccao
- Mudancas de item durante checkout sem reset de conversa.
- Continuidade por contexto: carrinho, endereco, pagamento e confirmacao final.
- Regras deterministicas para taxa, total e fechamento de pedido.

### 4) Auto-Recuperacao de Sessao WhatsApp
- Monitoramento de status de instancia.
- Gatilho de reconexao quando necessario.
- Geracao e entrega de QR Code em base64 para o proprio usuario final recuperar o bot sem acionar suporte.

---

## Estrutura do Repositorio

```text
.
├─ API/
│  ├─ main.py
│  ├─ banco.py
│  ├─ zap.py
│  ├─ cerebro_v2_agente.py
│  ├─ checklist_agent_v2_regressions.py
│  └─ ...
├─ Dashboard/
│  ├─ app.py
│  └─ ...
├─ CONFIGURACAO_DO_BOT.txt
├─ DOCUMENTACAO_COMPLETA_DO_CODIGO.txt
├─ SECURITY_NEXT_STEPS.md
└─ docker-compose.yml
```

---

## Operacao Rapida

### 1) API
```bash
cd API
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 2) Dashboard
```bash
cd Dashboard
pip install -r requirements.txt
streamlit run app.py
```

### 3) Variaveis essenciais
- SUPABASE_URL
- SUPABASE_KEY
- GROQ_API_KEY
- WEBHOOK_SECRET
- PUBLIC_BASE_URL
- CACHE_INVALIDATE_URL
- CACHE_INVALIDATE_TOKEN

---

## Showcase Disclaimer (Obrigatorio)

Este repositorio e um **showcase arquitetural e tecnico**.

Ele demonstra design de sistema, padroes de engenharia e recortes seguros de implementacao. O motor principal de negocio, componentes proprietarios e partes criticas de producao pertencem a um **SaaS de codigo fechado**.

Nao representa o produto comercial completo nem contem todos os ativos internos utilizados em ambiente real.

---

## Licenciamento e Uso

Use este repositorio como referencia de arquitetura, boas praticas e estudo de integracao entre IA conversacional e operacao de delivery.
