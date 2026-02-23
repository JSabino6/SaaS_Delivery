🍕 AI Delivery SaaS (Assistente Virtual para Restaurantes)
Um sistema completo de Software as a Service (SaaS) focado em automatizar o atendimento via WhatsApp para pizzarias e hamburguerias. O sistema utiliza Inteligência Artificial para interpretar áudios confusos, mensagens picotadas e mudanças de ideia do cliente, transformando tudo em um pedido estruturado (JSON) que cai direto em um painel Kanban na cozinha.

🚀 O Problema que Resolvemos
Restaurantes perdem vendas nos horários de pico (sextas e sábados) porque os atendentes humanos não conseguem responder múltiplos clientes ao mesmo tempo. Sistemas tradicionais de bot (com botões "Digite 1 para Pizza") são frustrantes. Nosso sistema oferece um atendimento conversacional, humanizado e à prova de alucinações, lidando com cardápios dinâmicos e regras matemáticas complexas.

🛠️ Stack Tecnológica
A arquitetura foi desenhada para ter custo operacional baixíssimo e altíssima escalabilidade.

Backend: Python 3 (FastAPI)

Frontend (Painel Kanban): Streamlit

Banco de Dados: Supabase (PostgreSQL)

Cache & Fila: Redis (Upstash)

Inteligência Artificial: Llama 3 70B via Groq API (ou GPT-4o-mini da OpenAI)

Integração WhatsApp: UAZAPI (Webhook)

Geocodificação: Google Maps API / Nominatim OpenStreetMap

Tarefas Agendadas (Crons): Cron-job.org

Infraestrutura: Docker & Docker Compose (Hetzner/VPS)

🧠 Arquitetura: O Padrão "Dois Cérebros"
Para otimizar custos e tempo de resposta (latência), o cérebro da IA é dividido em duas etapas:

Roteador de Intenção (Rápido e Barato): Analisa a mensagem e classifica a intenção (saudacao, fazer_pedido, reclamacao, solicitar_humano).

Slot Filling (Pesado e Preciso): Se a intenção for fazer_pedido, um modelo mais robusto (ex: Llama 70B) é acionado para extrair os itens, quantidades, observações e endereço, cruzando com o cardápio oficial para evitar alucinações.

📂 Estrutura do Projeto
main.py: O coração do FastAPI. Recebe os webhooks da UAZAPI e expõe os endpoints para os serviços de Cron (tarefas agendadas).

zap.py: Gerencia as filas de mensagens. Interage com o Redis para criar o Buffer de Digitação (juntando mensagens picotadas do cliente) e garantindo Idempotência (evitando processar o mesmo webhook duas vezes).

cerebro.py: Onde a mágica da IA acontece. Faz as chamadas para a API da Groq/OpenAI, injeta o System Prompt, valida regras de negócio e garante que a IA não invente itens ou preços.

banco.py: Camada de persistência. Gerencia toda a comunicação com o Supabase (salvar pedidos, buscar histórico de clientes, verificar cardápio).

utils.py: Funções auxiliares vitais, como o gerenciador de estado do Redis (redis_claim_event_once) e roteamento de modelos de IA.

app.py: O painel Administrativo em Streamlit. Uma visão Kanban "Ao Vivo" para a cozinha, com paginação otimizada para não derrubar o servidor.

⚙️ Funcionalidades Principais (Features)
Atendimento Humanizado: Delay artificial ("digitando...") e capacidade de lidar com gírias e erros de digitação.

Buffer Anti-Spam (Redis): O cliente digita 5 mensagens separadas ("Oi", "Quero pizza", "de calabresa") e o sistema aguarda 3 segundos para agrupar tudo em um único bloco de texto para a IA processar.

Transbordo Humano (Handoff): Se o cliente fizer uma pergunta complexa, xingar ou pedir para falar com o gerente, a IA pausa o chat e aciona a cozinha.

Geocodificação Automática: O cliente envia "Rua X, Número Y" e o sistema busca automaticamente o Bairro usando APIs de mapas.

Crons de Vendas (Retargeting):

/cron/abandoned-carts: Lembra clientes que pararam no meio do pedido.

/cron/inactive-customers: Envia cupons para quem não pede há mais de 30 dias.

/cron/ask-review: Pede avaliação no Google Meu Negócio 2 horas após a entrega.

💻 Como Rodar o Projeto (Ambiente de Desenvolvimento)
1. Pré-requisitos
Docker e Docker Compose instalados.

Contas gratuitas no Supabase, Upstash (Redis) e Groq.

2. Configurando Variáveis de Ambiente
Crie um arquivo .env na raiz do projeto e preencha com as suas chaves:

Snippet de código
# Configurações da IA
GROQ_API_KEY=sua_chave_groq_aqui
INTENT_ROUTER_MODEL=llama3-8b-8192
SLOT_FILLING_MODEL=llama-3.1-70b-versatile

# Banco de Dados e Cache
SUPABASE_URL=sua_url_supabase
SUPABASE_KEY=sua_chave_supabase
REDIS_URL=sua_url_upstash

# WhatsApp
UAZAPI_TOKEN=seu_token_webhook
CRON_SECRET=senha_segura_para_os_crons
3. Iniciando com Docker
Basta rodar o comando abaixo para subir o Backend (FastAPI), Frontend (Streamlit) e o Redis (se local):

Bash
docker-compose up --build -d
API FastAPI: http://localhost:8000

Painel Streamlit: http://localhost:8501

🔒 Regras de Segurança e Proteção
Matemática Burra na IA: A IA nunca calcula o valor final do pedido. Ela apenas extrai os itens. O banco.py puxa os preços oficiais do Supabase e faz a soma, garantindo zero prejuízo por alucinação.

Política de Uso Justo (Rate Limit): Todas as chamadas de API são contabilizadas no banco de dados. Caso um restaurante passe de um limite de requisições, o sistema acusa excesso para evitar surpresas na fatura da Groq/OpenAI.

Isolamento de Dados: Cada restaurante_id possui seus próprios fluxos de caixa e painéis, garantindo que a Pizzaria A não veja os dados da Hambugueria B.

🗺️ Roadmap Futuro
[ ] Migração do Webhook UAZAPI para a WhatsApp Cloud API Oficial (Meta) para contas verificadas.

[ ] Refatoração do Frontend (app.py) de Streamlit para React / Next.js para suportar centenas de weblogs simultâneos na cozinha sem sobrecarregar o servidor.

[ ] Integração nativa com APIs de pagamento (Mercado Pago / Asaas) para geração de PIX Copia e Cola automático.