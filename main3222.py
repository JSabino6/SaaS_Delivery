import os
import asyncio
import time
import requests
import json
import re
import difflib
import hashlib
import uuid
import copy
import os
import sys
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone
import unicodedata
from fastapi import FastAPI, Request
from fastapi.responses import Response
from fastapi.responses import JSONResponse

_WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(_WORKSPACE_ROOT) not in sys.path:
    sys.path.append(str(_WORKSPACE_ROOT))
from logging_setup import setup_logging

logger = setup_logging("api", log_dir=os.getenv("LOG_DIR") or str(_WORKSPACE_ROOT / "logs"))


def _only_digits(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        value = str(int(value))
    if isinstance(value, dict):
        for k in ("id", "jid", "number", "remoteJid", "remote_jid", "chatid", "chatId", "from"):
            if k in value:
                return _only_digits(value.get(k))
        value = str(value)
    text = str(value)
    return "".join(ch for ch in text if ch.isdigit())


def _extract_cliente_zap(body: dict, msg_data: dict, phone_id: str) -> str:
    """Best-effort extraction of customer number for incoming webhook.

    Some providers put the customer jid in different fields; we try several.
    We also avoid returning the restaurant's own phone_id.
    """

    allow_self_test = (os.getenv("ALLOW_SELF_TEST") or "").strip() == "1"
    enforce_country_prefix = (os.getenv("ENFORCE_COUNTRY_PREFIX") or "1").strip() != "0"

    def _score(candidate, *, field_weight: int) -> tuple[int, str]:
        raw = "" if candidate is None else str(candidate)
        d = _only_digits(candidate)
        if not d:
            return (-10_000, "")

        digits_phone_id = _only_digits(phone_id)
        country_prefix = digits_phone_id[:2] if len(digits_phone_id) >= 2 else ""


        raw_lower = raw.lower()
        is_group = "@g.us" in raw_lower
        is_user_jid = ("@c.us" in raw_lower) or ("@s.whatsapp.net" in raw_lower)


        if is_group:
            return (-9_000, "")
        if d == digits_phone_id and not allow_self_test:
            return (-5_000, "")
        if len(d) < 10 or len(d) > 15:  # E.164 practical bounds
            return (-4_000, "")

        score = field_weight
        if is_user_jid:
            score += 50


        if country_prefix and d.startswith(country_prefix):
            score += 15
        elif country_prefix:
            if enforce_country_prefix:
                return (-8_000, "")
            score -= 150


        if country_prefix == "55" and d.startswith("55"):
            score += 5


        if d == digits_phone_id and allow_self_test:
            score -= 20

        return (score, d)



    candidates = [
        ("sender_pn", msg_data.get("sender_pn"), 60),
        ("sender_lid", msg_data.get("sender_lid"), 55),
        ("participant", msg_data.get("participant") or msg_data.get("author"), 40),
        ("sender", msg_data.get("sender") or msg_data.get("from") or msg_data.get("remoteJid") or msg_data.get("remote_jid"), 30),
        ("key.remoteJid", (msg_data.get("key") or {}).get("remoteJid"), 25),
        ("body.sender", body.get("sender"), 20),
        ("body.from", body.get("from"), 15),
        ("chatid", msg_data.get("chatid") or msg_data.get("chatId") or msg_data.get("chat_id"), 0),
    ]

    best_score = -10_000
    best_digits = ""
    for _, cand, w in candidates:
        s, d = _score(cand, field_weight=w)
        if s > best_score:
            best_score, best_digits = s, d


    if best_score < -1000:
        return ""

    return best_digits

from decimal import Decimal, ROUND_HALF_UP
from supabase import create_client, Client
from groq import Groq
from dotenv import load_dotenv
import urllib3
from datetime import datetime, timedelta, timezone
import redis 

try:
    from cryptography.fernet import Fernet
except Exception:
    Fernet = None

try:
    import qrcode
except Exception:
    qrcode = None

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
UAZAPI_BASE_URL = "https://free.uazapi.com" 
PUBLIC_BASE_URL = (os.getenv("PUBLIC_BASE_URL") or "").rstrip("/")
MAX_HISTORICO = 15





MAX_WEBHOOK_BODY_BYTES = int(os.getenv("MAX_WEBHOOK_BODY_BYTES", "262144"))  # 256 KB
MAX_INCOMING_TEXT_CHARS = int(os.getenv("MAX_INCOMING_TEXT_CHARS", "4000"))
MAX_BUFFER_TEXT_CHARS = int(os.getenv("MAX_BUFFER_TEXT_CHARS", "8000"))


MAX_QTD_ITEM = int(os.getenv("MAX_QTD_ITEM", "10"))

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
CRON_SECRET = os.getenv("CRON_SECRET")
ALLOW_ABANDONED_CLEANUP_WITHOUT_REDIS = (os.getenv("ALLOW_ABANDONED_CLEANUP_WITHOUT_REDIS", "1").strip() != "0")

MP_WEBHOOK_TOKEN = os.getenv("MP_WEBHOOK_TOKEN")
CRED_ENCRYPTION_KEY = os.getenv("CRED_ENCRYPTION_KEY")

def _money_2(value) -> float:
    """Round money to 2 decimals deterministically."""
    try:
        return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    except Exception:
        return float(value or 0)

def _fernet():
    if not CRED_ENCRYPTION_KEY or not Fernet:
        return None
    try:
        key = CRED_ENCRYPTION_KEY.encode() if isinstance(CRED_ENCRYPTION_KEY, str) else CRED_ENCRYPTION_KEY
        return Fernet(key)
    except Exception:
        return None

def decrypt_secret(enc: str) -> str:
    """Decrypts a Fernet-encrypted secret. Returns empty string when not possible."""
    if not enc:
        return ""
    f = _fernet()
    if not f:
        return ""
    try:
        return f.decrypt(enc.encode()).decode()
    except Exception:
        return ""

def _payer_email_from_cliente(cliente_zap: str) -> str:

    safe = re.sub(r"\D", "", str(cliente_zap or ""))
    if not safe:
        safe = "cliente"
    return f"{safe}@noemail.local"

def mp_create_pix_payment(access_token: str, *, amount: float, description: str, external_reference: str, payer_email: str):
    url = "https://api.mercadopago.com/v1/payments"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "transaction_amount": _money_2(amount),
        "description": (description or "")[:240],
        "payment_method_id": "pix",
        "external_reference": str(external_reference),
        "payer": {"email": payer_email},
    }
    r = requests.post(url, json=payload, headers=headers, timeout=20, verify=HTTP_VERIFY_TLS)
    r.raise_for_status()
    return r.json()

def mp_get_payment(access_token: str, payment_id: str):
    url = f"https://api.mercadopago.com/v1/payments/{payment_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.get(url, headers=headers, timeout=20, verify=HTTP_VERIFY_TLS)
    r.raise_for_status()
    return r.json()


HTTP_VERIFY_TLS = os.getenv("HTTP_VERIFY_TLS", "true").strip().lower() in ("1", "true", "yes")
if not HTTP_VERIFY_TLS:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

WEBHOOK_RATE_LIMIT_PER_MIN = int(os.getenv("WEBHOOK_RATE_LIMIT_PER_MIN", "60"))




MESSAGE_DEBOUNCE_SECONDS = float(os.getenv("MESSAGE_DEBOUNCE_SECONDS", "5").strip() or "5")




CART_ABANDONED_REMINDER_MIN = int(os.getenv("CART_ABANDONED_REMINDER_MIN", "10") or "10")
CART_ABANDONED_CANCEL_MIN = int(os.getenv("CART_ABANDONED_CANCEL_MIN", "15") or "15")
MAX_ABANDONED_SWEEP = int(os.getenv("MAX_ABANDONED_SWEEP", "200") or "200")

STATE_STALE_RESET_MIN = int(os.getenv("STATE_STALE_RESET_MIN", "120") or "120")  # 2h
MAX_STATE_RESET_SWEEP = int(os.getenv("MAX_STATE_RESET_SWEEP", "500") or "500")

REPEAT_ORDER_LOOKBACK_DAYS = int(os.getenv("REPEAT_ORDER_LOOKBACK_DAYS", "30") or "30")

AVALIACAO_DELAY_MIN = int(os.getenv("AVALIACAO_DELAY_MIN", "30") or "30")
MAX_AVALIACAO_SWEEP = int(os.getenv("MAX_AVALIACAO_SWEEP", "200") or "200")

REDIS_URL = os.getenv("REDIS_URL")
redis_client = None

if REDIS_URL:
    try:

        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        print("✅ Redis conectado!")
    except Exception as e:
        print(f"❌ Erro ao conectar Redis: {e}")

app = FastAPI()

from API.health_startup import startup_diagnostics

@app.on_event("startup")
async def _startup():
    startup_diagnostics()

@app.middleware("http")
async def _log_requests(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
    request.state.request_id = request_id
    try:
        response = await call_next(request)
        logger.info(
            "http %s %s -> %s | request_id=%s",
            request.method,
            request.url.path,
            getattr(response, "status_code", "?"),
            request_id,
        )
        response.headers["x-request-id"] = request_id
        return response
    except Exception:
        logger.exception("Unhandled exception in request | request_id=%s | path=%s", request_id, request.url.path)
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "internal_error", "request_id": request_id},
            headers={"x-request-id": request_id},
        )

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    groq_client = Groq(api_key=GROQ_API_KEY)
except Exception as e:
    print(f"❌ Erro Config: {e}")




def redis_set_cache(chave, dados, ttl=600):
    """Salva dicionário como JSON no Redis com tempo de vida (TTL)"""
    if redis_client:
        try:

            dado_str = json.dumps(dados, default=str)
            redis_client.setex(chave, ttl, dado_str)
        except Exception as e:
            print(f"⚠️ Erro Redis SET: {e}")

def redis_get_cache(chave):
    """Recupera dicionário do Redis"""
    if redis_client:
        try:
            dado_str = redis_client.get(chave)
            if dado_str:
                return json.loads(dado_str)
        except Exception as e:
            print(f"⚠️ Erro Redis GET: {e}")
    return None

def redis_add_buffer(cliente_zap, texto):
    """Adiciona mensagem na lista temporária do Redis"""
    if redis_client:
        chave = f"buffer:{cliente_zap}"
        try:
            redis_client.rpush(chave, texto)
         
            redis_client.expire(chave, 600)
        except Exception as e:
            print(f"⚠️ Erro Redis Buffer Add: {e}")
 
def redis_get_clear_buffer(cliente_zap):
    """Pega todas as mensagens e limpa a lista"""
    if redis_client:
        chave = f"buffer:{cliente_zap}"
        try:
            
            mensagens = redis_client.lrange(chave, 0, -1)
            if mensagens:
                redis_client.delete(chave) # Limpa após ler
                texto = " ".join(mensagens) # Junta tudo num texto só
                if MAX_BUFFER_TEXT_CHARS and len(texto) > MAX_BUFFER_TEXT_CHARS:

                    texto = texto[-MAX_BUFFER_TEXT_CHARS:]
                return texto
        except Exception as e:
            print(f"⚠️ Erro Redis Buffer Get: {e}")
    return ""

def _stable_event_id(payload: dict) -> str:
    """Best-effort extraction of a stable event/message id; falls back to hash of the payload."""
    if not isinstance(payload, dict):
        return ""

    msg = payload.get("message") or {}
    candidates = [
        msg.get("id"),
        msg.get("messageId"),
        msg.get("message_id"),
        msg.get("wamid"),
    ]

    key = msg.get("key") or {}
    if isinstance(key, dict):
        candidates.extend([
            key.get("id"),
            key.get("messageId"),
            key.get("message_id"),
        ])

    for c in candidates:
        if isinstance(c, str) and c.strip():
            return c.strip()

    try:
        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
    except Exception:
        return ""

def redis_claim_event_once(dedup_key: str, ttl_seconds: int = 3600) -> bool:
    """True if this process claims the event (first time), False if it's a duplicate."""
    if not redis_client:

        return True
    try:
        return bool(redis_client.set(dedup_key, "1", nx=True, ex=ttl_seconds))
    except Exception as e:
        print(f"⚠️ Erro Redis Dedup: {e}")
        return True

def redis_try_lock(lock_key: str, ttl_seconds: int = 30) -> bool:
    """Best-effort distributed lock to reduce duplicate processing across workers."""
    if not redis_client:
        return True
    try:
        return bool(redis_client.set(lock_key, "1", nx=True, ex=ttl_seconds))
    except Exception as e:
        print(f"⚠️ Erro Redis Lock: {e}")
        return True

def redis_acquire_lock(lock_key: str, ttl_seconds: int = 30) -> str:
    """Returns a lock token if acquired, else empty string. Uses Redis when available."""
    if not redis_client:
        return "local"
    token = uuid.uuid4().hex
    try:
        ok = redis_client.set(lock_key, token, nx=True, ex=ttl_seconds)
        return token if ok else ""
    except Exception as e:
        print(f"⚠️ Erro Redis Lock Acquire: {e}")

        return "local"

def redis_release_lock(lock_key: str, token: str) -> None:
    """Releases lock only if token matches. Best-effort (safe to call even when Redis is down)."""
    if not redis_client:
        return
    if not token or token == "local":
        return
    try:

        redis_client.eval(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            1,
            lock_key,
            token,
        )
    except Exception as e:
        print(f"⚠️ Erro Redis Lock Release: {e}")

def redis_rate_limit(key: str, limit: int, window_seconds: int) -> bool:
    """Returns True if allowed, False if rate-limited. Best-effort: allows when Redis is down."""
    if limit <= 0:
        return True
    if not redis_client:
        return True
    try:
        current = redis_client.incr(key)
        if current == 1:
            redis_client.expire(key, window_seconds)
        return current <= limit
    except Exception as e:
        print(f"⚠️ Erro Redis RateLimit: {e}")
        return True

processing_tasks = {}
processing_followup_tasks = {}
processing_task_phase = {}  # conv_key -> "sleep" | "processing"



def normalizar_texto(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if unicodedata.category(c) != 'Mn').lower()

def _is_retirada_text(txt_norm: str) -> bool:
    """Detecta intenção de retirada/pegar no local durante o checkout."""
    t = (txt_norm or "").strip()
    if not t:
        return False

    keywords = [
        "retirada", "retirar", "vou retirar",
        "vou buscar", "buscar", "pegar", "vou pegar",
        "pegar ai", "pegar aí", "passar ai", "passar aí",
        "no balcao", "no balcão", "balcao", "balcão",
        "pegar no local", "retirar no local", "retirada no local",
        "vou ai", "vou aí",
    ]
    return any(k in t for k in keywords)

def encontrar_melhor_match(termo_busca, lista_opcoes):
    termo_busca = normalizar_texto(termo_busca)
    termo_limpo = re.sub(r'\b(bairro|do|da|de|no|na|em|rua|moro|no)\b', '', termo_busca).strip()
    matches = difflib.get_close_matches(termo_limpo, lista_opcoes, n=1, cutoff=0.6) 
    return matches[0] if matches else None

def extrair_precos_do_cardapio(texto_cardapio):
    tabela = {}
    if not texto_cardapio: return tabela
    padrao = r"(?:^|\n)[ \t]*[-*]?[ \t]*(.+?)(?::| -| --|R\$)\s*R?\$?\s*(\d+[.,]\d{2})"
    for linha in texto_cardapio.split('\n'):
        match = re.search(padrao, linha)
        if match:
            item_nome = match.group(1).strip()
            tabela[normalizar_texto(item_nome)] = float(match.group(2).replace(',', '.'))
            palavras = item_nome.split()
            if len(palavras) > 0:
                tabela[normalizar_texto(palavras[0])] = float(match.group(2).replace(',', '.'))
    return tabela

def carregar_taxas_bairros(restaurante_db_id):
    """
    Busca bairros na tabela 'bairros' e retorna um dicionário {nome_norm: valor}
    """
    try:

        resp = supabase.table("bairros").select("*")\
            .eq("restaurante_id", restaurante_db_id)\
            .eq("ativo", True)\
            .execute()
        
        bairros = resp.data or []
        dict_taxas = {}
        
        for b in bairros:

            nome_norm = normalizar_texto(b['nome'])
            dict_taxas[nome_norm] = float(b['taxa'])
            

            dict_taxas[b['nome']] = float(b['taxa']) 
            
        return dict_taxas

    except Exception as e:
        print(f"❌ Erro ao carregar bairros: {e}")
        return {}

def extrair_taxas_entrega(texto_taxas):
    taxas = {}
    if not texto_taxas: return taxas
    padrao = r"(.*?)(?::| -| --|R\$)\s*R?\$?\s*([\d,.]+)"
    for linha in texto_taxas.split('\n'):
        match = re.search(padrao, linha)
        if match:
            bairro = normalizar_texto(match.group(1).strip())
            try: taxas[bairro] = float(match.group(2).replace(',', '.'))
            except: continue
    return taxas

def carregar_taxas_entrega_fresh(restaurante_db_id: int, dados_loja: dict | None = None) -> tuple[dict, float]:
    """Carrega taxas de entrega do banco, priorizando tabela `bairros`.

    Fallback: coluna `restaurantes.taxas_entrega` (texto) + `taxa_entrega_padrao`.
    Retorna: (dict_taxas, taxa_padrao)
    """
    taxas = {}
    taxa_padrao = 0.0

    try:
        taxas = carregar_taxas_bairros(int(restaurante_db_id)) or {}
    except Exception:
        taxas = {}

    try:
        if dados_loja is not None:
            taxa_padrao = float((dados_loja or {}).get("taxa_entrega_padrao") or 0.0)
    except Exception:
        taxa_padrao = 0.0

    try:
        r = (
            supabase.table("restaurantes")
            .select("taxas_entrega,taxa_entrega_padrao")
            .eq("id", int(restaurante_db_id))
            .limit(1)
            .execute()
        )
        if r.data:
            row = r.data[0] or {}
            if not taxa_padrao:
                try:
                    taxa_padrao = float(row.get("taxa_entrega_padrao") or 0.0)
                except Exception:
                    taxa_padrao = 0.0

            if not taxas:
                taxas_txt = row.get("taxas_entrega") or ""
                taxas = extrair_taxas_entrega(taxas_txt) or {}
    except Exception:

        pass

    return (taxas or {}), float(taxa_padrao or 0.0)

def atualizar_estoque_real_time(restaurante_id, nome_exato, delta_qtd):
    """
    Chama a função segura no banco de dados.
    Retorna: (sucesso: bool, mensagem_ou_dados: dict)
    """
    try:

        params = {
            "p_restaurante_id": int(restaurante_id),
            "p_nome_produto": nome_exato, # Passar nome OFICIAL, não 'coca'
            "p_delta_qtd": int(delta_qtd)
        }
        
        resp = supabase.rpc("movimentar_estoque_seguro", params).execute()
        resultado = resp.data # É o JSON que definimos no SQL

        if resultado.get('sucesso'):
            logger.info("Estoque atualizado | item=%s | novo=%s", nome_exato, resultado.get('novo_estoque'))
            return True, resultado
        else:

            msg = str(resultado.get('msg') or '')
            msg_norm = msg.strip().lower()
            esperado = (
                msg_norm == 'estoque insuficiente'
                or 'não encontrado' in msg_norm
                or 'nao encontrado' in msg_norm
                or 'not found' in msg_norm
            )
            if not esperado:
                logger.warning("Falha estoque | item=%s | msg=%s", nome_exato, msg)
            return False, resultado

    except Exception as e:
        print(f"❌ Erro RPC Estoque: {e}")
        return False, {"msg": str(e)}

def carregar_cardapio_estruturado(restaurante_db_id):
    try:

        resp = supabase.table("produtos").select("*")\
            .eq("restaurante_id", restaurante_db_id)\
            .eq("disponivel", True)\
            .execute()
        
        produtos = resp.data or []
        
        texto_para_ia = ""
        dict_precos = {}
        dict_estoque = {}
        dict_categorias = {}
        categorias = {}

        for p in produtos:
            estoque = p.get('estoque')
            


            
            cat = p.get('categoria', 'Outros')
            cat_norm = normalizar_texto(cat)
            if cat not in categorias: categorias[cat] = []
            

            nome_norm = normalizar_texto(p['nome'])
            preco = float(p['preco'])
            dict_precos[nome_norm] = preco
            dict_estoque[nome_norm] = estoque
            dict_categorias[nome_norm] = cat_norm
            

            partes = p['nome'].split()
            if len(partes) > 1:
                curto = normalizar_texto(partes[0])
                dict_precos[curto] = preco
                dict_estoque[curto] = estoque
                dict_categorias[curto] = cat_norm


            nome_visual = p['nome']
            if estoque is not None and estoque <= 0:
                nome_visual += " 🚫 (ESGOTADO)"
            

            p_visual = p.copy()
            p_visual['nome_display'] = nome_visual
            categorias[cat].append(p_visual)


        for cat, itens in categorias.items():
            texto_para_ia += f"\n--- {cat.upper()} ---\n"
            for item in itens:
                desc = f" ({item['descricao']})" if item.get('descricao') else ""

                texto_para_ia += f"- {item['nome_display']}{desc}: R$ {item['preco']:.2f}\n"
        
        return texto_para_ia, dict_precos, dict_estoque, dict_categorias

    except Exception as e:
        print(f"❌ Erro ao carregar produtos: {e}")
        return "", {}, {}, {}

def get_dados_restaurante(identificador, tipo="phone_id", force_refresh: bool = False):



    chave_cache = f"restaurante:{identificador}"
    dados_cache = None if force_refresh else redis_get_cache(chave_cache)
    
    if dados_cache:

        return dados_cache
    

    try:
        coluna = "phone_id" if tipo == "phone_id" else "instance_name"
        resp = supabase.table("restaurantes").select("*").eq(coluna, identificador).execute()
        
        if resp.data:
            dados = resp.data[0]
            restaurante_db_id = dados['id']
            

            txt_cardapio, dict_precos, dict_estoque, dict_categorias = carregar_cardapio_estruturado(restaurante_db_id)
            
            if txt_cardapio:
                dados['cardapio'] = txt_cardapio
                dados['precos_dict'] = dict_precos
                dados['estoque_dict'] = dict_estoque
                dados['categorias_dict'] = dict_categorias
            else:
                dados['precos_dict'] = extrair_precos_do_cardapio(dados.get('cardapio', ''))
                dados['estoque_dict'] = {} 
                dados['categorias_dict'] = {}
            
            taxas_dict = carregar_taxas_bairros(restaurante_db_id)

            if not taxas_dict:
                taxas_dict = extrair_taxas_entrega(dados.get('taxas_entrega', '') or '')
            dados['taxas_dict'] = taxas_dict or {}
            


            redis_set_cache(f"restaurante:{dados['phone_id']}", dados, 600)
            redis_set_cache(f"restaurante:{dados['instance_name']}", dados, 600)
            
            return dados
            
    except Exception as e: 
        print(f"❌ Erro Banco Rest: {e}")
        import traceback
        traceback.print_exc()
        
    return None

def get_pedido_ativo(cliente_zap, restaurante_db_id):  # <--- Recebe ID numérico agora
    try:
        resp = (
            supabase.table("pedidos").select("*")
            .eq("cliente_zap", cliente_zap)
            .eq("restaurante_id", restaurante_db_id)

            .in_("status", ["novo", "confirmado"])
            .order("created_at", desc=True).limit(1).execute()
        )
        if resp.data:
            return resp.data[0]
    except:
        pass
    return None

def get_pix_settings_for_restaurante(restaurante_db_id: int):
    """Carrega configuração de Pix (por restaurante). Retorna dict ou None."""
    try:
        resp = supabase.table("restaurantes").select(
            "id,pix_whatsapp_enabled,pix_provider,mp_access_token_enc,phone_id"
        ).eq("id", int(restaurante_db_id)).limit(1).execute()
        if not resp.data:
            return None

        row = resp.data[0]
        enabled = bool(row.get("pix_whatsapp_enabled"))
        provider = row.get("pix_provider") or "mercadopago"
        token_enc = row.get("mp_access_token_enc") or ""
        token = decrypt_secret(token_enc)
        return {
            "enabled": enabled,
            "provider": provider,
            "mp_token": token,
            "phone_id": row.get("phone_id"),
        }
    except Exception as e:
        print(f"❌ Erro ao carregar config Pix: {e}")
        return None

@app.get("/payments/qr/{payment_id}.png")
def payment_qr_png(payment_id: str):
    """Serviço simples de QR (PNG) para o Pix copia-e-cola associado ao payment_id."""
    if not qrcode:
        return Response(status_code=500, content=b"qrcode lib not installed")
    try:
        resp = supabase.table("pedidos").select("payment_qr_code").eq("payment_id", payment_id).limit(1).execute()
        if not resp.data:
            return Response(status_code=404, content=b"not found")
        qr_code = (resp.data[0] or {}).get("payment_qr_code")
        if not qr_code:
            return Response(status_code=404, content=b"no qr")

        img = qrcode.make(qr_code)
        import io
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return Response(content=buf.getvalue(), media_type="image/png")
    except Exception as e:
        print(f"❌ Erro QR PNG: {e}")
        return Response(status_code=500, content=b"error")

@app.post("/webhook/mercadopago")
async def webhook_mercadopago(request: Request):

    if MP_WEBHOOK_TOKEN:
        got = request.query_params.get("token") or request.headers.get("x-webhook-secret")
        if not got or got != MP_WEBHOOK_TOKEN:
            return "unauthorized"

    body = await request.json()
    try:
        data = body.get("data") or {}
        mp_payment_id = str(data.get("id") or body.get("id") or "").strip()
        if not mp_payment_id:
            return "ok"

        pedido_resp = supabase.table("pedidos").select(
            "id,restaurante_id,cliente_zap,total_valor,status,payment_status"
        ).eq("payment_id", mp_payment_id).limit(1).execute()
        if not pedido_resp.data:
            return "ok"
        pedido = pedido_resp.data[0]
        pedido_id = pedido.get("id")
        restaurante_db_id = pedido.get("restaurante_id")

        if (pedido.get("payment_status") or "").lower() == "approved":
            return "ok"

        settings = get_pix_settings_for_restaurante(int(restaurante_db_id))
        if not settings or not settings.get("mp_token"):
            return "ok"

        mp = mp_get_payment(settings["mp_token"], mp_payment_id)
        status = (mp.get("status") or "").lower()
        amount = _money_2(mp.get("transaction_amount") or 0)
        ext_ref = str(mp.get("external_reference") or "")

        expected_amount = _money_2(pedido.get("total_valor") or 0)
        if ext_ref and str(pedido_id) != ext_ref:
            supabase.table("pedidos").update({"payment_status": "reference_mismatch"}).eq("id", pedido_id).execute()
            return "ok"

        if abs(amount - expected_amount) > 0.01:
            supabase.table("pedidos").update({"payment_status": "amount_mismatch", "payment_amount": amount}).eq("id", pedido_id).execute()
            return "ok"

        updates = {
            "payment_provider": "mercadopago",
            "payment_id": mp_payment_id,
            "payment_status": status,
            "payment_amount": amount,
        }

        if status == "approved":
            updates["payment_paid_at"] = datetime.now(timezone.utc).isoformat()

            if (pedido.get("status") or "").lower() == "novo":
                updates["status"] = "confirmado"
                updates["forma_pagamento"] = "Pix (Pago no WhatsApp)"

        supabase.table("pedidos").update(updates).eq("id", pedido_id).execute()

        if status == "approved":
            try:
                r = supabase.table("restaurantes").select("phone_id").eq("id", int(restaurante_db_id)).limit(1).execute()
                if r.data:
                    phone_id = r.data[0].get("phone_id")
                    if phone_id:
                        enviar_zap(phone_id, pedido.get("cliente_zap"), f"✅ Pagamento aprovado! Pedido #{pedido_id} confirmado. Já vamos preparar.")
            except Exception:
                pass

    except Exception as e:
        print(f"❌ Erro webhook MP: {e}")
    return "ok"

def enviar_zap(phone_id, numero, texto):
    dados = get_dados_restaurante(phone_id, tipo="phone_id")
    if not dados:
        logger.error("enviar_zap: restaurante não encontrado | phone_id=%s", phone_id)
        return

    texto = (texto or "").strip()
    if not texto:
        logger.warning("enviar_zap: texto vazio (nada enviado) | numero=%s | phone_id=%s", numero, phone_id)
        return

    try:
        digits_phone_id = _only_digits(phone_id)
        digits_numero = _only_digits(numero)
        if digits_phone_id and digits_numero and digits_phone_id == digits_numero:
            logger.warning(
                "Self-test: enviando mensagem para o mesmo número do restaurante (pode não aparecer no WhatsApp) | phone_id=%s",
                phone_id,
            )

        timeout_base = int(os.getenv("UAZAPI_TIMEOUT", "15") or "15")
        max_attempts = int(os.getenv("UAZAPI_SEND_RETRIES", "2") or "2")
        token = (dados.get("instance_token") or "").strip()

        last_exc = None
        r = None
        for attempt in range(1, max_attempts + 1):
            try:

                timeout = (5, max(timeout_base, 10) + (attempt - 1) * 10)
                r = requests.post(
                    f"{UAZAPI_BASE_URL}/send/text",
                    json={"number": numero, "text": texto},
                    headers={"token": token, "Content-Type": "application/json"},
                    verify=HTTP_VERIFY_TLS,
                    timeout=timeout,
                )
                last_exc = None
                break
            except requests.exceptions.ReadTimeout as e:
                last_exc = e
            except requests.exceptions.ConnectionError as e:
                last_exc = e
            except requests.exceptions.Timeout as e:
                last_exc = e

            if attempt < max_attempts:

                time.sleep(0.6 * attempt)

        if r is None:
            raise last_exc or Exception("UAZAPI send failed (no response)")


        if logger.isEnabledFor(10):  # DEBUG
            logger.debug("UAZAPI response body | status=%s | body=%s", r.status_code, (r.text or "")[:1200])

        if r.status_code >= 400:
            logger.error(
                "UAZAPI send failed | status=%s | numero=%s | phone_id=%s | body=%s",
                r.status_code,
                numero,
                phone_id,
                (r.text or "")[:800],
            )
            if "not on whatsapp" in (r.text or "").lower():
                logger.error(
                    "UAZAPI indicates number is invalid/not registered on WhatsApp | parsed_numero=%s",
                    _only_digits(numero),
                )
        else:
            preview = texto.replace("\n", " ")[:120]
            logger.info(
                "UAZAPI send ok | status=%s | numero=%s | phone_id=%s | preview=%s",
                r.status_code,
                numero,
                phone_id,
                preview,
            )

    except Exception:
        logger.exception("Erro Envio Zap | numero=%s | phone_id=%s", numero, phone_id)



def get_estado(cliente_zap, phone_id):
    try:
        resp = supabase.table("clientes_estado").select("*")\
            .eq("cliente_zap", cliente_zap).eq("restaurante_id", phone_id).execute()
        if resp.data: return resp.data[0]
    except: pass
    return None

def set_estado(cliente_zap, phone_id, novo_estado, dados_extras=None):
    try:
        payload = {
            "cliente_zap": cliente_zap,
            "restaurante_id": phone_id,
            "estado_conversa": novo_estado,
            "ultima_mensagem_em": datetime.now(timezone.utc).isoformat()
        }
        if dados_extras:
            payload["dados_parciais"] = dados_extras

        supabase.table("clientes_estado").upsert(payload).execute()
    except Exception as e: print(f"❌ Erro Set Estado: {e}")

def touch_estado_last_message(cliente_zap: str, phone_id: str) -> None:
    """Atualiza ultima_mensagem_em sem mudar estado/dados (evita falso abandono)."""
    try:
        supabase.table("clientes_estado").update({
            "ultima_mensagem_em": datetime.now(timezone.utc).isoformat()
        }).eq("cliente_zap", cliente_zap).eq("restaurante_id", phone_id).execute()
    except Exception:
        pass

def _parse_dt_utc(value) -> datetime | None:
    if not value:
        return None
    try:
        s = str(value)
        if s.endswith("Z"):
            s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _pedido_has_pix_pending(pedido: dict) -> bool:
    try:
        st = (pedido.get("payment_status") or "").lower()
        pid = (pedido.get("payment_id") or "").strip()
        return bool(pid) and st in ("pending", "in_process")
    except Exception:
        return False

def _safe_dict(v):
    if isinstance(v, dict):
        return v
    if isinstance(v, str) and v.strip():
        try:
            parsed = json.loads(v)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}

def _is_greeting(txt_norm: str) -> bool:
    t = (txt_norm or "").strip()
    if not t:
        return False

    if len(t) > 30:
        return False
    greetings = [
        "oi", "ola", "olá", "bom dia", "boa tarde", "boa noite",
        "eai", "e aí", "opa", "menu", "cardapio", "cardápio",
    ]
    return any(t == g or t.startswith(g + " ") for g in greetings)

def _should_reset_state_by_inactivity(estado: str) -> bool:

    if not estado:
        return False
    est = estado.strip().upper()
    if est in ("INICIO",):
        return False
    if est in ("AGUARDANDO_PAGAMENTO_PIX",):
        return False
    return True

def _reset_state_if_stale(cliente_zap: str, phone_id: str, estado_data: dict | None) -> bool:
    if not estado_data:
        return False

    estado = (estado_data.get("estado_conversa") or "INICIO")
    if not _should_reset_state_by_inactivity(estado):
        return False

    last_dt = _parse_dt_utc(estado_data.get("ultima_mensagem_em"))
    if not last_dt:
        return False

    minutes_inactive = int((datetime.now(timezone.utc) - last_dt).total_seconds() // 60)
    if minutes_inactive < int(STATE_STALE_RESET_MIN or 0):
        return False


    set_estado(cliente_zap, phone_id, "INICIO", {})
    return True

def _get_last_finalizado(restaurante_db_id: int, cliente_zap: str):
    """Último pedido finalizado (pra 'pedir o de sempre')."""
    try:
        th = (datetime.now(timezone.utc) - timedelta(days=REPEAT_ORDER_LOOKBACK_DAYS)).isoformat()
        r = (
            supabase.table("pedidos")
            .select("id,carrinho_json,resumo_pedido,total_valor,finalizado_em,status")
            .eq("cliente_zap", cliente_zap)
            .eq("restaurante_id", int(restaurante_db_id))
            .eq("status", "finalizado")
            .gte("finalizado_em", th)
            .order("finalizado_em", desc=True)
            .limit(1)
            .execute()
        )
        return (r.data or [None])[0]
    except Exception:
        return None

def _send_repeat_offer(phone_id: str, cliente_zap: str, last_pedido: dict) -> None:
    pedido_id = int(last_pedido.get("id") or 0)
    resumo = (last_pedido.get("resumo_pedido") or "").replace("|", "\n")
    try:
        total = float(last_pedido.get("total_valor") or 0.0)
    except Exception:
        total = 0.0

    msg = (
        "👋 Oi! Quer pedir *o de sempre*?\n\n"
        f"🧾 *Último pedido (#{pedido_id}):*\n{(resumo or 'Sem resumo')}\n"
        f"💰 Total (sem taxa): R$ {total:.2f}\n\n"
        "Responda:\n"
        "1 - Sim (repetir)\n"
        "2 - Não (fazer outro pedido)"
    )
    enviar_zap(phone_id, cliente_zap, msg)
    try:
        supabase.table("conversas").insert({
            "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
        }).execute()
    except Exception:
        pass

    set_estado(cliente_zap, phone_id, "CONFIRMAR_PEDIDO_DE_SEMPRE", {"pedido_id_repetir": pedido_id})

def _repeat_order_from_finalizado(restaurante_db_id: int, cliente_zap: str, pedido_id: int):
    """Cria um novo pedido 'novo' copiando carrinho do pedido finalizado, com reserva de estoque."""
    try:

        r = supabase.table("pedidos").select(
            "id,carrinho_json,resumo_pedido,total_valor,status"
        ).eq("id", int(pedido_id)).limit(1).execute()
        if not (r.data or []):
            return False, "Não achei seu último pedido para repetir."

        old = r.data[0] or {}
        carrinho = _safe_dict(old.get("carrinho_json"))
        if not carrinho:
            return False, "Seu último pedido não tem itens para repetir."


        reserved = []
        for chave_item, dados_item in (carrinho or {}).items():
            try:
                qtd = int((dados_item or {}).get("qtd") or 0)
            except Exception:
                qtd = 0
            if qtd <= 0:
                continue
            ok, info = atualizar_estoque_real_time(int(restaurante_db_id), str(chave_item), -qtd)
            if not ok:

                for it_name, it_qtd in reserved:
                    try:
                        atualizar_estoque_real_time(int(restaurante_db_id), it_name, +it_qtd)
                    except Exception:
                        pass
                msg_fail = (info or {}).get("msg") or "Estoque insuficiente para repetir o pedido."
                return False, str(msg_fail)
            reserved.append((str(chave_item), qtd))

        payload = {
            "cliente_zap": cliente_zap,
            "restaurante_id": int(restaurante_db_id),
            "carrinho_json": carrinho,
            "resumo_pedido": old.get("resumo_pedido") or "",
            "total_valor": float(old.get("total_valor") or 0.0),
            "status": "novo",
        }
        supabase.table("pedidos").insert(payload).execute()
        return True, "✅ Perfeito! Repeti seu pedido. Se quiser finalizar, diga *pode fechar*."
    except Exception:
        return False, "Tive um erro ao tentar repetir o pedido. Pode me dizer o que você quer pedir?"



def _cron_authed(request: Request) -> bool:
    if not CRON_SECRET:
        return True
    got = request.query_params.get("token") or request.headers.get("x-cron-secret")
    return bool(got and got == CRON_SECRET)

def _redis_setnx_once(key: str, ttl_seconds: int) -> bool:
    """Best-effort dedup. If Redis is down, returns False (safer: do nothing)."""
    if not redis_client:
        return False
    try:
        return bool(redis_client.set(key, "1", nx=True, ex=ttl_seconds))
    except Exception:
        return False

def _abandoned_cleanup_pedido(restaurante_db_id: int, phone_id: str, cliente_zap: str, pedido: dict) -> bool:
    """Cancela + limpa carrinho + devolve estoque (idempotência via Redis lock/dedup)."""
    if not redis_client:
        return False  # sem Redis, risco de dupla devolução de estoque
    pedido_id = pedido.get("id")
    if not pedido_id:
        return False


    once_key = f"abandoned:cleanup:once:{pedido_id}"
    if not _redis_setnx_once(once_key, ttl_seconds=24 * 3600):
        return False

    lock_key = f"lock:abandoned:cleanup:{pedido_id}"
    token = redis_acquire_lock(lock_key, ttl_seconds=60)
    if not token:
        return False

    carrinho = _safe_dict(pedido.get("carrinho_json"))
    try:

        supabase.table("pedidos").update({
            "status": "cancelado",
            "carrinho_json": {},
            "resumo_pedido": "Carrinho vazio",
            "total_valor": 0,
        }).eq("id", int(pedido_id)).execute()


        for chave_item, dados_item in (carrinho or {}).items():
            try:
                qtd = int((dados_item or {}).get("qtd") or 0)
            except Exception:
                qtd = 0
            if qtd > 0:
                atualizar_estoque_real_time(restaurante_db_id, chave_item, +qtd)


        set_estado(cliente_zap, phone_id, "INICIO", {})


        msg = "⏳ Seu carrinho expirou por inatividade e foi cancelado. Se quiser, é só me dizer o que você gostaria de pedir."
        enviar_zap(phone_id, cliente_zap, msg)
        try:
            supabase.table("conversas").insert({
                "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
            }).execute()
        except Exception:
            pass

        return True
    finally:
        redis_release_lock(lock_key, token)

def _sanitize_ia_response(raw):
    allowed = {
        "adicionar_item",
        "fixar_item",
        "remover_item",
        "adicionar_observacao",
        "pedir_fechamento",
        "cancelar",
        "perguntar",
    }
    if not isinstance(raw, dict):
        return "perguntar", "Não entendi. Pode repetir de um jeito mais simples?", []

    intent = raw.get("intencao")
    if not isinstance(intent, str):
        intent = "perguntar"
    intent = intent.strip()
    if intent not in allowed:
        intent = "perguntar"

    msg = raw.get("mensagem")
    if not isinstance(msg, str):
        msg = ""
    msg = msg.strip()

    itens = raw.get("itens")
    if not isinstance(itens, list):
        itens = []

    itens_ok = []
    for it in itens[:30]:
        if not isinstance(it, dict):
            continue
        nome = it.get("nome")
        if not isinstance(nome, str):
            continue
        nome = nome.strip()
        if not nome:
            continue

        qtd = it.get("qtd", 1)
        try:
            qtd = int(qtd)
        except Exception:
            qtd = 1

        obs = it.get("observacao")
        if obs is not None and not isinstance(obs, str):
            obs = ""

        itens_ok.append({"nome": nome, "qtd": qtd, "observacao": (obs or "").strip()})

    if not msg:

        msg = "Certo! O que mais você gostaria de pedir?"

    return intent, msg, itens_ok

def _abandoned_send_reminder(phone_id: str, cliente_zap: str, pedido: dict, minutes_left: int) -> bool:
    """Envia lembrete 1x (dedup via Redis)."""
    if not redis_client:
        return False
    pedido_id = pedido.get("id")
    if not pedido_id:
        return False
    once_key = f"abandoned:reminder:once:{pedido_id}"
    if not _redis_setnx_once(once_key, ttl_seconds=24 * 3600):
        return False

    resumo = (pedido.get("resumo_pedido") or "").replace("|", "\n")
    try:
        total = float(pedido.get("total_valor") or 0.0)
    except Exception:
        total = 0.0

    msg = (
        "👋 Vi que você não completou o pedido.\n\n"
        f"🛒 *Seu carrinho:*\n{(resumo or 'Carrinho vazio')}\n"
        f"💰 Total (sem taxa): R$ {total:.2f}\n\n"
        f"Se quiser continuar, me diga o endereço ou responda *pode fechar*.\n"
        f"(Seu carrinho expira em ~{max(1, minutes_left)} min por inatividade.)"
    )
    enviar_zap(phone_id, cliente_zap, msg)
    try:
        supabase.table("conversas").insert({
            "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
        }).execute()
    except Exception:
        pass
    return True



def _avaliacao_msg(pedido_id: int) -> str:
    return (
        f"⭐ Avaliação do atendimento (Pedido #{pedido_id})\n\n"
        "De 1 a 5, qual nota você dá para o atendimento?\n"
        "Responda apenas com um número: 1, 2, 3, 4 ou 5."
    )

def _avaliacao_mark_sent(pedido_id: int) -> None:
    try:
        supabase.table("pedidos").update({"msg_avaliacao_enviada": True}).eq("id", int(pedido_id)).execute()
    except Exception:
        pass

def _avaliacao_send(phone_id: str, cliente_zap: str, pedido_id: int) -> bool:
    if not phone_id or not cliente_zap or not pedido_id:
        return False


    if redis_client:
        once_key = f"avaliacao:once:{pedido_id}"
        if not _redis_setnx_once(once_key, ttl_seconds=7 * 24 * 3600):
            return False

    msg = _avaliacao_msg(int(pedido_id))


    _avaliacao_mark_sent(int(pedido_id))


    set_estado(cliente_zap, phone_id, "AGUARDANDO_AVALIACAO_POS_VENDA", {"pedido_id_avaliacao": int(pedido_id)})


    enviar_zap(phone_id, cliente_zap, msg)
    try:
        supabase.table("conversas").insert({
            "cliente_zap": cliente_zap,
            "restaurante_id": phone_id,
            "role": "assistant",
            "mensagem": msg
        }).execute()
    except Exception:
        pass

    return True


import logging
from pythonjsonlogger import jsonlogger


log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
log_datefmt = "%Y-%m-%d %H:%M:%S"
log_level = logging.INFO


file_handler = logging.FileHandler("api.log")
file_handler.setLevel(log_level)
formatter = jsonlogger.JsonFormatter()
file_handler.setFormatter(formatter)


console_handler = logging.StreamHandler()
console_handler.setLevel(log_level)
console_handler.setFormatter(formatter)


logging.basicConfig(
    level=log_level,
    handlers=[
        file_handler,
        console_handler,
    ],
    format=log_format,
    datefmt=log_datefmt,
)

logger = logging.getLogger("api")



@app.get("/health")
def health_check():
    return {"status": "ok"}






@app.get("/cron/abandoned-carts")
async def cron_abandoned_carts(request: Request):
    """Lembrete + limpeza de carrinho abandonado, com devolução de estoque."""
    if not _cron_authed(request):
        return {"status": "unauthorized"}

    if (not redis_client) and (not ALLOW_ABANDONED_CLEANUP_WITHOUT_REDIS):
        return {"status": "skipped", "detail": "redis_missing_and_fallback_disabled"}

    now = datetime.now(timezone.utc)
    reminded = 0
    cleaned = 0
    scanned = 0

    th_remind = (now - timedelta(minutes=CART_ABANDONED_REMINDER_MIN)).isoformat()
    th_cancel = (now - timedelta(minutes=CART_ABANDONED_CANCEL_MIN)).isoformat()

    try:
        resp = (
            supabase.table("clientes_estado")
            .select("cliente_zap,restaurante_id,estado_conversa,ultima_mensagem_em")
            .lt("ultima_mensagem_em", th_remind)
            .limit(MAX_ABANDONED_SWEEP)
            .execute()
        )
        rows = resp.data or []
    except Exception as e:
        return {"status": "erro", "detalhe": str(e)}

    for row in rows:
        scanned += 1
        cliente_zap = row.get("cliente_zap")
        phone_id = row.get("restaurante_id")  # aqui é phone_id (texto)
        estado = (row.get("estado_conversa") or "").strip().upper()
        last_dt = _parse_dt_utc(row.get("ultima_mensagem_em"))
        if not cliente_zap or not phone_id or not last_dt:
            continue

        if estado in ("AGUARDANDO_AVALIACAO_POS_VENDA",):
            continue

        minutes_inactive = int((now - last_dt).total_seconds() // 60)
        if minutes_inactive < CART_ABANDONED_REMINDER_MIN:
            continue

        dados_loja = get_dados_restaurante(phone_id, tipo="phone_id")
        if not dados_loja:
            continue
        restaurante_db_id = int(dados_loja.get("id") or 0)
        if not restaurante_db_id:
            continue

        pedido = get_pedido_ativo(cliente_zap, restaurante_db_id)
        if not pedido:
            continue
        if (pedido.get("status") or "").lower() != "novo":
            continue


        if estado == "AGUARDANDO_PAGAMENTO_PIX" or _pedido_has_pix_pending(pedido):
            continue

        if minutes_inactive >= CART_ABANDONED_CANCEL_MIN:
            if _abandoned_cleanup_pedido(restaurante_db_id, phone_id, cliente_zap, pedido):
                cleaned += 1
            continue


        if not redis_client:
            continue  # sem Redis, evita duplicar em múltiplos workers

        minutes_left = CART_ABANDONED_CANCEL_MIN - minutes_inactive
        if _abandoned_send_reminder(phone_id, cliente_zap, pedido, minutes_left=minutes_left):
            reminded += 1

    return {"status": "ok", "scanned": scanned, "reminded": reminded, "cleaned": cleaned}


@app.get("/cron/reset-states")
async def cron_reset_states(request: Request):
    """Reseta estados travados após inatividade (volta para INICIO)."""
    if not _cron_authed(request):
        return {"status": "unauthorized"}

    now = datetime.now(timezone.utc)
    th = (now - timedelta(minutes=STATE_STALE_RESET_MIN)).isoformat()

    try:
        resp = (
            supabase.table("clientes_estado")
            .select("cliente_zap,restaurante_id,estado_conversa,ultima_mensagem_em")
            .neq("estado_conversa", "INICIO")
            .lt("ultima_mensagem_em", th)
            .limit(MAX_STATE_RESET_SWEEP)
            .execute()
        )
        rows = resp.data or []
    except Exception as e:
        return {"status": "erro", "detalhe": str(e)}

    reset = 0
    for row in rows:
        cliente_zap = (row.get("cliente_zap") or "").strip()
        phone_id = (row.get("restaurante_id") or "").strip()
        estado = (row.get("estado_conversa") or "").strip().upper()
        if not cliente_zap or not phone_id:
            continue
        if not _should_reset_state_by_inactivity(estado):
            continue

        set_estado(cliente_zap, phone_id, "INICIO", {})
        reset += 1

    return {"status": "ok", "scanned": len(rows), "reset": reset}


@app.get("/cron/avaliar")
async def cron_avaliar(request: Request):
    """Envia avaliação 1..5 após X minutos do pedido finalizado."""
    if not _cron_authed(request):
        return {"status": "unauthorized"}

    now = datetime.now(timezone.utc)
    th = (now - timedelta(minutes=AVALIACAO_DELAY_MIN)).isoformat()

    try:
        resp = (
            supabase.table("pedidos")
            .select("id,cliente_zap,restaurante_id,status,finalizado_em,msg_avaliacao_enviada,avaliacao")
            .eq("status", "finalizado")
            .eq("msg_avaliacao_enviada", False)
            .lt("finalizado_em", th)
            .limit(MAX_AVALIACAO_SWEEP)
            .execute()
        )
        pedidos = resp.data or []
    except Exception as e:
        return {"status": "erro", "detalhe": str(e)}

    if not pedidos:
        return {"status": "ok", "scanned": 0, "sent": 0}


    rest_ids = sorted({int(p.get("restaurante_id") or 0) for p in pedidos if p.get("restaurante_id")})
    rest_map = {}
    try:
        if rest_ids:
            r = supabase.table("restaurantes").select("id,phone_id").in_("id", rest_ids).execute()
            for row in (r.data or []):
                rid = int(row.get("id") or 0)
                pid = (row.get("phone_id") or "").strip()
                if rid and pid:
                    rest_map[rid] = pid
    except Exception:
        rest_map = {}

    sent = 0
    for p in pedidos:
        try:
            pedido_id = int(p.get("id") or 0)
            cliente_zap = (p.get("cliente_zap") or "").strip()
            rest_id = int(p.get("restaurante_id") or 0)
            phone_id = rest_map.get(rest_id, "")
            if not pedido_id or not cliente_zap or not phone_id:
                continue


            if p.get("avaliacao") is not None:
                continue

            if _avaliacao_send(phone_id, cliente_zap, pedido_id):
                sent += 1
        except Exception:
            continue

    return {"status": "ok", "scanned": len(pedidos), "sent": sent}


@app.post("/webhook")
async def webhook(request: Request):

    try:
        raw = await request.body()
        if MAX_WEBHOOK_BODY_BYTES and len(raw) > MAX_WEBHOOK_BODY_BYTES:
            return JSONResponse(status_code=413, content={"ok": False, "error": "payload_too_large"})

        body = json.loads(raw.decode("utf-8") or "{}") if raw else {}
        msg_data = body.get("message") or {}

        instance = (body.get("instanceName") or body.get("instance") or "").strip()
        if not instance:
            return "ok"

        if msg_data.get("fromMe"):
            return "ok"

        texto = (msg_data.get("text") or msg_data.get("conversation") or "").strip()
        if not texto:
            return "ok"
        if MAX_INCOMING_TEXT_CHARS and len(texto) > MAX_INCOMING_TEXT_CHARS:
            texto = texto[:MAX_INCOMING_TEXT_CHARS]

        restaurante = get_dados_restaurante(instance, tipo="instance_name")
        if not restaurante:
            return "ok"


        event_id = _stable_event_id(body)
        if event_id:
            dedup_key = f"dedup:webhook:{instance}:{event_id}"
            if not redis_claim_event_once(dedup_key, ttl_seconds=6 * 3600):
                return "ok"

        phone_id = restaurante.get("phone_id")
        if not phone_id:
            return "ok"

        remote_jid = (msg_data.get("chatid") or msg_data.get("sender") or msg_data.get("remoteJid") or msg_data.get("from") or "")
        cliente_zap = _extract_cliente_zap(body, msg_data, phone_id) or _only_digits(remote_jid.split("@")[0]) or remote_jid.split("@")[0]
        if not cliente_zap:
            return "ok"


        rl_key = f"ratelimit:webhook:{phone_id}:{cliente_zap}"
        if not redis_rate_limit(rl_key, WEBHOOK_RATE_LIMIT_PER_MIN, 60):
            return "ok"

        nome = msg_data.get("senderName") or "Cliente"


        buffer_key = f"{instance}:{cliente_zap}"


        redis_add_buffer(buffer_key, texto)


        if buffer_key in processing_tasks:
            try:
                processing_tasks[buffer_key].cancel()
            except Exception:
                pass

        processing_tasks[buffer_key] = asyncio.create_task(
            executar_ia_com_delay(buffer_key, cliente_zap, phone_id, nome)
        )

    except Exception as e:
        print(f"Erro Webhook: {e}")
    return "ok"

async def executar_ia_com_delay(buffer_key: str, cliente_zap: str, phone_id: str, nome: str):
    try:
        await asyncio.sleep(3)  # Espera 3 segundos para juntar mensagens

        texto_final = redis_get_clear_buffer(buffer_key)
        if texto_final:
            await processar_mensagem_final(phone_id, cliente_zap, nome, texto_final)
    except asyncio.CancelledError:
        return
    except Exception as e:
        print(f"Erro executar_ia_com_delay: {e}")

async def processar_mensagem_final(phone_id, cliente_zap, nome_cliente, texto_completo):
    dados_loja = get_dados_restaurante(phone_id, tipo="phone_id")
    if not dados_loja or not dados_loja.get("bot_ativo", True):
        return


    try:
        restaurante_db_id = int(dados_loja["id"])
    except Exception:
        return

    texto_completo = (texto_completo or "").strip()
    if not texto_completo:
        return

    print(f"📩 Msg de {nome_cliente}: {texto_completo}")


    txt_norm = normalizar_texto(texto_completo)

    estado_data = get_estado(cliente_zap, phone_id)
    estado_atual = (estado_data["estado_conversa"] if estado_data else "INICIO") or "INICIO"
    dados_parciais = (estado_data.get("dados_parciais") or {}) if estado_data else {}


    pedido_ativo = get_pedido_ativo(cliente_zap, restaurante_db_id)



    try:
        if _reset_state_if_stale(cliente_zap, phone_id, estado_data):
            estado_data = get_estado(cliente_zap, phone_id)
            estado_atual = (estado_data["estado_conversa"] if estado_data else "INICIO") or "INICIO"
            dados_parciais = (estado_data.get("dados_parciais") or {}) if estado_data else {}
    except Exception:
        pass


    if (not pedido_ativo) and (estado_atual == "INICIO") and _is_greeting(txt_norm):
        last_final = _get_last_finalizado(restaurante_db_id, cliente_zap)
        if last_final:
            _send_repeat_offer(phone_id, cliente_zap, last_final)
            return



    if pedido_ativo and (pedido_ativo.get("status") or "").lower() == "em_preparo":
        enviar_zap(
            phone_id,
            cliente_zap,
            "⚠️ Seu pedido já está em preparo e não pode mais ser alterado.\n"
            "Caso precise falar com o restaurante, aguarde o atendimento humano.",
        )
        return

    bairros_dict = dados_loja.get("taxas_dict", {}) or {}
    lista_bairros_txt = ", ".join([str(b).title() for b in bairros_dict.keys()])




    palavras_comando = [
        "mudar", "trocar", "alterar", "adicionar", "remover", "tira", "poe", "põe",
        "esquece", "cancelar", "nao quero", "não quero", "quero mais", "obs", "observacao", "observação", "sem",
    ]
    possivel_mudanca = any(p in txt_norm for p in palavras_comando)
    if estado_atual != "INICIO" and possivel_mudanca:
        estado_atual = "FORCAR_IA"





    if estado_atual == "AGUARDANDO_AVALIACAO_POS_VENDA":
        try:
            nota = int(re.sub(r"\D", "", texto_completo))
            if 1 <= nota <= 5:
                pedido_id = dados_parciais.get("pedido_id_avaliacao")
                if pedido_id:
                    supabase.table("pedidos").update({"avaliacao": nota}).eq("id", pedido_id).execute()

                if nota == 5:
                    msg = "Uau! 😍 Muito obrigado! Sua avaliação ajuda muito!"
                elif nota >= 4:
                    msg = "Obrigado! Fico feliz que tenha gostado! 👍"
                else:
                    msg = "Poxa, obrigado pelo feedback. Vamos melhorar! 🙏"

                enviar_zap(phone_id, cliente_zap, msg)
                set_estado(cliente_zap, phone_id, "INICIO")
            else:
                enviar_zap(phone_id, cliente_zap, "Por favor, digite apenas uma nota de 1 a 5. ⭐")
        except Exception:
            enviar_zap(phone_id, cliente_zap, "Não entendi a nota. Poderia digitar um número de 1 a 5?")
        return


    if estado_atual == "CONFIRMAR_PEDIDO_DE_SEMPRE":
        t = (txt_norm or "").strip()
        pedido_id = int((dados_parciais or {}).get("pedido_id_repetir") or 0)

        if t in ("1", "sim", "s", "yes"):
            ok, msg = _repeat_order_from_finalizado
            enviar_zap(phone_id, cliente_zap, msg)
            set_estado(cliente_zap, phone_id, "INICIO", {})
            return

        if t in ("2", "nao", "não", "n"):
            enviar_zap(phone_id, cliente_zap, "Beleza! Me diga o que você gostaria de pedir hoje. 😊")
            set_estado(cliente_zap, phone_id, "INICIO", {})
            return

        enviar_zap(phone_id, cliente_zap, "Responda 1 (sim) para repetir ou 2 (não) para fazer outro pedido.")
        return

    

    if estado_atual == "AGUARDANDO_ENDERECO":
        bairro_match = encontrar_melhor_match(texto_completo, list(bairros_dict.keys()))
        if bairro_match:
            taxa = float(bairros_dict[bairro_match])
            novos_dados = {"endereco_txt": texto_completo, "bairro": bairro_match, "taxa": taxa}
            set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", novos_dados)

            total_prod = float(pedido_ativo["total_valor"]) if pedido_ativo else 0.0
            total_com_taxa = total_prod + taxa

            msg = (
                f"📍 Identifiquei: *{str(bairro_match).title()}* (Taxa: R$ {taxa:.2f}).\n"
                f"💰 *Total Final: R$ {total_com_taxa:.2f}*\n\n"
                "Qual a forma de pagamento? (Pix, Dinheiro ou Cartão)"
            )
            enviar_zap(phone_id, cliente_zap, msg)
            try:
                supabase.table("conversas").insert({
                    "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
                }).execute()
            except Exception:
                pass
        else:
            msg = (
                "🤔 Não consegui identificar um bairro atendido no seu texto.\n\n"
                f"Atendemos apenas em: {lista_bairros_txt}.\n"
                "Por favor, digite o endereço novamente com o nome do bairro."
            )
            enviar_zap(phone_id, cliente_zap, msg)
            try:
                supabase.table("conversas").insert({
                    "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
                }).execute()
            except Exception:
                pass
        return

    if estado_atual == "AGUARDANDO_PAGAMENTO":
        pgto_limpo = txt_norm
        forma_escolhida = None

        if "pix" in pgto_limpo:
            forma_escolhida = "Pix"
        elif "dinheiro" in pgto_limpo or "especie" in pgto_limpo or "espécie" in pgto_limpo:
            forma_escolhida = "Dinheiro"
        elif "cartao" in pgto_limpo or "cartão" in pgto_limpo or "credito" in pgto_limpo or "crédito" in pgto_limpo or "debito" in pgto_limpo or "débito" in pgto_limpo:
            forma_escolhida = "Cartão"

        if forma_escolhida:
            endereco_final = dados_parciais.get("endereco_txt", "Endereço não capturado")
            bairro_final = dados_parciais.get("bairro", "")
            taxa_final = float(dados_parciais.get("taxa", 0.0) or 0.0)

            total_final = 0.0
            if pedido_ativo:
                total_final = float(pedido_ativo["total_valor"] or 0.0) + taxa_final


                pix_created = False
                pix_payload = None
                pix_settings = None

                if forma_escolhida == "Pix":
                    pix_settings = get_pix_settings_for_restaurante(int(restaurante_db_id))


                update_base = {
                    "endereco_completo": f"{endereco_final} ({bairro_final})",
                    "tipo_entrega": "entrega",
                    "forma_pagamento": forma_escolhida,
                    "total_valor": total_final,
                    "status": "confirmado",  # comportamento atual
                }


                if (
                    forma_escolhida == "Pix"
                    and pix_settings
                    and pix_settings.get("enabled")
                    and (pix_settings.get("provider") or "mercadopago") == "mercadopago"
                    and pix_settings.get("mp_token")
                ):
                    update_base["status"] = "novo"
                    update_base["forma_pagamento"] = "Pix (Aguardando pagamento)"

                supabase.table("pedidos").update(update_base).eq("id", pedido_ativo["id"]).execute()

                if (
                    forma_escolhida == "Pix"
                    and pix_settings
                    and pix_settings.get("enabled")
                    and (pix_settings.get("provider") or "mercadopago") == "mercadopago"
                    and pix_settings.get("mp_token")
                ):
                    try:
                        pix_payload = mp_create_pix_payment(
                            pix_settings["mp_token"],
                            amount=total_final,
                            description=f"Pedido #{pedido_ativo['id']}",  # ✅ fecha a string corretamente
                            external_reference=str(pedido_ativo["id"]),
                            payer_email=_payer_email_from_cliente(cliente_zap),
                        )

                        payment_id = str(pix_payload.get("id") or "").strip()
                        payment_status = str(pix_payload.get("status") or "pending").strip().lower()

                        poi = (pix_payload.get("point_of_interaction") or {}) if isinstance(pix_payload, dict) else {}
                        tx = (poi.get("transaction_data") or {}) if isinstance(poi, dict) else {}

                        payment_qr_code = (tx.get("qr_code") or "") if isinstance(tx, dict) else ""
                        payment_ticket_url = (tx.get("ticket_url") or "") if isinstance(tx, dict) else ""

                        if payment_id:
                            supabase.table("pedidos").update({
                                "payment_provider": "mercadopago",
                                "payment_id": payment_id,
                                "payment_status": payment_status,
                                "payment_amount": _money_2(total_final),
                                "payment_qr_code": payment_qr_code,
                                "payment_ticket_url": payment_ticket_url,
                            }).eq("id", pedido_ativo["id"]).execute()
                            pix_created = True
                    except Exception as e:
                        print(f"❌ Erro ao criar Pix MP: {e}")


            if forma_escolhida == "Pix" and pedido_ativo:
                if pix_created:
                    payment_id = str((pix_payload or {}).get("id") or "").strip()
                    poi = ((pix_payload or {}).get("point_of_interaction") or {}) if isinstance(pix_payload, dict) else {}
                    tx = (poi.get("transaction_data") or {}) if isinstance(poi, dict) else {}
                    qr_code = (tx.get("qr_code") or "") if isinstance(tx, dict) else ""
                    ticket_url = (tx.get("ticket_url") or "") if isinstance(tx, dict) else ""

                    qr_png_url = ""
                    if PUBLIC_BASE_URL and payment_id:
                        qr_png_url = f"{PUBLIC_BASE_URL}/payments/qr/{payment_id}.png"

                    msg = (
                        "✅ *Pedido recebido!*\n\n"
                        f"📍 Entrega em: {endereco_final}\n"
                        f"💰 Total: R$ {total_final:.2f}\n\n"
                        "💠 *Pix (pague para confirmar):*\n"
                        + (f"🔗 Link: {ticket_url}\n" if ticket_url else "")
                        + (f"🖼️ QR Code (imagem): {qr_png_url}\n" if qr_png_url else "")
                        + ("\n📋 *Copia e cola:*\n" + qr_code if qr_code else "")
                    )
                else:

                    msg = (
                        "✅ *Pedido Confirmado!*\n\n"
                        f"📍 Entrega em: {endereco_final}\n"
                        f"💳 Pagamento: {forma_escolhida}\n"
                        f"💰 Total: R$ {total_final:.2f}\n\n"
                        "⏳ Seu pedido será enviado para a cozinha em instantes."
                    )

                enviar_zap(phone_id, cliente_zap, msg)
                set_estado(cliente_zap, phone_id, "INICIO", {})
                try:
                    supabase.table("conversas").insert({
                        "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
                    }).execute()
                except Exception:
                    pass
                return

            msg = (
                "✅ *Pedido Confirmado!*\n\n"
                f"📍 Entrega em: {endereco_final}\n"
                f"💳 Pagamento: {forma_escolhida}\n"
                f"💰 Total: R$ {total_final:.2f}\n\n"
                "⏳ Seu pedido será enviado para a cozinha em instantes."
            )
            enviar_zap(phone_id, cliente_zap, msg)
            set_estado(cliente_zap, phone_id, "INICIO", {})
            try:
                supabase.table("conversas").insert({
                    "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
                }).execute()
            except Exception:
                pass
        else:
            enviar_zap(phone_id, cliente_zap, "Não entendi a forma de pagamento. Aceitamos: Pix, Dinheiro ou Cartão.")
        return





    prompt_usuario_banco = dados_loja.get("system_prompt", "") or ""
    info_carrinho = f"CARRINHO ATUAL: {pedido_ativo['resumo_pedido']}" if pedido_ativo else "Carrinho Vazio"

    prompt_sistema = f"""
Você é um atendente virtual (Fase de Escolha de Itens).
Personalidade: {prompt_usuario_banco}

Cardápio OFICIAL: {dados_loja.get('cardapio', '')}
Status Carrinho: {info_carrinho}

REGRAS CRÍTICAS DE INTELIGÊNCIA:
1. 🧠 CONTEXTO E MEMÓRIA: Sua prioridade é a última mensagem, mas VOCÊ DEVE consultar o histórico recente para entender correções.
2. 🚫 ANTI-DUPLICIDADE: Antes de adicionar um item, verifique no "Status Carrinho" se ele JÁ foi adicionado.
3. Use nomes o MAIS PRÓXIMO possível do Cardápio OFICIAL.
4. 💰 PREÇOS: Se pedir cardápio, mostre os preços (copie do oficial).
5. 📝 OBSERVAÇÕES: Ingredientes para retirar ou ponto da carne vão em "adicionar_observacao".

6. 🍕 REGRA DE MISTURA (IMPORTANTE):
- Se pedir meia/meio a meio, isso é UM ÚNICO ITEM.
- O nome deve conter "Meio": "Meio [Sabor A] e Meio [Sabor B]".

7. ❗ CONFIRMAÇÃO: só use "pedir_fechamento" se o cliente disser algo CLARO como "fechar pedido", "pode entregar", "finaliza".

8. ❓ PEDIDO VAGO: se não disser o sabor claramente, NÃO crie item. Intenção: "perguntar".

9. 🚫 PRODUTOS INEXISTENTES: se não estiver no Cardápio OFICIAL, NÃO crie item.

10. 🔢 QUANTIDADE: se não informar, assuma 1. Nunca assuma >1.

FORMATO JSON:
{{
  "intencao": "...",
  "mensagem": "...",
  "itens": [ {{"nome": "...", "qtd": 1, "observacao": "..."}} ]
}}
""".strip()

    try:
        historico = (
            supabase.table("conversas")
            .select("role, mensagem")
            .eq("cliente_zap", cliente_zap)
            .eq("restaurante_id", phone_id)
            .order("created_at", desc=False)
            .limit(6)
            .execute()
        )
    except Exception:
        historico = type("x", (), {"data": []})()

    messages = [{"role": "system", "content": prompt_sistema}]
    for h in (historico.data or []):
        messages.append({"role": h["role"], "content": h["mensagem"]})
    messages.append({"role": "user", "content": texto_completo})

    try:
        chat = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=messages,
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        dados_ia_raw = json.loads(chat.choices[0].message.content)
        intencao, mensagem_ia, itens_ia = _sanitize_ia_response(dados_ia_raw)
    except Exception as e:
        print(f"Erro IA: {e}")
        return


    try:
        supabase.table("conversas").insert({
            "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "user", "mensagem": texto_completo
        }).execute()
    except Exception:
        pass


    if intencao == "cancelar":
        if pedido_ativo:
            supabase.table("pedidos").update({"status": "cancelado"}).eq("id", pedido_ativo["id"]).execute()
        enviar_zap(phone_id, cliente_zap, "Pedido cancelado. Se mudar de ideia, é só chamar! 👋")
        set_estado(cliente_zap, phone_id, "INICIO")
        return

    if intencao == "pedir_fechamento":
        if not pedido_ativo:
            enviar_zap(phone_id, cliente_zap, "Seu carrinho está vazio. Escolha algo do cardápio primeiro! 🍕")
            return

        set_estado(cliente_zap, phone_id, "AGUARDANDO_ENDERECO")
        resumo = pedido_ativo.get("resumo_pedido", "Carrinho vazio")
        try:
            total = float(pedido_ativo.get("total_valor") or 0.0)
        except Exception:
            total = 0.0

        msg = (
            f"{mensagem_ia}\n\n"
            f"📝 *Resumo:*\n{str(resumo).replace('|', '\n')}\n"
            f"💰 Subtotal: R$ {total:.2f}\n\n"
            "📍 *Para onde vamos enviar?* (Digite o endereço com bairro)"
        )
        enviar_zap(phone_id, cliente_zap, msg)
        try:
            supabase.table("conversas").insert({
                "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
            }).execute()
        except Exception:
            pass
        return


    if intencao in ("adicionar_item", "fixar_item", "remover_item", "adicionar_observacao") and itens_ia:
        tabela_precos = dados_loja.get("precos_dict", {}) or {}
        nomes_oficiais = list(tabela_precos.keys())

        def _match_item(nome: str) -> str | None:
            termo = normalizar_texto(nome)
            if not termo or not nomes_oficiais:
                return None
            m = difflib.get_close_matches(termo, nomes_oficiais, n=1, cutoff=0.6)
            return m[0] if m else None

        carrinho_atual = _safe_dict((pedido_ativo or {}).get("carrinho_json")) if pedido_ativo else {}

        avisos_estoque = []
        bloquear_msg_ia = False

        for item in itens_ia:
            nome_ia = (item.get("nome") or "").strip()
            if not nome_ia:
                continue

            try:
                qtd_ia = int(item.get("qtd", 1))
            except Exception:
                qtd_ia = 1
            qtd_ia = max(1, min(int(MAX_QTD_ITEM or 10), qtd_ia))

            obs_ia = item.get("observacao")
            obs_ia = (obs_ia or "").strip() if isinstance(obs_ia, str) else ""

            nome_ia_norm = normalizar_texto(nome_ia)
            chave_item = ""
            preco_unitario = 0.0
            nome_exibicao = ""


            if "meio" in nome_ia_norm or "metade" in nome_ia_norm or "/" in nome_ia:
                sabores_identificados = []
                precos_identificados = []
                for nome_oficial, preco in tabela_precos.items():
                    if len(nome_oficial) < 3:
                        continue
                    if nome_oficial in nome_ia_norm:
                        sabores_identificados.append(nome_oficial.title())
                        try:
                            precos_identificados.append(float(preco))
                        except Exception:
                            pass

                if precos_identificados:
                    sabores_unicos = sorted(list(set(sabores_identificados)))
                    preco_unitario = max(precos_identificados)
                    if len(sabores_unicos) > 1:
                        chave_item = normalizar_texto("Meio " + " e Meio ".join(sabores_unicos))
                        nome_exibicao = "Meio " + " / ".join(sabores_unicos)
                    else:
                        chave_item = normalizar_texto(sabores_unicos[0])
                        nome_exibicao = sabores_unicos[0]


            if not chave_item:
                match_nome = _match_item(nome_ia)
                if match_nome:
                    chave_item = match_nome
                    try:
                        preco_unitario = float(tabela_precos[chave_item])
                    except Exception:
                        preco_unitario = 0.0
                    nome_exibicao = chave_item.title()
                else:
                    chave_item = nome_ia_norm
                    nome_exibicao = nome_ia


            if chave_item not in carrinho_atual:
                carrinho_atual[chave_item] = {
                    "nome_exibicao": nome_exibicao,
                    "qtd": 0,
                    "preco_unitario": float(preco_unitario),
                    "observacao": "",
                }


            if intencao == "adicionar_item":
                sucesso, dados_retorno = atualizar_estoque_real_time(restaurante_db_id, chave_item, -qtd_ia)
                if sucesso:
                    carrinho_atual[chave_item]["qtd"] += qtd_ia
                    if obs_ia:
                        carrinho_atual[chave_item]["observacao"] = obs_ia
                    if (dados_retorno or {}).get("novo_estoque") == 0:
                        avisos_estoque.append
                else:
                    bloquear_msg_ia = True
                    msg_erro = (dados_retorno or {}).get("msg", "")
                    estoque_restante = (dados_retorno or {}).get("estoque_atual", 0) or 0
                    if str(msg_erro).strip().lower() in ("estoque insuficiente",):
                        if int(estoque_restante) > 0:
                            avisos_estoque.append(
                                f"⚠️ Ops! Só restam *{estoque_restante}* unidades de *{nome_exibicao}*. Nada foi adicionado."
                            )
                        else:
                            avisos_estoque.append(f"⚠️ O item *{nome_exibicao}* acabou de esgotar.")
                    else:

                        carrinho_atual[chave_item]["qtd"] += qtd_ia
                        if obs_ia:
                            carrinho_atual[chave_item]["observacao"] = obs_ia

            elif intencao == "remover_item":
                qtd_atual = int(carrinho_atual[chave_item].get("qtd") or 0)
                qtd_remover = min(qtd_ia, qtd_atual)
                if qtd_remover > 0:
                    atualizar_estoque_real_time(restaurante_db_id, chave_item, +qtd_remover)
                    carrinho_atual[chave_item]["qtd"] -= qtd_remover
                    if carrinho_atual[chave_item]["qtd"] <= 0:
                        carrinho_atual.pop(chave_item, None)

            elif intencao == "fixar_item":
                qtd_atual = int(carrinho_atual[chave_item].get("qtd") or 0)
                diferenca = qtd_ia - qtd_atual
                if diferenca > 0:
                    sucesso, _ = atualizar_estoque_real_time(restaurante_db_id, chave_item, -diferenca)
                    if sucesso:
                        carrinho_atual[chave_item]["qtd"] = qtd_ia
                    else:
                        bloquear_msg_ia = True
                        avisos_estoque.append(f"⚠️ Não há estoque suficiente de *{nome_exibicao}* para completar {qtd_ia}.")
                elif diferenca < 0:
                    qtd_devolver = abs(diferenca)
                    atualizar_estoque_real_time(restaurante_db_id, chave_item, +qtd_devolver)
                    carrinho_atual[chave_item]["qtd"] = qtd_ia

                if int(carrinho_atual.get(chave_item, {}).get("qtd", 0)) <= 0:
                    carrinho_atual.pop(chave_item, None)
                elif obs_ia:
                    carrinho_atual[chave_item]["observacao"] = obs_ia

            elif intencao == "adicionar_observacao":
                if obs_ia:
                    carrinho_atual[chave_item]["observacao"] = obs_ia


        resumo_list = []
        total_geral = 0.0
        for _, dados_item in (carrinho_atual or {}).items():
            qtd = int(dados_item.get("qtd") or 0)
            preco_u = float(dados_item.get("preco_unitario") or 0.0)
            total_item = qtd * preco_u
            total_geral += total_item
            txt_obs = f" ({dados_item.get('observacao')})" if dados_item.get("observacao") else ""
            resumo_list.append(f"{qtd}x {dados_item.get('nome_exibicao', '')}{txt_obs} (R$ {total_item:.2f})")

        novo_resumo = " | ".join(resumo_list) if resumo_list else "Carrinho vazio"

        dados_update = {
            "carrinho_json": carrinho_atual,
            "resumo_pedido": novo_resumo,
            "total_valor": total_geral,
            "status": "novo",
        }

        if pedido_ativo:
            supabase.table("pedidos").update(dados_update).eq("id", pedido_ativo["id"]).execute()
        else:
            dados_update.update({
                "cliente_zap": cliente_zap,
                "restaurante_id": restaurante_db_id,  # 🔥 ID numérico
                "cliente_nome": nome_cliente,
            })
            supabase.table("pedidos").insert(dados_update).execute()

        set_estado(cliente_zap, phone_id, "INICIO")


        if avisos_estoque:
            enviar_zap(phone_id, cliente_zap, "\n".join(avisos_estoque))

        if not bloquear_msg_ia:
            enviar_zap(phone_id, cliente_zap, mensagem_ia)
            try:
                supabase.table("conversas").insert({
                    "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": mensagem_ia
                }).execute()
            except Exception:
                pass

        if carrinho_atual:
            enviar_zap(
                phone_id,
                cliente_zap,
                f"🛒 *Carrinho Atualizado:*\n{novo_resumo.replace('|', '\n')}\n💰 Total: R$ {total_geral:.2f}",
            )
        return


    enviar_zap(phone_id, cliente_zap, mensagem_ia)
    try:
        supabase.table("conversas").insert({
            "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": mensagem_ia
        }).execute()
    except Exception:
        pass


    palavras_gatilho = ["pedi", "pedido", "carrinho", "resumo", "lista", "conta", "total", "comprado"]
    if pedido_ativo and any(p in txt_norm for p in palavras_gatilho):
        resumo_txt = str(pedido_ativo.get("resumo_pedido", "") or "").replace("|", "\n")
        try:
            total_val = float(pedido_ativo.get("total_valor") or 0.0)
        except Exception:
            total_val = 0.0
        if resumo_txt and resumo_txt != "Carrinho vazio":
            msg_resumo = f"🛒 *Seu Carrinho:*\n{resumo_txt}\n💰 Total: R$ {total_val:.2f}"
            enviar_zap(phone_id, cliente_zap, msg_resumo)
