import os
import asyncio
import json
import re
import difflib
import hashlib
import uuid
import unicodedata
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP

import redis

from cryptography.fernet import Fernet, InvalidToken


# ==== Load .env early (so module-level os.getenv works) ====
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None

_WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if load_dotenv:
    # Prefer root .env (shared by API + Dashboard); fallback to API/.env.
    load_dotenv(dotenv_path=str(_WORKSPACE_ROOT / ".env"), override=False)
    load_dotenv(dotenv_path=str(Path(__file__).resolve().parent / ".env"), override=False)


# ==== Config (.env / environment) ====
SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").strip()
SUPABASE_KEY = (os.getenv("SUPABASE_KEY") or "").strip()
GROQ_API_KEY = (os.getenv("GROQ_API_KEY") or "").strip()

PUBLIC_BASE_URL = (os.getenv("PUBLIC_BASE_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()
CRON_SECRET = (os.getenv("CRON_SECRET") or "").strip()
MP_WEBHOOK_TOKEN = (os.getenv("MP_WEBHOOK_TOKEN") or "").strip()
ALLOW_QUERY_TOKEN_AUTH = (os.getenv("ALLOW_QUERY_TOKEN_AUTH", "0") or "0").strip().lower() in ("1", "true", "yes", "on")

CACHE_PREFIX = (os.getenv("CACHE_PREFIX") or "saas").strip()
CACHE_INVALIDATE_TOKEN = (os.getenv("CACHE_INVALIDATE_TOKEN") or "").strip()

CRED_ENCRYPTION_KEY = (os.getenv("CRED_ENCRYPTION_KEY") or "").strip()

UAZAPI_BASE_URL = (os.getenv("UAZAPI_BASE_URL") or "https://free.uazapi.com").strip().rstrip("/")
UAZAPI_TIMEOUT = float(os.getenv("UAZAPI_TIMEOUT", "15") or "15")
UAZAPI_PRESENCE_ENABLED = (os.getenv("UAZAPI_PRESENCE_ENABLED", "1") or "1").strip().lower() in ("1", "true", "yes", "on")
UAZAPI_PRESENCE_DELAY_MS = int(os.getenv("UAZAPI_PRESENCE_DELAY_MS", "25000") or "25000")

SUPABASE_TIMEOUT_SECONDS = float(os.getenv("SUPABASE_TIMEOUT_SECONDS", "20") or "20")
GROQ_TIMEOUT_SECONDS = float(os.getenv("GROQ_TIMEOUT_SECONDS", "20") or "20")
GROQ_MAX_CONCURRENCY = int(os.getenv("GROQ_MAX_CONCURRENCY", "2") or "2")

MAX_WEBHOOK_BODY_BYTES = int(os.getenv("MAX_WEBHOOK_BODY_BYTES", "262144") or "262144")
MAX_INCOMING_TEXT_CHARS = int(os.getenv("MAX_INCOMING_TEXT_CHARS", "4000") or "4000")
MAX_BUFFER_TEXT_CHARS = int(os.getenv("MAX_BUFFER_TEXT_CHARS", "8000") or "8000")
MAX_QTD_ITEM = int(os.getenv("MAX_QTD_ITEM", "10") or "10")
MAX_HISTORICO = int(os.getenv("MAX_HISTORICO", "15") or "15")

DEBUG_WEBHOOK = (os.getenv("DEBUG_WEBHOOK", "0") or "0").strip().lower() in ("1", "true", "yes", "on")

ALLOW_ABANDONED_CLEANUP_WITHOUT_REDIS = (os.getenv("ALLOW_ABANDONED_CLEANUP_WITHOUT_REDIS", "0") or "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

INTENT_ROUTER_ENABLED = (os.getenv("INTENT_ROUTER_ENABLED", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
INTENT_ROUTER_MODEL = (os.getenv("INTENT_ROUTER_MODEL") or "llama-3.3-70b-versatile").strip() or "llama-3.3-70b-versatile"

SLOT_FILLING_ENABLED = (os.getenv("SLOT_FILLING_ENABLED", "1") or "1").strip().lower() in ("1", "true", "yes", "on")
SLOT_FILLING_MODEL = (os.getenv("SLOT_FILLING_MODEL") or "llama-3.3-70b-versatile").strip() or "llama-3.3-70b-versatile"

NORMALIZE_TEXT_ENABLED = (os.getenv("NORMALIZE_TEXT_ENABLED", "1") or "1").strip().lower() in ("1", "true", "yes", "on")
NORMALIZE_TEXT_MODEL = (os.getenv("NORMALIZE_TEXT_MODEL") or "llama-3.3-70b-versatile").strip() or "llama-3.3-70b-versatile"
NORMALIZE_TEXT_FOR_AUDIO = (os.getenv("NORMALIZE_TEXT_FOR_AUDIO", "1") or "1").strip().lower() in ("1", "true", "yes", "on")
NORMALIZE_TEXT_FOR_CONFUSING = (os.getenv("NORMALIZE_TEXT_FOR_CONFUSING", "1") or "1").strip().lower() in ("1", "true", "yes", "on")

HTTP_VERIFY_TLS = os.getenv("HTTP_VERIFY_TLS", "true").strip().lower() in ("1", "true", "yes")
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


def decrypt_secret(enc: str) -> str:
    """Decrypt a Fernet-encrypted secret stored as text.

    If decryption is not possible (missing key/invalid token), returns the original input.
    """

    enc = (enc or "").strip()
    if not enc:
        return ""
    f = _fernet()
    if not f:
        return enc
    try:
        return f.decrypt(enc.encode()).decode()
    except InvalidToken:
        return enc
    except Exception:
        return enc


def _fernet() -> Fernet | None:
    key = (CRED_ENCRYPTION_KEY or "").strip()
    if not key:
        return None
    try:
        return Fernet(key.encode())
    except Exception:
        return None


def _money_2(value) -> float:
    """Round money to 2 decimals deterministically."""
    try:
        return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    except Exception:
        try:
            return float(value or 0)
        except Exception:
            return 0.0


def _format_brl(value) -> str:
    try:
        v = float(value or 0)
    except Exception:
        v = 0.0
    return f"{v:.2f}".replace(".", ",")


def _format_carrinho_display(carrinho_atual: dict) -> tuple[str, float]:
    lines = []
    total_geral = 0.0
    for _, dados_item in (carrinho_atual or {}).items():
        try:
            qtd = int(dados_item.get("qtd") or 0)
        except Exception:
            qtd = 0
        if qtd <= 0:
            continue
        try:
            preco_u = float(dados_item.get("preco_unitario") or 0.0)
        except Exception:
            preco_u = 0.0
        total_item = qtd * preco_u
        total_geral += total_item

        obs_parts = []
        obs_comp = dados_item.get("obs_componentes") or {}
        comps = dados_item.get("componentes") or []
        if isinstance(obs_comp, dict) and obs_comp:
            for comp in comps:
                o = (obs_comp.get(comp) or "").strip()
                if o:
                    obs_parts.append(f"1/2 {str(comp).title()}: {o}")

        obs_geral = (dados_item.get("observacao") or "").strip()
        if obs_geral:
            obs_parts.append(obs_geral)

        txt_obs = f" ({'; '.join(obs_parts)})" if obs_parts else ""

        nome_disp = dados_item.get("nome_exibicao", "") or ""
        comps = dados_item.get("componentes") or []
        if isinstance(comps, list) and len(comps) == 2:
            nome_disp = "Meio " + " / ".join([str(c).title() for c in comps])

        lines.append(f"*-{qtd}x* {nome_disp}{txt_obs} | R$ {_format_brl(total_item)}")

    return ("\n".join(lines) if lines else "Carrinho vazio"), total_geral


def _payer_email_from_cliente(cliente_zap: str) -> str:
    safe = re.sub(r"\D", "", str(cliente_zap or ""))
    if not safe:
        safe = "cliente"
    return f"{safe}@noemail.local"


def _only_digits(v: str) -> str:
    return re.sub(r"\D", "", str(v or "")).strip()


def _extract_cliente_zap(body: dict, msg_data: dict, phone_id: str | None = None) -> str | None:
    """Best-effort extraction of customer phone/identifier from webhook payload.

    Returns only digits when possible.
    """

    candidates: list[str] = []

    if isinstance(msg_data, dict):
        for k in ("chatid", "sender", "remoteJid", "from", "participant", "author", "number"):
            v = msg_data.get(k)
            if v:
                candidates.append(str(v))
        key = msg_data.get("key")
        if isinstance(key, dict):
            for k in ("remoteJid", "participant"):
                v = key.get(k)
                if v:
                    candidates.append(str(v))

    if isinstance(body, dict):
        for k in ("chatid", "sender", "remoteJid", "from", "number"):
            v = body.get(k)
            if v:
                candidates.append(str(v))

        data = body.get("data")
        if isinstance(data, dict):
            for k in ("chatid", "sender", "remoteJid", "from", "number"):
                v = data.get(k)
                if v:
                    candidates.append(str(v))
            msgs = data.get("messages")
            if isinstance(msgs, list) and msgs:
                m0 = msgs[0]
                if isinstance(m0, dict):
                    for k in ("chatid", "sender", "remoteJid", "from", "number"):
                        v = m0.get(k)
                        if v:
                            candidates.append(str(v))

    phone_id_digits = _only_digits(phone_id or "")

    for raw in candidates:
        s = (raw or "").strip()
        if not s:
            continue
        if "@" in s:
            s = s.split("@", 1)[0]
        digits = _only_digits(s)
        if not digits:
            continue
        if phone_id_digits and digits == phone_id_digits:
            continue
        return digits

    return None


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


def _dbg_webhook(msg: str, **fields):
    if not DEBUG_WEBHOOK:
        return
    try:
        extra = " ".join(f"{k}={v}" for k, v in fields.items() if v is not None and v != "")
        print(f"[webhook] {msg}" + (f" | {extra}" if extra else ""))
    except Exception:
        pass


def _first_dict(v):
    if isinstance(v, dict):
        return v
    if isinstance(v, list):
        for item in v:
            if isinstance(item, dict):
                return item
    return {}


async def _run_blocking(fn, *, timeout: float | None = None):
    # roda função síncrona sem travar o event loop
    if timeout and timeout > 0:
        return await asyncio.wait_for(asyncio.to_thread(fn), timeout=timeout)
    return await asyncio.to_thread(fn)


SEND_MAX_CONCURRENCY = int(os.getenv("SEND_MAX_CONCURRENCY", "12") or "12")
DB_MAX_CONCURRENCY = int(os.getenv("DB_MAX_CONCURRENCY", "12") or "12")

_send_sem = asyncio.Semaphore(max(1, SEND_MAX_CONCURRENCY))
_db_sem = asyncio.Semaphore(max(1, DB_MAX_CONCURRENCY))


async def sb_exec(fn, *, timeout: float = SUPABASE_TIMEOUT_SECONDS):
    async with _db_sem:
        return await _run_blocking(fn, timeout=timeout)


def normalizar_texto(texto):
    if not texto:
        return ""
    return "".join(c for c in unicodedata.normalize("NFD", str(texto)) if unicodedata.category(c) != "Mn").lower()


def _is_meio_a_meio_item(chave_item: str, dados_item: dict | None = None) -> bool:
    k = str(chave_item or "")
    if k.startswith("meio "):
        return True
    if isinstance(dados_item, dict) and (dados_item.get("componentes") or dados_item.get("obs_componentes")):
        return True
    return False


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
    termo_limpo = re.sub(r"\b(bairro|do|da|de|no|na|em|rua|moro|no)\b", "", termo_busca).strip()
    if not termo_limpo:
        return None

    norm_map = {}
    norm_list = []
    for opt in (lista_opcoes or []):
        n = normalizar_texto(opt)
        if not n or n in norm_map:
            continue
        norm_map[n] = opt
        norm_list.append(n)

    if termo_limpo in norm_map:
        return norm_map[termo_limpo]

    matches = difflib.get_close_matches(termo_limpo, norm_list, n=1, cutoff=0.85)
    if matches:
        best = matches[0]
        if difflib.SequenceMatcher(None, termo_limpo, best).ratio() >= 0.88:
            return norm_map.get(best, best)
    return None


def _match_bairro_from_input(termo_raw: str, bairros_dict: dict, *, strict_if_short: bool = True) -> str | None:
    if not bairros_dict:
        return None
    raw = str(termo_raw or "").strip()
    if not raw:
        return None
    t_norm = normalizar_texto(raw)
    if not t_norm:
        return None

    # match exato (normalizado)
    if t_norm in bairros_dict:
        return t_norm

    # match exato contra chaves originais
    for k in (bairros_dict or {}).keys():
        if normalizar_texto(k) == t_norm:
            return k

    if strict_if_short and _texto_parece_bairro(raw, t_norm):
        # tenta fuzzy mais permissivo para pequenos erros de digitação (ex.: modubim -> mondubim)
        norm_map = {}
        norm_list = []
        for opt in (bairros_dict or {}).keys():
            n = normalizar_texto(opt)
            if not n or n in norm_map:
                continue
            norm_map[n] = opt
            norm_list.append(n)
        if norm_list:
            matches = difflib.get_close_matches(t_norm, norm_list, n=1, cutoff=0.75)
            if matches:
                best = matches[0]
                if difflib.SequenceMatcher(None, t_norm, best).ratio() >= 0.80:
                    return norm_map.get(best, best)
        return None

    return encontrar_melhor_match(raw, list(bairros_dict.keys()))


def _texto_parece_endereco(texto_raw: str, txt_norm: str) -> bool:
    """Heurística simples para diferenciar 'só bairro' de endereço completo (rua+número/etc)."""
    raw = str(texto_raw or "")
    t = (txt_norm or "").strip()
    if not raw or not t:
        return False
    if re.search(r"\d", raw):
        return True
    if "," in raw:
        return True
    hints = (
        "rua ", "r. ", "av ", "av. ", "avenida ", "travessa ", "tv ", "tv. ", "alameda ",
        "estrada ", "rodovia ", "quadra ", "bloco ", "casa ", "apto", "ap ", "apartamento", "condominio", "condomínio",
        "beco ", "viela ", "vila ", "passagem ", "comunidade ",
        "portao", "portão", "porta ", "fundos", "frente", "esquina",
        "numero", "número", "nº", "no.", "cep",
    )
    return any(h in t for h in hints)


def _texto_parece_complemento_endereco(texto_raw: str, txt_norm: str) -> bool:
    """Heurística para identificar complemento de endereço (número, ap, bloco etc)."""
    raw = str(texto_raw or "")
    t = (txt_norm or "").strip()
    if not raw or not t:
        return False
    if re.search(r"\b\d{1,5}\b", raw):
        return True
    hints = (
        "numero", "número", "nº", "no.", "apto", "ap ", "apartamento",
        "bloco", "casa", "fundos", "frente", "portao", "portão", "porta", "esquina",
        "andar", "sala", "lote", "quadra", "km",
    )
    return any(h in t for h in hints)


def extrair_endereco_de_texto(texto_raw: str) -> str | None:
    """Extrai endereço (rua/av/etc + número + complemento) de mensagens longas."""
    raw = str(texto_raw or "").strip()
    if not raw:
        return None

    padroes = [
        r"\b(?:rua|r\.|avenida|av\.?|travessa|tv\.?|alameda|estrada|rodovia|quadra|qd\.?|bloco|lote|setor|sitio|sítio|chacara|chácara)\b[^,;\n]*?\b\d{1,5}\b[^,;\n]*",
        r"\b(?:rua|r\.|avenida|av\.?|travessa|tv\.?|alameda)\b[^,;\n]*?\b(?:nº|numero|número)\s*\d{1,5}\b[^,;\n]*",
    ]

    for p in padroes:
        m = re.search(p, raw, flags=re.IGNORECASE)
        if m:
            end = m.group(0).strip()
            # corta partes de pagamento/observações após o endereço
            end = re.split(r"\b(vou pagar|pagar|pagamento|pix|dinheiro|cartao|cartão|troco)\b", end, flags=re.IGNORECASE)[0].strip()
            end = end.rstrip(". ,;:-")
            return end if len(end) >= 6 else None

    return None


def _texto_e_so_bairro(txt_norm: str, bairro_match: str) -> bool:
    """True quando a mensagem é basicamente o nome do bairro (sem rua/número)."""
    t = (txt_norm or "").strip()
    if not t:
        return False
    bairro_norm = normalizar_texto(bairro_match)
    if not bairro_norm:
        return False
    # Remove palavras comuns que aparecem junto do bairro
    limpo = re.sub(r"\b(bairro|do|da|de|no|na|em)\b", " ", t).strip()
    limpo = re.sub(r"[^a-z0-9\s]", " ", limpo).strip()
    limpo = re.sub(r"\s+", " ", limpo)
    return limpo == bairro_norm


def _texto_parece_bairro(texto_raw: str, txt_norm: str) -> bool:
    """Heurística simples para identificar mensagem que parece só bairro."""
    raw = str(texto_raw or "").strip()
    t = (txt_norm or "").strip()
    if not raw or not t:
        return False
    if _texto_parece_endereco(raw, t):
        return False
    if re.search(r"\d", raw):
        return False
    return len(t) >= 3


def _extract_bairro_from_text(texto_raw: str) -> str | None:
    """Extrai provável nome do bairro de frases como 'o bairro é X' ou 'bairro: X'."""
    raw = str(texto_raw or "").strip()
    if not raw:
        return None

    t = normalizar_texto(raw)
    m = re.search(r"\b(?:para|pra)\s+(?:o|a)\s+(.+)$", raw, flags=re.IGNORECASE)
    if m:
        cand = m.group(1).strip().strip(".-,;:")
        return cand or None

    if "bairro" in t:
        m = re.search(r"bairro\s*(?:e|eh|é|:)?\s*(.+)$", raw, flags=re.IGNORECASE)
        if m:
            cand = m.group(1).strip().strip(".-,;:")
            return cand or None

    if re.search(r"\b(e|eh|é)\b", t):
        parts = re.split(r"\b(?:e|eh|é)\b", raw, flags=re.IGNORECASE)
        if parts:
            cand = parts[-1].strip().strip(".-,;:")
            return cand or None

    return raw


def redis_set_cache(chave, dados, ttl=600):
    """Salva dicionário como JSON no Redis com tempo de vida (TTL)"""
    if redis_client:
        try:
            dado_str = json.dumps(dados, default=str)
            redis_client.setex(chave, ttl, dado_str)
        except Exception as e:
            print(f"⚠️ Erro Redis SET: {e}")


def _redis_key(key: str) -> str:
    """Prefixa a chave com CACHE_PREFIX para evitar colisões entre ambientes/serviços."""
    k = str(key or "").strip()
    if not k:
        return k
    prefix = str(CACHE_PREFIX or "").strip().strip(":")
    if not prefix:
        return k
    if k.startswith(prefix + ":"):
        return k
    return f"{prefix}:{k.lstrip(':')}"


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


def redis_del_cache(chave: str) -> bool:
    """Remove uma chave do Redis. Best-effort: retorna False se não deletou."""
    if not redis_client:
        return False
    try:
        return bool(redis_client.delete(chave))
    except Exception as e:
        print(f"⚠️ Erro Redis DEL: {e}")
        return False


def redis_add_buffer(cliente_zap, texto):
    """Adiciona mensagem na lista temporária do Redis"""
    if redis_client:
        chave = _redis_key(f"buffer:{cliente_zap}")
        try:
            redis_client.rpush(chave, texto)
            redis_client.expire(chave, 600)
        except Exception as e:
            print(f"⚠️ Erro Redis Buffer Add: {e}")


def redis_get_clear_buffer(cliente_zap):
    """Pega todas as mensagens e limpa a lista"""
    if redis_client:
        chave = _redis_key(f"buffer:{cliente_zap}")
        try:
            mensagens = redis_client.lrange(chave, 0, -1)
            if mensagens:
                redis_client.delete(chave)
                texto = " ".join(mensagens)
                if MAX_BUFFER_TEXT_CHARS and len(texto) > MAX_BUFFER_TEXT_CHARS:
                    texto = texto[-MAX_BUFFER_TEXT_CHARS:]
                return texto
        except Exception as e:
            print(f"⚠️ Erro Redis Buffer Get: {e}")
    return ""


def redis_claim_event_once(dedup_key: str, ttl_seconds: int = 3600) -> bool:
    """True if this process claims the event (first time), False if it's a duplicate."""
    if not redis_client:
        return True
    try:
        return bool(redis_client.set(_redis_key(dedup_key), "1", nx=True, ex=ttl_seconds))
    except Exception as e:
        print(f"⚠️ Erro Redis Dedup: {e}")
        return True


def redis_try_lock(lock_key: str, ttl_seconds: int = 30) -> bool:
    """Best-effort distributed lock to reduce duplicate processing across workers."""
    if not redis_client:
        return True
    try:
        return bool(redis_client.set(_redis_key(lock_key), "1", nx=True, ex=ttl_seconds))
    except Exception as e:
        print(f"⚠️ Erro Redis Lock: {e}")
        return True


def redis_acquire_lock(lock_key: str, ttl_seconds: int = 30) -> str:
    """Returns a lock token if acquired, else empty string. Uses Redis when available."""
    if not redis_client:
        return "local"
    token = uuid.uuid4().hex
    try:
        ok = redis_client.set(_redis_key(lock_key), token, nx=True, ex=ttl_seconds)
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
        lock_key = _redis_key(lock_key)
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
        current = redis_client.incr(_redis_key(key))
        if current == 1:
            redis_client.expire(_redis_key(key), window_seconds)
        return current <= limit
    except Exception as e:
        print(f"⚠️ Erro Redis RateLimit: {e}")
        return True


def _redis_setnx_once(key: str, ttl_seconds: int) -> bool:
    """Best-effort dedup. If Redis is down, returns False (safer: do nothing)."""
    if not redis_client:
        return False
    try:
        return bool(redis_client.set(_redis_key(key), "1", nx=True, ex=ttl_seconds))
    except Exception:
        return False


import logging
try:
    from pythonjsonlogger import jsonlogger  # type: ignore
except Exception:
    jsonlogger = None


log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
log_datefmt = "%Y-%m-%d %H:%M:%S"
log_level = logging.INFO


file_handler = logging.FileHandler("api.log")
file_handler.setLevel(log_level)
if jsonlogger:
    formatter = jsonlogger.JsonFormatter()
else:
    formatter = logging.Formatter(fmt=log_format, datefmt=log_datefmt)
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

#