import os
import sys
import asyncio
import time
import json
import re
import difflib
import hashlib
import uuid
import copy
from datetime import datetime, timedelta, timezone
import unicodedata
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP
import base64

import requests
import urllib3
import redis

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from supabase import create_client, Client
from groq import Groq

try:
    import qrcode  # type: ignore
except Exception:
    qrcode = None

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

CACHE_PREFIX = (os.getenv("CACHE_PREFIX") or "saas").strip()
CACHE_INVALIDATE_TOKEN = (os.getenv("CACHE_INVALIDATE_TOKEN") or "").strip()

CRED_ENCRYPTION_KEY = (os.getenv("CRED_ENCRYPTION_KEY") or "").strip()

UAZAPI_BASE_URL = (os.getenv("UAZAPI_BASE_URL") or "https://free.uazapi.com").strip().rstrip("/")
UAZAPI_TIMEOUT = float(os.getenv("UAZAPI_TIMEOUT", "15") or "15")

SUPABASE_TIMEOUT_SECONDS = float(os.getenv("SUPABASE_TIMEOUT_SECONDS", "20") or "20")
GROQ_TIMEOUT_SECONDS = float(os.getenv("GROQ_TIMEOUT_SECONDS", "20") or "20")
GROQ_MAX_CONCURRENCY = int(os.getenv("GROQ_MAX_CONCURRENCY", "2") or "2")
_groq_sem = asyncio.Semaphore(max(1, GROQ_MAX_CONCURRENCY))

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

# Slot Filling (fluxo não-linear): extrai itens + endereço + pagamento na mesma frase e
# persiste slots em `clientes_estado.dados_parciais` com validação tardia no checkout.


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


def _build_slot_filling_system_prompt(*, prompt_usuario: str, cardapio: str) -> str:
    return (
        "Você é um extrator de entidades para um bot de delivery.\n"
        "Tarefa: dado o contexto e a mensagem do cliente, extraia TODAS as informações possíveis (itens, endereço, bairro, tipo de entrega, pagamento)\n"
        "mesmo quando múltiplas coisas aparecem juntas e em qualquer ordem.\n\n"
        "Regras:\n"
        "- Responda APENAS um JSON válido (sem texto fora do JSON).\n"
        "- Não invente itens fora do cardápio.\n"
        "- Condicionais: se o cliente disser 'se nao tiver X, manda Y', adicione SOMENTE X e coloque observacao 'se nao tiver, substituir por Y'.\n"
        "- Borda recheada: trate como observacao do item de pizza (ex.: 'borda cheddar'); não crie item separado.\n"
        "- Prioridade/Conflitos: a ÚLTIMA instrução do cliente vence. Se disser 'retirada', não force endereço. Se mandar endereço completo, infira entrega.\n"
        "- Aceite correções a qualquer momento (trocar pagamento, mudar endereço, remover item).\n"
        "- Se o cliente pedir para finalizar/fechar, marque intencao_primaria=checkout mesmo que faltem slots (a validação será feita no Python).\n"
        "- Se a mensagem for dúvida (cardápio/ingredientes), marque intencao_primaria=duvida e preencha duvida_texto.\n\n"
        "Contexto do restaurante (personalidade/instruções): " + (prompt_usuario or "") + "\n"
        "Cardápio oficial: " + (cardapio or "") + "\n\n"
        "Formato obrigatório do JSON:\n"
        "{\n"
        "  \"intencao_primaria\": \"checkout\"|\"continuar_pedindo\"|\"duvida\",\n"
        "  \"itens_adicionar\": [{\"nome\": string, \"qtd\": number|null, \"meio_a_meio\": {\"sabor1\":string|null, \"sabor2\":string|null}|null, \"observacao\": string|null, \"confianca\": number}],\n"
        "  \"itens_remover\":   [{\"nome\": string, \"qtd\": number|null, \"confianca\": number}],\n"
        "  \"endereco_novo\": string|null,\n"
        "  \"bairro\": string|null,\n"
        "  \"tipo_entrega\": \"entrega\"|\"retirada\"|null,\n"
        "  \"forma_pagamento\": \"pix\"|\"dinheiro\"|\"cartao\"|null,\n"
        "  \"troco_para\": number|null,\n"
        "  \"duvida_texto\": string|null,\n"
        "  \"perguntas_followup\": [string],\n"
        "  \"observacoes\": [string]\n"
        "}\n\n"
        "Retorne APENAS o JSON."
    ).strip()


def _sanitize_slot_filling_response(raw: dict) -> dict | None:
    if not isinstance(raw, dict):
        return None

    def _as_str(v):
        return v.strip() if isinstance(v, str) else ""

    intent = _as_str(raw.get("intencao_primaria"))
    if intent not in ("checkout", "continuar_pedindo", "duvida"):
        intent = "continuar_pedindo"

    def _clamp01(x) -> float:
        try:
            f = float(x)
        except Exception:
            f = 0.0
        return max(0.0, min(1.0, f))

    def _clean_items(v, *, allow_meio: bool) -> list[dict]:
        if not isinstance(v, list):
            return []
        out = []
        for it in v:
            if not isinstance(it, dict):
                continue
            nome = _as_str(it.get("nome"))
            if not nome:
                continue
            qtd = it.get("qtd")
            try:
                qtd = int(qtd) if qtd is not None else None
            except Exception:
                qtd = None
            if qtd is not None:
                qtd = max(1, min(int(MAX_QTD_ITEM or 10), qtd))

            obs = it.get("observacao")
            obs = _as_str(obs) or None

            conf = _clamp01(it.get("confianca"))

            meio = None
            if allow_meio:
                m = it.get("meio_a_meio")
                if isinstance(m, dict):
                    s1 = _as_str(m.get("sabor1")) or None
                    s2 = _as_str(m.get("sabor2")) or None
                    if s1 or s2:
                        meio = {"sabor1": s1, "sabor2": s2}

            out.append({
                "nome": nome,
                "qtd": qtd,
                "meio_a_meio": meio,
                "observacao": obs,
                "confianca": conf,
            })
        return out

    itens_add = _clean_items(raw.get("itens_adicionar"), allow_meio=True)

    itens_rem_raw = raw.get("itens_remover")
    itens_rem = []
    if isinstance(itens_rem_raw, list):
        for it in itens_rem_raw:
            if not isinstance(it, dict):
                continue
            nome = _as_str(it.get("nome"))
            if not nome:
                continue
            qtd = it.get("qtd")
            try:
                qtd = int(qtd) if qtd is not None else None
            except Exception:
                qtd = None
            if qtd is not None:
                qtd = max(1, min(int(MAX_QTD_ITEM or 10), qtd))
            itens_rem.append({"nome": nome, "qtd": qtd, "confianca": _clamp01(it.get("confianca"))})

    tipo_entrega = _as_str(raw.get("tipo_entrega")).lower() or None
    if tipo_entrega not in ("entrega", "retirada", None):
        tipo_entrega = None

    forma = _as_str(raw.get("forma_pagamento")).lower()
    if forma in ("cartão", "credito", "crédito", "debito", "débito"):
        forma = "cartao"
    if forma not in ("pix", "dinheiro", "cartao"):
        forma = None

    troco_para = raw.get("troco_para")
    try:
        troco_para = float(troco_para) if troco_para is not None else None
    except Exception:
        troco_para = None
    if troco_para is not None:
        troco_para = max(0.0, troco_para)
        # Se a pessoa falou em troco, o pagamento é dinheiro por definição.
        if forma is None:
            forma = "dinheiro"

    end = _as_str(raw.get("endereco_novo")) or None
    bairro = _as_str(raw.get("bairro")) or None
    duvida = _as_str(raw.get("duvida_texto")) or None

    follow = raw.get("perguntas_followup")
    if not isinstance(follow, list):
        follow = []
    follow = [x.strip() for x in follow if isinstance(x, str) and x.strip()][:5]

    obs = raw.get("observacoes")
    if not isinstance(obs, list):
        obs = []
    obs = [x.strip() for x in obs if isinstance(x, str) and x.strip()][:8]

    return {
        "intencao_primaria": intent,
        "itens_adicionar": itens_add,
        "itens_remover": itens_rem,
        "endereco_novo": end,
        "bairro": bairro,
        "tipo_entrega": tipo_entrega,
        "forma_pagamento": forma,
        "troco_para": troco_para,
        "duvida_texto": duvida,
        "perguntas_followup": follow,
        "observacoes": obs,
    }


def _slot_should_force_checkout(txt_norm: str, slot_obj: dict | None) -> bool:
    t = (txt_norm or "").strip()
    if not t:
        return False
    if slot_obj and slot_obj.get("intencao_primaria") == "checkout":
        return True
    return any(k in t for k in ("finaliz", "fech", "encerr", "pode fechar", "pode finalizar"))


def _slot_to_prefill(slot_obj: dict | None) -> dict | None:
    if not isinstance(slot_obj, dict):
        return None
    prefill: dict = {}
    if slot_obj.get("tipo_entrega") == "retirada":
        prefill["tipo_entrega"] = "retirada"
    b = (slot_obj.get("bairro") or "").strip() if isinstance(slot_obj.get("bairro"), str) else ""
    e = (slot_obj.get("endereco_novo") or "").strip() if isinstance(slot_obj.get("endereco_novo"), str) else ""
    p = (slot_obj.get("forma_pagamento") or "").strip().lower() if isinstance(slot_obj.get("forma_pagamento"), str) else ""
    if b:
        prefill["bairro"] = b
    if e:
        prefill["endereco_txt"] = e
    if p in ("pix", "dinheiro", "cartao"):
        prefill["forma_pagamento"] = p
    return prefill or None


def _merge_slots_into_dados_parciais(dados_parciais: dict, slot_obj: dict | None) -> dict:
    if not isinstance(dados_parciais, dict):
        dados_parciais = {}
    if not isinstance(slot_obj, dict):
        return dados_parciais

    out = dict(dados_parciais)

    te = slot_obj.get("tipo_entrega")
    if te in ("entrega", "retirada"):
        out["tipo_entrega"] = te

    fp = slot_obj.get("forma_pagamento")
    if fp in ("pix", "dinheiro", "cartao"):
        out["forma_pagamento"] = fp

    troco_para = slot_obj.get("troco_para")
    try:
        troco_para = float(troco_para) if troco_para is not None else None
    except Exception:
        troco_para = None
    if troco_para is not None:
        out["troco_para"] = _money_2(troco_para)
        # Se existe troco, infere dinheiro.
        out.setdefault("forma_pagamento", "dinheiro")

    end = slot_obj.get("endereco_novo")
    if isinstance(end, str) and end.strip():
        out["endereco_txt"] = end.strip()
        # Inferência de contexto: endereço => entrega (a menos que o cliente tenha dito retirada depois)
        if str(out.get("tipo_entrega") or "").strip().lower() != "retirada":
            out.setdefault("tipo_entrega", "entrega")

    bairro = slot_obj.get("bairro")
    if isinstance(bairro, str) and bairro.strip():
        out["bairro"] = bairro.strip()

    return out


async def _slot_advance_checkout(
    *,
    phone_id,
    cliente_zap,
    restaurante_db_id: int,
    pedido_ativo: dict | None,
    dados_parciais: dict,
    bairros_dict: dict,
    lista_bairros_txt: str,
    now_iso: str,
) -> bool:
    """Tenta avançar o checkout com validação tardia (slot filling).

    Retorna True se respondeu/avançou algo e deve interromper o fluxo atual.
    """

    carrinho_ok = bool(pedido_ativo and _safe_dict((pedido_ativo or {}).get("carrinho_json")))
    if not carrinho_ok:
        await enviar_zap_async(phone_id, cliente_zap, "Seu carrinho está vazio. Me diga o que você quer pedir. 🙂")
        await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
        return True

    tipo_entrega = str((dados_parciais or {}).get("tipo_entrega") or "").strip().lower()
    if tipo_entrega not in ("entrega", "retirada"):
        tipo_entrega = ""

    if tipo_entrega == "retirada":
        # Retirada não precisa de endereço.
        dados_next = dict(dados_parciais or {})
        dados_next["tipo_entrega"] = "retirada"
        dados_next.setdefault("taxa", 0.0)
        await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", dados_next))

        fp = str((dados_parciais or {}).get("forma_pagamento") or "").strip().lower()
        if fp in ("pix", "dinheiro", "cartao"):
            # Chama o fluxo de pagamento usando o slot.
            handled = await _handle_pagamento_flow(
                phone_id=phone_id,
                cliente_zap=cliente_zap,
                restaurante_db_id=int(restaurante_db_id),
                pedido_ativo=pedido_ativo,
                dados_parciais=dados_next,
                txt_norm=fp,
                texto_completo=fp,
                now_iso=now_iso,
            )
            return bool(handled)

        await enviar_zap_async(phone_id, cliente_zap, "Beleza! ✅ Vai ser *retirada no local*.\nQual a forma de pagamento? *(Pix, Dinheiro ou Cartão)*")
        return True

    # Entrega (padrão) exige endereço.
    end = str((dados_parciais or {}).get("endereco_txt") or "").strip()
    bairro = str((dados_parciais or {}).get("bairro") or "").strip()

    fp = str((dados_parciais or {}).get("forma_pagamento") or "").strip().lower()
    if fp not in ("pix", "dinheiro", "cartao"):
        fp = ""

    if not end:
        await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_ENDERECO", (dados_parciais or {})))
        resumo = (pedido_ativo.get("resumo_pedido") or "Carrinho vazio")
        try:
            total = float(pedido_ativo.get("total_valor") or 0.0)
        except Exception:
            total = 0.0
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "Perfeito! Vou fechar seu pedido. ✅\n\n"
            "📝 *Resumo:*\n" + str(resumo).replace("|", "\n") + f"\n💰 Subtotal: R$ {total:.2f}\n\n"
            "📍 Me mande o *endereço completo com bairro* (ou diga *retirada*).",
        )
        return True

    # Validação/normalização local (para permitir one-shot: endereço+bairro+pagamento na mesma frase)
    bairro_match = None
    if bairros_dict and bairro:
        bairro_match = _match_bairro_from_input(bairro, bairros_dict)
    if bairros_dict and (not bairro_match):
        bairro_match = _match_bairro_from_input(end, bairros_dict)

    if not bairro_match:
        await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_ENDERECO", (dados_parciais or {})))
        bairro_candidato = bool(bairro) or (bool(end) and not _texto_parece_endereco(end, normalizar_texto(end)))
        if bairro_candidato:
            bairro_label = (bairro or end or "").strip()
            if bairro_label:
                await enviar_zap_async(phone_id, cliente_zap, f"Não entregamos para o bairro *{bairro_label}*. Caso considere que é algum erro, posso chamar um atendente.")
            else:
                await enviar_zap_async(phone_id, cliente_zap, "Não entregamos para esse bairro. Caso considere que é algum erro, posso chamar um atendente.")
        else:
            await enviar_zap_async(phone_id, cliente_zap, "Qual é o *bairro*? Assim eu confirmo a taxa certinho.")
        return True

    try:
        taxa = float((bairros_dict or {}).get(bairro_match) or 0.0)
    except Exception:
        taxa = 0.0

    if not _texto_parece_endereco(end, normalizar_texto(end)):
        await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_ENDERECO", {"bairro": bairro_match, "taxa": taxa, **(dados_parciais or {})}))
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            f"📍 Bairro: *{str(bairro_match).title()}* (Taxa: R$ {taxa:.2f}).\n\n"
            "Agora me envie o *endereço completo*: *rua/avenida + número* (e complemento, se tiver).",
        )
        return True

    dados_next = dict(dados_parciais or {})
    dados_next.update({
        "tipo_entrega": "entrega",
        "endereco_txt": end,
        "bairro": bairro_match,
        "taxa": taxa,
    })
    if _audio_flags_by_conv.pop((str(phone_id), str(cliente_zap)), False):
        await sb_exec(lambda: set_estado(cliente_zap, phone_id, "CONFIRMAR_ENDERECO_AUDIO", dados_next))
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "📍 Confirma pra mim, por favor:\n"
            f"Endereço: *{end}*\n"
            f"Bairro: *{str(bairro_match).title()}* (Taxa: R$ {taxa:.2f})\n\n"
            "Se estiver correto, responda *sim*. Se não, envie o endereço novamente.",
        )
        return True

    await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", dados_next))

    # One-shot: se já temos pagamento, finaliza agora.
    if fp:
        handled = await _handle_pagamento_flow(
            phone_id=phone_id,
            cliente_zap=cliente_zap,
            restaurante_db_id=int(restaurante_db_id),
            pedido_ativo=pedido_ativo,
            dados_parciais=dados_next,
            txt_norm=fp,
            texto_completo=fp,
            now_iso=now_iso,
        )
        return bool(handled)

    total_prod = float((pedido_ativo or {}).get("total_valor") or 0.0)
    total_com_taxa = total_prod + float(taxa or 0.0)
    await enviar_zap_async(
        phone_id,
        cliente_zap,
        f"📍 Identifiquei: *{str(bairro_match).title()}* (Taxa: R$ {taxa:.2f}).\n"
        f"💰 *Total Final: R$ {total_com_taxa:.2f}*\n\n"
        "Qual a forma de pagamento? *(Pix, Dinheiro ou Cartão)*",
    )
    return True


async def slot_extract_universal(*, estado_atual: str, pedido_ativo: dict | None, dados_loja: dict, texto: str) -> dict | None:
    if not SLOT_FILLING_ENABLED:
        return None
    if not GROQ_API_KEY or not groq_client:
        return None

    prompt_usuario_banco = (dados_loja.get("system_prompt", "") or "").strip()
    cardapio = (dados_loja.get("cardapio", "") or "").strip()

    sys_prompt = _build_slot_filling_system_prompt(prompt_usuario=prompt_usuario_banco, cardapio=cardapio)

    carrinho_resumo = ""
    try:
        carrinho_resumo = (pedido_ativo or {}).get("resumo_pedido") or ""
    except Exception:
        carrinho_resumo = ""

    user_payload = {
        "estado_atual": str(estado_atual or ""),
        "carrinho_resumo": str(carrinho_resumo or ""),
        "mensagem": str(texto or ""),
        "cardapio": str(cardapio or ""),
    }

    try:
        async with _groq_sem:
            chat = await _run_blocking(
                lambda: groq_client.chat.completions.create(
                    model=SLOT_FILLING_MODEL,
                    messages=[
                        {"role": "system", "content": sys_prompt},
                        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                    ],
                    temperature=0.0,
                    response_format={"type": "json_object"},
                ),
                timeout=GROQ_TIMEOUT_SECONDS,
            )

        raw_txt = chat.choices[0].message.content if chat and chat.choices else ""
        raw_obj = _extract_json_object_from_text(raw_txt)
        if not raw_obj:
            return None
        return _sanitize_slot_filling_response(raw_obj)
    except Exception:
        return None


class _MissingSupabase:
    def __getattr__(self, name):
        raise RuntimeError(
            "Supabase não configurado. Defina SUPABASE_URL e SUPABASE_KEY no ambiente/.env "
            "(ex.: rode o uvicorn com --env-file ..\\.env)"
        )

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

app = FastAPI(
    title="AI Atendimento API",
    version=os.getenv("APP_VERSION", "1.0.0"),
    description="Backend do projeto AI Atendimento (webhooks, pedidos, estoque, Pix e rotinas cron).",
)

from health_startup import startup_diagnostics
#from API.health_startup import startup_diagnostics

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

supabase: Client | _MissingSupabase = _MissingSupabase()
groq_client: Groq | None = None
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
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

processing_tasks = {}
processing_followup_tasks = {}
processing_task_phase = {}  # conv_key -> "sleep" | "processing"

# Fallback buffer (quando REDIS_URL estiver vazio). Mantém o comportamento de "juntar" mensagens.
_mem_buffers: dict[str, list[str]] = {}
_audio_flags_by_conv: dict[tuple[str, str], bool] = {}
_audio_transcribed_by_conv: dict[tuple[str, str], bool] = {}


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

_groq_sem = asyncio.Semaphore(max(1, GROQ_MAX_CONCURRENCY))

async def _run_blocking(fn, *, timeout: float | None = None):
    # roda função síncrona sem travar o event loop
    if timeout and timeout > 0:
        return await asyncio.wait_for(asyncio.to_thread(fn), timeout=timeout)
    return await asyncio.to_thread(fn)

SEND_MAX_CONCURRENCY = int(os.getenv("SEND_MAX_CONCURRENCY", "12") or "12")
DB_MAX_CONCURRENCY = int(os.getenv("DB_MAX_CONCURRENCY", "12") or "12")

_send_sem = asyncio.Semaphore(max(1, SEND_MAX_CONCURRENCY))
_db_sem = asyncio.Semaphore(max(1, DB_MAX_CONCURRENCY))

async def enviar_zap_async(phone_id, numero, texto, *, timeout: float = 25):
    async with _send_sem:
        return await _run_blocking(lambda: enviar_zap(phone_id, numero, texto), timeout=timeout)

async def sb_exec(fn, *, timeout: float = SUPABASE_TIMEOUT_SECONDS):
    async with _db_sem:
        return await _run_blocking(fn, timeout=timeout)

async def atualizar_estoque_real_time_async(restaurante_id, nome_exato, delta_qtd):
    # RPC Supabase (bloqueante) -> roda fora do event loop
    return await sb_exec(lambda: atualizar_estoque_real_time(restaurante_id, nome_exato, delta_qtd))

def normalizar_texto(texto):
    if not texto: return ""
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if unicodedata.category(c) != 'Mn').lower()

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
    termo_limpo = re.sub(r'\b(bairro|do|da|de|no|na|em|rua|moro|no)\b', '', termo_busca).strip()
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



    tipo_norm = (tipo or "phone_id").strip().lower()
    chave_cache = f"{CACHE_PREFIX}:restaurante:{tipo_norm}:{identificador}"
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
            


            redis_set_cache(f"{CACHE_PREFIX}:restaurante:phone_id:{dados['phone_id']}", dados, 600)
            redis_set_cache(f"{CACHE_PREFIX}:restaurante:instance_name:{dados['instance_name']}", dados, 600)
            
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


def get_ultimo_pedido_aberto(cliente_zap, restaurante_db_id):
    """Retorna o último pedido que não esteja finalizado/cancelado (inclui cozinha)."""
    try:
        resp = (
            supabase.table("pedidos").select("*")
            .eq("cliente_zap", cliente_zap)
            .eq("restaurante_id", restaurante_db_id)
            .neq("status", "finalizado")
            .neq("status", "cancelado")
            .order("created_at", desc=True).limit(1).execute()
        )
        if resp.data:
            return resp.data[0]
    except Exception:
        pass
    return None

def count_pedidos_abertos(restaurante_db_id: int) -> int:
    """Conta pedidos em aberto (não finalizados/cancelados) para estimar fila."""
    try:
        resp = (
            supabase.table("pedidos")
            .select("id")
            .eq("restaurante_id", int(restaurante_db_id))
            .neq("status", "finalizado")
            .neq("status", "cancelado")
            .limit(500)
            .execute()
        )
        return len(resp.data or [])
    except Exception:
        return 0

def estimate_tempo_entrega_min(pedidos_abertos: int) -> int:
    """Estimativa simples de tempo com base na fila de pedidos."""
    try:
        n = int(pedidos_abertos or 0)
    except Exception:
        n = 0
    if n > 15:
        return 30
    if n > 5:
        return 20
    return 15

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

        pedido_resp = await sb_exec(lambda: supabase.table("pedidos").select(
            "id,restaurante_id,cliente_zap,total_valor,status,payment_status,carrinho_json"
        ).eq("payment_id", mp_payment_id).limit(1).execute())
        if not pedido_resp.data:
            return "ok"
        pedido = pedido_resp.data[0]
        pedido_id = pedido.get("id")
        restaurante_db_id = pedido.get("restaurante_id")

        if (pedido.get("payment_status") or "").lower() == "approved":
            return "ok"

        settings = await sb_exec(lambda: get_pix_settings_for_restaurante(int(restaurante_db_id)))
        if not settings or not settings.get("mp_token"):
            return "ok"

        mp = await _run_blocking(lambda: mp_get_payment(settings["mp_token"], mp_payment_id), timeout=20)
        status = (mp.get("status") or "").lower()
        amount = _money_2(mp.get("transaction_amount") or 0)
        ext_ref = str(mp.get("external_reference") or "")

        expected_amount = _money_2(pedido.get("total_valor") or 0)
        if ext_ref and str(pedido_id) != ext_ref:
            await sb_exec(lambda: supabase.table("pedidos").update({"payment_status": "reference_mismatch"}).eq("id", pedido_id).execute())
            return "ok"

        if abs(amount - expected_amount) > 0.01:
            await sb_exec(lambda: supabase.table("pedidos").update({"payment_status": "amount_mismatch", "payment_amount": amount}).eq("id", pedido_id).execute())
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
                updates["bot_finalizado"] = True
                updates["bot_finalizado_em"] = datetime.now(timezone.utc).isoformat()

        try:
            await sb_exec(lambda: supabase.table("pedidos").update(updates).eq("id", pedido_id).execute())
        except Exception:
            safe_updates = dict(updates)
            safe_updates.pop("bot_finalizado", None)
            safe_updates.pop("bot_finalizado_em", None)
            await sb_exec(lambda: supabase.table("pedidos").update(safe_updates).eq("id", pedido_id).execute())

        if status == "approved":
            try:
                r = await sb_exec(lambda: supabase.table("restaurantes").select("phone_id").eq("id", int(restaurante_db_id)).limit(1).execute())
                if r.data:
                    phone_id = r.data[0].get("phone_id")
                    if phone_id:
                        await enviar_zap_async(phone_id, pedido.get("cliente_zap"), f"✅ Pagamento aprovado! Pedido #{pedido_id} confirmado. Agora aguarde o restaurante aceitar.")
            except Exception:
                pass
            try:
                await _persist_pedido_itens(
                    restaurante_db_id=int(restaurante_db_id),
                    pedido_id=int(pedido_id or 0),
                    carrinho_json=(pedido or {}).get("carrinho_json"),
                )
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
        instance_name = (dados.get("instance_name") or dados.get("instance") or "").strip()
        token = (dados.get("instance_token") or "").strip()

        def _uazapi_status_snapshot():
            if not instance_name or not token:
                return None
            try:
                rr = requests.get(
                    f"{UAZAPI_BASE_URL}/instance/status/{instance_name}",
                    headers={"token": token},
                    verify=HTTP_VERIFY_TLS,
                    timeout=8,
                )
                try:
                    return rr.json()
                except Exception:
                    return {"status_code": rr.status_code, "text": (rr.text or "")[:500]}
            except Exception:
                return None

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

            # Se o token/instância foi atualizado no Supabase, mas ainda está em cache, force refresh e tente 1x.
            lower_body = (r.text or "").lower()
            if r.status_code in (401, 403) or (r.status_code == 503 and "disconnected" in lower_body):
                snap = _uazapi_status_snapshot()
                if snap is not None:
                    logger.warning("UAZAPI instance status snapshot | instance=%s | phone_id=%s | status=%s", instance_name, phone_id, str(snap)[:900])

                try:
                    fresh = get_dados_restaurante(phone_id, tipo="phone_id", force_refresh=True)
                except Exception:
                    fresh = None

                if fresh:
                    fresh_token = (fresh.get("instance_token") or "").strip()
                    fresh_instance = (fresh.get("instance_name") or fresh.get("instance") or instance_name or "").strip()
                    if fresh_token and fresh_token != token:
                        logger.warning(
                            "UAZAPI token refreshed from DB; retrying send once | instance=%s -> %s | phone_id=%s",
                            instance_name,
                            fresh_instance,
                            phone_id,
                        )
                        try:
                            rr = requests.post(
                                f"{UAZAPI_BASE_URL}/send/text",
                                json={"number": numero, "text": texto},
                                headers={"token": fresh_token, "Content-Type": "application/json"},
                                verify=HTTP_VERIFY_TLS,
                                timeout=(5, max(timeout_base, 10) + 10),
                            )
                            if rr.status_code < 400:
                                preview = texto.replace("\n", " ")[:120]
                                logger.info(
                                    "UAZAPI send ok (after token refresh) | status=%s | numero=%s | phone_id=%s | preview=%s",
                                    rr.status_code,
                                    numero,
                                    phone_id,
                                    preview,
                                )
                                return
                            else:
                                logger.error(
                                    "UAZAPI retry failed | status=%s | numero=%s | phone_id=%s | body=%s",
                                    rr.status_code,
                                    numero,
                                    phone_id,
                                    (rr.text or "")[:800],
                                )
                        except Exception:
                            logger.exception("Erro Envio Zap (retry after refresh) | numero=%s | phone_id=%s", numero, phone_id)

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


async def _persist_pedido_itens(*, restaurante_db_id: int, pedido_id: int, carrinho_json: dict | None) -> None:
    if not restaurante_db_id or not pedido_id:
        return
    carrinho = _safe_dict(carrinho_json or {})
    if not carrinho:
        return

    rows = []
    for k, v in carrinho.items():
        if not isinstance(v, dict):
            continue
        try:
            qtd = int(v.get("qtd") or 0)
        except Exception:
            qtd = 0
        if qtd <= 0:
            continue

        item_base = str(v.get("item_base") or k or "").strip()
        item_nome = item_base or str(k or "").strip()
        item_exib = str(v.get("nome_exibicao") or item_nome or "").strip()

        borda = (v.get("borda_sabor") or "").strip() if isinstance(v.get("borda_sabor"), str) else ""
        borda = borda or None

        preco_u = _money_2(v.get("preco_unitario") or 0.0)

        rows.append({
            "pedido_id": int(pedido_id),
            "restaurante_id": int(restaurante_db_id),
            "item_nome": item_nome,
            "item_nome_exibicao": item_exib or None,
            "qtd": int(qtd),
            "preco_unitario": float(preco_u),
            "borda": borda,
        })

    if not rows:
        return

    try:
        await sb_exec(lambda: supabase.table("pedidos_itens").delete().eq("pedido_id", int(pedido_id)).execute())
    except Exception:
        pass

    try:
        await sb_exec(lambda: supabase.table("pedidos_itens").insert(rows).execute())
    except Exception:
        pass

def _is_greeting(txt_norm: str) -> bool:
    t = (txt_norm or "").strip()
    if not t:
        return False

    # Em vez de limitar por tamanho rígido, usamos gatilhos
    greetings_exact = {
        "oi", "ola", "olá", "bom dia", "boa tarde", "boa noite",
        "eai", "e aí", "opa", "menu", "cardapio", "cardápio",
    }
    if t in greetings_exact:
        return True

    # Gatilhos comuns de início "aberto"
    triggers = [
        "to com fome", "tô com fome", "estou com fome",
        "tem o que hoje", "tem oq hoje", "tem o que tem hoje",
        "o que tem", "oq tem", "quais as opcoes", "quais as opções",
        "manda o cardapio", "manda o cardápio", "me passa o cardapio", "me passa o cardápio",
    ]
    return any(tr in t for tr in triggers)


def _is_short_greeting(txt_norm: str) -> bool:
    t = (txt_norm or "").strip()
    if not t:
        return False
    short = {
        "oi", "ola", "bom dia", "boa tarde", "boa noite",
        "eai", "e ai", "opa",
    }
    return t in short  

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

async def _send_repeat_offer(phone_id: str, cliente_zap: str, last_pedido: dict) -> None:
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

    await enviar_zap_async(phone_id, cliente_zap, msg)

    try:
        await sb_exec(lambda: supabase.table("conversas").insert({
            "cliente_zap": cliente_zap,
            "restaurante_id": phone_id,
            "role": "assistant",
            "mensagem": msg
        }).execute())
    except Exception:
        pass

    await sb_exec(lambda: set_estado(
        cliente_zap, phone_id, "CONFIRMAR_PEDIDO_DE_SEMPRE", {"pedido_id_repetir": pedido_id}
    ))

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

            # --- AQUI ESTÁ A ALTERAÇÃO EXATA QUE A IA PEDIU ---
            # Se for meio a meio, pula a reserva de estoque
            if _is_meio_a_meio_item(str(chave_item), dados_item):
                continue
            # --------------------------------------------------

            ok, info = atualizar_estoque_real_time(int(restaurante_db_id), str(chave_item), -qtd)
            if not ok:
                # Rollback: devolve o que já tinha reservado
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
        return bool(redis_client.set(_redis_key(key), "1", nx=True, ex=ttl_seconds))
    except Exception:
        return False

def _abandoned_cleanup_pedido(restaurante_db_id: int, phone_id: str, cliente_zap: str, pedido: dict) -> bool:
    """Cancela + limpa carrinho + devolve estoque (idempotência via Redis lock/dedup)."""
    if not redis_client:
        return False  # sem Redis, risco de dupla devolução de estoque
    pedido_id = pedido.get("id")
    if not pedido_id:
        return False


    once_key = f"abandoned:cleanup:once:{int(restaurante_db_id)}:{pedido_id}"
    if not _redis_setnx_once(once_key, ttl_seconds=24 * 3600):
        return False

    lock_key = f"lock:abandoned:cleanup:{int(restaurante_db_id)}:{pedido_id}"
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
            if qtd > 0 and (not _is_meio_a_meio_item(chave_item, dados_item)):
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


def _extract_json_object_from_text(txt: str):
    """Best-effort extraction of a JSON object from model output."""
    s = (txt or "").strip()
    if not s:
        return None
    # Fast path
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass

    # Try to extract first {...} block
    start = s.find("{")
    end = s.rfind("}")
    if start >= 0 and end > start:
        chunk = s[start : end + 1]
        try:
            obj = json.loads(chunk)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None
    return None


def _is_confusing_message(texto: str, dados_loja: dict | None = None) -> bool:
    raw = str(texto or "").strip()
    if not raw:
        return False
    if len(raw) < 8:
        return False

    letters = sum(1 for c in raw if c.isalpha())
    alpha_ratio = letters / max(1, len(raw))
    if len(raw) >= 12 and alpha_ratio < 0.45:
        return True

    t_norm = normalizar_texto(raw)
    tokens = [t for t in re.split(r"\s+", t_norm) if t]
    if not tokens:
        return True

    has_address_hint = any(k in t_norm for k in ("rua ", "avenida", "av ", "bairro", "cep", "numero", "nº", "no."))
    has_payment_hint = any(k in t_norm for k in ("pix", "dinheiro", "cartao", "cartão", "credito", "crédito", "debito", "débito"))
    has_item_hint = any(k in t_norm for k in ("pizza", "piza", "hamb", "burger", "lanche", "batata", "bebida", "refri", "refriger", "coca", "combo"))

    if (len(tokens) >= 6) and (not has_address_hint) and (not has_payment_hint) and (not has_item_hint):
        return True

    tabela_precos = (dados_loja or {}).get("precos_dict", {}) or {}
    if isinstance(tabela_precos, dict) and tabela_precos:
        for k in list(tabela_precos.keys())[:120]:
            nk = normalizar_texto(k)
            if nk and nk in t_norm:
                return False

    if has_address_hint or has_payment_hint or has_item_hint:
        return False

    return True


def _build_normalize_system_prompt() -> str:
    return (
        "Você reescreve mensagens de clientes de delivery de forma clara e curta.\n"
        "Interprete a mensagem muito bem, extraindo todos os dados relevantes evitando erros para o pedido.\n"
        "Extraia e preserve: itens, quantidades, endereço, número, bairro, observações, dúvidas e pagamento.\n"
        "Não invente dados. Se algo estiver faltando, deixe em branco.\n"
        "Regras de normalização úteis:\n"
        "- Se o cliente pedir pizza \"gigante\" ou \"maior\", normalize o tamanho para \"grande\".\n"
        "- Para meio a meio, escreva no formato: \"1 pizza [tamanho] meia [sabor1] e meia [sabor2]\".\n"
        "- Corrija erros comuns de grafia em sabores (ex.: calabreza->calabresa, catupiri->catupiry), sem criar itens novos.\n"
        "- Se houver borda recheada, preserve no texto como \"com borda [sabor]\".\n"
        "- Se houver alternativa (ex.: 'se não tiver X, manda Y'), mantenha no texto como substituição/alternativa.\n"
        "- A mensagem_normalizada deve conter TODOS os dados relevantes (itens, endereço, bairro, pagamento, observações).\n"
        "Retorne APENAS JSON no formato:\n"
        "{\n"
        "  \"mensagem_normalizada\": \"...\",\n"
        "  \"endereco\": \"...\",\n"
        "  \"bairro\": \"...\",\n"
        "  \"pagamento\": \"...\",\n"
        "  \"observacoes\": \"...\",\n"
        "  \"duvidas\": \"...\"\n"
        "}\n"
    ).strip()


def _sanitize_normalize_response(raw: dict) -> str | None:
    if not isinstance(raw, dict):
        return None
    def _as_str(v) -> str:
        return v.strip() if isinstance(v, str) else ""

    msg = _as_str(raw.get("mensagem_normalizada"))
    end = _as_str(raw.get("endereco"))
    bairro = _as_str(raw.get("bairro"))
    pagamento = _as_str(raw.get("pagamento"))
    obs = _as_str(raw.get("observacoes"))
    duv = _as_str(raw.get("duvidas"))

    parts = []
    if end:
        parts.append(f"endereço: {end}")
    if bairro and (bairro.lower() not in end.lower()):
        parts.append(f"bairro: {bairro}")
    if pagamento:
        parts.append(f"pagamento: {pagamento}")
    if obs:
        parts.append(f"observações: {obs}")
    if duv:
        parts.append(f"dúvida: {duv}")

    if parts:
        if msg:
            msg = f"{msg} | " + " | ".join(parts)
        else:
            msg = " | ".join(parts)

    msg = (msg or "").strip()
    if not msg:
        return None
    return msg[:800]


async def _normalize_message_via_groq(texto: str) -> str | None:
    if not (NORMALIZE_TEXT_ENABLED and GROQ_API_KEY and groq_client):
        return None
    try:
        sys_prompt = _build_normalize_system_prompt()
        user_payload = {"mensagem": str(texto or "")}
        async with _groq_sem:
            chat = await _run_blocking(
                lambda: groq_client.chat.completions.create(
                    model=NORMALIZE_TEXT_MODEL,
                    messages=[
                        {"role": "system", "content": sys_prompt},
                        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                    ],
                    temperature=0.2,
                    response_format={"type": "json_object"},
                ),
                timeout=GROQ_TIMEOUT_SECONDS,
            )
        raw_txt = chat.choices[0].message.content if chat and chat.choices else ""
        raw_obj = _extract_json_object_from_text(raw_txt)
        return _sanitize_normalize_response(raw_obj or {})
    except Exception:
        return None


def _sanitize_intent_router_response(raw: dict):
    """Validates the intent-router JSON. Returns a normalized dict or None."""
    if not isinstance(raw, dict):
        return None

    allowed = {
        # Carrinho
        "adicionar_item",
        "remover_item",
        "fixar_item",
        "adicionar_observacao",
        "trocar_item",
        "pedir_fechamento",
        # Endereço/entrega
        "definir_endereco",
        "definir_retirada",
        # Pagamento
        "definir_pagamento",
        "confirmar_pagamento_pix",
        "reenviar_pix",
        # Pedido
        "cancelar",
        # FAQ/Outros
        "perguntar",
        "perguntar_cardapio",
        "perguntar_ingredientes",
        "perguntar_taxa_entrega",
        "perguntar_bairros",
        "status_pedido",
        "outro",
    }

    intent = raw.get("intencao")
    if not isinstance(intent, str):
        return None
    intent = intent.strip()
    if intent == "checkout":
        intent = "pedir_fechamento"
    if intent not in allowed:
        intent = "outro"

    conf = raw.get("confianca")
    try:
        conf = float(conf)
    except Exception:
        conf = None
    if conf is not None:
        conf = max(0.0, min(1.0, conf))

    params = raw.get("parametros")
    if not isinstance(params, dict):
        params = {}

    follow = raw.get("pergunta_followup")
    if follow is not None and not isinstance(follow, str):
        follow = None
    follow = (follow or "").strip() or None

    return {
        "intencao": intent,
        "confianca": conf,
        "parametros": params,
        "pergunta_followup": follow,
    }


def _build_intent_router_system_prompt(*, prompt_usuario: str, cardapio: str) -> str:
    # Prompt minimalista, focado em classificação + extração, não em conversar.
    return (
        "Você é o CÉREBRO de um sistema de delivery.\n"
        "Sua missão: Classificar a intenção e extrair dados estruturados em JSON.\n\n"
        "Inclua um campo 'raciocinio' com um resumo curto (1 frase) do que entendeu.\n\n"
        
        "⚠️ REGRAS DE PRIORIDADE (LEIA COM ATENÇÃO):\n"
        "1. COMBO (Item + Endereço): Se o usuário pedir comida E mandar endereço junto, a intenção É 'adicionar_item'.\n"
        "   - Coloque os itens na lista 'acoes_carrinho'.\n"
        "   - Coloque o endereço em 'parametros.endereco_txt'.\n"
        "2. ENDEREÇO PURO: Use 'definir_endereco' APENAS se não houver pedido de comida junto.\n"
        "3. NUMEROS: 'Rua 1031' é número da casa, NÃO é quantidade de item. '2 coca' é quantidade.\n"
        "4. PREÇOS: Ignore preços que o usuário falar (ex: 'que seja 3 reais'). Use apenas o nome do produto.\n\n"

        f"CONTEXTO DO RESTAURANTE: {prompt_usuario}\n"
        f"CARDAPIO OFICIAL: {cardapio}\n\n"

        "INTENÇÕES VÁLIDAS:\n"
        "- adicionar_item, remover_item, trocar_item, adicionar_observacao (Para qualquer alteração de comida)\n"
        "- definir_endereco, definir_retirada\n"
        "- definir_pagamento\n"
        "- pedir_fechamento (finalizar)\n"
        "- cancelar\n"
        "- perguntar (dúvidas gerais)\n\n"

        "FORMATO JSON OBRIGATÓRIO (Exemplo de Combo):\n"
        "{\n"
        "  \"raciocinio\": \"O cliente pediu uma coca e informou endereço completo com bairro.\",\n"
        "  \"intencao\": \"adicionar_item\",\n"
        "  \"confianca\": 1.0,\n"
        "  \"acoes_carrinho\": [\n"
        "     {\"nome\": \"Coca-Cola\", \"qtd\": 1, \"observacao\": \"gelada\"}\n"
        "  ],\n"
        "  \"parametros\": {\n"
        "     \"endereco_txt\": \"Rua Vicente Celestino, 1031\",\n"
        "     \"bairro\": \"Centro\",\n"
        "     \"forma_pagamento\": \"pix\"\n"
        "  }\n"
        "}\n"
        "Retorne APENAS o JSON."
    ).strip()


async def _classify_global_intent(*, phone_id: str, cliente_zap: str, texto: str, estado_atual: str, pedido_ativo: dict | None, dados_loja: dict) -> dict | None:
    if not GROQ_API_KEY or not groq_client:
        return None

    prompt_usuario_banco = (dados_loja.get("system_prompt", "") or "").strip()
    cardapio = (dados_loja.get("cardapio", "") or "").strip()

    carrinho_resumo = ""
    try:
        carrinho_resumo = (pedido_ativo or {}).get("resumo_pedido") or ""
    except Exception:
        carrinho_resumo = ""

    sys_prompt = _build_intent_router_system_prompt(prompt_usuario=prompt_usuario_banco, cardapio=cardapio)
    user_payload = {
        "estado_atual": str(estado_atual or ""),
        "pedido_status": str((pedido_ativo or {}).get("status") or ""),
        "payment_status": str((pedido_ativo or {}).get("payment_status") or ""),
        "carrinho_resumo": str(carrinho_resumo),
        "mensagem": str(texto or ""),
    }

    try:
        async with _groq_sem:
            chat = await _run_blocking(
                lambda: groq_client.chat.completions.create(
                    model=INTENT_ROUTER_MODEL,
                    messages=[
                        {"role": "system", "content": sys_prompt},
                        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                    ],
                    temperature=0.0,
                    response_format={"type": "json_object"},
                ),
                timeout=GROQ_TIMEOUT_SECONDS,
            )

        raw_txt = chat.choices[0].message.content if chat and chat.choices else ""
        raw_obj = _extract_json_object_from_text(raw_txt)
        if not raw_obj:
            return None
        return _sanitize_intent_router_response(raw_obj)
    except Exception:
        return None


async def _handle_troca_item_deterministica(
    *,
    phone_id,
    cliente_zap,
    restaurante_db_id: int,
    pedido_ativo: dict | None,
    dados_loja: dict,
    texto_completo: str,
    old_raw: str | None,
    new_raw: str | None,
) -> bool:
    """Troca determinística: remove item antigo do carrinho e adiciona o novo, com ajuste de estoque.

    Retorna True se tratou (respondeu ao cliente). False se não foi possível tratar.
    """

    def _strip_artigos(s: str) -> str:
        s = (s or "").strip()
        for pref in ("a ", "o ", "as ", "os ", "um ", "uma "):
            if s.startswith(pref):
                return s[len(pref):].strip()
        return s

    old_raw = _strip_artigos(old_raw or "")
    new_raw = _strip_artigos(new_raw or "")

    if not pedido_ativo or not _safe_dict((pedido_ativo or {}).get("carrinho_json")):
        await enviar_zap_async(phone_id, cliente_zap, "Seu carrinho está vazio. Me diga o que você quer pedir. 🙂")
        return True

    carrinho_atual = _safe_dict(pedido_ativo.get("carrinho_json"))
    carrinho_keys = list((carrinho_atual or {}).keys())

    def _listar_itens_carrinho() -> str:
        itens_txt = []
        for k, v in (carrinho_atual or {}).items():
            try:
                qtd = int((v or {}).get("qtd") or 0)
            except Exception:
                qtd = 0
            if qtd <= 0:
                continue
            nome = (v or {}).get("nome_exibicao") or k.title()
            itens_txt.append(f"- {qtd}x {nome}")
        return "\n".join(itens_txt) if itens_txt else "(carrinho vazio)"

    # Se faltou alguma parte, pede clarificação.
    if not old_raw and not new_raw:
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "Entendi que você quer trocar um item.\n\n"
            "Me diga assim: *trocar X por Y*\n"
            "Ex.: *trocar coca por guaraná*",
        )
        return True
    if not old_raw:
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "Qual item do seu carrinho eu devo trocar?\n" + _listar_itens_carrinho(),
        )
        return True
    if not new_raw:
        await enviar_zap_async(phone_id, cliente_zap, "Trocar por qual item do cardápio?")
        return True

    old_term = normalizar_texto(old_raw)

    def _pick_old_key_for_swap() -> str | None:
        if old_term and old_term in (carrinho_atual or {}):
            return old_term

        m1 = difflib.get_close_matches(old_term, carrinho_keys, n=1, cutoff=0.55)
        if m1:
            return m1[0]

        display_terms = []
        display_to_key = {}
        for k, v in (carrinho_atual or {}).items():
            disp = normalizar_texto(((v or {}).get("nome_exibicao") or k))
            if disp:
                display_terms.append(disp)
                display_to_key[disp] = k

        m2 = difflib.get_close_matches(old_term, display_terms, n=1, cutoff=0.60)
        if m2:
            return display_to_key.get(m2[0])

        categorias_dict = dados_loja.get("categorias_dict", {}) or {}

        def _is_pizza_item(key: str) -> bool:
            cat = str(categorias_dict.get(key) or "")
            return key.startswith("meio ") or ("pizza" in cat) or ("pizza" in key)

        if "pizza" in old_term:
            pizza_candidates = [k for k in carrinho_keys if _is_pizza_item(k)]
            if len(pizza_candidates) == 1:
                return pizza_candidates[0]

        if len(carrinho_keys) == 1:
            return carrinho_keys[0]

        return None

    old_key = _pick_old_key_for_swap()
    if not old_key:
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            f"Não consegui identificar qual item você quis trocar por *{old_raw}*.\n"
            "Me diga exatamente qual destes eu devo trocar:\n"
            + _listar_itens_carrinho(),
        )
        return True

    tabela_precos = dados_loja.get("precos_dict", {}) or {}
    nomes_oficiais = list(tabela_precos.keys())

    nomes_oficiais_norm = []
    norm_to_key = {}
    for k in nomes_oficiais:
        nk = normalizar_texto(k)
        if not nk:
            continue
        if nk not in norm_to_key:
            norm_to_key[nk] = k
            nomes_oficiais_norm.append(nk)

    new_term = normalizar_texto(new_raw)
    if new_term in norm_to_key:
        new_key = norm_to_key[new_term]
    else:
        new_match_norm = difflib.get_close_matches(new_term, nomes_oficiais_norm, n=1, cutoff=0.60)
        if not new_match_norm:
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                f"Esse item não existe no cardápio: *{new_raw}*.\nQuer que eu te mande o cardápio?",
            )
            return True
        new_key = norm_to_key.get(new_match_norm[0])

    if not new_key:
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            f"Esse item não existe no cardápio: *{new_raw}*.\nQuer que eu te mande o cardápio?",
        )
        return True

    try:
        qtd_old = int((carrinho_atual.get(old_key, {}) or {}).get("qtd") or 0)
    except Exception:
        qtd_old = 0
    if qtd_old <= 0:
        await enviar_zap_async(phone_id, cliente_zap, "Esse item já está zerado no seu carrinho. Me diga o que você quer adicionar. 🙂")
        return True

    old_nome = (carrinho_atual.get(old_key, {}) or {}).get("nome_exibicao") or old_key.title()
    new_nome = new_key.title()

    ok_new, info_new = await atualizar_estoque_real_time_async(restaurante_db_id, new_key, -qtd_old)
    if not ok_new:
        estoque_restante = (info_new or {}).get("estoque_atual", 0) or 0
        if int(estoque_restante) > 0:
            await enviar_zap_async(phone_id, cliente_zap, f"⚠️ Só restam *{estoque_restante}* de *{new_nome}*. Não consegui trocar agora.")
        else:
            await enviar_zap_async(phone_id, cliente_zap, f"⚠️ *{new_nome}* acabou de esgotar. Não consegui trocar agora.")
        return True

    old_dados = (carrinho_atual.get(old_key) or {}) if isinstance(carrinho_atual, dict) else {}
    if not _is_meio_a_meio_item(old_key, old_dados):
        await atualizar_estoque_real_time_async(restaurante_db_id, old_key, +qtd_old)

    carrinho_atual.pop(old_key, None)

    try:
        preco_unit = float(tabela_precos.get(new_key) or 0.0)
    except Exception:
        preco_unit = 0.0

    if new_key not in carrinho_atual:
        carrinho_atual[new_key] = {
            "nome_exibicao": new_nome,
            "qtd": 0,
            "preco_unitario": float(preco_unit),
            "observacao": "",
        }
    carrinho_atual[new_key]["qtd"] = int(carrinho_atual[new_key].get("qtd") or 0) + qtd_old

    resumo_list = []
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

        resumo_list.append(f"{qtd}x {nome_disp}{txt_obs} (R$ {total_item:.2f})")

    novo_resumo = " | ".join(resumo_list) if resumo_list else "Carrinho vazio"
    carrinho_display, _ = _format_carrinho_display(carrinho_atual)
    carrinho_display, _ = _format_carrinho_display(carrinho_atual)
    carrinho_display, _ = _format_carrinho_display(carrinho_atual)

    try:
        await sb_exec(
            lambda: supabase.table("pedidos").update({
                "carrinho_json": carrinho_atual,
                "resumo_pedido": novo_resumo,
                "total_valor": total_geral,
                "status": "novo",
            }).eq("id", pedido_ativo["id"]).execute()
        )
    except Exception:
        try:
            await atualizar_estoque_real_time_async(restaurante_db_id, new_key, +qtd_old)
        except Exception:
            pass
        raise

    await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_MAIS_ALGO", {}))

    await enviar_zap_async(phone_id, cliente_zap, f"✅ Troquei *{old_nome}* por *{new_nome}* (x{qtd_old}).")
    await enviar_zap_async(
        phone_id,
        cliente_zap,
        f"🛒 *Carrinho Atualizado:*\n{carrinho_display}\n💰 Total: *R$ {_format_brl(total_geral)}*",
    )
    await enviar_zap_async(
        phone_id,
        cliente_zap,
        "Gostaria de mais alguma coisa? Responda *sim* para adicionar mais itens ou *não* para finalizar.",
    )
    return True


async def _handle_remover_item_deterministica(
    *,
    phone_id,
    cliente_zap,
    restaurante_db_id: int,
    pedido_ativo: dict | None,
    dados_loja: dict,
    item_raw: str | None,
    qtd: int | None = None,
) -> bool:
    """Remove determinístico: remove item do carrinho (total ou quantidade)."""

    def _strip_artigos(s: str) -> str:
        s = (s or "").strip()
        for pref in ("a ", "o ", "as ", "os ", "um ", "uma "):
            if s.startswith(pref):
                return s[len(pref):].strip()
        return s

    item_raw = _strip_artigos(item_raw or "")

    if not pedido_ativo or not _safe_dict((pedido_ativo or {}).get("carrinho_json")):
        await enviar_zap_async(phone_id, cliente_zap, "Seu carrinho está vazio. Me diga o que você quer pedir. 🙂")
        return True

    carrinho_atual = _safe_dict(pedido_ativo.get("carrinho_json"))
    carrinho_keys = list((carrinho_atual or {}).keys())

    def _listar_itens_carrinho() -> str:
        itens_txt = []
        for k, v in (carrinho_atual or {}).items():
            try:
                qtd_local = int((v or {}).get("qtd") or 0)
            except Exception:
                qtd_local = 0
            if qtd_local <= 0:
                continue
            nome = (v or {}).get("nome_exibicao") or k.title()
            itens_txt.append(f"- {qtd_local}x {nome}")
        return "\n".join(itens_txt) if itens_txt else "(carrinho vazio)"

    if not item_raw:
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "Qual item você quer remover?\n" + _listar_itens_carrinho(),
        )
        return True

    termo = normalizar_texto(item_raw)
    if not termo:
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "Qual item você quer remover?\n" + _listar_itens_carrinho(),
        )
        return True

    def _pick_key() -> str | None:
        if termo in (carrinho_atual or {}):
            return termo

        m1 = difflib.get_close_matches(termo, carrinho_keys, n=1, cutoff=0.55)
        if m1:
            return m1[0]

        display_terms = []
        display_to_key = {}
        for k, v in (carrinho_atual or {}).items():
            disp = normalizar_texto(((v or {}).get("nome_exibicao") or k))
            if disp:
                display_terms.append(disp)
                display_to_key[disp] = k

        m2 = difflib.get_close_matches(termo, display_terms, n=1, cutoff=0.60)
        if m2:
            return display_to_key.get(m2[0])

        if len(carrinho_keys) == 1:
            return carrinho_keys[0]

        return None

    chave_item = _pick_key()
    if not chave_item:
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            f"Não consegui identificar qual item remover: *{item_raw}*.\n"
            "Me diga exatamente qual destes eu devo remover:\n"
            + _listar_itens_carrinho(),
        )
        return True

    try:
        qtd_atual = int((carrinho_atual.get(chave_item, {}) or {}).get("qtd") or 0)
    except Exception:
        qtd_atual = 0

    if qtd_atual <= 0:
        await enviar_zap_async(phone_id, cliente_zap, "Esse item já não está no carrinho.")
        return True

    qtd_remover = qtd_atual
    if qtd is not None:
        try:
            qtd_remover = max(1, min(qtd_atual, int(qtd)))
        except Exception:
            qtd_remover = qtd_atual

    dados_item = carrinho_atual.get(chave_item) or {}
    if not _is_meio_a_meio_item(chave_item, dados_item):
        try:
            await atualizar_estoque_real_time_async(restaurante_db_id, chave_item, +qtd_remover)
        except Exception:
            pass

    carrinho_atual[chave_item]["qtd"] = max(0, qtd_atual - qtd_remover)
    if carrinho_atual[chave_item]["qtd"] <= 0:
        carrinho_atual.pop(chave_item, None)

    resumo_list = []
    total_geral = 0.0
    for _, dados_item_loop in (carrinho_atual or {}).items():
        try:
            qtd_loop = int(dados_item_loop.get("qtd") or 0)
        except Exception:
            qtd_loop = 0
        if qtd_loop <= 0:
            continue
        try:
            preco_u = float(dados_item_loop.get("preco_unitario") or 0.0)
        except Exception:
            preco_u = 0.0
        total_item = qtd_loop * preco_u
        total_geral += total_item

        obs_parts = []
        obs_comp = dados_item_loop.get("obs_componentes") or {}
        comps = dados_item_loop.get("componentes") or []
        if isinstance(obs_comp, dict) and obs_comp:
            for comp in comps:
                o = (obs_comp.get(comp) or "").strip()
                if o:
                    obs_parts.append(f"1/2 {str(comp).title()}: {o}")

        obs_geral = (dados_item_loop.get("observacao") or "").strip()
        if obs_geral:
            obs_parts.append(obs_geral)

        txt_obs = f" ({'; '.join(obs_parts)})" if obs_parts else ""
        nome_disp_loop = dados_item_loop.get("nome_exibicao", "") or ""
        comps = dados_item_loop.get("componentes") or []
        if isinstance(comps, list) and len(comps) == 2:
            nome_disp_loop = "Meio " + " / ".join([str(c).title() for c in comps])

        resumo_list.append(f"{qtd_loop}x {nome_disp_loop}{txt_obs} (R$ {total_item:.2f})")

    novo_resumo = " | ".join(resumo_list) if resumo_list else "Carrinho vazio"
    carrinho_display, _ = _format_carrinho_display(carrinho_atual)
    try:
        await sb_exec(
            lambda: supabase.table("pedidos").update({
                "carrinho_json": carrinho_atual,
                "resumo_pedido": novo_resumo,
                "total_valor": float(total_geral or 0.0),
                "status": "novo",
            }).eq("id", pedido_ativo["id"]).execute()
        )
    except Exception:
        pass

    await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_MAIS_ALGO", {}))
    nome_disp = (dados_item.get("nome_exibicao") or chave_item.title())
    await enviar_zap_async(phone_id, cliente_zap, f"✅ Removi *{nome_disp}* do seu carrinho.")
    await enviar_zap_async(
        phone_id,
        cliente_zap,
        f"🛒 *Carrinho Atualizado:*\n{carrinho_display}\n💰 Total: *R$ {_format_brl(total_geral)}*",
    )
    await enviar_zap_async(
        phone_id,
        cliente_zap,
        "Gostaria de mais alguma coisa? Responda *sim* para adicionar mais itens ou *não* para finalizar.",
    )
    return True


async def _handle_definir_endereco(
    *,
    phone_id,
    cliente_zap,
    texto_completo: str,
    endereco_param: str | None,
    bairro_param: str | None,
    bairros_dict: dict,
    lista_bairros_txt: str,
    pedido_ativo: dict | None,
    restaurante_db_id: int | None = None,
    dados_parciais: dict | None = None,
    forma_pagamento: str | None = None,
    now_iso: str | None = None,
) -> bool:
    """Resolve endereço/bairro e avança para pagamento quando possível."""

    dados_parciais = dict(dados_parciais or {})
    bairro_param = (bairro_param or "").strip()
    endereco_param = (endereco_param or "").strip()

    # Complemento fragmentado (ex.: "número 50 ap 202") após ter enviado a rua antes
    try:
        endereco_prev = str(dados_parciais.get("endereco_txt") or "").strip()
        txt_norm_local = normalizar_texto(texto_completo or "")
        logradouros = ("rua ", "r. ", "av ", "av. ", "avenida ", "travessa ", "tv ", "tv. ", "alameda ", "estrada ", "rodovia ", "beco ", "viela ", "vila ")
        tem_logradouro = any(l in txt_norm_local for l in logradouros)
        if endereco_prev and _texto_parece_complemento_endereco(texto_completo, txt_norm_local) and not tem_logradouro:
            if endereco_prev not in texto_completo:
                endereco_param = f"{endereco_prev}, {texto_completo}".strip(" ,")
    except Exception:
        pass

    bairro_match = None
    if bairros_dict and bairro_param:
        bairro_match = _match_bairro_from_input(bairro_param, bairros_dict)
    if bairros_dict and (not bairro_match):
        bairro_match = _match_bairro_from_input(texto_completo, bairros_dict)
    if (not bairro_match) and (not bairros_dict) and bairro_param:
        bairro_match = bairro_param

    if not bairro_match and isinstance(dados_parciais, dict):
        bairro_prev = str(dados_parciais.get("bairro") or "").strip()
        if bairro_prev:
            bairro_match = bairro_prev

    if not bairro_match:
        endereco_txt = endereco_param or texto_completo
        endereco_norm = normalizar_texto(endereco_txt or "")
        if endereco_norm in ("a determinar", "a definir", "a confirmar", "a combinar", "nao sei", "não sei"):
            endereco_txt = ""
        end_extrato = extrair_endereco_de_texto(endereco_txt) if _texto_parece_endereco(endereco_txt, normalizar_texto(endereco_txt)) else None
        dados_next = dict(dados_parciais or {})
        if end_extrato:
            dados_next["endereco_txt"] = end_extrato
        elif _texto_parece_endereco(endereco_txt, normalizar_texto(endereco_txt)) and endereco_txt.strip():
            dados_next["endereco_txt"] = endereco_txt.strip()
        elif str(dados_parciais.get("endereco_txt") or "").strip():
            dados_next["endereco_txt"] = str(dados_parciais.get("endereco_txt") or "").strip()
        await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_ENDERECO", dados_next))
        bairro_candidato = bool(bairro_param) or (bool(endereco_param) and not _texto_parece_endereco(endereco_param, normalizar_texto(endereco_param)))
        if bairro_candidato:
            bairro_label = _extract_bairro_from_text(bairro_param or endereco_param or "") or ""
            if bairro_label:
                await enviar_zap_async(phone_id, cliente_zap, f"Não entregamos para o bairro *{bairro_label}*. Caso considere que é algum erro, posso chamar um atendente.")
            else:
                await enviar_zap_async(phone_id, cliente_zap, "Não entregamos para esse bairro. Caso considere que é algum erro, posso chamar um atendente.")
        else:
            await enviar_zap_async(phone_id, cliente_zap, "Qual é o *bairro*? Assim eu confirmo a taxa certinho.")
        return True

    try:
        taxa = float(bairros_dict[bairro_match])
    except Exception:
        taxa = 0.0

    endereco_txt = endereco_param or texto_completo
    endereco_norm = normalizar_texto(endereco_txt or "")
    if endereco_norm in ("a determinar", "a definir", "a confirmar", "a combinar", "nao sei", "não sei"):
        endereco_txt = ""
    if not _texto_parece_endereco(endereco_txt, normalizar_texto(endereco_txt)):
        endereco_prev = str((dados_parciais or {}).get("endereco_txt") or "").strip()
        if endereco_prev and _texto_parece_endereco(endereco_prev, normalizar_texto(endereco_prev)):
            endereco_txt = endereco_prev
        else:
            dados_next = dict(dados_parciais or {})
            dados_next.update({"bairro": bairro_match, "taxa": taxa})
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_ENDERECO", dados_next))
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                f"📍 Bairro: *{str(bairro_match).title()}* (Taxa: R$ {taxa:.2f}).\n\n"
                "Agora me envie o *endereço completo*: *rua/avenida + número* (e complemento, se tiver).",
            )
            return True

    fp_norm = str(forma_pagamento or (dados_parciais or {}).get("forma_pagamento") or "").strip().lower()
    if fp_norm not in ("pix", "dinheiro", "cartao"):
        fp_norm = ""

    dados_next = {"endereco_txt": endereco_txt, "bairro": bairro_match, "taxa": taxa}
    if fp_norm:
        dados_next["forma_pagamento"] = fp_norm

    if _audio_flags_by_conv.pop((str(phone_id), str(cliente_zap)), False):
        await sb_exec(lambda: set_estado(cliente_zap, phone_id, "CONFIRMAR_ENDERECO_AUDIO", dados_next))
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "📍 Confirma pra mim, por favor:\n"
            f"Endereço: *{endereco_txt}*\n"
            f"Bairro: *{str(bairro_match).title()}* (Taxa: R$ {taxa:.2f})\n\n"
            "Se estiver correto, responda *sim*. Se não, envie o endereço novamente.",
        )
        return True

    await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", dados_next))
    if fp_norm and pedido_ativo and restaurante_db_id and now_iso:
        handled = await _handle_pagamento_flow(
            phone_id=phone_id,
            cliente_zap=cliente_zap,
            restaurante_db_id=int(restaurante_db_id),
            pedido_ativo=pedido_ativo,
            dados_parciais=dados_next,
            txt_norm=fp_norm,
            texto_completo=fp_norm,
            now_iso=now_iso,
        )
        return bool(handled)
    total_prod = float((pedido_ativo or {}).get("total_valor") or 0.0)
    total_com_taxa = total_prod + float(taxa or 0.0)
    await enviar_zap_async(
        phone_id,
        cliente_zap,
        f"📍 Identifiquei: *{str(bairro_match).title()}* (Taxa: R$ {taxa:.2f}).\n"
        f"💰 *Total Final: R$ {total_com_taxa:.2f}*\n\n"
        "Qual a forma de pagamento? *(Pix, Dinheiro ou Cartão)*",
    )
    return True


async def _handle_pagamento_flow(
    *,
    phone_id,
    cliente_zap,
    restaurante_db_id: int,
    pedido_ativo: dict | None,
    dados_parciais: dict,
    txt_norm: str,
    texto_completo: str,
    now_iso: str,
) -> bool:
    """Processa a escolha de pagamento (Pix/Dinheiro/Cartão) e finaliza o pedido.

    Retorna True quando respondeu algo ao cliente.
    """

    pgto_limpo = txt_norm
    forma_escolhida = None
    dados_parciais = dict(dados_parciais or {})

    aguardando_troco = bool(dados_parciais.get("aguardando_troco"))
    if aguardando_troco and str(dados_parciais.get("forma_pagamento") or "").strip().lower() == "dinheiro":
        t_all = normalizar_texto(texto_completo or "")
        if re.search(r"\b(sem\s+troco|nao\s+precisa\s+de\s+troco|não\s+precisa\s+de\s+troco)\b", t_all):
            dados_parciais["troco_para"] = 0.0
        else:
            m_troco = re.search(r"\btroco\b\s*(?:pra|para)?\s*(?:r\$\s*)?(\d+(?:[\.,]\d{1,2})?)", t_all)
            if m_troco:
                raw_val = (m_troco.group(1) or "").strip().replace(".", "").replace(",", ".")
                try:
                    dados_parciais["troco_para"] = float(raw_val)
                except Exception:
                    pass
            elif re.fullmatch(r"\d+(?:[\.,]\d{1,2})?", (texto_completo or "").strip()):
                raw_val = (texto_completo or "").strip().replace(".", "").replace(",", ".")
                try:
                    dados_parciais["troco_para"] = float(raw_val)
                except Exception:
                    pass

        if "troco_para" not in dados_parciais:
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", dados_parciais))
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                "Vai precisar de troco? Se sim, me diga: *troco para R$ X*. Se não, responda *sem troco*.",
            )
            return True

        dados_parciais.pop("aguardando_troco", None)
        try:
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", dados_parciais))
        except Exception:
            pass
        pgto_limpo = "dinheiro"

    t = (pgto_limpo or "").strip()
    parece_pergunta = (
        ("?" in (texto_completo or ""))
        or t.startswith(("vem ", "tem ", "tem ", "vai ", "pode "))
        or any(k in t for k in ("vem cebola", "tem cebola", "ingrediente", "ingredientes", "vem com", "vai com", "tem ", "cebola"))
    )

    if parece_pergunta and not any(k in t for k in ("pix", "dinheiro", "especie", "espécie", "cartao", "cartão", "credito", "crédito", "debito", "débito")):
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "Posso te ajudar nisso. 🙂\n"
            "Se você quiser tirar algum ingrediente, é só dizer por exemplo: *'sem cebola'*.\n\n"
            "Agora, pra eu finalizar: qual a forma de pagamento? *(Pix, Dinheiro ou Cartão)*",
        )
        return True

    if "pix" in pgto_limpo:
        forma_escolhida = "Pix"
    elif "dinheiro" in pgto_limpo or "especie" in pgto_limpo or "espécie" in pgto_limpo:
        forma_escolhida = "Dinheiro"
    elif any(k in pgto_limpo for k in ("cartao", "cartão", "credito", "crédito", "debito", "débito")):
        forma_escolhida = "Cartão"

    if not forma_escolhida:
        await enviar_zap_async(phone_id, cliente_zap, "Não entendi a forma de pagamento. Aceitamos: Pix, Dinheiro ou Cartão.")
        return True

    if forma_escolhida == "Dinheiro":
        t_all = normalizar_texto(texto_completo or "")
        if re.search(r"\b(sem\s+troco|nao\s+precisa\s+de\s+troco|não\s+precisa\s+de\s+troco)\b", t_all):
            dados_parciais["troco_para"] = 0.0
        else:
            m_troco = re.search(r"\btroco\b\s*(?:pra|para)?\s*(?:r\$\s*)?(\d+(?:[\.,]\d{1,2})?)", t_all)
            if m_troco:
                raw_val = (m_troco.group(1) or "").strip().replace(".", "").replace(",", ".")
                try:
                    dados_parciais["troco_para"] = float(raw_val)
                except Exception:
                    pass

        if "troco_para" not in dados_parciais:
            dados_parciais["forma_pagamento"] = "dinheiro"
            dados_parciais["aguardando_troco"] = True
            try:
                await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", dados_parciais))
            except Exception:
                pass
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                "Vai precisar de troco? Se sim, me diga: *troco para R$ X*. Se não, responda *sem troco*.",
            )
            return True

    endereco_final = (dados_parciais or {}).get("endereco_txt", "Endereço não capturado")
    bairro_final = (dados_parciais or {}).get("bairro", "")
    tipo_entrega_raw = str((dados_parciais or {}).get("tipo_entrega") or "entrega").strip().lower()
    tipo_entrega_final = "retirada" if tipo_entrega_raw in ("retirada", "retirar", "buscar", "vou buscar") else "entrega"
    taxa_final = 0.0 if tipo_entrega_final == "retirada" else float((dados_parciais or {}).get("taxa", 0.0) or 0.0)

    total_final = 0.0
    if pedido_ativo:
        total_final = float((pedido_ativo or {}).get("total_valor") or 0.0) + float(taxa_final or 0.0)

    pix_created = False
    pix_payload = None
    pix_settings = None

    if pedido_ativo and forma_escolhida == "Pix":
        pix_settings = await sb_exec(lambda: get_pix_settings_for_restaurante(int(restaurante_db_id)))
        if not (pix_settings and pix_settings.get("enabled")):
            # Pix desativado no WhatsApp: trata como Pix na entrega (maquininha/QR no local)
            update_base = {
                "endereco_completo": ("Retirada no local" if tipo_entrega_final == "retirada" else f"{endereco_final} ({bairro_final})"),
                "tipo_entrega": tipo_entrega_final,
                "forma_pagamento": "Pix (na entrega)",
                "total_valor": total_final,
                "status": "confirmado",
                "bot_finalizado": True,
                "bot_finalizado_em": now_iso,
            }
            if pedido_ativo:
                try:
                    await sb_exec(lambda: supabase.table("pedidos").update(update_base).eq("id", pedido_ativo["id"]).execute())
                except Exception:
                    safe_base = dict(update_base)
                    safe_base.pop("bot_finalizado", None)
                    safe_base.pop("bot_finalizado_em", None)
                    await sb_exec(lambda: supabase.table("pedidos").update(safe_base).eq("id", pedido_ativo["id"]).execute())
            if pedido_ativo:
                await _persist_pedido_itens(
                    restaurante_db_id=int(restaurante_db_id),
                    pedido_id=int(pedido_ativo.get("id") or 0),
                    carrinho_json=(pedido_ativo or {}).get("carrinho_json"),
                )
            msg = (
                "✅ *Pedido Confirmado!*\n\n"
                f"📍 Entrega em: {endereco_final}\n"
                f"💳 Pagamento: Pix na entrega\n"
                f"🧾 Taxa de entrega: R$ {taxa_final:.2f}\n"
                f"💰 Total: R$ {total_final:.2f}\n\n"
                "O entregador levará a maquininha/QR para você pagar no local."
            )
            await enviar_zap_async(phone_id, cliente_zap, msg)
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
            try:
                await sb_exec(lambda: supabase.table("conversas").insert({
                    "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
                }).execute())
            except Exception:
                pass
            return True

    update_base = {
        "endereco_completo": ("Retirada no local" if tipo_entrega_final == "retirada" else f"{endereco_final} ({bairro_final})"),
        "tipo_entrega": tipo_entrega_final,
        "forma_pagamento": forma_escolhida,
        "total_valor": total_final,
        "status": "confirmado",
        "bot_finalizado": True,
        "bot_finalizado_em": now_iso,
    }

    # Inferência: se a pessoa falou em "troco para X", registra no pagamento.
    if forma_escolhida == "Dinheiro":
        try:
            troco_para = (dados_parciais or {}).get("troco_para")
            troco_para = float(troco_para) if troco_para is not None else None
        except Exception:
            troco_para = None
        if troco_para is not None and troco_para > 0:
            update_base["forma_pagamento"] = f"Dinheiro (troco para R$ {_money_2(troco_para):.2f})"

    if (
        pedido_ativo
        and forma_escolhida == "Pix"
        and pix_settings
        and pix_settings.get("enabled")
        and (pix_settings.get("provider") or "mercadopago") == "mercadopago"
        and pix_settings.get("mp_token")
    ):
        update_base["status"] = "novo"
        update_base["forma_pagamento"] = "Pix (Aguardando pagamento)"
        update_base["bot_finalizado"] = False
        update_base["bot_finalizado_em"] = None

    if pedido_ativo:
        try:
            await sb_exec(lambda: supabase.table("pedidos").update(update_base).eq("id", pedido_ativo["id"]).execute())
        except Exception:
            safe_base = dict(update_base)
            safe_base.pop("bot_finalizado", None)
            safe_base.pop("bot_finalizado_em", None)
            await sb_exec(lambda: supabase.table("pedidos").update(safe_base).eq("id", pedido_ativo["id"]).execute())

    if pedido_ativo and update_base.get("bot_finalizado") is True:
        await _persist_pedido_itens(
            restaurante_db_id=int(restaurante_db_id),
            pedido_id=int(pedido_ativo.get("id") or 0),
            carrinho_json=(pedido_ativo or {}).get("carrinho_json"),
        )

    if (
        pedido_ativo
        and forma_escolhida == "Pix"
        and pix_settings
        and pix_settings.get("enabled")
        and (pix_settings.get("provider") or "mercadopago") == "mercadopago"
        and pix_settings.get("mp_token")
    ):
        try:
            pix_payload = await _run_blocking(
                lambda: mp_create_pix_payment(
                    pix_settings["mp_token"],
                    amount=total_final,
                    description=f"Pedido #{pedido_ativo['id']}",
                    external_reference=str(pedido_ativo["id"]),
                    payer_email=_payer_email_from_cliente(cliente_zap),
                ),
                timeout=20,
            )

            payment_id = str((pix_payload or {}).get("id") or "").strip()
            payment_status = str((pix_payload or {}).get("status") or "pending").strip().lower()

            poi = (pix_payload.get("point_of_interaction") or {}) if isinstance(pix_payload, dict) else {}
            tx = (poi.get("transaction_data") or {}) if isinstance(poi, dict) else {}

            payment_qr_code = (tx.get("qr_code") or "") if isinstance(tx, dict) else ""
            payment_ticket_url = (tx.get("ticket_url") or "") if isinstance(tx, dict) else ""

            if payment_id and pedido_ativo:
                await sb_exec(lambda: supabase.table("pedidos").update({
                    "payment_provider": "mercadopago",
                    "payment_id": payment_id,
                    "payment_status": payment_status,
                    "payment_amount": _money_2(total_final),
                    "payment_qr_code": payment_qr_code,
                    "payment_ticket_url": payment_ticket_url,
                }).eq("id", pedido_ativo["id"]).execute())
                pix_created = True
        except Exception as e:
            print(f"❌ Erro ao criar Pix MP: {e}")

    if forma_escolhida == "Pix" and pedido_ativo:
        if pix_created:
            payment_id = str((pix_payload or {}).get("id") or "").strip()
            poi = ((pix_payload or {}).get("point_of_interaction") or {}) if isinstance(pix_payload, dict) else {}
            tx = (poi.get("transaction_data") or {}) if isinstance(tx, dict) else {}
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
                "⏳ Aguarde o restaurante aceitar seu pedido."
            )

        await enviar_zap_async(phone_id, cliente_zap, msg)
        await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
        try:
            await sb_exec(lambda: supabase.table("conversas").insert({
                "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
            }).execute())
        except Exception:
            pass
        return True

    msg = (
        "✅ *Pedido Confirmado!*\n\n"
        f"📍 Entrega em: {endereco_final}\n"
        f"💳 Pagamento: {forma_escolhida}\n"
        f"🧾 Taxa de entrega: R$ {taxa_final:.2f}\n"
        f"💰 Total: R$ {total_final:.2f}\n\n"
        "⏳ Aguarde o restaurante aceitar seu pedido."
    )
    await enviar_zap_async(phone_id, cliente_zap, msg)
    await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
    try:
        await sb_exec(lambda: supabase.table("conversas").insert({
            "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
        }).execute())
    except Exception:
        pass
    return True


async def _persist_and_respond_carrinho_update(
    *,
    phone_id,
    cliente_zap,
    restaurante_db_id: int,
    nome_cliente: str,
    pedido_ativo: dict | None,
    carrinho_atual: dict,
    mensagem_ia: str,
    avisos_validacao: list[str],
    avisos_estoque: list[str],
    bloquear_msg_ia: bool,
    intent_router_prefill: dict | None,
    bairros_dict: dict,
    lista_bairros_txt: str,
    cardapio_txt: str,
    texto_completo: str,
    txt_norm: str,
    dados_parciais: dict | None = None,
    skip_post_prompt: bool = False,
) -> bool:
    handled_checkout = False
    resumo_list = []
    total_geral = 0.0
    for _, dados_item in (carrinho_atual or {}).items():
        qtd = int(dados_item.get("qtd") or 0)
        if qtd <= 0:
            continue
        preco_u = float(dados_item.get("preco_unitario") or 0.0)
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

        resumo_list.append(f"{qtd}x {nome_disp}{txt_obs} (R$ {total_item:.2f})")

    novo_resumo = " | ".join(resumo_list) if resumo_list else "Carrinho vazio"
    carrinho_display, _ = _format_carrinho_display(carrinho_atual)

    dados_update = {
        "carrinho_json": carrinho_atual,
        "resumo_pedido": novo_resumo,
        "total_valor": total_geral,
        "status": "novo",
        "bot_finalizado": False,
        "bot_finalizado_em": None,
    }

    if pedido_ativo:
        try:
            await sb_exec(lambda: supabase.table("pedidos").update(dados_update).eq("id", pedido_ativo["id"]).execute())
        except Exception:
            safe_update = dict(dados_update)
            safe_update.pop("bot_finalizado", None)
            safe_update.pop("bot_finalizado_em", None)
            await sb_exec(lambda: supabase.table("pedidos").update(safe_update).eq("id", pedido_ativo["id"]).execute())
    else:
        dados_update.update({
            "cliente_zap": cliente_zap,
            "restaurante_id": restaurante_db_id,
            "cliente_nome": nome_cliente,
        })
        try:
            await sb_exec(lambda: supabase.table("pedidos").insert(dados_update).execute())
        except Exception:
            safe_insert = dict(dados_update)
            safe_insert.pop("bot_finalizado", None)
            safe_insert.pop("bot_finalizado_em", None)
            await sb_exec(lambda: supabase.table("pedidos").insert(safe_insert).execute())

    if avisos_validacao:
        await enviar_zap_async(phone_id, cliente_zap, "\n".join(avisos_validacao))

    if avisos_estoque:
        await enviar_zap_async(phone_id, cliente_zap, "\n".join(avisos_estoque))

    if not bloquear_msg_ia:
        await enviar_zap_async(phone_id, cliente_zap, mensagem_ia)
        try:
            await sb_exec(lambda: supabase.table("conversas").insert({
                "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": mensagem_ia
            }).execute())
        except Exception:
            pass

    faq_bairro_match: str | None = None

    if carrinho_atual:
        # Responde dúvidas junto com o pedido (sem travar o fluxo)
        faq_msgs: list[str] = []
        t = (txt_norm or "").strip()
        fp_state = str((dados_parciais or {}).get("forma_pagamento") or "").strip().lower()
        if fp_state not in ("pix", "dinheiro", "cartao"):
            fp_state = ""

        prefill_tipo = str((intent_router_prefill or {}).get("tipo_entrega") or "").strip().lower()
        prefill_end = str((intent_router_prefill or {}).get("endereco_txt") or "").strip()
        prefill_bairro = str((intent_router_prefill or {}).get("bairro") or "").strip()
        prefill_has_addr = bool(prefill_end or prefill_bairro or prefill_tipo == "retirada")

        dados_tipo = str((dados_parciais or {}).get("tipo_entrega") or "").strip().lower()
        dados_end = str((dados_parciais or {}).get("endereco_txt") or "").strip()
        dados_bairro = str((dados_parciais or {}).get("bairro") or "").strip()
        dados_has_addr = bool(dados_end or dados_bairro or dados_tipo == "retirada")

        has_prefill_addr = prefill_has_addr or dados_has_addr

        def _clean_query_for_menu_lookup(raw: str) -> str:
            q = normalizar_texto(raw or "")
            q = re.sub(r"[^a-z0-9\s]", " ", q)
            q = re.sub(r"\b(quanto|custa|custam|valor|preco|preço|o|a|os|as|de|da|do|por|pra|para|no|na|em|tem|vem|vai|com|qual|quais|oq|o\s+que)\b", " ", q)
            q = re.sub(r"\s+", " ", q).strip()
            return q

        def _find_cardapio_line(cardapio_txt: str, query: str) -> str | None:
            txt = str(cardapio_txt or "")
            if not txt.strip():
                return None
            lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
            if not lines:
                return None

            def _is_heading_line(ln: str) -> bool:
                s = (ln or "").strip()
                if not s:
                    return True
                if re.match(r"^[\-\s]+$", s):
                    return True
                if re.match(r"^[A-Z\s\-]+$", s) and not re.search(r"\d", s):
                    return True
                return False
            q = normalizar_texto(query)
            tq = {p for p in re.sub(r"[^a-z0-9\s]", " ", q).split() if len(p) >= 3}
            if not tq:
                return None
            for ln in lines:
                if _is_heading_line(ln):
                    continue
                nln = normalizar_texto(ln)
                if all(tok in nln for tok in list(tq)[:2]):
                    return ln
            # fallback: any token match
            for ln in lines:
                if _is_heading_line(ln):
                    continue
                nln = normalizar_texto(ln)
                if any(tok in nln for tok in tq):
                    return ln
            return None

        # Pergunta sobre entrega/taxa/bairro
        pergunta_entrega = any(k in t for k in ("entrega", "entregam", "taxa", "frete", "delivery")) and (
            "?" in (texto_completo or "") or "para " in t or "pra " in t or "pro " in t
        )
        if pergunta_entrega:
            if not bairros_dict:
                faq_msgs.append("No momento estou sem as taxas de entrega carregadas. Me diga seu bairro que eu confirmo com o restaurante.")
            else:
                bairro_match = _match_bairro_from_input(_extract_bairro_from_text(texto_completo) or texto_completo, bairros_dict)

                if bairro_match:
                    try:
                        taxa = float(bairros_dict[bairro_match])
                    except Exception:
                        taxa = 0.0
                    faq_msgs.append(f"📍 Entregamos em *{str(bairro_match).title()}* — Taxa: *R$ {taxa:.2f}*.")
                    faq_bairro_match = str(bairro_match)
                elif not has_prefill_addr:
                    faq_msgs.append("Qual é o bairro? Assim eu confirmo a taxa certinho.")

        # Pergunta sobre formas de pagamento
        pergunta_pagamento = (
            ("pagamento" in t or "forma de pagamento" in t or "aceita" in t or "aceitam" in t)
            and any(k in t for k in ("pix", "dinheiro", "cartao", "cartão"))
        )
        if pergunta_pagamento and (not fp_state):
            pix_enabled = False
            try:
                pix_settings = await sb_exec(lambda: get_pix_settings_for_restaurante(int(restaurante_db_id)))
                pix_enabled = bool(pix_settings and pix_settings.get("enabled"))
            except Exception:
                pix_enabled = False
            if pix_enabled:
                faq_msgs.append("Aceitamos *Pix*, *Dinheiro* e *Cartão*.")
            else:
                faq_msgs.append("Aceitamos *Pix na entrega*, *Dinheiro* e *Cartão*.")

        # Pergunta sobre ingredientes/"vem com"/itens do cardápio
        parece_pergunta_ingredientes = (
            t.startswith(("vem ", "vai ", "pode "))
            or re.search(r"\b(o que vem|oq vem|o que tem|oq tem|vem com|ingrediente|ingredientes)\b", t)
        )
        if parece_pergunta_ingredientes and not any(k in t for k in ("rua", "avenida", "av ", "bairro", "cep")):
            q = _clean_query_for_menu_lookup(texto_completo)
            ln = _find_cardapio_line(cardapio_txt, q)
            if ln:
                faq_msgs.append(f"📌 Sobre o item pedido:\n{ln}")
            elif cardapio_txt:
                faq_msgs.append("Não encontrei esse item no cardápio agora. Quer tentar outro sabor?")
            else:
                faq_msgs.append("No momento estou sem o cardápio carregado. Me diga o item que você procura que eu confirmo.")

        for msg_faq in faq_msgs:
            await enviar_zap_async(phone_id, cliente_zap, msg_faq)

        await enviar_zap_async(
            phone_id,
            cliente_zap,
            f"🛒 *Carrinho Atualizado:*\n{carrinho_display}\n💰 Total: *R$ {_format_brl(total_geral)}*",
        )

        try:
            if (INTENT_ROUTER_ENABLED or SLOT_FILLING_ENABLED) and isinstance(intent_router_prefill, dict) and intent_router_prefill:
                if intent_router_prefill.get("tipo_entrega") == "retirada":
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", {"tipo_entrega": "retirada", "taxa": 0.0}))
                    await enviar_zap_async(phone_id, cliente_zap, "Beleza! ✅ Vai ser *retirada no local*.\nQual a forma de pagamento? *(Pix, Dinheiro ou Cartão)*")
                    return True

                btxt = str(intent_router_prefill.get("bairro") or "").strip()
                if not btxt and faq_bairro_match:
                    btxt = str(faq_bairro_match)
                etxt = str(intent_router_prefill.get("endereco_txt") or "").strip()

                try:
                    pedido_for_addr = dict(pedido_ativo or {})
                    if pedido_for_addr:
                        pedido_for_addr["total_valor"] = float(total_geral or pedido_for_addr.get("total_valor") or 0.0)
                except Exception:
                    pedido_for_addr = pedido_ativo

                fp_prefill = str(intent_router_prefill.get("forma_pagamento") or (dados_parciais or {}).get("forma_pagamento") or "").strip().lower()
                if fp_prefill not in ("pix", "dinheiro", "cartao"):
                    fp_prefill = ""

                handled_addr = await _handle_definir_endereco(
                    phone_id=phone_id,
                    cliente_zap=cliente_zap,
                    texto_completo=str(texto_completo or ""),
                    endereco_param=etxt or None,
                    bairro_param=btxt or None,
                    bairros_dict=bairros_dict or {},
                    lista_bairros_txt=lista_bairros_txt,
                    pedido_ativo=pedido_for_addr,
                    restaurante_db_id=int(restaurante_db_id) if restaurante_db_id else None,
                    dados_parciais=(dados_parciais or {}),
                    forma_pagamento=fp_prefill or None,
                    now_iso=datetime.now(timezone.utc).isoformat(),
                )
                if handled_addr:
                    handled_checkout = True
                    return True
        except Exception:
            pass

        try:
            if (not handled_checkout) and isinstance(dados_parciais, dict) and dados_parciais:
                has_addr = any(k in dados_parciais for k in ("endereco_txt", "bairro", "tipo_entrega", "forma_pagamento"))
                if has_addr:
                    btxt = str(dados_parciais.get("bairro") or "").strip() or None
                    etxt = str(dados_parciais.get("endereco_txt") or "").strip() or None
                    pedido_for_addr = dict(pedido_ativo or {}) if pedido_ativo else None
                    handled_addr = await _handle_definir_endereco(
                        phone_id=phone_id,
                        cliente_zap=cliente_zap,
                        texto_completo=str(texto_completo or ""),
                        endereco_param=etxt,
                        bairro_param=btxt,
                        bairros_dict=bairros_dict or {},
                        lista_bairros_txt=lista_bairros_txt,
                        pedido_ativo=pedido_for_addr,
                        restaurante_db_id=int(restaurante_db_id) if restaurante_db_id else None,
                        dados_parciais=(dados_parciais or {}),
                        forma_pagamento=str(dados_parciais.get("forma_pagamento") or "") or None,
                        now_iso=datetime.now(timezone.utc).isoformat(),
                    )
                    if handled_addr:
                        handled_checkout = True
                        return True
        except Exception:
            pass

        if skip_post_prompt:
            return handled_checkout

        pediu_cardapio = any(k in (txt_norm or "") for k in [
            "cardapio", "cardápio", "menu",
            "quais as pizza", "quais as pizzas", "quais pizzas",
            "quais as piza", "pizzas que voces tem", "pizzas que vocês tem",
        ])
        if pediu_cardapio:
            # Nota: mantemos o comportamento atual de mandar cardápio e voltar pro início.
            await enviar_zap_async(phone_id, cliente_zap, (cardapio_txt or "No momento estou sem cardápio carregado."))
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
        else:
            next_data = {}
            if isinstance(dados_parciais, dict) and any(k in dados_parciais for k in ("endereco_txt", "bairro", "tipo_entrega", "forma_pagamento", "troco_para")):
                next_data = dict(dados_parciais)
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_MAIS_ALGO", next_data))
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                "Se quiser adicionar mais itens, me diga o que você quer.\n"
                "Quando estiver pronto, diga *finalizar* (ou mande seu endereço com bairro / ou diga *retirada*).",
            )

            return bool(handled_checkout)
def _abandoned_send_reminder(phone_id: str, cliente_zap: str, pedido: dict, minutes_left: int) -> bool:
    """Envia lembrete 1x (dedup via Redis)."""
    if not redis_client:
        return False
    pedido_id = pedido.get("id")
    if not pedido_id:
        return False
    try:
        rid = int(pedido.get("restaurante_id") or 0)
    except Exception:
        rid = 0
    once_key = f"abandoned:reminder:once:{rid}:{pedido_id}"
    if not _redis_setnx_once(once_key, ttl_seconds=24 * 3600):
        return False

    resumo = (pedido.get("resumo_pedido") or "").replace("|", "\n")
    try:
        total = float(pedido.get("total_valor") or 0.0)
    except Exception:
        total = 0.0

    # Pix pendente: lembrete específico (UX)
    try:
        if _pedido_has_pix_pending(pedido):
            qr_code = (pedido.get("payment_qr_code") or "").strip()
            ticket_url = (pedido.get("payment_ticket_url") or "").strip()
            msg = (
                "💠 *Seu Pix ainda está pendente.*\n\n"
                + (f"🔗 Link: {ticket_url}\n" if ticket_url else "")
                + (f"📋 *Copia e cola:*\n{qr_code}\n\n" if qr_code else "")
                + "Se precisar, peça: *reenviar chave pix*.\n"
                + "Quando concluir, responda: *paguei*.\n\n"
                + f"(Cancelamento automático em ~{max(1, minutes_left)} min por inatividade.)"
            )

            enviar_zap(phone_id, cliente_zap, msg)
            try:
                supabase.table("conversas").insert({
                    "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
                }).execute()
            except Exception:
                pass
            return True
    except Exception:
        pass

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
        once_key = f"avaliacao:once:{(phone_id or '').strip()}:{int(pedido_id)}"
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



@app.get("/health")
def health_check():
    return {"status": "ok"}


def _cache_invalidate_authed(request: Request) -> bool:
    """Auth for cache invalidation endpoint (shared secret)."""
    if not CACHE_INVALIDATE_TOKEN:
        return False
    got = (
        request.query_params.get("token")
        or request.headers.get("x-cache-invalidate-token")
        or request.headers.get("x-cache-token")
        or request.headers.get("x-admin-token")
    )
    return bool(got and got == CACHE_INVALIDATE_TOKEN)


def _admin_authed(request: Request) -> bool:
    """Auth for admin endpoints (shared secret)."""
    return _cache_invalidate_authed(request)


@app.post("/admin/cache/invalidate")
async def admin_cache_invalidate(request: Request):
    """Invalidate Redis cache for a single restaurant (by phone_id/instance_name/restaurante_id)."""
    if not _cache_invalidate_authed(request):
        return JSONResponse(status_code=401, content={"status": "unauthorized"})

    try:
        body = await request.json()
    except Exception:
        body = {}

    body = body if isinstance(body, dict) else {}

    phone_id = str(body.get("phone_id") or "").strip()
    instance_name = str(body.get("instance_name") or "").strip()
    restaurante_id = body.get("restaurante_id")

    # If only restaurante_id was provided, resolve identifiers from DB.
    if (not phone_id or not instance_name) and restaurante_id:
        try:
            r = await sb_exec(
                lambda: (
                    supabase.table("restaurantes")
                    .select("phone_id,instance_name")
                    .eq("id", int(restaurante_id))
                    .limit(1)
                    .execute()
                )
            )
            if r.data:
                row = r.data[0] or {}
                if not phone_id:
                    phone_id = str(row.get("phone_id") or "").strip()
                if not instance_name:
                    instance_name = str(row.get("instance_name") or "").strip()
        except Exception:
            pass

    keys = []
    if phone_id:
        keys.append(f"{CACHE_PREFIX}:restaurante:phone_id:{phone_id}")
    if instance_name:
        keys.append(f"{CACHE_PREFIX}:restaurante:instance_name:{instance_name}")

    deleted = 0
    for k in keys:
        if redis_del_cache(k):
            deleted += 1

    return {"status": "ok", "deleted": deleted, "keys": keys}


@app.post("/admin/chat/toggle_pause")
async def admin_chat_toggle_pause(request: Request):
    """Pause/resume AI responses for a single customer conversation."""
    if not _admin_authed(request):
        return JSONResponse(status_code=401, content={"status": "unauthorized"})

    try:
        body = await request.json()
    except Exception:
        body = {}

    body = body if isinstance(body, dict) else {}
    cliente_zap = str(body.get("cliente_zap") or "").strip()
    phone_id = str(body.get("phone_id") or "").strip()
    try:
        minutes = int(body.get("minutes") or 0)
    except Exception:
        minutes = 0

    if not cliente_zap or not phone_id:
        return JSONResponse(status_code=400, content={"status": "error", "detail": "missing_cliente_zap_or_phone_id"})

    now = datetime.now(timezone.utc)
    paused_until = None
    if minutes and minutes > 0:
        paused_until = (now + timedelta(minutes=minutes)).isoformat()

    # Preserve existing state fields when creating a new row.
    try:
        estado = await sb_exec(lambda: get_estado(cliente_zap, phone_id))
    except Exception:
        estado = None

    if estado:
        try:
            await sb_exec(
                lambda: (
                    supabase.table("clientes_estado")
                    .update({"ai_paused_until": paused_until})
                    .eq("cliente_zap", cliente_zap)
                    .eq("restaurante_id", phone_id)
                    .execute()
                )
            )
        except Exception:
            return JSONResponse(status_code=500, content={"status": "error", "detail": "db_update_failed"})
    else:
        # Create minimal state row (avoids upsert failing if estado_conversa is required)
        payload = {
            "cliente_zap": cliente_zap,
            "restaurante_id": phone_id,
            "estado_conversa": "INICIO",
            "ultima_mensagem_em": now.isoformat(),
            "ai_paused_until": paused_until,
        }
        try:
            await sb_exec(lambda: supabase.table("clientes_estado").insert(payload).execute())
        except Exception:
            # If insert fails due to race/duplicate, retry as update.
            try:
                await sb_exec(
                    lambda: (
                        supabase.table("clientes_estado")
                        .update({"ai_paused_until": paused_until})
                        .eq("cliente_zap", cliente_zap)
                        .eq("restaurante_id", phone_id)
                        .execute()
                    )
                )
            except Exception:
                return JSONResponse(status_code=500, content={"status": "error", "detail": "db_write_failed"})

    return {
        "status": "ok",
        "cliente_zap": cliente_zap,
        "phone_id": phone_id,
        "paused": bool(paused_until),
        "paused_until": paused_until,
    }






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
        resp = await sb_exec(lambda: (
            supabase.table("clientes_estado")
            .select("cliente_zap,restaurante_id,estado_conversa,ultima_mensagem_em")
            .lt("ultima_mensagem_em", th_remind)
            .limit(MAX_ABANDONED_SWEEP)
            .execute()
        ))
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

        dados_loja = await _run_blocking(lambda: get_dados_restaurante(phone_id, tipo="phone_id"), timeout=SUPABASE_TIMEOUT_SECONDS)
        if not dados_loja:
            continue
        restaurante_db_id = int(dados_loja.get("id") or 0)
        if not restaurante_db_id:
            continue

        pedido = await _run_blocking(lambda: get_pedido_ativo(cliente_zap, restaurante_db_id), timeout=SUPABASE_TIMEOUT_SECONDS)
        if not pedido:
            continue
        if (pedido.get("status") or "").lower() != "novo":
            continue


        # Pix pendente: não ignorar para sempre.
        # Regra sugerida: lembrar depois de 20 min e cancelar depois de 60 min.
        if estado == "AGUARDANDO_PAGAMENTO_PIX" or _pedido_has_pix_pending(pedido):
            PIX_REMINDER_MIN = int(os.getenv("PIX_ABANDONED_REMINDER_MIN", "20") or "20")
            PIX_CANCEL_MIN = int(os.getenv("PIX_ABANDONED_CANCEL_MIN", "60") or "60")

            if minutes_inactive >= PIX_CANCEL_MIN:
                if await _run_blocking(lambda: _abandoned_cleanup_pedido(restaurante_db_id, phone_id, cliente_zap, pedido), timeout=SUPABASE_TIMEOUT_SECONDS):
                    cleaned += 1
                continue

            if minutes_inactive >= PIX_REMINDER_MIN:
                if redis_client:
                    minutes_left = PIX_CANCEL_MIN - minutes_inactive
                    if await _run_blocking(lambda: _abandoned_send_reminder(phone_id, cliente_zap, pedido, minutes_left=minutes_left), timeout=SUPABASE_TIMEOUT_SECONDS):
                        reminded += 1
                continue

        if minutes_inactive >= CART_ABANDONED_CANCEL_MIN:
            if await _run_blocking(lambda: _abandoned_cleanup_pedido(restaurante_db_id, phone_id, cliente_zap, pedido), timeout=SUPABASE_TIMEOUT_SECONDS):
                cleaned += 1
            continue


        if not redis_client:
            continue  # sem Redis, evita duplicar em múltiplos workers

        minutes_left = CART_ABANDONED_CANCEL_MIN - minutes_inactive
        if await _run_blocking(lambda: _abandoned_send_reminder(phone_id, cliente_zap, pedido, minutes_left=minutes_left), timeout=SUPABASE_TIMEOUT_SECONDS):
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
        resp = await sb_exec(lambda: (
            supabase.table("clientes_estado")
            .select("cliente_zap,restaurante_id,estado_conversa,ultima_mensagem_em")
            .neq("estado_conversa", "INICIO")
            .lt("ultima_mensagem_em", th)
            .limit(MAX_STATE_RESET_SWEEP)
            .execute()
        ))
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

        await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
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
        resp = await sb_exec(lambda: (
            supabase.table("pedidos")
            .select("id,cliente_zap,restaurante_id,status,finalizado_em,msg_avaliacao_enviada,avaliacao")
            .eq("status", "finalizado")
            .eq("msg_avaliacao_enviada", False)
            .lt("finalizado_em", th)
            .limit(MAX_AVALIACAO_SWEEP)
            .execute()
        ))
        pedidos = resp.data or []
    except Exception as e:
        return {"status": "erro", "detalhe": str(e)}

    if not pedidos:
        return {"status": "ok", "scanned": 0, "sent": 0}


    rest_ids = sorted({int(p.get("restaurante_id") or 0) for p in pedidos if p.get("restaurante_id")})
    rest_map = {}
    try:
        if rest_ids:
            r = await sb_exec(lambda: supabase.table("restaurantes").select("id,phone_id").in_("id", rest_ids).execute())
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

            if await _run_blocking(lambda: _avaliacao_send(phone_id, cliente_zap, pedido_id), timeout=SUPABASE_TIMEOUT_SECONDS):
                sent += 1
        except Exception:
            continue

    return {"status": "ok", "scanned": len(pedidos), "sent": sent}

def _extract_text_or_fallback(msg_data: dict) -> tuple[str, str]:
    """
    Returns (text, kind)
    kind: "text" | "location" | "media" | "interactive" | "unknown"
    """
    if not isinstance(msg_data, dict):
        return "", "unknown"

    # 1) Plain text fields (including some nested variants)
    texto = (msg_data.get("text") or msg_data.get("conversation") or "").strip()
    if not texto:
        nested = msg_data.get("message")
        if isinstance(nested, dict):
            texto = (nested.get("text") or nested.get("conversation") or "").strip()
    if texto:
        return texto, "text"

    # 2) Some providers put buttons/interactive replies like this
    # Try common shapes safely
    interactive = msg_data.get("interactive") or {}
    if isinstance(interactive, dict):
        # button reply
        br = interactive.get("button_reply") or interactive.get("buttonReply") or {}
        if isinstance(br, dict):
            t = (br.get("title") or br.get("text") or br.get("id") or "").strip()
            if t:
                return t, "interactive"

        # list reply
        lr = interactive.get("list_reply") or interactive.get("listReply") or {}
        if isinstance(lr, dict):
            t = (lr.get("title") or lr.get("description") or lr.get("id") or "").strip()
            if t:
                return t, "interactive"

    # 3) Location
    loc = msg_data.get("location") or {}
    if isinstance(loc, dict):
        lat = loc.get("latitude")
        lng = loc.get("longitude")
        if lat is not None and lng is not None:
            return "", "location"

    # 4) Media (image/video/audio/document)
    msg_type = (msg_data.get("type") or "").lower().strip()
    if msg_type in ("image", "video", "audio", "document"):
        caption = (msg_data.get("caption") or "").strip()
        if caption:
            return caption, "media"
        return "", "media"

    return "", "unknown"


def _extract_audio_message_from_msg_data(msg_data: dict) -> dict | None:
    if not isinstance(msg_data, dict):
        return None
    audio_obj = msg_data.get("audioMessage") or msg_data.get("voiceMessage")
    if isinstance(audio_obj, dict):
        return audio_obj
    audio_obj = msg_data.get("audio")
    if isinstance(audio_obj, dict):
        return audio_obj
    nested = msg_data.get("message") or {}
    if isinstance(nested, dict):
        audio_obj = nested.get("audioMessage") or nested.get("voiceMessage")
        if isinstance(audio_obj, dict):
            return audio_obj
        audio_obj = nested.get("audio")
        if isinstance(audio_obj, dict):
            return audio_obj
    msg_type = str(msg_data.get("messageType") or msg_data.get("type") or "").strip().lower()
    if msg_type in ("audiomessage", "audio"):
        return {}
    return None


def _extract_message_id_from_msg_data(msg_data: dict, data: dict | None = None) -> str | None:
    if isinstance(msg_data, dict):
        key = msg_data.get("key") or {}
        if isinstance(key, dict):
            mid = key.get("id")
            if isinstance(mid, str) and mid.strip():
                return mid.strip()
        mid = msg_data.get("id")
        if isinstance(mid, str) and mid.strip():
            return mid.strip()
    if isinstance(data, dict):
        key = data.get("key") or {}
        if isinstance(key, dict):
            mid = key.get("id")
            if isinstance(mid, str) and mid.strip():
                return mid.strip()
    return None


def _uazapi_download_audio_bytes(
    *,
    message_id: str,
    instance_token: str,
    timeout: float,
) -> tuple[bytes, str]:
    url = f"{UAZAPI_BASE_URL}/message/download"
    headers = {"token": instance_token, "Content-Type": "application/json"}
    payload = {
        "id": str(message_id),
        "generate_mp3": False,
        "return_link": False,
        "return_base64": True,
        "transcribe": False,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=timeout, verify=HTTP_VERIFY_TLS)
    r.raise_for_status()
    data = r.json() if r.content else {}
    if isinstance(data, dict):
        b64 = data.get("base64Data") or data.get("base64") or data.get("data")
        if b64:
            raw = base64.b64decode(b64)
            _dbg_webhook("audio_download_base64", size=len(raw) if raw else 0)
            return raw, "audio/ogg"
        media_url = data.get("fileURL") or data.get("url") or data.get("media_url")
        if media_url:
            resp = requests.get(media_url, timeout=timeout, verify=HTTP_VERIFY_TLS)
            resp.raise_for_status()
            _dbg_webhook("audio_download_link", mime=resp.headers.get("content-type"), size=len(resp.content) if resp.content else 0)
            return resp.content, resp.headers.get("content-type") or "audio/mpeg"
    raise RuntimeError("Nenhum dado de áudio encontrado (Base64/URL)")


def _groq_transcribe_audio_bytes(*, audio_bytes: bytes, mime_type: str, timeout: float) -> str:
    if not GROQ_API_KEY:
        return ""
    url = "https://api.groq.com/openai/v1/audio/transcriptions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}"}
    files = {"file": ("audio.ogg", audio_bytes, "audio/ogg")}
    data = {"model": "whisper-large-v3", "response_format": "json", "language": "pt"}
    r = requests.post(url, headers=headers, files=files, data=data, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    text = (payload.get("text") or "").strip()
    _dbg_webhook("audio_transcription_done", text_len=len(text))
    return text

@app.post("/webhook")
async def webhook(request: Request):

    try:
        # Proteção contra payload muito grande (evita ler tudo na RAM)
        try:
            content_length = request.headers.get("content-length")
            if content_length and MAX_WEBHOOK_BODY_BYTES and int(content_length) > MAX_WEBHOOK_BODY_BYTES:
                return JSONResponse(status_code=413, content={"ok": False, "error": "payload_too_large"})
        except Exception:
            pass

        raw = await request.body()
        if MAX_WEBHOOK_BODY_BYTES and len(raw) > MAX_WEBHOOK_BODY_BYTES:
            return JSONResponse(status_code=413, content={"ok": False, "error": "payload_too_large"})

        body = json.loads(raw.decode("utf-8") or "{}") if raw else {}
        data = body.get("data") or {}

        # Normaliza msg_data para formatos comuns:
        # - body.message
        # - body.data.message
        # - body.messages[0]
        # - body.data.messages[0]
        msg_data = _first_dict(body.get("message"))
        if not msg_data and isinstance(data, dict):
            msg_data = _first_dict(data.get("message"))
        if not msg_data:
            msg_data = _first_dict(body.get("messages"))
        if not msg_data and isinstance(data, dict):
            msg_data = _first_dict(data.get("messages"))
        if not msg_data and isinstance(data, dict):
            # alguns provedores colocam a própria mensagem em data
            if any(k in data for k in ("text", "conversation", "fromMe", "sender", "remoteJid", "from", "chatid")):
                msg_data = data

        logger.info("webhook received | has_message=%s", bool(msg_data))
        _dbg_webhook("received", has_message=bool(msg_data), keys=list(body.keys())[:10])

        # 1. Validações Iniciais
        instance = (body.get("instanceName") or body.get("instance") or "").strip()
        if not instance and isinstance(data, dict):
            instance = (data.get("instanceName") or data.get("instance") or "").strip()
        # fallback: alguns providers colocam o nome dentro do message
        if not instance and isinstance(msg_data, dict):
            instance = (msg_data.get("instanceName") or msg_data.get("instance") or "").strip()
        if not instance:
            _dbg_webhook("skip:no_instance")
            return "ok"

        if msg_data.get("fromMe"):
            _dbg_webhook("skip:from_me", instance=instance)
            return "ok"

        # 2. Obter dados do Restaurante e Cliente (Necessário para responder o Fallback)
        restaurante = await sb_exec(lambda: get_dados_restaurante(instance, tipo="instance_name"))
        if not restaurante:
            _dbg_webhook("skip:no_restaurante", instance=instance)
            return "ok"

        phone_id = restaurante.get("phone_id")
        if not phone_id:
            _dbg_webhook("skip:no_phone_id", instance=instance)
            return "ok"

        remote_jid = (msg_data.get("chatid") or msg_data.get("sender") or msg_data.get("remoteJid") or msg_data.get("from") or "")
        # Tenta extrair do body, senão limpa o remote_jid
        cliente_zap = _extract_cliente_zap(body, msg_data, phone_id) or _only_digits(remote_jid.split("@")[0]) or remote_jid.split("@")[0]
        
        if not cliente_zap:
            _dbg_webhook("skip:no_cliente", instance=instance, phone_id=phone_id)
            return "ok"

        # 3. Deduplicação (Evitar processar a mesma mensagem 2x)
        event_id = _stable_event_id(body)
        if event_id:
            dedup_key = f"dedup:webhook:{instance}:{event_id}"
            if not redis_claim_event_once(dedup_key, ttl_seconds=6 * 3600):
                _dbg_webhook("skip:dedup", instance=instance)
                return "ok"

        # --- TRECHO NOVO ADICIONADO AQUI ---
        # 3.5 Atualizar última atividade (evita abandono falso até em mídia/localização)
        try:
            await sb_exec(lambda: touch_estado_last_message(cliente_zap, phone_id))
        except Exception:
            pass
        # -----------------------------------

        # 4. Extração de Texto e Tratamento de Tipos (Sua lógica nova)
        texto, kind = _extract_text_or_fallback(msg_data)

        _dbg_webhook("parsed", instance=instance, phone_id=phone_id, cliente=cliente_zap, kind=kind, text_len=len(texto or ""))

        # Fallback: alguns payloads trazem apenas messageType (AudioMessage) sem msg_data.type
        if (not texto) and kind == "unknown":
            msg_type_alt = str((msg_data.get("messageType") or msg_data.get("type") or data.get("messageType") or "")).strip().lower()
            if msg_type_alt == "audiomessage":
                kind = "media"
                _dbg_webhook("audio_kind_fallback", instance=instance, phone_id=phone_id, cliente=cliente_zap)

        audio_transcribed = False
        # 4.1 Transcrição de áudio (sem alterar fluxo de texto existente)
        if (not texto) and kind == "media":
            audio_msg = _extract_audio_message_from_msg_data(msg_data)
            if isinstance(audio_msg, dict):
                try:
                    instance_token = (restaurante.get("instance_token") or "").strip()
                    message_id = _extract_message_id_from_msg_data(msg_data, data)
                    _dbg_webhook(
                        "audio_detected",
                        instance=instance,
                        phone_id=phone_id,
                        cliente=cliente_zap,
                        has_token=bool(instance_token),
                        has_message_id=bool(message_id),
                    )
                    if instance_token and message_id:
                        audio_bytes, mime_type = await _run_blocking(
                            lambda: _uazapi_download_audio_bytes(
                                message_id=message_id,
                                instance_token=instance_token,
                                timeout=UAZAPI_TIMEOUT,
                            ),
                            timeout=GROQ_TIMEOUT_SECONDS,
                        )
                        transcript = await _run_blocking(
                            lambda: _groq_transcribe_audio_bytes(
                                audio_bytes=audio_bytes,
                                mime_type=mime_type,
                                timeout=GROQ_TIMEOUT_SECONDS,
                            ),
                            timeout=GROQ_TIMEOUT_SECONDS,
                        )
                        if transcript:
                            print(f"🎤 Transcrição de áudio ({cliente_zap}): {transcript}")
                            texto = transcript
                            kind = "text"
                            audio_transcribed = True
                            _dbg_webhook("audio_transcribed", instance=instance, phone_id=phone_id, cliente=cliente_zap, text_len=len(texto))
                except Exception as e:
                    _dbg_webhook("audio_transcribe_failed", instance=instance, phone_id=phone_id, cliente=cliente_zap, error=str(e))

        # Truncar texto se necessário
        if texto and MAX_INCOMING_TEXT_CHARS and len(texto) > MAX_INCOMING_TEXT_CHARS:
            texto = texto[:MAX_INCOMING_TEXT_CHARS]

        # 5. Lógica de Fallback (Se não houver texto)
        if not texto:
            # fallback UX: Responde ao cliente dependendo do tipo de anexo
            if kind == "location":
                await enviar_zap_async(phone_id, cliente_zap, "Recebi sua localização. Agora me diga em texto: bairro/rua/número e ponto de referência, por favor.")
            elif kind == "media":
                await enviar_zap_async(phone_id, cliente_zap, "Recebi seu arquivo, mas aqui eu só consigo atender por texto. Me diga o pedido por mensagem, por favor.")
            elif kind == "interactive":
                # Botões clicados geralmente vêm como texto no _extract_text_or_fallback
                # Se caiu aqui, é algo interativo desconhecido, ignoramos ou mandamos msg genérica
                pass 
            else:
                await enviar_zap_async(phone_id, cliente_zap, "Não consegui ler sua mensagem. Pode me mandar em texto, por favor?")
            
            return "ok"

        # 6. Rate Limit (Só conta se for mensagem de texto válida)
        rl_key = f"ratelimit:webhook:{phone_id}:{cliente_zap}"
        if not redis_rate_limit(rl_key, WEBHOOK_RATE_LIMIT_PER_MIN, 60):
            _dbg_webhook("skip:ratelimit", instance=instance, phone_id=phone_id, cliente=cliente_zap)
            return "ok"

        nome = msg_data.get("senderName") or "Cliente"

        # 7. Adicionar ao Buffer de Processamento
        buffer_key = f"{instance}:{cliente_zap}"
        if audio_transcribed:
            _audio_flags_by_conv[(str(phone_id), str(cliente_zap))] = True
            _audio_transcribed_by_conv[(str(phone_id), str(cliente_zap))] = True
        if redis_client:
            redis_add_buffer(buffer_key, texto)
        else:
            _mem_buffers.setdefault(buffer_key, []).append(texto)

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
    # 1. Pega a referência da tarefa atual para comparar depois
    this_task = asyncio.current_task()

    try:
        await asyncio.sleep(3)  # Espera 3 segundos para juntar mensagens

        if redis_client:
            texto_final = redis_get_clear_buffer(buffer_key)
        else:
            parts = _mem_buffers.pop(buffer_key, [])
            texto_final = " ".join([p for p in parts if p]).strip()
        if texto_final:
            await processar_mensagem_final(phone_id, cliente_zap, nome, texto_final)

    except asyncio.CancelledError:
        # Se for cancelada (pq chegou msg nova), só para silenciosamente
        return
    except Exception as e:
        print(f"Erro executar_ia_com_delay: {e}")
    
    finally:
        # 2. Limpeza segura (Garbage Collection manual)
        try:
            # Só remove do dicionário se a tarefa lá guardada FOR ESTA AQUI.
            # Se já tiver OUTRA tarefa rodando (nova mensagem), não removemos.
            if processing_tasks.get(buffer_key) is this_task:
                processing_tasks.pop(buffer_key, None)
            
            # Limpa fase se existir
            if 'processing_task_phase' in globals():
                processing_task_phase.pop(buffer_key, None)
                
        except Exception:
            pass
        

async def processar_mensagem_final(phone_id, cliente_zap, nome_cliente, texto_completo):

    # UNIVERSAL HUMAN-ATTENDANT TRIGGER
    texto_completo = (texto_completo or "").strip()
    if not texto_completo:
        return

    # List of explicit human-attendant trigger phrases (expand as needed)
    HUMAN_TRIGGER_PHRASES = [
        "falar com atendente", "falar com humano", "quero falar com atendente", "quero falar com humano",
        "quero falar com alguém", "quero falar com alguem", "atendente", "humano", "ajuda", "preciso de ajuda",
        "pessoa de verdade", "pessoa real", "posso falar com atendente", "posso falar com humano"
    ]
    txt_norm = normalizar_texto(texto_completo)
    if any(phrase in txt_norm for phrase in HUMAN_TRIGGER_PHRASES):
        # Get restaurant and order context for dashboard alert
        dados_loja = await _run_blocking(lambda: get_dados_restaurante(phone_id, tipo="phone_id"), timeout=SUPABASE_TIMEOUT_SECONDS)
        if not dados_loja or not dados_loja.get("bot_ativo", True):
            return
        try:
            restaurante_db_id = int(dados_loja["id"])
        except Exception:
            return
        pedido_aberto = await sb_exec(lambda: get_ultimo_pedido_aberto(cliente_zap, restaurante_db_id))
        now_iso = datetime.now(timezone.utc).isoformat()
        if pedido_aberto and pedido_aberto.get("id"):
            try:
                await sb_exec(lambda: supabase.table("pedidos").update({
                    "needs_human": True,
                    "needs_human_reason": "cliente_pediu_atendente_explicitamente",
                    "needs_human_at": now_iso,
                    "last_cliente_msg_at": now_iso,
                }).eq("id", int(pedido_aberto.get("id") or 0)).execute())
            except Exception:
                pass
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "👩‍💼 Chamando um atendente humano do restaurante para te ajudar! Aguarde um momento, por favor."
        )
        return

    dados_loja = await _run_blocking(lambda: get_dados_restaurante(phone_id, tipo="phone_id"), timeout=SUPABASE_TIMEOUT_SECONDS)
    if not dados_loja or not dados_loja.get("bot_ativo", True):
        return

    try:
        restaurante_db_id = int(dados_loja["id"])
    except Exception:
        return

    print(f"📩 Msg de {nome_cliente}: {texto_completo}")

    estado_data = await _run_blocking(lambda: get_estado(cliente_zap, phone_id), timeout=SUPABASE_TIMEOUT_SECONDS)
    estado_atual = (estado_data["estado_conversa"] if estado_data else "INICIO") or "INICIO"
    dados_parciais = (estado_data.get("dados_parciais") or {}) if estado_data else {}

    async def _maybe_resume_prompt() -> None:
        try:
            if estado_atual == "AGUARDANDO_ENDERECO":
                bairro_prev = str((dados_parciais or {}).get("bairro") or "").strip()
                if bairro_prev:
                    await enviar_zap_async(
                        phone_id,
                        cliente_zap,
                        "Agora me envie o *endereço completo* (rua/avenida + número e complemento, se tiver).",
                    )
                else:
                    await enviar_zap_async(
                        phone_id,
                        cliente_zap,
                        "Me envie o *endereço completo com bairro* (rua/avenida + número e complemento, se tiver).",
                    )
            elif estado_atual == "AGUARDANDO_PAGAMENTO":
                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "Qual a forma de pagamento? *(Pix, Dinheiro ou Cartão)*",
                )
            elif estado_atual == "CONFIRMAR_ENDERECO_AUDIO":
                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "Só para confirmar: o endereço está correto? Responda *sim* ou *não*.",
                )
            elif estado_atual == "AGUARDANDO_MAIS_ALGO":
                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "Você quer *adicionar mais itens* ou *finalizar*?\n\n"
                    "- Para adicionar: mande o item (ex.: *\"2 coca\"*, *\"1 pizza calabresa\"*)\n"
                    "- Para finalizar: diga *finalizar* (ou mande endereço com bairro / ou diga *retirada*)",
                )
        except Exception:
            pass

    # Inferência determinística: "troco pra 50" => dinheiro + troco_para=50
    try:
        if isinstance(dados_parciais, dict):
            m_troco = re.search(r"\btroco\b\s*(?:pra|para)?\s*(?:r\$\s*)?(\d+(?:[\.,]\d{1,2})?)", txt_norm)
            if m_troco:
                raw_val = (m_troco.group(1) or "").strip().replace(".", "").replace(",", ".")
                v = float(raw_val)
                if v > 0:
                    dados_parciais = dict(dados_parciais)
                    dados_parciais["troco_para"] = _money_2(v)
                    dados_parciais.setdefault("forma_pagamento", "dinheiro")
                    try:
                        await sb_exec(lambda: set_estado(cliente_zap, phone_id, estado_atual, dados_parciais))
                    except Exception:
                        pass
    except Exception:
        pass

    now_iso = datetime.now(timezone.utc).isoformat()

    audio_transcribed = _audio_transcribed_by_conv.pop((str(phone_id), str(cliente_zap)), False)
    if NORMALIZE_TEXT_ENABLED:
        normalized = await _normalize_message_via_groq(texto_completo)
        if normalized:
            texto_completo = normalized
            txt_norm = normalizar_texto(texto_completo)
            print(f"🧠 Texto normalizado (IA) ({cliente_zap}): {texto_completo}")

    # Se existe escolha pendente de tamanho (ex.: Coca 1L/2L), tenta resolver antes do fluxo principal.
    try:
        pending_size = (dados_parciais or {}).get("pending_size") if isinstance(dados_parciais, dict) else None
        if isinstance(pending_size, dict) and (pending_size.get("base") or pending_size.get("options")):
            def _normalize_size_text(raw: str) -> str:
                s = normalizar_texto(raw or "")
                s = re.sub(r"\bum\s+litro\b", "1 litro", s)
                s = re.sub(r"\buma\s+litro\b", "1 litro", s)
                s = re.sub(r"\bmeio\s+litro\b", "0.5 litro", s)
                return s

            def _extract_size_token(raw_norm: str) -> str | None:
                m = re.search(r"\b(\d+(?:[\.,]\d+)?)\s*(l|ml)\b", raw_norm)
                if m:
                    num = (m.group(1) or "").replace(",", ".")
                    unit = m.group(2) or ""
                    return f"{num}{unit}"
                for k in ("pequena", "media", "média", "grande", "gigante", "familia", "família", "brotinho"):
                    if k in raw_norm:
                        return k
                return None

            t_norm_local = _normalize_size_text(texto_completo)
            size_token = _extract_size_token(t_norm_local)
            if size_token:
                opts = pending_size.get("options") if isinstance(pending_size.get("options"), list) else []
                base = (pending_size.get("base") or "").strip()
                chosen = ""
                if size_token and opts:
                    for opt in opts:
                        opt_norm = normalizar_texto(opt)
                        if size_token in opt_norm.replace(" ", "") or size_token in opt_norm:
                            chosen = opt
                            break
                if not chosen and opts and size_token:
                    for opt in opts:
                        opt_norm = normalizar_texto(opt)
                        if any(k in opt_norm and k in t_norm_local for k in ("pequena", "media", "média", "grande", "gigante", "familia", "família", "brotinho")):
                            chosen = opt
                            break
                if not chosen:
                    chosen = f"{base} {texto_completo}".strip() if base else ""

                merged = dict(dados_parciais or {})
                merged.pop("pending_size", None)
                try:
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, estado_atual, merged))
                    dados_parciais = merged
                except Exception:
                    pass

                if chosen:
                    combined = str(texto_completo or "").strip()
                    if normalizar_texto(chosen) not in normalizar_texto(combined):
                        combined = f"{chosen}. {combined}" if combined else chosen
                        return await processar_mensagem_final(phone_id, cliente_zap, nome_cliente, combined)
    except Exception:
        pass

    # Se existe escolha pendente de borda, tenta resolver antes do fluxo principal.
    try:
        pending_borda = (dados_parciais or {}).get("pending_borda") if isinstance(dados_parciais, dict) else None
        if isinstance(pending_borda, dict) and pending_borda.get("options"):
            opts = pending_borda.get("options") if isinstance(pending_borda.get("options"), list) else []
            base = (pending_borda.get("base") or "").strip()
            t_norm_local = normalizar_texto(texto_completo)

            if "sem borda" in t_norm_local:
                merged = dict(dados_parciais or {})
                merged.pop("pending_borda", None)
                try:
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, estado_atual, merged))
                    dados_parciais = merged
                except Exception:
                    pass
                if base:
                    return await processar_mensagem_final(phone_id, cliente_zap, nome_cliente, base)

            def _pick_borda_from_pending(raw_norm: str, options: list[str]) -> str | None:
                if not raw_norm or not options:
                    return None
                for opt in options:
                    opt_norm = normalizar_texto(opt)
                    if opt_norm and opt_norm in raw_norm:
                        return opt
                tokens = [t for t in raw_norm.split() if len(t) >= 3 and t not in ("borda", "com")]
                for opt in options:
                    opt_norm = normalizar_texto(opt)
                    if any(tok in opt_norm for tok in tokens):
                        return opt
                m = difflib.get_close_matches(raw_norm, [normalizar_texto(o) for o in options], n=1, cutoff=0.6)
                if m:
                    for opt in options:
                        if normalizar_texto(opt) == m[0]:
                            return opt
                return None

            chosen = _pick_borda_from_pending(t_norm_local, opts)
            if chosen:
                merged = dict(dados_parciais or {})
                merged.pop("pending_borda", None)
                try:
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, estado_atual, merged))
                    dados_parciais = merged
                except Exception:
                    pass
                if base:
                    combined = f"{base} com borda {chosen}".strip()
                    return await processar_mensagem_final(phone_id, cliente_zap, nome_cliente, combined)
    except Exception:
        pass

    # Router can capture parameters from mixed messages (item + endereço/pagamento) and
    # use them to move to checkout right after cart updates.
    intent_router_prefill: dict | None = None

    def _is_cancel_request(t: str) -> bool:
        t = (t or "").strip()
        if not t:
            return False
        return any(k in t for k in (
            "cancel", "cancela", "cancelar", "desist", "desistir",
        ))

    def _is_new_order_request(t: str) -> bool:
        t = (t or "").strip()
        if not t:
            return False
        return any(k in t for k in (
            "novo pedido", "outro pedido", "fazer outro", "começar de novo", "comecar de novo", "reiniciar",
        ))

    def _is_probably_order_change(t: str, raw: str) -> bool:
        # heurística para alterações: quantidade, "adiciona", "tira", endereço, pagamento, finalizar
        if re.search(r"\b\d+\b", raw or ""):
            return True
        return any(k in t for k in (
            "quero", "queria", "adicion", "coloca", "manda", "tirar", "remove", "troca",
            "endereco", "endereço", "rua ", "avenida", "av ", "bairro",
            "pix", "dinheiro", "cartao", "cartão",
            "finaliz", "fech",
        ))

    # Atualiza/cria pedido visível no dashboard desde a 1ª mensagem
    pedido_aberto = await sb_exec(lambda: get_ultimo_pedido_aberto(cliente_zap, restaurante_db_id))

    # Se existe pedido em preparo/entrega, bloqueia alterações/cancelamento (mas permite pedir "novo pedido")
    locked_statuses = {"em preparo", "em_preparo", "pronto", "saiu para entrega", "saiu_entrega"}
    st_aberto = (pedido_aberto.get("status") or "").strip().lower() if pedido_aberto else ""

    if pedido_aberto and st_aberto in locked_statuses and (not _is_new_order_request(txt_norm)):
        # Se o cliente está tentando cancelar/alterar, abre chamado humano no painel
        if _is_cancel_request(txt_norm) or _is_probably_order_change(txt_norm, texto_completo):
            try:
                await sb_exec(lambda: supabase.table("pedidos").update({
                    "needs_human": True,
                    "needs_human_reason": "cliente_pediu_cancelar_ou_alterar_em_preparo",
                    "needs_human_at": now_iso,
                    "last_cliente_msg_at": now_iso,
                }).eq("id", int(pedido_aberto.get("id") or 0)).execute())
            except Exception:
                pass

        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "⚠️ Seu pedido já está em preparo (ou já saiu) e não consigo mais alterar ou cancelar.\n"
            "Chamando um atendente do restaurante...",
        )
        return

    # Se o Pix já foi aprovado, não permite mais alterações no pedido (mas ainda permite cancelar).
    try:
        if pedido_aberto and (str(pedido_aberto.get("payment_status") or "").strip().lower() == "approved"):
            if (not _is_cancel_request(txt_norm)) and _is_probably_order_change(txt_norm, texto_completo):
                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "✅ Seu pedido já foi pago via Pix.\n"
                    "Por segurança, não consigo mais alterar itens/endereço/pagamento.\n\n"
                    "Você ainda pode *cancelar* o pedido (o restaurante resolve o reembolso manualmente)\n"
                    "ou fazer um *novo pedido*.",
                )
                return
    except Exception:
        pass

    # Cancelamento global: permitido até o pedido entrar em preparo/entrega.
    if _is_cancel_request(txt_norm):
        if not pedido_aberto:
            await enviar_zap_async(phone_id, cliente_zap, "Não encontrei um pedido em aberto para cancelar. 🙂")
            return

        if st_aberto in locked_statuses:
            try:
                await sb_exec(lambda: supabase.table("pedidos").update({
                    "needs_human": True,
                    "needs_human_reason": "cliente_pediu_cancelar_em_preparo",
                    "needs_human_at": now_iso,
                    "last_cliente_msg_at": now_iso,
                }).eq("id", int(pedido_aberto.get("id") or 0)).execute())
            except Exception:
                pass

            await enviar_zap_async(
                phone_id,
                cliente_zap,
                "⚠️ Não é possível cancelar porque o pedido já está em preparo (ou já saiu).\n"
                "Chamando um atendente do restaurante...",
            )
            return

        # Pode cancelar (mesmo se Pix já estiver aprovado — reembolso manual)
        paid = (str(pedido_aberto.get("payment_status") or "").strip().lower() == "approved")
        try:
            await sb_exec(lambda: supabase.table("pedidos").update({
                "status": "cancelado",
                "last_cliente_msg_at": now_iso,
            }).eq("id", int(pedido_aberto.get("id") or 0)).execute())
        except Exception:
            pass

        if paid:
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                "✅ Pedido cancelado.\n"
                "Como o Pix já foi pago, o restaurante vai tratar o reembolso *manualmente*.\n\n"
                "Se quiser, você pode fazer um novo pedido.",
            )
        else:
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                "✅ Pedido cancelado.\n\nSe quiser, você pode fazer um novo pedido.",
            )
        return

    # Se o cliente pediu explicitamente um novo pedido enquanto há um em preparo, cria outro stub.
    if pedido_aberto and st_aberto in locked_statuses and _is_new_order_request(txt_norm):
        pedido_aberto = None

    # Cria stub se não existir pedido aberto
    if not pedido_aberto:
        try:
            payload = {
                "cliente_zap": cliente_zap,
                "restaurante_id": restaurante_db_id,
                "cliente_nome": nome_cliente,
                "status": "novo",
                "carrinho_json": {},
                "resumo_pedido": "Carrinho vazio",
                "total_valor": 0.0,
                "bot_finalizado": False,
                "last_cliente_msg_at": now_iso,
            }
            try:
                await sb_exec(lambda: supabase.table("pedidos").insert(payload).execute())
            except Exception:
                payload.pop("bot_finalizado", None)
                payload.pop("last_cliente_msg_at", None)
                await sb_exec(lambda: supabase.table("pedidos").insert(payload).execute())
        except Exception:
            pass
    else:
        # Atualiza last activity no pedido atual
        try:
            try:
                await sb_exec(lambda: supabase.table("pedidos").update({
                    "last_cliente_msg_at": now_iso,
                }).eq("id", int(pedido_aberto.get("id") or 0)).execute())
            except Exception:
                pass
        except Exception:
            pass

    # ===== Admin feature: Pause AI for this customer =====
    # If paused, we keep receiving messages + saving history, but do NOT reply nor call Groq.
    try:
        paused_until_dt = _parse_dt_utc((estado_data or {}).get("ai_paused_until"))
        if paused_until_dt and paused_until_dt > datetime.now(timezone.utc):
            try:
                await sb_exec(
                    lambda: supabase.table("conversas").insert({
                        "cliente_zap": cliente_zap,
                        "restaurante_id": phone_id,
                        "role": "user",
                        "mensagem": texto_completo,
                    }).execute()
                )
            except Exception:
                pass
            return
    except Exception:
        pass


    pedido_ativo = await _run_blocking(lambda: get_pedido_ativo(cliente_zap, restaurante_db_id), timeout=SUPABASE_TIMEOUT_SECONDS)


    # ===== Intercept UX: Pix pendente (reenviar chave copia-e-cola / link) =====
    try:
        if pedido_ativo and _pedido_has_pix_pending(pedido_ativo):
            t = (txt_norm or "").strip()
            pediu_chave = any(k in t for k in ("chave", "copia", "copia e cola", "pix", "qr", "qrcode", "qr code"))
            if pediu_chave:
                qr_code = (pedido_ativo.get("payment_qr_code") or "").strip()
                ticket_url = (pedido_ativo.get("payment_ticket_url") or "").strip()
                msg = "💠 *Pix do seu pedido está pendente.*\n"
                if ticket_url:
                    msg += f"\n🔗 Link: {ticket_url}\n"
                if qr_code:
                    msg += f"\n📋 *Copia e cola (chave):*\n{qr_code}"
                else:
                    msg += "\nSe você não recebeu o copia-e-cola, aguarde alguns segundos e me peça novamente."
                await enviar_zap_async(phone_id, cliente_zap, msg)
                return
    except Exception:
        pass

    # ===== Intercept UX: Cliente diz 'paguei' (confere no provedor) =====
    try:
        if pedido_ativo and _pedido_has_pix_pending(pedido_ativo):
            t = (txt_norm or "").strip()
            if any(k in t for k in ("paguei", "pago", "pagamento feito", "pix feito", "já paguei", "ja paguei")):
                provider = (pedido_ativo.get("payment_provider") or "").strip().lower()
                payment_id = (pedido_ativo.get("payment_id") or "").strip()
                if provider == "mercadopago" and payment_id:
                    settings = await sb_exec(lambda: get_pix_settings_for_restaurante(int(restaurante_db_id)))
                    mp_token = (settings or {}).get("mp_token")
                    if mp_token:
                        try:
                            pay = await _run_blocking(lambda: mp_get_payment(mp_token, payment_id), timeout=10)
                            st = str((pay or {}).get("status") or "").strip().lower()
                            if st == "approved":
                                upd = {
                                    "payment_status": "approved",
                                    "status": "confirmado",
                                    "forma_pagamento": "Pix (Pago no WhatsApp)",
                                    "bot_finalizado": True,
                                    "bot_finalizado_em": now_iso,
                                }
                                try:
                                    await sb_exec(lambda: supabase.table("pedidos").update(upd).eq("id", int(pedido_ativo.get("id") or 0)).execute())
                                except Exception:
                                    upd.pop("bot_finalizado", None)
                                    upd.pop("bot_finalizado_em", None)
                                    await sb_exec(lambda: supabase.table("pedidos").update(upd).eq("id", int(pedido_ativo.get("id") or 0)).execute())

                                await _persist_pedido_itens(
                                    restaurante_db_id=int(restaurante_db_id),
                                    pedido_id=int(pedido_ativo.get("id") or 0),
                                    carrinho_json=(pedido_ativo or {}).get("carrinho_json"),
                                )
                                
                                await enviar_zap_async(phone_id, cliente_zap, "✅ Pagamento confirmado! Seu pedido foi confirmado. Agora aguarde o restaurante aceitar.")
                                return

                            await enviar_zap_async(phone_id, cliente_zap, "⏳ Ainda não apareceu como pago aqui. Pode levar alguns minutos. Se quiser, me diga *'reenviar chave pix'*." )
                            return
                        except asyncio.TimeoutError:
                            await enviar_zap_async(phone_id, cliente_zap, "⏳ Estou demorando para confirmar agora. Tenta de novo em 1 min dizendo *'paguei'*." )
                            return
                        except Exception:
                            await enviar_zap_async(phone_id, cliente_zap, "⚠️ Não consegui confirmar o pagamento agora. Aguarde um pouco (ou envie *'reenviar chave pix'*)." )
                            return
    except Exception:
        pass



    try:
        if _reset_state_if_stale(cliente_zap, phone_id, estado_data):
            estado_data = await _run_blocking(lambda: get_estado(cliente_zap, phone_id), timeout=SUPABASE_TIMEOUT_SECONDS)
            estado_atual = (estado_data["estado_conversa"] if estado_data else "INICIO") or "INICIO"
            dados_parciais = (estado_data.get("dados_parciais") or {}) if estado_data else {}
    except Exception:
        pass


    if estado_atual == "CONFIRMAR_ENDERECO_AUDIO":
        t = (txt_norm or "").strip()
        is_yes = t in ("sim", "s", "isso", "isso mesmo", "correto", "confirmo", "pode", "pode sim", "ok", "certo")
        is_no = t in ("nao", "não", "n", "negativo", "errado", "nao é", "não é")

        if is_yes:
            end_confirm = str((dados_parciais or {}).get("endereco_txt") or "").strip()
            bairro_confirm = str((dados_parciais or {}).get("bairro") or "").strip()
            taxa_confirm = float((dados_parciais or {}).get("taxa") or 0.0)
            fp_confirm = str((dados_parciais or {}).get("forma_pagamento") or "").strip().lower()
            if fp_confirm not in ("pix", "dinheiro", "cartao"):
                fp_confirm = ""

            dados_next = {
                "endereco_txt": end_confirm,
                "bairro": bairro_confirm,
                "taxa": taxa_confirm,
            }
            if fp_confirm:
                dados_next["forma_pagamento"] = fp_confirm

            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", dados_next))

            if fp_confirm and pedido_ativo and now_iso:
                handled = await _handle_pagamento_flow(
                    phone_id=phone_id,
                    cliente_zap=cliente_zap,
                    restaurante_db_id=int(restaurante_db_id),
                    pedido_ativo=pedido_ativo,
                    dados_parciais=dados_next,
                    txt_norm=fp_confirm,
                    texto_completo=fp_confirm,
                    now_iso=now_iso,
                )
                if handled:
                    return

            total_prod = float((pedido_ativo or {}).get("total_valor") or 0.0)
            total_com_taxa = total_prod + float(taxa_confirm or 0.0)
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                f"📍 Identifiquei: *{str(bairro_confirm).title()}* (Taxa: R$ {taxa_confirm:.2f}).\n"
                f"💰 *Total Final: R$ {total_com_taxa:.2f}*\n\n"
                "Qual a forma de pagamento? *(Pix, Dinheiro ou Cartão)*",
            )
            return

        if is_no:
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_ENDERECO", {}))
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                "Sem problema. Me envie o *endereço completo* e o *bairro*, por favor.",
            )
            return

        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "Só para confirmar: o endereço está correto? Responda *sim* ou *não*.",
        )
        return


    if (estado_atual == "INICIO") and _is_short_greeting(txt_norm):
        last_final = await sb_exec(lambda: _get_last_finalizado(restaurante_db_id, cliente_zap))
        if last_final:
            await _send_repeat_offer(phone_id, cliente_zap, last_final)
            return
        t = (txt_norm or "").strip()
        nome_loja = (dados_loja.get("nome") or "").strip()
        nome_txt = f" a {nome_loja}" if nome_loja else ""
        if "boa noite" in t:
            saudacao = f"Boa noite, bem-vindo{nome_txt}!"
        elif "boa tarde" in t:
            saudacao = f"Boa tarde, bem-vindo{nome_txt}!"
        elif "bom dia" in t:
            saudacao = f"Bom dia, bem-vindo{nome_txt}!"
        else:
            saudacao = "Oi! 🙂 Me diga o que você gostaria de pedir hoje."

        await enviar_zap_async(phone_id, cliente_zap, saudacao)

        cardapio_txt = (dados_loja.get("cardapio") or "").strip()
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            (cardapio_txt or "No momento estou sem cardápio carregado."),
        )
        return



    status_pedido_norm = (pedido_ativo.get("status") or "").strip().lower() if pedido_ativo else ""
    if pedido_ativo and status_pedido_norm in ("em preparo", "em_preparo"):
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "⚠️ Seu pedido já está em preparo e não pode mais ser alterado.\n"
            "Caso precise falar com o restaurante, aguarde o atendimento humano.",
        )
        return

    bairros_dict = dados_loja.get("taxas_dict", {}) or {}

    def _bairros_lista_unica(bdict: dict) -> list[str]:
        # `taxas_dict` hoje pode ter duplicado: chave normalizada + chave original
        seen = set()
        out = []
        for k in (bdict or {}).keys():
            nk = normalizar_texto(k)
            if not nk or nk in seen:
                continue
            seen.add(nk)
            out.append(nk.title())
        out.sort()
        return out

    bairros_lista = _bairros_lista_unica(bairros_dict)
    lista_bairros_txt = ", ".join(bairros_lista)

    def _is_offtopic_question() -> bool:
        t = (txt_norm or "").strip()
        if not t:
            return False

        questionish = (
            ("?" in (texto_completo or ""))
            or t.startswith(("qual", "quais", "oque", "o que", "como", "por que", "porque", "pra que", "para que", "quem", "onde", "quando"))
        )
        if not questionish:
            return False

        # Se houver intenção clara de pedido/cardápio/checkout, não bloqueia.
        on_topic = any(k in t for k in (
            "pizza", "piza", "hamb", "burger", "x ", "lanche", "batata", "bebida", "refriger", "refri", "coca", "guarana", "guaraná",
            "cardapio", "cardápio", "menu", "ingrediente", "ingredientes", "vem com", "sem ",
            "pedido", "carrinho", "finaliz", "fech", "endereco", "endereço", "bairro", "entrega", "retirada",
            "pix", "dinheiro", "cartao", "cartão",
        ))
        if on_topic:
            return False

        # Gatilhos comuns de pergunta fora do contexto (química/geral)
        if any(k in t for k in (
            "soda caustica", "soda cáustica", "naoh", "hidroxido", "hidróxido",
            "composto quimico", "composto químico", "quimica", "química", "formula", "fórmula",
            "molecula", "molécula", "equacao", "equação",
        )):
            return True

        return False

    def _is_tempo_entrega_question() -> bool:
        t = (txt_norm or "").strip()
        if not t:
            return False
        questionish = (
            ("?" in (texto_completo or ""))
            or t.startswith(("qual", "quais", "oque", "o que", "como", "por que", "porque", "pra que", "para que", "quanto", "quando"))
        )
        tempo_kw = (
            "tempo", "demora", "demorar", "quanto tempo", "prazo", "sair", "sai", "chegar", "chega",
            "entrega", "entregar", "pronto", "preparo",
        )
        if not questionish and not any(k in t for k in ("demora", "tempo", "prazo")):
            return False
        return any(k in t for k in tempo_kw)

    def _is_pagamento_question() -> bool:
        t = (txt_norm or "").strip()
        if not t:
            return False
        questionish = (
            ("?" in (texto_completo or ""))
            or t.startswith(("qual", "quais", "oque", "o que", "como", "por que", "porque", "pra que", "para que", "aceita", "aceitam"))
        )
        if not questionish:
            return False
        return any(k in t for k in ("pagamento", "pix", "dinheiro", "cartao", "cartão", "maquininha", "credito", "crédito", "debito", "débito"))

    def _message_has_order_or_action() -> bool:
        t = (txt_norm or "").strip()
        if not t:
            return False
        # checkout/endereço/pagamento explícito
        if any(k in t for k in ("finaliz", "fech", "encerr", "retirada")):
            return True
        if _texto_parece_endereco(texto_completo, t) or ("bairro" in t):
            return True
        if any(k in t for k in ("endereco", "endereço", "localizacao", "localização")):
            return True
        if any(k in t for k in ("pix", "dinheiro", "cartao", "cartão", "maquininha", "credito", "crédito", "debito", "débito")):
            return True
        # heurística por itens do cardápio
        tabela_precos = (dados_loja or {}).get("precos_dict", {}) or {}
        if isinstance(tabela_precos, dict) and tabela_precos:
            for k in tabela_precos.keys():
                nk = normalizar_texto(str(k or ""))
                if nk and nk in t:
                    return True
        # palavras típicas de pedido
        if any(k in t for k in (
            "quero", "queria", "manda", "adicion", "coloca", "mais",
            "pizza", "piza", "hamb", "burger", "lanche", "batata", "bebida", "refri", "refriger", "coca",
            "meio a meio", "metade",
        )):
            return True
        return False

    def _detect_loja_info_intent() -> str | None:
        t = (txt_norm or "").strip()
        if not t:
            return None

        if _texto_parece_endereco(texto_completo, t):
            return None

        raw_lower = (texto_completo or "").strip().lower()
        addr_placeholders = (
            "a determinar", "a definir", "a confirmar", "a combinar",
            "depois informo", "informo depois", "vou informar",
            "nao sei", "não sei",
        )
        intent_to_provide_addr = (
            "meu endereco" in t
            or t.startswith("endereco")
            or "endereco:" in raw_lower
            or "endereço:" in raw_lower
            or any(k in t for k in addr_placeholders)
        )
        if "endereco" in t and intent_to_provide_addr:
            return None

        questionish = (
            ("?" in (texto_completo or ""))
            or t.startswith(("qual", "quais", "oque", "o que", "como", "onde", "quando", "que horas", "até", "ate"))
            or any(k in t for k in ("telefone", "contato", "endereco", "endereço", "horario", "horário", "funciona", "abre", "fecha"))
        )

        store_ref = any(k in t for k in (
            "pizzaria", "restaurante", "lanchonete", "loja",
            "da pizzaria", "do restaurante", "da loja", "de voces", "de vocês", "de vcs",
        ))

        if any(k in t for k in ("telefone", "fone", "whatsapp", "contato", "celular", "número", "numero")):
            if questionish or store_ref:
                return "telefone"

        if any(k in t for k in ("horario", "horário", "funciona", "funcionamento", "abre", "fecha", "até que horas", "ate que horas")):
            if questionish or store_ref:
                return "horario"

        if any(k in t for k in ("endereco", "endereço", "onde fica", "localizacao", "localização", "mapa", "maps")):
            if questionish or store_ref:
                return "endereco"

        return None

    def _get_loja_info() -> dict:
        return {
            "endereco": str((dados_loja or {}).get("endereco_loja") or "").strip(),
            "telefone": str((dados_loja or {}).get("telefone_loja") or "").strip(),
            "horario": str((dados_loja or {}).get("horario_loja") or "").strip(),
        }

    # ===== Intercept: "quais locais / bairros que entrega" (em qualquer etapa) =====
    if estado_atual not in ("AGUARDANDO_AVALIACAO_POS_VENDA", "CONFIRMAR_PEDIDO_DE_SEMPRE"):
        t = (txt_norm or "").strip()
        pergunta_area = (
            ("entrega" in t or "entregam" in t or "delivery" in t)
            and any(w in t for w in ("quais", "qual", "onde", "locais", "bairros", "area", "área", "regiao", "região"))
        )
        if pergunta_area:
            if bairros_lista:
                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "📍 *Entregamos nestes bairros:*\n"
                    + "\n".join([f"- {b}" for b in bairros_lista])
                    + "\n\nSe quiser, me diga seu bairro que eu confirmo a taxa certinho.",
                )
            else:
                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "No momento estou sem a lista de bairros cadastrada. Me diga seu bairro que eu confirmo com o restaurante.",
                )
            await _maybe_resume_prompt()
            return

    # ===== Intercept: informações da loja (endereço/telefone/horário) =====
    if estado_atual not in ("AGUARDANDO_AVALIACAO_POS_VENDA", "CONFIRMAR_PEDIDO_DE_SEMPRE"):
        loja_intent = _detect_loja_info_intent()
        if loja_intent:
            info = _get_loja_info()
            if loja_intent == "endereco":
                if info["endereco"]:
                    await enviar_zap_async(phone_id, cliente_zap, f"📍 Endereço da loja: *{info['endereco']}*.")
                else:
                    await enviar_zap_async(phone_id, cliente_zap, "Ainda não tenho o endereço da loja cadastrado. Posso chamar um atendente se você quiser.")
            elif loja_intent == "telefone":
                if info["telefone"]:
                    await enviar_zap_async(phone_id, cliente_zap, f"📞 Telefone da loja: *{info['telefone']}*.")
                else:
                    await enviar_zap_async(phone_id, cliente_zap, "Ainda não tenho o telefone da loja cadastrado. Posso chamar um atendente se você quiser.")
            elif loja_intent == "horario":
                if info["horario"]:
                    await enviar_zap_async(phone_id, cliente_zap, f"🕒 Horário de funcionamento: *{info['horario']}*.")
                else:
                    await enviar_zap_async(phone_id, cliente_zap, "Ainda não tenho o horário de funcionamento cadastrado. Posso chamar um atendente se você quiser.")

            if not _message_has_order_or_action():
                await _maybe_resume_prompt()
                return

    # ===== Intercept: perguntas fora de contexto (não responder conteúdo) =====
    if estado_atual in ("INICIO", "AGUARDANDO_MAIS_ALGO") and _is_offtopic_question():
        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "Consigo te ajudar com *pedidos* e dúvidas do *cardápio*. 🙂\n"
            "Se for sobre ingredientes, pode perguntar por exemplo: *\"o que vem no X Burger?\"*\n\n"
            "O que você gostaria de pedir?",
        )
        return

    # ===== Intercept: tempo de entrega/preparo (responde e retoma fluxo) =====
    if estado_atual not in ("AGUARDANDO_AVALIACAO_POS_VENDA", "CONFIRMAR_PEDIDO_DE_SEMPRE"):
        if _is_tempo_entrega_question():
            fp_state = str((dados_parciais or {}).get("forma_pagamento") or "").strip().lower()
            if estado_atual == "AGUARDANDO_PAGAMENTO" and (not fp_state) and not (pedido_ativo and _pedido_has_pix_pending(pedido_ativo)):
                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "Ainda falta confirmar a *forma de pagamento* para eu liberar o pedido.\n"
                    "Responda com *pix*, *dinheiro* ou *cartão*.",
                )
                if not _message_has_order_or_action():
                    return
            if pedido_ativo and _pedido_has_pix_pending(pedido_ativo):
                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "Seu *Pix* ainda está pendente. Se quiser, me diga *'reenviar chave pix'*.",
                )
                if not _message_has_order_or_action():
                    return
            fila = await _run_blocking(lambda: count_pedidos_abertos(int(restaurante_db_id)), timeout=SUPABASE_TIMEOUT_SECONDS)
            eta_min = estimate_tempo_entrega_min(int(fila or 0))
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                f"⏳ No momento a estimativa é de *{eta_min} min*. Assim que o pedido for confirmado, seguimos preparando."
            )
            if not _message_has_order_or_action():
                await _maybe_resume_prompt()
                return

    # ===== Intercept: pergunta sobre chave Pix =====
    if estado_atual not in ("AGUARDANDO_AVALIACAO_POS_VENDA", "CONFIRMAR_PEDIDO_DE_SEMPRE"):
        t = (txt_norm or "").strip()
        pergunta_pix = any(k in t for k in ("chave pix", "pix")) and any(k in t for k in ("chave", "copia", "copia e cola", "copiar", "qr", "qrcode", "qr code", "código"))
        if pergunta_pix:
            if pedido_ativo and _pedido_has_pix_pending(pedido_ativo):
                qr_code = (pedido_ativo.get("payment_qr_code") or "").strip()
                ticket_url = (pedido_ativo.get("payment_ticket_url") or "").strip()
                msg = "💠 *Pix do seu pedido está pendente.*\n"
                if ticket_url:
                    msg += f"\n🔗 Link: {ticket_url}\n"
                if qr_code:
                    msg += f"\n📋 *Copia e cola (chave):*\n{qr_code}"
                else:
                    msg += "\nSe você não recebeu o copia-e-cola, aguarde alguns segundos e me peça novamente."
                await enviar_zap_async(phone_id, cliente_zap, msg)
                return

            await enviar_zap_async(
                phone_id,
                cliente_zap,
                "A chave Pix é gerada *quando o pedido é finalizado*. 🙂\n"
                "Se quiser, posso fechar seu pedido agora."
            )
            if not _message_has_order_or_action():
                await _maybe_resume_prompt()
                return

    # ===== Intercept: perguntas sobre pagamento (responde e retoma fluxo) =====
    if estado_atual not in ("AGUARDANDO_AVALIACAO_POS_VENDA", "CONFIRMAR_PEDIDO_DE_SEMPRE"):
        if _is_pagamento_question():
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                "Aceitamos *Pix, Dinheiro e Cartão*.",
            )
            if not _message_has_order_or_action():
                await _maybe_resume_prompt()
                return

    # ===== Intercept: pergunta de taxa de entrega (funciona fora do AGUARDANDO_ENDERECO) =====
    # Não atrapalha estados "especiais"
    if estado_atual not in ("AGUARDANDO_AVALIACAO_POS_VENDA", "CONFIRMAR_PEDIDO_DE_SEMPRE"):
        # Detecta se a mensagem está perguntando taxa/frete/entrega
        taxa_keywords = ("taxa", "entrega", "frete", "delivery")
        pergunta_keywords = ("quanto", "valor", "fica", "custa", "é")
        parece_pergunta_taxa = (
            any(k in txt_norm for k in taxa_keywords) and any(k in txt_norm for k in pergunta_keywords)
        ) or ("quanto" in txt_norm and ("pro " in txt_norm or "para " in txt_norm or "pra " in txt_norm))

        # Também detecta se existe intenção de troca na mesma mensagem (pra não dar return cedo)
        swap_re = re.search(
            r"\b(?:troca(?:r)?|substitui(?:r)?|muda(?:r)?)\b\s+(?P<old>.+?)\s+(?:por|pra|para|no lugar de|em vez de)\s+(?P<new>.+)",
            txt_norm,
        )
        tem_troca = bool(swap_re)

        # Se a mesma mensagem também parece um pedido, não respondemos aqui (evita bloquear o carrinho)
        tabela_precos = (dados_loja or {}).get("precos_dict", {}) or {}
        tem_item_cardapio = False
        if isinstance(tabela_precos, dict) and tabela_precos:
            for k in tabela_precos.keys():
                nk = normalizar_texto(str(k or ""))
                if nk and nk in txt_norm:
                    tem_item_cardapio = True
                    break
        tem_pedido_na_msg = tem_item_cardapio or any(k in txt_norm for k in (
            "quero", "queria", "adicion", "manda", "pizza", "piza", "hamb", "burger",
            "lanche", "batata", "bebida", "refriger", "refri", "coca", "combo",
        ))

        if parece_pergunta_taxa:
            # Se a mesma mensagem também tem troca, evitamos responder duas coisas conflitantes.
            # A troca vai responder e manter o fluxo (UX menos caótica).
            if tem_troca:
                bairro_match = encontrar_melhor_match(texto_completo, list(bairros_dict.keys())) if bairros_dict else None
                # Só envia a taxa se CONSEGUIR identificar com certeza.
                if bairro_match:
                    taxa = float(bairros_dict[bairro_match])
                    await enviar_zap_async(phone_id, cliente_zap, f"📍 *{str(bairro_match).title()}* — Taxa de entrega: *R$ {taxa:.2f}*.")
                    await _maybe_resume_prompt()
                # Não pede bairro aqui (para não misturar com a resposta do swap).
            elif not tem_pedido_na_msg:
                if not bairros_dict:
                    await enviar_zap_async(phone_id, cliente_zap, "No momento estou sem as taxas de entrega carregadas. Me diga seu bairro que eu confirmo com o restaurante.")
                    await _maybe_resume_prompt()
                    return

                bairro_match = encontrar_melhor_match(texto_completo, list(bairros_dict.keys()))
                if not bairro_match:
                    for k in list(bairros_dict.keys()):
                        nk = normalizar_texto(str(k or ""))
                        if nk and nk in txt_norm:
                            bairro_match = k
                            break
                if bairro_match:
                    taxa = float(bairros_dict[bairro_match])
                    await enviar_zap_async(phone_id, cliente_zap, f"📍 *{str(bairro_match).title()}* — Taxa de entrega: *R$ {taxa:.2f}*.")
                    await _maybe_resume_prompt()
                    return

                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "Qual é o *bairro*? Assim eu te digo a taxa certinho.",
                )
                await _maybe_resume_prompt()
                return

    # ===== Intercept: swap pendente (quando o cliente só respondeu o item antigo) =====
    if estado_atual not in ("AGUARDANDO_AVALIACAO_POS_VENDA", "CONFIRMAR_PEDIDO_DE_SEMPRE"):
        pending_new = str((dados_parciais or {}).get("pending_swap_new") or "").strip()
        if pending_new and not re.search(r"\b(troca(?:r)?|substitui(?:r)?|muda(?:r)?)\b", txt_norm):
            try:
                handled = await _handle_troca_item_deterministica(
                    phone_id=phone_id,
                    cliente_zap=cliente_zap,
                    restaurante_db_id=int(restaurante_db_id),
                    pedido_ativo=pedido_ativo,
                    dados_loja=dados_loja,
                    texto_completo=str(texto_completo or ""),
                    old_raw=str(texto_completo or "").strip(),
                    new_raw=pending_new,
                )
                if handled:
                    try:
                        merged = dict(dados_parciais or {})
                        merged.pop("pending_swap_new", None)
                        await sb_exec(lambda: set_estado(cliente_zap, phone_id, estado_atual, merged))
                        dados_parciais = merged
                    except Exception:
                        pass
                    return
            except Exception:
                pass

    # ===== Intercept: "troca por Y" (sem item antigo explícito) =====
    if estado_atual not in ("AGUARDANDO_AVALIACAO_POS_VENDA", "CONFIRMAR_PEDIDO_DE_SEMPRE"):
        m_troca_sem_old = re.search(
            r"\b(?:troca(?:r)?|substitui(?:r)?|muda(?:r)?)\b\s+(?:por|pra|para)\s+(?P<new>.+)$",
            txt_norm,
        )
        if m_troca_sem_old and pedido_ativo:
            new_raw = (m_troca_sem_old.group("new") or "").strip()
            carrinho_atual = _safe_dict((pedido_ativo or {}).get("carrinho_json"))
            old_key = None

            if len(carrinho_atual or {}) == 1:
                old_key = next(iter(carrinho_atual))
            else:
                last_key = str((dados_parciais or {}).get("last_item_key") or "").strip()
                if last_key and last_key in (carrinho_atual or {}):
                    old_key = last_key

            if not old_key and carrinho_atual:
                def _score_overlap(a: str, b: str) -> int:
                    ta = {p for p in re.sub(r"[^a-z0-9\s]", " ", normalizar_texto(a)).split() if len(p) >= 3}
                    tb = {p for p in re.sub(r"[^a-z0-9\s]", " ", normalizar_texto(b)).split() if len(p) >= 3}
                    return len(ta & tb)

                best = (0, None)
                for k, v in (carrinho_atual or {}).items():
                    nome_disp = (v or {}).get("nome_exibicao") or k
                    score = _score_overlap(new_raw, nome_disp)
                    if score > best[0]:
                        best = (score, k)
                if best[0] >= 1:
                    old_key = best[1]

            if old_key:
                old_raw = (carrinho_atual.get(old_key, {}) or {}).get("nome_exibicao") or old_key
                handled = await _handle_troca_item_deterministica(
                    phone_id=phone_id,
                    cliente_zap=cliente_zap,
                    restaurante_db_id=int(restaurante_db_id),
                    pedido_ativo=pedido_ativo,
                    dados_loja=dados_loja,
                    texto_completo=str(texto_completo or ""),
                    old_raw=str(old_raw or ""),
                    new_raw=new_raw,
                )
                if handled:
                    return

            # Se não encontrou item antigo, pede confirmação e guarda o destino
            try:
                merged = dict(dados_parciais or {})
                merged["pending_swap_new"] = new_raw
                await sb_exec(lambda: set_estado(cliente_zap, phone_id, estado_atual, merged))
                dados_parciais = merged
            except Exception:
                pass
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                f"Qual item do seu carrinho devo trocar por *{new_raw}*?",
            )
            return

    # ===== Intercept: remoção direta (ex.: "não quero X", "tira X") =====
    if estado_atual not in ("AGUARDANDO_AVALIACAO_POS_VENDA", "CONFIRMAR_PEDIDO_DE_SEMPRE"):
        m_remove = re.search(r"\b(?:nao|não)\s+quero\s+(?P<item>.+)$", txt_norm)
        if not m_remove:
            m_remove = re.search(r"\b(?:tira|remova|remove)\s+(?P<item>.+)$", txt_norm)

        if m_remove and pedido_ativo:
            item_raw = (m_remove.group("item") or "").strip()
            carrinho_atual = _safe_dict((pedido_ativo or {}).get("carrinho_json"))

            def _match_cart(term: str) -> str | None:
                term_n = normalizar_texto(term or "")
                if not term_n:
                    return None
                for k, v in (carrinho_atual or {}).items():
                    kn = normalizar_texto(k)
                    dn = normalizar_texto((v or {}).get("nome_exibicao") or "")
                    if term_n in kn or term_n in dn:
                        return k
                m = difflib.get_close_matches(term_n, [normalizar_texto(k) for k in (carrinho_atual or {}).keys()], n=1, cutoff=0.65)
                if m:
                    for k in (carrinho_atual or {}).keys():
                        if normalizar_texto(k) == m[0]:
                            return k
                return None

            if _match_cart(item_raw):
                handled = await _handle_remover_item_deterministica(
                    phone_id=phone_id,
                    cliente_zap=cliente_zap,
                    restaurante_db_id=int(restaurante_db_id),
                    pedido_ativo=pedido_ativo,
                    dados_loja=dados_loja,
                    item_raw=item_raw,
                )
                if handled:
                    return

    # ===== Intercept: "troca X por Y" determinístico (remove + adiciona, com estoque) =====
    if estado_atual not in ("AGUARDANDO_AVALIACAO_POS_VENDA", "CONFIRMAR_PEDIDO_DE_SEMPRE"):
        m_troca = re.search(
            r"\b(?:troca(?:r)?|substitui(?:r)?|muda(?:r)?)\b\s+(?P<old>.+?)\s+(?:por|pra|para|no lugar de|em vez de)\s+(?P<new>.+)",
            txt_norm,
        )

        if m_troca:
            old_raw = (m_troca.group("old") or "").strip()
            new_raw = (m_troca.group("new") or "").strip()
            handled = await _handle_troca_item_deterministica(
                phone_id=phone_id,
                cliente_zap=cliente_zap,
                restaurante_db_id=int(restaurante_db_id),
                pedido_ativo=pedido_ativo,
                dados_loja=dados_loja,
                texto_completo=str(texto_completo or ""),
                old_raw=old_raw,
                new_raw=new_raw,
            )
            if handled:
                return

    # ===== Slot Filling (não-linear): extrai e persiste slots mesmo fora do checkout =====
    slot_obj: dict | None = None
    if estado_atual not in ("AGUARDANDO_AVALIACAO_POS_VENDA", "CONFIRMAR_PEDIDO_DE_SEMPRE"):
        try:
            slot_obj = await slot_extract_universal(
                estado_atual=str(estado_atual),
                pedido_ativo=pedido_ativo,
                dados_loja=dados_loja,
                texto=str(texto_completo or ""),
            )
        except Exception:
            slot_obj = None

        # Persiste slots extraídos em dados_parciais (validação tardia)
        try:
            merged = _merge_slots_into_dados_parciais(dados_parciais or {}, slot_obj)
            if isinstance(merged, dict) and merged != (dados_parciais or {}):
                await sb_exec(lambda: set_estado(cliente_zap, phone_id, estado_atual, merged))
                dados_parciais = merged
        except Exception:
            pass

        # Heurística: se a mensagem contém endereço e o slot não capturou, persistimos mesmo assim
        try:
            if isinstance(dados_parciais, dict) and not str((dados_parciais or {}).get("endereco_txt") or "").strip():
                if _texto_parece_endereco(str(texto_completo or ""), str(txt_norm or "")):
                    end_extrato = extrair_endereco_de_texto(str(texto_completo or ""))
                    end_bruto = ""
                    if not end_extrato:
                        raw_addr = str(texto_completo or "")
                        m_addr = re.search(
                            r"\b(?:rua|r\.|avenida|av\.?|travessa|tv\.?|alameda|estrada|rodovia|beco|viela|vila|quadra|bloco|passagem|comunidade|portao|portão)\b[^\n\.]*",
                            raw_addr,
                            flags=re.IGNORECASE,
                        )
                        if m_addr:
                            end_bruto = (m_addr.group(0) or "").strip()
                    if end_extrato or end_bruto:
                        dados_parciais = dict(dados_parciais or {})
                        dados_parciais["endereco_txt"] = end_extrato or end_bruto
                        await sb_exec(lambda: set_estado(cliente_zap, phone_id, estado_atual, dados_parciais))
        except Exception:
            pass

        # Prefill para avançar endereço/retirada após atualização do carrinho
        try:
            pfill = _slot_to_prefill(slot_obj)
            if pfill:
                intent_router_prefill = pfill
        except Exception:
            pass

        # Se a mensagem estiver em modo checkout (finalizar), tenta avançar sem depender de ordem.
        # Obs: quando houver itens na mesma frase, o carrinho será atualizado e o prefill pode avançar.
        force_checkout = _slot_should_force_checkout(txt_norm, slot_obj)
        if force_checkout and (not (slot_obj or {}).get("itens_adicionar")) and (not (slot_obj or {}).get("itens_remover")):
            try:
                handled = await _slot_advance_checkout(
                    phone_id=phone_id,
                    cliente_zap=cliente_zap,
                    restaurante_db_id=int(restaurante_db_id),
                    pedido_ativo=pedido_ativo,
                    dados_parciais=(dados_parciais or {}),
                    bairros_dict=bairros_dict or {},
                    lista_bairros_txt=lista_bairros_txt,
                    now_iso=now_iso,
                )
                if handled:
                    return
            except Exception:
                pass




    palavras_comando = [
        "mudar", "trocar", "alterar", "adicionar", "remover", "tira", "poe", "põe",
        "esquece", "cancelar", "nao quero", "não quero", "quero mais", "obs", "observacao", "observação", "sem",
    ]
    possivel_mudanca = any(p in txt_norm for p in palavras_comando)
    if estado_atual != "INICIO" and possivel_mudanca:
        estado_atual = "FORCAR_IA"

    # ===== Intent Router (IA primeiro) =====
    # Mantém guardrails acima (em preparo, pix aprovado, cancelamento, pix pendente etc).
    # Se o classificador falhar ou estiver com baixa confiança, cai no fluxo antigo.
    if INTENT_ROUTER_ENABLED and estado_atual not in ("AGUARDANDO_AVALIACAO_POS_VENDA", "CONFIRMAR_PEDIDO_DE_SEMPRE"):
        try:
            intent_obj = await _classify_global_intent(
                phone_id=str(phone_id),
                cliente_zap=str(cliente_zap),
                texto=str(texto_completo),
                estado_atual=str(estado_atual),
                pedido_ativo=pedido_ativo,
                dados_loja=dados_loja,
            )
        except Exception:
            intent_obj = None

        try:
            conf = intent_obj.get("confianca") if isinstance(intent_obj, dict) else None
            confident = (conf is None) or (float(conf) >= 0.60)
        except Exception:
            confident = False

        if isinstance(intent_obj, dict) and confident:
            intencao = (intent_obj.get("intencao") or "").strip()
            params = intent_obj.get("parametros") if isinstance(intent_obj.get("parametros"), dict) else {}
            followup = (intent_obj.get("pergunta_followup") or "").strip()
            has_slot_items = bool(
                SLOT_FILLING_ENABLED
                and isinstance(slot_obj, dict)
                and ((slot_obj.get("itens_adicionar") or []) or (slot_obj.get("itens_remover") or []))
            )

            if has_slot_items and intencao in (
                "perguntar",
                "outro",
                "perguntar_cardapio",
                "perguntar_ingredientes",
                "perguntar_taxa_entrega",
                "perguntar_bairros",
            ):
                intencao = "adicionar_item"

            # Se a mensagem traz itens + endereço/pagamento/fechamento, adiciona itens primeiro
            if has_slot_items and intencao in ("definir_endereco", "definir_pagamento", "pedir_fechamento"):
                intencao = "adicionar_item"

            # Prefill de dados_parciais a partir dos parâmetros do intent router
            try:
                p_bairro = str((params or {}).get("bairro") or "").strip()
                p_end = str((params or {}).get("endereco_txt") or "").strip()
                p_pg = str((params or {}).get("forma_pagamento") or "").strip().lower()
                p_tipo = str((params or {}).get("tipo_entrega") or "").strip().lower()
                p_troco = (params or {}).get("troco_para")

                merged = dict(dados_parciais or {})
                if p_bairro:
                    merged["bairro"] = p_bairro
                if p_end:
                    merged["endereco_txt"] = p_end
                if p_pg in ("pix", "dinheiro", "cartao"):
                    merged["forma_pagamento"] = p_pg
                if p_tipo in ("entrega", "retirada"):
                    merged["tipo_entrega"] = p_tipo
                try:
                    if p_troco is not None:
                        merged["troco_para"] = _money_2(float(p_troco))
                        merged.setdefault("forma_pagamento", "dinheiro")
                except Exception:
                    pass

                if isinstance(merged, dict) and merged != (dados_parciais or {}):
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, estado_atual, merged))
                    dados_parciais = merged
            except Exception:
                pass

            # 1) Perguntas diretas (responde e retorna)
            if intencao == "perguntar_cardapio":
                cardapio_txt = (dados_loja.get("cardapio") or "").strip()
                if cardapio_txt:
                    await enviar_zap_async(phone_id, cliente_zap, cardapio_txt)
                else:
                    await enviar_zap_async(phone_id, cliente_zap, "No momento estou sem o cardápio carregado. Me diga o que você procura (pizza, hambúrguer, bebida) que eu te ajudo.")
                await _maybe_resume_prompt()
                return

            if intencao == "perguntar_bairros":
                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "Me diga seu *bairro* que eu confirmo a taxa e disponibilidade certinho.",
                )
                await _maybe_resume_prompt()
                return

            # 1b) Troca de item (handler determinístico) — cobre casos sem regex "trocar X por Y"
            if intencao == "trocar_item":
                old_raw = str((params or {}).get("item_antigo") or "").strip()
                new_raw = str((params or {}).get("item_novo") or "").strip()

                # fallback: tenta extrair do texto se o classificador não preencheu
                if (not old_raw) or (not new_raw):
                    m_troca2 = re.search(
                        r"\b(?:troca(?:r)?|substitui(?:r)?|muda(?:r)?)\b\s+(?P<old>.+?)\s+(?:por|pra|para|no lugar de|em vez de)\s+(?P<new>.+)",
                        txt_norm,
                    )
                    if m_troca2:
                        old_raw = old_raw or str((m_troca2.group("old") or "")).strip()
                        new_raw = new_raw or str((m_troca2.group("new") or "")).strip()

                handled = await _handle_troca_item_deterministica(
                    phone_id=phone_id,
                    cliente_zap=cliente_zap,
                    restaurante_db_id=int(restaurante_db_id),
                    pedido_ativo=pedido_ativo,
                    dados_loja=dados_loja,
                    texto_completo=str(texto_completo or ""),
                    old_raw=old_raw,
                    new_raw=new_raw,
                )
                if handled:
                    return

            # 2) Retirada (não pede endereço)
            if intencao == "definir_retirada":
                await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", {"tipo_entrega": "retirada", "taxa": 0.0}))
                total_prod = float((pedido_ativo or {}).get("total_valor") or 0.0)
                msg = (
                    "Beleza! ✅ Vai ser *retirada no local*.\n\n"
                    f"💰 Total: R$ {total_prod:.2f}\n\n"
                    "Qual a forma de pagamento? *(Pix, Dinheiro ou Cartão)*"
                )
                await enviar_zap_async(phone_id, cliente_zap, msg)
                return

            # 2b) Status do pedido
            if intencao == "status_pedido":
                if not pedido_ativo:
                    await enviar_zap_async(phone_id, cliente_zap, "Ainda não encontrei um pedido em aberto aqui. Quer fazer um pedido?")
                    return
                st = str((pedido_ativo or {}).get("status") or "").strip() or "novo"
                pay = str((pedido_ativo or {}).get("payment_status") or "").strip().lower()
                resumo_txt = str((pedido_ativo or {}).get("resumo_pedido") or "").replace("|", "\n")
                msg = f"📦 Status: *{st}*"
                if pay:
                    msg += f"\n💳 Pagamento: *{pay}*"
                if resumo_txt and resumo_txt != "Carrinho vazio":
                    msg += f"\n\n🛒 Itens:\n{resumo_txt}"
                await enviar_zap_async(phone_id, cliente_zap, msg)
                return

            # 2c) Pix: reenviar/confirmar
            if intencao in ("reenviar_pix", "confirmar_pagamento_pix"):
                if pedido_ativo and _pedido_has_pix_pending(pedido_ativo):
                    if intencao == "reenviar_pix":
                        qr_code = (pedido_ativo.get("payment_qr_code") or "").strip()
                        ticket_url = (pedido_ativo.get("payment_ticket_url") or "").strip()
                        msg = "💠 *Pix do seu pedido está pendente.*\n"
                        if ticket_url:
                            msg += f"\n🔗 Link: {ticket_url}\n"
                        if qr_code:
                            msg += f"\n📋 *Copia e cola (chave):*\n{qr_code}"
                        else:
                            msg += "\nSe você não recebeu o copia-e-cola, aguarde alguns segundos e me peça novamente."
                        await enviar_zap_async(phone_id, cliente_zap, msg)
                        return

                    # confirmar_pagamento_pix
                    provider = (pedido_ativo.get("payment_provider") or "").strip().lower()
                    payment_id = (pedido_ativo.get("payment_id") or "").strip()
                    if provider == "mercadopago" and payment_id:
                        settings = await sb_exec(lambda: get_pix_settings_for_restaurante(int(restaurante_db_id)))
                        mp_token = (settings or {}).get("mp_token")
                        if mp_token:
                            try:
                                pay = await _run_blocking(lambda: mp_get_payment(mp_token, payment_id), timeout=10)
                                st = str((pay or {}).get("status") or "").strip().lower()
                                if st == "approved":
                                    upd = {
                                        "payment_status": "approved",
                                        "status": "confirmado",
                                        "forma_pagamento": "Pix (Pago no WhatsApp)",
                                        "bot_finalizado": True,
                                        "bot_finalizado_em": now_iso,
                                    }
                                    try:
                                        await sb_exec(lambda: supabase.table("pedidos").update(upd).eq("id", int(pedido_ativo.get("id") or 0)).execute())
                                    except Exception:
                                        upd.pop("bot_finalizado", None)
                                        upd.pop("bot_finalizado_em", None)
                                        await sb_exec(lambda: supabase.table("pedidos").update(upd).eq("id", int(pedido_ativo.get("id") or 0)).execute())
                                    await _persist_pedido_itens(
                                        restaurante_db_id=int(restaurante_db_id),
                                        pedido_id=int(pedido_ativo.get("id") or 0),
                                        carrinho_json=(pedido_ativo or {}).get("carrinho_json"),
                                    )
                                    await enviar_zap_async(phone_id, cliente_zap, "✅ Pagamento confirmado! Seu pedido foi confirmado. Agora aguarde o restaurante aceitar.")
                                    return

                                await enviar_zap_async(phone_id, cliente_zap, "⏳ Ainda não apareceu como pago aqui. Pode levar alguns minutos. Se quiser, me diga *'reenviar chave pix'*." )
                                return
                            except asyncio.TimeoutError:
                                await enviar_zap_async(phone_id, cliente_zap, "⏳ Estou demorando para confirmar agora. Tenta de novo em 1 min dizendo *'paguei'*." )
                                return
                            except Exception:
                                await enviar_zap_async(phone_id, cliente_zap, "⚠️ Não consegui confirmar o pagamento agora. Aguarde um pouco (ou envie *'reenviar chave pix'*)." )
                                return

                # se não tem pix pendente, cai no fluxo antigo

            # 2d) Finalizar pedido (checkout) em qualquer etapa
            if intencao == "pedir_fechamento":
                if not pedido_ativo or not _safe_dict((pedido_ativo or {}).get("carrinho_json")):
                    await enviar_zap_async(phone_id, cliente_zap, "Seu carrinho está vazio. Me diga o que você quer pedir. 🙂")
                    return

                if _is_retirada_text(txt_norm):
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", {"tipo_entrega": "retirada", "taxa": 0.0}))
                    total_prod = float((pedido_ativo or {}).get("total_valor") or 0.0)
                    await enviar_zap_async(
                        phone_id,
                        cliente_zap,
                        "Perfeito! ✅ Vai ser *retirada no local*.\n\n"
                        f"💰 Total: R$ {total_prod:.2f}\n\n"
                        "Qual a forma de pagamento? *(Pix, Dinheiro ou Cartão)*",
                    )
                    return

                await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_ENDERECO", {}))
                resumo = (pedido_ativo.get("resumo_pedido") or "Carrinho vazio")
                try:
                    total = float(pedido_ativo.get("total_valor") or 0.0)
                except Exception:
                    total = 0.0
                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "📝 *Resumo:*\n" + str(resumo).replace("|", "\n") + f"\n💰 Subtotal: R$ {total:.2f}\n\n"
                    "📍 Me mande o *endereço completo com bairro* (ou diga *retirada*).",
                )
                return

            # 3) Definir endereço/bairro (resolve números de endereço sem virar quantidade de item)
            if intencao == "definir_endereco":
                fp_prefill = str((params or {}).get("forma_pagamento") or (dados_parciais or {}).get("forma_pagamento") or "").strip().lower()
                if fp_prefill not in ("pix", "dinheiro", "cartao"):
                    fp_prefill = ""
                handled = await _handle_definir_endereco(
                    phone_id=phone_id,
                    cliente_zap=cliente_zap,
                    texto_completo=str(texto_completo or ""),
                    endereco_param=str((params or {}).get("endereco_txt") or "").strip() or None,
                    bairro_param=str((params or {}).get("bairro") or "").strip() or None,
                    bairros_dict=bairros_dict or {},
                    lista_bairros_txt=lista_bairros_txt,
                    pedido_ativo=pedido_ativo,
                    restaurante_db_id=int(restaurante_db_id) if restaurante_db_id else None,
                    dados_parciais=(dados_parciais or {}),
                    forma_pagamento=fp_prefill or None,
                    now_iso=now_iso,
                )
                if handled:
                    return

            # 4) Definir pagamento em qualquer etapa: força a cair no bloco de pagamento
            if intencao == "definir_pagamento":
                # Se a pessoa ainda não definiu entrega/retirada e nem endereço, pede isso antes.
                tipo_entrega_raw = str((dados_parciais or {}).get("tipo_entrega") or "entrega").strip().lower()
                tem_endereco = bool((dados_parciais or {}).get("endereco_txt"))
                if tipo_entrega_raw != "retirada" and not tem_endereco:
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_ENDERECO", {}))
                    await enviar_zap_async(
                        phone_id,
                        cliente_zap,
                        "Perfeito. 🙂 Antes de escolher o pagamento, vai ser *entrega* ou *retirada*?\n"
                        "Se for entrega, me mande *endereço completo com bairro*.",
                    )
                    return

                # Ajusta estado local para reutilizar o bloco existente de AGUARDANDO_PAGAMENTO.
                try:
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", (dados_parciais or {})))
                except Exception:
                    pass
                estado_atual = "AGUARDANDO_PAGAMENTO"
                handled = await _handle_pagamento_flow(
                    phone_id=phone_id,
                    cliente_zap=cliente_zap,
                    restaurante_db_id=int(restaurante_db_id),
                    pedido_ativo=pedido_ativo,
                    dados_parciais=(dados_parciais or {}),
                    txt_norm=txt_norm,
                    texto_completo=str(texto_completo or ""),
                    now_iso=now_iso,
                )
                if handled:
                    return
                # Se não conseguiu resolver, cai no fluxo antigo.

            # 5) Intenções de carrinho: força o caminho da IA de carrinho e captura prefill.
            if intencao in ("adicionar_item", "remover_item", "fixar_item", "adicionar_observacao", "trocar_item"):
                try:
                    p_bairro = str((params or {}).get("bairro") or "").strip()
                    p_end = str((params or {}).get("endereco_txt") or "").strip()
                    p_pg = str((params or {}).get("forma_pagamento") or "").strip().lower()

                    prefill = {}
                    if _is_retirada_text(txt_norm):
                        prefill["tipo_entrega"] = "retirada"
                    if p_bairro:
                        prefill["bairro"] = p_bairro
                    if p_end:
                        prefill["endereco_txt"] = p_end
                    if p_pg in ("pix", "dinheiro", "cartao"):
                        prefill["forma_pagamento"] = p_pg

                    if prefill:
                        intent_router_prefill = prefill
                except Exception:
                    pass

                estado_atual = "FORCAR_IA"

            # Se o classificador indicar que falta algo, pergunta e mantém fluxo antigo.
            if intencao in ("perguntar", "outro") and followup and not has_slot_items:
                await enviar_zap_async(phone_id, cliente_zap, followup)
                return





    if estado_atual == "AGUARDANDO_AVALIACAO_POS_VENDA":
        try:
            nota = int(re.sub(r"\D", "", texto_completo))
            if 1 <= nota <= 5:
                pedido_id = dados_parciais.get("pedido_id_avaliacao")
                if pedido_id:
                    await sb_exec(lambda: supabase.table("pedidos").update({"avaliacao": nota}).eq("id", pedido_id).execute())

                if nota == 5:
                    msg = "Uau! 😍 Muito obrigado! Sua avaliação ajuda muito!"
                elif nota >= 4:
                    msg = "Obrigado! Fico feliz que tenha gostado! 👍"
                else:
                    msg = "Poxa, obrigado pelo feedback. Vamos melhorar! 🙏"

                await enviar_zap_async(phone_id, cliente_zap, msg)
                await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO"))
            else:
                await enviar_zap_async(phone_id, cliente_zap, "Por favor, digite apenas uma nota de 1 a 5. ⭐")
        except Exception:
            await enviar_zap_async(phone_id, cliente_zap, "Não entendi a nota. Poderia digitar um número de 1 a 5?")
        return


    if estado_atual == "CONFIRMAR_PEDIDO_DE_SEMPRE":
        t = (txt_norm or "").strip()
        pedido_id = int((dados_parciais or {}).get("pedido_id_repetir") or 0)

        if t in ("1", "sim", "s", "yes"):
            ok, msg = await _run_blocking(lambda: _repeat_order_from_finalizado(restaurante_db_id, cliente_zap, pedido_id), timeout=SUPABASE_TIMEOUT_SECONDS)
            await enviar_zap_async(phone_id, cliente_zap, msg)
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
            return

        if t in ("2", "nao", "não", "n"):
            await enviar_zap_async(phone_id, cliente_zap, "Beleza! Me diga o que você gostaria de pedir hoje. 😊")
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
            return

        await enviar_zap_async(phone_id, cliente_zap, "Responda 1 (sim) para repetir ou 2 (não) para fazer outro pedido.")
        return

# --- CORREÇÃO: Autocorreção de estado (evita loop "Beleza") ---
    if estado_atual == "AGUARDANDO_MAIS_ALGO":
        if not pedido_ativo or not _safe_dict((pedido_ativo or {}).get("carrinho_json")):
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
            estado_atual = "INICIO"
# -------------------------------------------------------------


    if estado_atual == "AGUARDANDO_MAIS_ALGO":
        t = (txt_norm or "").strip()

        # Se estamos aguardando escolha de tamanho (ex.: Coca 1L/2L) e o cliente respondeu apenas o tamanho,
        # reconstrói o texto completo e reprocessa como pedido de item.
        pending_size = (dados_parciais or {}).get("pending_size") if isinstance(dados_parciais, dict) else None
        if isinstance(pending_size, dict) and (pending_size.get("base") or pending_size.get("options")):
            def _normalize_size_text(raw: str) -> str:
                s = normalizar_texto(raw or "")
                s = re.sub(r"\bum\s+litro\b", "1 litro", s)
                s = re.sub(r"\buma\s+litro\b", "1 litro", s)
                s = re.sub(r"\bmeio\s+litro\b", "0.5 litro", s)
                return s

            def _extract_size_token(raw_norm: str) -> str | None:
                m = re.search(r"\b(\d+(?:[\.,]\d+)?)\s*(l|ml)\b", raw_norm)
                if m:
                    num = (m.group(1) or "").replace(",", ".")
                    unit = m.group(2) or ""
                    return f"{num}{unit}"
                for k in ("pequena", "media", "média", "grande", "gigante", "familia", "família", "brotinho"):
                    if k in raw_norm:
                        return k
                return None

            def _strip_size_only(raw_norm: str) -> str:
                s = re.sub(r"\b\d+(?:[\.,]\d+)?\s*(l|ml)\b", " ", raw_norm)
                s = re.sub(r"\b(pequena|media|média|grande|gigante|familia|família|brotinho)\b", " ", s)
                s = re.sub(r"\b(de|da|do|pra|para|o|a|e|eh|é|um|uma)\b", " ", s)
                s = re.sub(r"\s+", " ", s).strip()
                return s

            t_norm = _normalize_size_text(texto_completo)
            size_token = _extract_size_token(t_norm)
            only_size = (not _strip_size_only(t_norm)) and bool(size_token)
            if only_size:
                opts = pending_size.get("options") if isinstance(pending_size.get("options"), list) else []
                base = (pending_size.get("base") or "").strip()
                chosen = ""
                if size_token and opts:
                    for opt in opts:
                        opt_norm = normalizar_texto(opt)
                        if size_token in opt_norm.replace(" ", "") or size_token in opt_norm:
                            chosen = opt
                            break
                if not chosen and opts and size_token:
                    for opt in opts:
                        opt_norm = normalizar_texto(opt)
                        if any(k in opt_norm and k in t_norm for k in ("pequena", "media", "média", "grande", "gigante", "familia", "família", "brotinho")):
                            chosen = opt
                            break
                if not chosen:
                    chosen = f"{base} {texto_completo}".strip() if base else texto_completo

                try:
                    merged = dict(dados_parciais or {})
                    merged.pop("pending_size", None)
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", merged))
                except Exception:
                    pass

                return await processar_mensagem_final(phone_id, cliente_zap, nome_cliente, chosen)

            # Se o cliente respondeu o tamanho junto com outras infos (bairro/endereço/etc),
            # injeta o item escolhido e reprocessa mantendo o resto da mensagem.
            if size_token:
                opts = pending_size.get("options") if isinstance(pending_size.get("options"), list) else []
                base = (pending_size.get("base") or "").strip()
                chosen = ""
                if size_token and opts:
                    for opt in opts:
                        opt_norm = normalizar_texto(opt)
                        if size_token in opt_norm.replace(" ", "") or size_token in opt_norm:
                            chosen = opt
                            break
                if not chosen and opts and size_token:
                    for opt in opts:
                        opt_norm = normalizar_texto(opt)
                        if any(k in opt_norm and k in t_norm for k in ("pequena", "media", "média", "grande", "gigante", "familia", "família", "brotinho")):
                            chosen = opt
                            break
                if not chosen:
                    chosen = f"{base} {texto_completo}".strip() if base else texto_completo

                try:
                    merged = dict(dados_parciais or {})
                    merged.pop("pending_size", None)
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", merged))
                except Exception:
                    pass

                combined = str(texto_completo or "").strip()
                if chosen and normalizar_texto(chosen) not in normalizar_texto(combined):
                    combined = f"{chosen}. {combined}" if combined else chosen
                return await processar_mensagem_final(phone_id, cliente_zap, nome_cliente, combined)

        # Se estamos aguardando escolha de borda, tenta resolver antes do fluxo principal.
        pending_borda = (dados_parciais or {}).get("pending_borda") if isinstance(dados_parciais, dict) else None
        if isinstance(pending_borda, dict) and pending_borda.get("options"):
            opts = pending_borda.get("options") if isinstance(pending_borda.get("options"), list) else []
            base = (pending_borda.get("base") or "").strip()
            t_norm_local = normalizar_texto(texto_completo)

            if "sem borda" in t_norm_local:
                try:
                    merged = dict(dados_parciais or {})
                    merged.pop("pending_borda", None)
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", merged))
                except Exception:
                    pass
                if base:
                    return await processar_mensagem_final(phone_id, cliente_zap, nome_cliente, base)

            def _pick_borda_from_pending(raw_norm: str, options: list[str]) -> str | None:
                if not raw_norm or not options:
                    return None
                for opt in options:
                    opt_norm = normalizar_texto(opt)
                    if opt_norm and opt_norm in raw_norm:
                        return opt
                tokens = [t for t in raw_norm.split() if len(t) >= 3 and t not in ("borda", "com")]
                for opt in options:
                    opt_norm = normalizar_texto(opt)
                    if any(tok in opt_norm for tok in tokens):
                        return opt
                m = difflib.get_close_matches(raw_norm, [normalizar_texto(o) for o in options], n=1, cutoff=0.6)
                if m:
                    for opt in options:
                        if normalizar_texto(opt) == m[0]:
                            return opt
                return None

            chosen = _pick_borda_from_pending(t_norm_local, opts)
            if chosen:
                try:
                    merged = dict(dados_parciais or {})
                    merged.pop("pending_borda", None)
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", merged))
                except Exception:
                    pass
                if base:
                    combined = f"{base} com borda {chosen}".strip()
                    return await processar_mensagem_final(phone_id, cliente_zap, nome_cliente, combined)

        # Se o carrinho sumiu, volta pro início
        if not pedido_ativo or not _safe_dict(pedido_ativo.get("carrinho_json")):
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
            await enviar_zap_async(phone_id, cliente_zap, "Beleza! Me diga o que você gostaria de pedir. 🙂")
            return


        negativos = {"nao", "não", "n", "nao quero", "não quero", "so isso", "só isso", "so isso mesmo", "só isso mesmo"}
        positivos = {"sim", "s", "quero", "quero sim", "mais", "mais um", "mais uma", "ainda"}

        def _tem_palavra(chaves: tuple[str, ...]) -> bool:
            return any(k in t for k in chaves)

        def _eh_confirmacao_sim() -> bool:
            # aceita variações: "sim", "sim pode", "quero sim", "bora"
            if t in ("sim", "s"):
                return True
            return _tem_palavra(("quero sim", "pode", "bora", "vamos", "continuar", "mais")) and ("nao" not in t and "não" not in t)

        def _eh_finalizar() -> bool:
            # aceita: "finalizar", "pode finalizar", "fechar pedido", "encerrar", "pode fechar"
            if t in negativos:
                return True
            if _tem_palavra(("finaliz", "fech", "encerr", "pode finalizar", "pode fechar", "fechar pedido", "pode encerrar")):
                return True
            return False

        def _parece_pedido_de_item() -> bool:
            # Se o cliente manda algo que parece um pedido (quantidade + produto), não faça a pergunta sim/não de novo.
            # Deixa o fluxo seguir para a IA interpretar e adicionar/remover.
            if not t:
                return False
            # NÃO tratar endereço como pedido de item (ex.: "rua X 1031")
            if _texto_parece_endereco(texto_completo, t):
                return False
            # NÃO tratar retirada/checkout como pedido de item
            if any(k in t for k in ("retirada", "vou buscar", "vou pegar", "buscar ai", "buscar aí", "pegar no local")):
                return False
            # números / "2x" / "x2" / etc (evita cair aqui só por ter número de casa)
            if re.search(r"\b\d+\s*x\b", t) or re.search(r"\bx\s*\d+\b", t):
                return True
            if _tem_palavra(("quero", "queria", "manda", "adiciona", "adicionar", "coloca", "por ", "pra ", "mais")):
                return True

            # Match fraco com itens do cardápio (usa as chaves normalizadas do precos_dict)
            tabela_precos = dados_loja.get("precos_dict", {}) or {}
            if tabela_precos:
                nomes = []
                for k in tabela_precos.keys():
                    nk = normalizar_texto(k)
                    if nk:
                        nomes.append(nk)
                nomes = list(dict.fromkeys(nomes))
                if nomes:
                    m = difflib.get_close_matches(normalizar_texto(texto_completo), nomes, n=1, cutoff=0.62)
                    if m:
                        return True
            return False

        # Atalho: se o cliente já mandou endereço/bairro (com ou sem "finalizar"), tenta avançar direto
        # Evita disparar esse atalho quando a mensagem parece um pedido de item.
        try:
            has_checkout_kw = any(k in t for k in ("finaliz", "fech", "encerr", "pode fechar", "pode finalizar"))
            has_addr = _texto_parece_endereco(texto_completo, t) or ("bairro" in t)
            if (has_addr or has_checkout_kw) and (not _parece_pedido_de_item()):
                fp_prefill = str((dados_parciais or {}).get("forma_pagamento") or "").strip().lower()
                if fp_prefill not in ("pix", "dinheiro", "cartao"):
                    fp_prefill = ""
                handled = await _handle_definir_endereco(
                    phone_id=phone_id,
                    cliente_zap=cliente_zap,
                    texto_completo=str(texto_completo or ""),
                    endereco_param=None,
                    bairro_param=None,
                    bairros_dict=bairros_dict or {},
                    lista_bairros_txt=lista_bairros_txt,
                    pedido_ativo=pedido_ativo,
                    restaurante_db_id=int(restaurante_db_id) if restaurante_db_id else None,
                    dados_parciais=(dados_parciais or {}),
                    forma_pagamento=fp_prefill or None,
                    now_iso=now_iso,
                )
                if handled:
                    return
        except Exception:
            pass

        # 1) Cliente quer finalizar (ou respondeu "não")
        if _eh_finalizar():
            # Se a mensagem já trouxe endereço/bairro, tenta avançar direto
            try:
                fp_prefill = str((dados_parciais or {}).get("forma_pagamento") or "").strip().lower()
                if fp_prefill not in ("pix", "dinheiro", "cartao"):
                    fp_prefill = ""
                handled = await _handle_definir_endereco(
                    phone_id=phone_id,
                    cliente_zap=cliente_zap,
                    texto_completo=str(texto_completo or ""),
                    endereco_param=None,
                    bairro_param=None,
                    bairros_dict=bairros_dict or {},
                    lista_bairros_txt=lista_bairros_txt,
                    pedido_ativo=pedido_ativo,
                    restaurante_db_id=int(restaurante_db_id) if restaurante_db_id else None,
                    dados_parciais=(dados_parciais or {}),
                    forma_pagamento=fp_prefill or None,
                    now_iso=now_iso,
                )
                if handled:
                    return
            except Exception:
                pass
            # Não continua escolhendo itens: vai pro checkout
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_ENDERECO", {}))

            resumo = pedido_ativo.get("resumo_pedido", "Carrinho vazio")
            try:
                total = float(pedido_ativo.get("total_valor") or 0.0)
            except Exception:
                total = 0.0

            msg = (
                "Perfeito! Vou fechar seu pedido. ✅\n\n"
                f"📝 *Resumo:*\n{str(resumo).replace('|', '\\n')}\n"
                f"💰 Subtotal: R$ {total:.2f}\n\n"
                "🚚 Vai ser *entrega* ou *retirada*?\n"
                "- Se for *entrega*: digite o endereço com bairro\n"
                "- Se for *retirada*: responda *retirada*"
            )
            await enviar_zap_async(phone_id, cliente_zap, msg)
            try:
                await sb_exec(lambda: supabase.table("conversas").insert({
                    "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
                }).execute())
            except Exception:
                pass
            return

        def _texto_indica_retirada() -> bool:
            t0 = (txt_norm or "").strip()
            if not t0:
                return False
            # Frases comuns de retirada
            if any(k in t0 for k in (
                "retirada", "retirar", "vou buscar", "vou pega", "vou pegar", "buscar ai", "buscar aí",
                "nao vai ser para entrega", "não vai ser para entrega", "nao vai ser entrega", "não vai ser entrega",
                "sem entrega", "nao entrega", "não entrega",
            )):
                return True
            return False

        # 1b) Cliente avisou que não é entrega (retirada) enquanto ainda está montando
        # -> guarda e já inicia checkout sem pedir endereço.
        if _texto_indica_retirada():
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", {"tipo_entrega": "retirada", "taxa": 0.0}))
            total_prod = float(pedido_ativo["total_valor"]) if pedido_ativo else 0.0
            msg = (
                "Beleza! ✅ Vai ser *retirada no local*.\n\n"
                f"💰 Total: R$ {total_prod:.2f}\n\n"
                "Qual a forma de pagamento? *(Pix, Dinheiro ou Cartão)*"
            )
            await enviar_zap_async(phone_id, cliente_zap, msg)
            try:
                await sb_exec(lambda: supabase.table("conversas").insert({
                    "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
                }).execute())
            except Exception:
                pass
            return

        def _parece_pergunta_menu() -> bool:
            t0 = (txt_norm or "").strip()
            if not t0:
                return False
            if "?" in (texto_completo or "") and any(k in t0 for k in (
                "pizza", "piza", "burger", "hamb", "lanche", "x ", "cardapio", "cardápio", "menu",
                "ingrediente", "ingredientes", "vem com", "tem ",
            )):
                return True
            if any(k in t0 for k in (
                "oq tem", "o que tem", "o que vem", "oq vem", "vem com", "ingrediente", "ingredientes",
                "tem o que", "pode me informar", "consegue me informar", "me informa",
            )):
                return True
            return False

        # 2) Cliente confirmou que quer continuar
        if _eh_confirmacao_sim() or t in positivos:
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
            await enviar_zap_async(phone_id, cliente_zap, "Show! O que mais você gostaria de pedir? 🙂")
            return

        async def _try_handle_endereco_ou_bairro_no_carrinho() -> bool:
            if not bairros_dict:
                return False

            bairro_match = encontrar_melhor_match(texto_completo, list(bairros_dict.keys())) if bairros_dict else None
            if not bairro_match:
                return False

            try:
                taxa = float(bairros_dict[bairro_match])
            except Exception:
                taxa = 0.0

            # Cliente mandou só o bairro (ou algo como "o endereço é mondubim")
            so_bairro = _texto_e_so_bairro(txt_norm, bairro_match)
            parece_endereco = _texto_parece_endereco(texto_completo, txt_norm)
            tem_numero = bool(re.search(r"\b\d+\b", texto_completo))

            if so_bairro or (("endereco" in t or "endereço" in t) and (not parece_endereco) and (not tem_numero)):
                await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_ENDERECO", {"bairro": bairro_match, "taxa": taxa}))
                msg = (
                    f"📍 Bairro: *{str(bairro_match).title()}* (Taxa: R$ {taxa:.2f}).\n\n"
                    "Agora me envie o *endereço completo*: *rua/avenida + número*"
                    " (e complemento, se tiver).\n\n"
                    "Ex.: Rua das Flores, 123 — Mondubim"
                )
                await enviar_zap_async(phone_id, cliente_zap, msg)
                try:
                    await sb_exec(lambda: supabase.table("conversas").insert({
                        "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
                    }).execute())
                except Exception:
                    pass
                return True

            # Cliente já mandou endereço completo com bairro
            if parece_endereco:
                await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", {"endereco_txt": texto_completo, "bairro": bairro_match, "taxa": taxa}))

                total_prod = float(pedido_ativo["total_valor"]) if pedido_ativo else 0.0
                total_com_taxa = total_prod + taxa

                msg = (
                    f"📍 Identifiquei: *{str(bairro_match).title()}* (Taxa: R$ {taxa:.2f}).\n"
                    f"💰 *Total Final: R$ {total_com_taxa:.2f}*\n\n"
                    "Qual a forma de pagamento? (Pix, Dinheiro ou Cartão)"
                )
                await enviar_zap_async(phone_id, cliente_zap, msg)
                try:
                    await sb_exec(lambda: supabase.table("conversas").insert({
                        "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
                    }).execute())
                except Exception:
                    pass
                return True

            return False

        # 2b) Cliente mandou bairro/endereço enquanto ainda está no carrinho
        if await _try_handle_endereco_ou_bairro_no_carrinho():
            return

        # 3) Cliente já enviou um pedido de item (ex.: "quero 3 coca colas")
        # => deixa seguir para a IA tratar (sem ficar perguntando sim/não)
        if _parece_pedido_de_item():
            pass
        # 3b) Cliente fez pergunta (ingredientes/cardápio/etc)
        # => deixa seguir para a IA responder, sem travar no "adicionar/finalizar".
        elif _parece_pergunta_menu():
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
            pass
        else:
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                "Você quer *adicionar mais itens* ou *finalizar*?\n\n"
                "- Para adicionar: mande o item (ex.: *\"2 coca\"*, *\"1 pizza calabresa\"*)\n"
                "- Para finalizar: diga *finalizar* (ou mande endereço com bairro / ou diga *retirada*)",
            )
            return






    # ===== Ouvidoria Total: dúvidas no meio do checkout (não bloquear) =====
    if estado_atual in ("AGUARDANDO_ENDERECO", "AGUARDANDO_PAGAMENTO"):
        t = (txt_norm or "").strip()

        def _checkout_reminder() -> str:
            dp = dados_parciais if isinstance(dados_parciais, dict) else {}
            tipo = str((dp or {}).get("tipo_entrega") or "").strip().lower()
            if tipo not in ("entrega", "retirada"):
                tipo = ""

            forma = str((dp or {}).get("forma_pagamento") or "").strip().lower()
            if forma not in ("pix", "dinheiro", "cartao"):
                forma = ""

            missing = []
            if tipo == "retirada":
                if estado_atual == "AGUARDANDO_PAGAMENTO" and not forma:
                    missing.append("pagamento")
            else:
                end = str((dp or {}).get("endereco_txt") or "").strip()
                bairro = str((dp or {}).get("bairro") or "").strip()
                if not end:
                    missing.append("endereço")
                if not bairro:
                    missing.append("bairro")
                if estado_atual == "AGUARDANDO_PAGAMENTO" and not forma:
                    missing.append("pagamento")

            if missing:
                return "\n\nFaltam apenas: *" + "*, *".join(missing) + "*."

            # fallback (casos raros)
            if estado_atual == "AGUARDANDO_PAGAMENTO":
                return "\n\nPra eu finalizar: qual a forma de pagamento? *(Pix, Dinheiro ou Cartão)*"
            return "\n\nPra eu finalizar: me mande o *endereço completo com bairro* (ou diga *retirada*)."

        def _clean_query_for_menu_lookup(raw: str) -> str:
            q = normalizar_texto(raw or "")
            q = re.sub(r"[^a-z0-9\s]", " ", q)
            q = re.sub(r"\b(quanto|custa|custam|valor|preco|preço|o|a|os|as|de|da|do|por|pra|para|no|na|em|tem|vem|vai|com|qual|quais|oq|o\s+que)\b", " ", q)
            q = re.sub(r"\s+", " ", q).strip()
            return q

        def _menu_match_best(query: str, precos_dict: dict) -> tuple[str | None, list[str]]:
            if not query or not isinstance(precos_dict, dict) or not precos_dict:
                return None, []

            keys = [k for k in precos_dict.keys() if isinstance(k, str) and k.strip()]
            if not keys:
                return None, []

            q = normalizar_texto(query)
            if q in keys:
                return q, [q]

            # 1) close match
            close = difflib.get_close_matches(q, keys, n=5, cutoff=0.55)

            # 2) token overlap scorer (melhor para frases longas)
            tq = {p for p in re.sub(r"[^a-z0-9\s]", " ", q).split() if len(p) >= 3}
            scored = []
            if tq:
                for k0 in keys:
                    nk = normalizar_texto(k0)
                    tk = {p for p in re.sub(r"[^a-z0-9\s]", " ", nk).split() if len(p) >= 3}
                    if not tk:
                        continue
                    inter = len(tq & tk)
                    if inter <= 0:
                        continue
                    ratio = difflib.SequenceMatcher(None, q, nk).ratio()
                    scored.append(((inter + (0.5 * ratio)), k0))
                scored.sort(key=lambda x: x[0], reverse=True)

            # merge suggestions, keep order, pick best
            suggestions = []
            for k0 in (close + [k for _, k in scored[:5]]):
                if k0 and k0 not in suggestions:
                    suggestions.append(k0)
            best = suggestions[0] if suggestions else None
            return best, suggestions[:5]

        def _find_cardapio_line(cardapio_txt: str, best_key: str | None, query: str) -> str | None:
            txt = str(cardapio_txt or "")
            if not txt.strip():
                return None
            lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
            if not lines:
                return None

            q = normalizar_texto(query)
            tq = {p for p in re.sub(r"[^a-z0-9\s]", " ", q).split() if len(p) >= 3}
            bk = normalizar_texto(best_key or "")

            # tenta achar linha que menciona diretamente o item
            for ln in lines:
                nln = normalizar_texto(ln)
                if bk and bk in nln:
                    return ln

            # fallback: match por tokens
            if tq:
                best_ln = None
                best_score = 0
                for ln in lines:
                    nln = normalizar_texto(ln)
                    tl = {p for p in re.sub(r"[^a-z0-9\s]", " ", nln).split() if len(p) >= 3}
                    inter = len(tq & tl)
                    if inter > best_score:
                        best_score = inter
                        best_ln = ln
                if best_ln and best_score >= 2:
                    return best_ln
            return None

        # Resumo do pedido durante checkout (evita cair em FAQ de ingredientes)
        palavras_gatilho = [
            "pedi", "pedido", "carrinho", "resumo", "lista", "conta", "total", "comprado",
            "meu pedido", "meu carrinho", "resumo do pedido", "o que eu pedi", "o que pedi", "o que eu pedi até agora", "o que pedi até agora",
            "mostrar pedido", "mostrar carrinho", "ver pedido", "ver carrinho", "quais itens", "quais produtos", "quais coisas", "o que tem no meu pedido", "o que tem no carrinho"
        ]
        if pedido_ativo and (any(p in t for p in palavras_gatilho) or re.search(r"(o que.*pedi|resumo.*pedido|meu.*pedido|meu.*carrinho|mostrar.*pedido|mostrar.*carrinho|ver.*pedido|ver.*carrinho|quais.*itens|quais.*produtos|o que tem.*pedido|o que tem.*carrinho)", t)):
            resumo_txt = str(pedido_ativo.get("resumo_pedido", "") or "").replace("|", "\n")
            try:
                total_txt = float(pedido_ativo.get("total_valor") or 0.0)
            except Exception:
                total_txt = 0.0
            msg = f"🛒 Seu Carrinho:\n{resumo_txt}\n💰 Total: R$ {total_txt:.2f}" + _checkout_reminder()
            await enviar_zap_async(phone_id, cliente_zap, msg)
            return

        faq_msgs: list[str] = []

        # Pergunta sobre entrega/taxa/bairro
        pergunta_entrega = any(k in t for k in ("entrega", "entregam", "taxa", "frete", "delivery")) and (
            "?" in (texto_completo or "") or "para " in t or "pra " in t or "pro " in t
        )
        if pergunta_entrega:
            if not bairros_dict:
                faq_msgs.append("No momento estou sem as taxas de entrega carregadas. Me diga seu bairro que eu confirmo com o restaurante.")
            else:
                bairro_match = encontrar_melhor_match(texto_completo, list(bairros_dict.keys()))
                if bairro_match:
                    try:
                        taxa = float(bairros_dict[bairro_match])
                    except Exception:
                        taxa = 0.0
                    faq_msgs.append(f"📍 Entregamos em *{str(bairro_match).title()}* — Taxa: *R$ {taxa:.2f}*.")
                else:
                    faq_msgs.append("Qual é o bairro? Assim eu confirmo a taxa certinho.")

        # Pergunta sobre formas de pagamento
        pergunta_pagamento = (
            ("pagamento" in t or "forma de pagamento" in t or "aceita" in t or "aceitam" in t)
            and any(k in t for k in ("pix", "dinheiro", "cartao", "cartão"))
        )
        if pergunta_pagamento:
            pix_enabled = False
            try:
                pix_settings = await sb_exec(lambda: get_pix_settings_for_restaurante(int(restaurante_db_id)))
                pix_enabled = bool(pix_settings and pix_settings.get("enabled"))
            except Exception:
                pix_enabled = False
            if pix_enabled:
                faq_msgs.append("Aceitamos *Pix*, *Dinheiro* e *Cartão*.")
            else:
                faq_msgs.append("Aceitamos *Pix na entrega*, *Dinheiro* e *Cartão*.")

        # Cardápio/menu
        if any(k in t for k in ("cardapio", "cardápio", "menu")):
            cardapio_txt = str((dados_loja or {}).get("cardapio") or "").strip()
            faq_msgs.append(cardapio_txt if cardapio_txt else "No momento estou sem cardápio carregado.")

        # Pergunta de ingredientes/"vem com" (não tentar interpretar como endereço/pagamento)
        parece_pergunta_ingredientes = (
            ("?" in (texto_completo or ""))
            or t.startswith(("vem ", "tem ", "vai ", "pode ", "o que vem", "oq vem", "oq tem", "o que tem"))
            or any(k in t for k in ("ingrediente", "ingredientes", "vem com", "vai com"))
        )
        if parece_pergunta_ingredientes and (not any(k in t for k in ("rua", "avenida", "av ", "bairro", "cep", "pix", "dinheiro", "cartao", "cartão"))):
            tabela_precos = (dados_loja or {}).get("precos_dict", {}) or {}
            q = _clean_query_for_menu_lookup(texto_completo)
            best, sugg = _menu_match_best(q, tabela_precos)

            cardapio_txt = str((dados_loja or {}).get("cardapio") or "")
            ln = _find_cardapio_line(cardapio_txt, best, q)

            if best and ln:
                faq_msgs.append(f"📌 Sobre *{str(best).title()}*:\n{ln}")
            elif best and not ln:
                faq_msgs.append(f"Consigo te ajudar nisso. 🙂 No meu cadastro eu não tenho a descrição completa de *{str(best).title()}* agora.")
            elif sugg:
                faq_msgs.append("Sobre qual item você quer saber? Você quis dizer:\n- " + "\n- ".join([str(x).title() for x in sugg[:4]]))
            else:
                faq_msgs.append("Posso te ajudar nisso. 🙂 Me diga o *nome do item* (ex.: *'o que vem no X Burger?'*).")

        # Pergunta de preço: "quanto custa ..."
        parece_pergunta_preco = (
            ("quanto" in t or "valor" in t or "custa" in t)
            and ("?" in (texto_completo or "") or t.startswith(("quanto", "qual", "quais", "oq", "o que")))
        )
        if parece_pergunta_preco:
            tabela_precos = (dados_loja or {}).get("precos_dict", {}) or {}
            q = _clean_query_for_menu_lookup(texto_completo)
            best, sugg = _menu_match_best(q, tabela_precos)

            if best:
                try:
                    preco = float(tabela_precos.get(best) or 0.0)
                except Exception:
                    preco = 0.0
                if preco > 0:
                    faq_msgs.append(f"💰 *{str(best).title()}* custa *R$ {preco:.2f}*.")

            if sugg:
                faq_msgs.append("Qual item você quer saber o preço? Você quis dizer:\n- " + "\n- ".join([str(x).title() for x in sugg[:4]]))
            else:
                faq_msgs.append("Qual item você quer saber o preço? Me diga o nome certinho (ex.: *coca 2l*, *pizza calabresa*).")

        if faq_msgs:
            faq_msgs[-1] = faq_msgs[-1] + _checkout_reminder()
            for msg_faq in faq_msgs:
                await enviar_zap_async(phone_id, cliente_zap, msg_faq)
            return


    if estado_atual == "AGUARDANDO_ENDERECO":
        def _looks_like_item_request(raw_txt: str, norm_txt: str) -> bool:
            t = (norm_txt or "").strip()
            if not t:
                return False
            if re.search(r"\b\d+\s*x\b", t) or re.search(r"\bx\s*\d+\b", t):
                return True
            if any(k in t for k in ("quero", "queria", "manda", "adicion", "coloca", "mais", "coca", "refri", "refriger", "pizza", "piza", "hamb", "burger")):
                return True
            tabela_precos = (dados_loja or {}).get("precos_dict", {}) or {}
            if isinstance(tabela_precos, dict) and tabela_precos:
                for k in tabela_precos.keys():
                    nk = normalizar_texto(str(k or ""))
                    if nk and nk in t:
                        return True
            return False

        # Se o cliente pede item enquanto aguardamos endereço, prioriza adicionar item e continua o fluxo.
        if _looks_like_item_request(str(texto_completo or ""), str(txt_norm or "")):
            estado_atual = "FORCAR_IA"
        else:
        # Retirada: cliente pode responder isso no checkout.
            t_end = (txt_norm or "").strip()
        if any(k in t_end for k in (
            "retirada", "retirar", "vou buscar", "vou pega", "vou pegar",
            "nao vai ser para entrega", "não vai ser para entrega", "nao vai ser entrega", "não vai ser entrega",
            "sem entrega", "nao entrega", "não entrega",
        )):
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", {"tipo_entrega": "retirada", "taxa": 0.0}))

            total_prod = float(pedido_ativo["total_valor"]) if pedido_ativo else 0.0
            msg = (
                "Perfeito! ✅ Vai ser *retirada no local*.\n\n"
                f"💰 Total: R$ {total_prod:.2f}\n\n"
                "Qual a forma de pagamento? (Pix, Dinheiro ou Cartão)"
            )
            await enviar_zap_async(phone_id, cliente_zap, msg)
            try:
                await sb_exec(lambda: supabase.table("conversas").insert({
                    "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
                }).execute())
            except Exception:
                pass
            return

        prev_bairro = str((dados_parciais or {}).get("bairro") or "").strip()
        prev_taxa = (dados_parciais or {}).get("taxa")

        bairro_match = _match_bairro_from_input(
            _extract_bairro_from_text(texto_completo) or texto_completo,
            bairros_dict,
        ) if bairros_dict else None

        # Caso especial: cliente manda o endereço sem bairro, mas o bairro já foi informado antes.
        if (not bairro_match) and prev_bairro and _texto_parece_endereco(texto_completo, txt_norm):
            bairro_match = prev_bairro
            try:
                taxa = float(prev_taxa) if prev_taxa is not None else float(bairros_dict.get(prev_bairro) or 0.0)
            except Exception:
                taxa = 0.0

            novos_dados = {"endereco_txt": texto_completo, "bairro": bairro_match, "taxa": taxa}
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", novos_dados))

            total_prod = float(pedido_ativo["total_valor"]) if pedido_ativo else 0.0
            total_com_taxa = total_prod + taxa

            msg = (
                f"📍 Identifiquei: *{str(bairro_match).title()}* (Taxa: R$ {taxa:.2f}).\n"
                f"💰 *Total Final: R$ {total_com_taxa:.2f}*\n\n"
                "Qual a forma de pagamento? (Pix, Dinheiro ou Cartão)"
            )
            await enviar_zap_async(phone_id, cliente_zap, msg)
            return

        endereco_prev = str((dados_parciais or {}).get("endereco_txt") or "").strip()
        endereco_param = None
        if _texto_parece_endereco(endereco_prev, normalizar_texto(endereco_prev)):
            endereco_param = endereco_prev
        elif _texto_parece_endereco(str(texto_completo or ""), txt_norm):
            endereco_param = extrair_endereco_de_texto(str(texto_completo or "")) or str(texto_completo or "").strip()

        fp_prev = str((dados_parciais or {}).get("forma_pagamento") or "").strip().lower()
        if fp_prev not in ("pix", "dinheiro", "cartao"):
            fp_prev = ""

        # Se já temos endereço + pagamento, e agora chegou o bairro, finaliza diretamente
        if endereco_param and bairro_match and fp_prev:
            try:
                taxa = float(bairros_dict.get(bairro_match) or 0.0)
            except Exception:
                taxa = 0.0
            dados_next = {
                "endereco_txt": endereco_param,
                "bairro": bairro_match,
                "taxa": taxa,
                "forma_pagamento": fp_prev,
            }
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_PAGAMENTO", dados_next))
            handled = await _handle_pagamento_flow(
                phone_id=phone_id,
                cliente_zap=cliente_zap,
                restaurante_db_id=int(restaurante_db_id),
                pedido_ativo=pedido_ativo,
                dados_parciais=dados_next,
                txt_norm=fp_prev,
                texto_completo=fp_prev,
                now_iso=datetime.now(timezone.utc).isoformat(),
            )
            if handled:
                return

        bairro_raw = None
        if not bairro_match and _texto_parece_bairro(str(texto_completo or ""), txt_norm):
            bairro_raw = _extract_bairro_from_text(str(texto_completo or "")) or str(texto_completo or "").strip()

        handled = await _handle_definir_endereco(
            phone_id=phone_id,
            cliente_zap=cliente_zap,
            texto_completo=str(texto_completo or ""),
            endereco_param=endereco_param,
            bairro_param=str(bairro_match) if bairro_match else (bairro_raw or None),
            bairros_dict=bairros_dict or {},
            lista_bairros_txt=lista_bairros_txt,
            pedido_ativo=pedido_ativo,
            restaurante_db_id=int(restaurante_db_id) if restaurante_db_id else None,
            dados_parciais=(dados_parciais or {}),
            forma_pagamento=fp_prev or None,
            now_iso=datetime.now(timezone.utc).isoformat(),
        )
        if handled:
            return

        await enviar_zap_async(
            phone_id,
            cliente_zap,
            "Qual é o *bairro*? Assim eu confirmo a taxa certinho.",
        )
        return

    if estado_atual == "AGUARDANDO_PAGAMENTO":
        if bool((dados_parciais or {}).get("aguardando_troco")):
            handled = await _handle_pagamento_flow(
                phone_id=phone_id,
                cliente_zap=cliente_zap,
                restaurante_db_id=int(restaurante_db_id),
                pedido_ativo=pedido_ativo,
                dados_parciais=(dados_parciais or {}),
                txt_norm=txt_norm,
                texto_completo=str(texto_completo or ""),
                now_iso=now_iso,
            )
            if handled:
                return

        t = (txt_norm or "").strip()
        has_payment_kw = any(k in t for k in (
            "pix", "dinheiro", "especie", "espécie", "cartao", "cartão", "credito", "crédito", "debito", "débito"
        ))

        if not has_payment_kw and (
            _texto_parece_bairro(str(texto_completo or ""), t)
            or _texto_parece_endereco(str(texto_completo or ""), t)
            or "bairro" in t
        ):
            raw = str(texto_completo or "").strip()
            bairro_hint = _extract_bairro_from_text(raw) or raw

            fp_prev = str((dados_parciais or {}).get("forma_pagamento") or "").strip().lower()
            if fp_prev not in ("pix", "dinheiro", "cartao"):
                fp_prev = ""

            handled_addr = await _handle_definir_endereco(
                phone_id=phone_id,
                cliente_zap=cliente_zap,
                texto_completo=str(texto_completo or ""),
                endereco_param=str((dados_parciais or {}).get("endereco_txt") or "").strip() or None,
                bairro_param=bairro_hint,
                bairros_dict=bairros_dict or {},
                lista_bairros_txt=lista_bairros_txt,
                pedido_ativo=pedido_ativo,
                restaurante_db_id=int(restaurante_db_id) if restaurante_db_id else None,
                dados_parciais=(dados_parciais or {}),
                forma_pagamento=fp_prev or None,
                now_iso=now_iso,
            )
            if handled_addr:
                return

        handled = await _handle_pagamento_flow(
            phone_id=phone_id,
            cliente_zap=cliente_zap,
            restaurante_db_id=int(restaurante_db_id),
            pedido_ativo=pedido_ativo,
            dados_parciais=(dados_parciais or {}),
            txt_norm=txt_norm,
            texto_completo=str(texto_completo or ""),
            now_iso=now_iso,
        )
        if handled:
            return

        # Fallback legado (mantido por segurança; deve ficar cada vez menos usado com o router).
        pgto_limpo = txt_norm
        forma_escolhida = None

        # Intercept "cliente caótico": pergunta fora do contexto de pagamento
        # Não muda o estado (continua aguardando pagamento), só responde e repete a pergunta de pagamento.
        t = (pgto_limpo or "").strip()

        parece_pergunta = (
            ("?" in texto_completo)
            or t.startswith(("vem ", "tem ", "tem ", "vai ", "pode "))
            or any(k in t for k in ("vem cebola", "tem cebola", "ingrediente", "ingredientes", "vem com", "vai com", "tem ", "cebola"))
        )

        # Se ele NÃO escolheu forma de pagamento e parece uma pergunta, responde sem dar o "não entendi".
        if parece_pergunta and not any(k in t for k in ("pix", "dinheiro", "especie", "espécie", "cartao", "cartão", "credito", "crédito", "debito", "débito")):
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                "Posso te ajudar nisso. 🙂\n"
                "Se você quiser tirar algum ingrediente, é só dizer por exemplo: *'sem cebola'*.\n\n"
                "Agora, pra eu finalizar: qual a forma de pagamento? *(Pix, Dinheiro ou Cartão)*"
            )
            return


        if "pix" in pgto_limpo:
            forma_escolhida = "Pix"
        elif "dinheiro" in pgto_limpo or "especie" in pgto_limpo or "espécie" in pgto_limpo:
            forma_escolhida = "Dinheiro"
        elif "cartao" in pgto_limpo or "cartão" in pgto_limpo or "credito" in pgto_limpo or "crédito" in pgto_limpo or "debito" in pgto_limpo or "débito" in pgto_limpo:
            forma_escolhida = "Cartão"

        if forma_escolhida:
            endereco_final = dados_parciais.get("endereco_txt", "Endereço não capturado")
            bairro_final = dados_parciais.get("bairro", "")
            tipo_entrega_raw = str((dados_parciais or {}).get("tipo_entrega") or "entrega").strip().lower()
            tipo_entrega_final = "retirada" if tipo_entrega_raw in ("retirada", "retirar", "buscar", "vou buscar") else "entrega"

            taxa_final = 0.0 if tipo_entrega_final == "retirada" else float(dados_parciais.get("taxa", 0.0) or 0.0)

            total_final = 0.0
            if pedido_ativo:
                total_final = float(pedido_ativo["total_valor"] or 0.0) + taxa_final


                pix_created = False
                pix_payload = None
                pix_settings = None

                if forma_escolhida == "Pix":
                    pix_settings = await sb_exec(lambda: get_pix_settings_for_restaurante(int(restaurante_db_id)))

                    # Se o restaurante não ativou Pix no painel, não aceita Pix como forma.
                    if not (pix_settings and pix_settings.get("enabled")):
                        await enviar_zap_async(
                            phone_id,
                            cliente_zap,
                            "No momento o Pix não está disponível neste restaurante.\n"
                            "Escolha outra forma de pagamento: *Dinheiro* ou *Cartão*.",
                        )
                        return


                update_base = {
                    "endereco_completo": (
                        "Retirada no local" if tipo_entrega_final == "retirada" else f"{endereco_final} ({bairro_final})"
                    ),
                    "tipo_entrega": tipo_entrega_final,
                    "forma_pagamento": forma_escolhida,
                    "total_valor": total_final,
                    "status": "confirmado",
                    "bot_finalizado": True,
                    "bot_finalizado_em": now_iso,
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
                    update_base["bot_finalizado"] = False
                    update_base["bot_finalizado_em"] = None

                try:
                    await sb_exec(lambda: supabase.table("pedidos").update(update_base).eq("id", pedido_ativo["id"]).execute())
                except Exception:
                    safe_base = dict(update_base)
                    safe_base.pop("bot_finalizado", None)
                    safe_base.pop("bot_finalizado_em", None)
                    await sb_exec(lambda: supabase.table("pedidos").update(safe_base).eq("id", pedido_ativo["id"]).execute())

                if (
                    forma_escolhida == "Pix"
                    and pix_settings
                    and pix_settings.get("enabled")
                    and (pix_settings.get("provider") or "mercadopago") == "mercadopago"
                    and pix_settings.get("mp_token")
                ):
                    try:
                        pix_payload = await _run_blocking(
                            lambda: mp_create_pix_payment(
                                pix_settings["mp_token"],
                                amount=total_final,
                                description=f"Pedido #{pedido_ativo['id']}",  # ✅ fecha a string corretamente
                                external_reference=str(pedido_ativo["id"]),
                                payer_email=_payer_email_from_cliente(cliente_zap),
                            ),
                            timeout=20,
                        )

                        payment_id = str(pix_payload.get("id") or "").strip()
                        payment_status = str(pix_payload.get("status") or "pending").strip().lower()

                        poi = (pix_payload.get("point_of_interaction") or {}) if isinstance(pix_payload, dict) else {}
                        tx = (poi.get("transaction_data") or {}) if isinstance(poi, dict) else {}

                        payment_qr_code = (tx.get("qr_code") or "") if isinstance(tx, dict) else ""
                        payment_ticket_url = (tx.get("ticket_url") or "") if isinstance(tx, dict) else ""

                        if payment_id:
                            await sb_exec(lambda: supabase.table("pedidos").update({
                                "payment_provider": "mercadopago",
                                "payment_id": payment_id,
                                "payment_status": payment_status,
                                "payment_amount": _money_2(total_final),
                                "payment_qr_code": payment_qr_code,
                                "payment_ticket_url": payment_ticket_url,
                            }).eq("id", pedido_ativo["id"]).execute())
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
                        "⏳ Aguarde o restaurante aceitar seu pedido."
                    )

                await enviar_zap_async(phone_id, cliente_zap, msg)
                await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
                try:
                    await sb_exec(lambda: supabase.table("conversas").insert({
                        "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
                    }).execute())
                except Exception:
                    pass
                return

            msg = (
                "✅ *Pedido Confirmado!*\n\n"
                f"📍 Entrega em: {endereco_final}\n"
                f"💳 Pagamento: {forma_escolhida}\n"
                f"💰 Total: R$ {total_final:.2f}\n\n"
                "⏳ Aguarde o restaurante aceitar seu pedido."
            )
            await enviar_zap_async(phone_id, cliente_zap, msg)
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO", {}))
            try:
                await sb_exec(lambda: supabase.table("conversas").insert({
                    "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
                }).execute())
            except Exception:
                pass
        else:
            await enviar_zap_async(phone_id, cliente_zap, "Não entendi a forma de pagamento. Aceitamos: Pix, Dinheiro ou Cartão.")
        return





    prompt_usuario_banco = dados_loja.get("system_prompt", "") or ""
    info_carrinho = f"CARRINHO ATUAL: {pedido_ativo['resumo_pedido']}" if pedido_ativo else "Carrinho Vazio"

    prompt_sistema = f"""
Você é um atendente virtual (Fase de Escolha de Itens).
Sua missão: Entender o que o cliente quer e extrair informações para montar o pedido, NÃO IMPORTA A ORDEM que ele fale.
Personalidade: {prompt_usuario_banco}

Cardápio OFICIAL: {dados_loja.get('cardapio', '')}
Status Carrinho: {info_carrinho}



TOM E ESTILO (humano, carismatico e leve):
- Responda de forma calorosa, amigavel e natural, como um atendente real.
- Use humor leve e ocasional quando o cliente brincar, sem ironia e sem ofender.
- Seja empatico e positivo; evite soar robotico.
- Mantenha respostas curtas e objetivas, sem perder a simpatia.

EXEMPLOS DE TOM (mantendo o objetivo do pedido):
- Cliente: "agiliza o pedido, se nao vou morrer de fome"
    Resposta: "Calma! Nao morre antes da pizza chegar kk 😄 Ja vou adiantar seu pedido aqui." 
- Cliente: "to com muita fome"
    Resposta: "Bora matar essa fome! Me diz o que voce quer hoje."

REGRAS CRÍTICAS DE INTELIGÊNCIA:
1. 🧠 CONTEXTO E MEMÓRIA: Sua prioridade é a última mensagem, mas VOCÊ DEVE consultar o histórico recente para entender correções.
2. 🚫 ANTI-DUPLICIDADE: Antes de adicionar um item, verifique no "Status Carrinho" se ele JÁ foi adicionado.
3. Use nomes o MAIS PRÓXIMO possível do Cardápio OFICIAL.
4. 💰 PREÇOS: Se pedir cardápio, mostre os preços (copie do oficial).
5. 📝 OBSERVAÇÕES: Ingredientes para retirar ou ponto da carne vão em "adicionar_observacao".

6. 🍕 REGRA DE MISTURA (IMPORTANTE):
- Se pedir meia/meio a meio, isso é UM ÚNICO ITEM.
- O nome deve conter "Meio": "Meio [Sabor A] e Meio [Sabor B]".
- Se houver observação só para UMA metade, escreva no campo "observacao" assim: "na [Sabor]: <obs>".
- Se houver observações nas DUAS metades: "na [Sabor A]: <obs>; na [Sabor B]: <obs>".

7. ❗ CONFIRMAÇÃO: só use "pedir_fechamento" se o cliente disser algo CLARO como "fechar pedido", "pode entregar", "finaliza".

8. ❓ PEDIDO VAGO: se não disser o sabor claramente, NÃO crie item. Intenção: "perguntar".

9. 🚫 PRODUTOS INEXISTENTES: se não estiver no Cardápio OFICIAL, NÃO crie item.

10. 🔢 QUANTIDADE: se não informar, assuma 1. Nunca assuma >1.

11. 🏠 ENDEREÇO: números em endereço (ex: 'Rua X, 1031') NÃO são quantidade de item.

12. 🔁 SUBSTITUICAO: se o cliente disser "se nao tiver X, manda Y", adicione SOMENTE X e use observacao: "se nao tiver, substituir por Y".

13. 🧀 BORDA RECHEADA: se o cliente pedir borda, trate como adicional da pizza (na observacao) e SOME ao preco; se nao disser o sabor, pergunte.



FORMATO JSON:
{{
  "intencao": "...",
  "mensagem": "...",
  "itens": [ {{"nome": "...", "qtd": 1, "observacao": "..."}} ]
}}
""".strip()

    try:
        historico = await sb_exec(
            lambda: (
                supabase.table("conversas")
                .select("role, mensagem")
                .eq("cliente_zap", cliente_zap)
                .eq("restaurante_id", phone_id)
                .order("created_at", desc=False)
                .limit(int(MAX_HISTORICO or 15))
                .execute()
            ),
            timeout=SUPABASE_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        historico = type("x", (), {"data": []})()
    except Exception:
        historico = type("x", (), {"data": []})()

    messages = [{"role": "system", "content": prompt_sistema}]
    for h in (historico.data or []):
        messages.append({"role": h["role"], "content": h["mensagem"]})
    messages.append({"role": "user", "content": texto_completo})

    try:
        # Slot Filling: se extraiu itens com confiança mínima, pula a IA "conversacional" e aplica direto.
        use_slot_items = False
        slot_add = []
        slot_rem = []
        if SLOT_FILLING_ENABLED and isinstance(slot_obj, dict):
            slot_add = slot_obj.get("itens_adicionar") or []
            slot_rem = slot_obj.get("itens_remover") or []
            if isinstance(slot_add, list) or isinstance(slot_rem, list):
                slot_add = slot_add if isinstance(slot_add, list) else []
                slot_rem = slot_rem if isinstance(slot_rem, list) else []
            # Override do fluxo conversacional sempre que houver extração de itens válida.
            use_slot_items = bool(slot_add) or bool(slot_rem)

        if use_slot_items:
            itens_ia = []

            def _slot_item_to_itens_ia(it: dict, *, op: str) -> dict | None:
                if not isinstance(it, dict):
                    return None
                nome = str(it.get("nome") or "").strip()
                if not nome:
                    return None
                qtd = it.get("qtd")
                try:
                    qtd = int(qtd) if qtd is not None else 1
                except Exception:
                    qtd = 1
                qtd = max(1, min(int(MAX_QTD_ITEM or 10), int(qtd)))

                obs = it.get("observacao")
                obs = str(obs).strip() if isinstance(obs, str) and obs.strip() else ""

                meio = it.get("meio_a_meio")
                if isinstance(meio, dict):
                    s1 = str(meio.get("sabor1") or "").strip()
                    s2 = str(meio.get("sabor2") or "").strip()
                    if s1 and s2:
                        nome = f"meio {s1} e meio {s2}"

                payload = {"nome": nome, "qtd": qtd, "observacao": (obs if op != "remover_item" else ""), "_op": op}
                return payload

            def _human_ack_message() -> str:
                t = (txt_norm or "").strip()
                if any(k in t for k in ("fome", "faminto", "com fome")):
                    return "Opa, vamos dar um jeito na sua fome!"
                if any(k in t for k in ("agiliza", "rapido", "rápido", "correria", "to com pressa", "tô com pressa", "pressa")):
                    return "Pode deixar, vou agilizar por aqui!"
                if any(k in t for k in ("vlw", "valeu", "obrigado", "obrigada")):
                    return "Fechado! Já organizei seu pedido."
                return "Fechado! Já organizei seu pedido."

            if slot_rem and slot_add:
                # Caso comum: "tira X e coloca Y" / "esqueci X, adiciona Y".
                # Processa remover -> adicionar no MESMO turno (batch update) com o mesmo pipeline determinístico.
                intencao = "batch_update"
                mensagem_ia = _human_ack_message()
                for it in slot_rem:
                    p = _slot_item_to_itens_ia(it, op="remover_item")
                    if p:
                        itens_ia.append(p)
                for it in slot_add:
                    p = _slot_item_to_itens_ia(it, op="adicionar_item")
                    if p:
                        itens_ia.append(p)
            elif slot_rem:
                intencao = "remover_item"
                mensagem_ia = "Beleza! 🙂"
                for it in slot_rem:
                    p = _slot_item_to_itens_ia(it, op="remover_item")
                    if p:
                        itens_ia.append(p)
            else:
                intencao = "adicionar_item"
                mensagem_ia = _human_ack_message()
                for it in slot_add:
                    p = _slot_item_to_itens_ia(it, op="adicionar_item")
                    if p:
                        itens_ia.append(p)
        else:
            async with _groq_sem:
                chat = await _run_blocking(
                    lambda: groq_client.chat.completions.create(
                        model="llama-3.3-70b-versatile",
                        messages=messages,
                        temperature=0.1,
                        response_format={"type": "json_object"},
                    ),
                    timeout=GROQ_TIMEOUT_SECONDS,
                )

            dados_ia_raw = json.loads(chat.choices[0].message.content)
            intencao, mensagem_ia, itens_ia = _sanitize_ia_response(dados_ia_raw)

    except asyncio.TimeoutError:
        # Fallback determinístico (evita "loop" de erro e dá próximos passos claros)
        try:
            if pedido_ativo and _safe_dict(pedido_ativo.get("carrinho_json")):
                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "⚠️ Estou um pouco lento agora, mas já vi seu carrinho.\n"
                    "Você quer *continuar comprando* ou *finalizar*?\n\n"
                    "- Responda *sim* para adicionar mais itens\n"
                    "- Responda *não* para informar o endereço",
                )
            else:
                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "⚠️ Estou com instabilidade agora.\n"
                    "Enquanto isso, você pode pedir assim: *'1 pizza calabresa e 1 coca'* ou pedir *'cardápio'*."
                )
        except Exception:
            await enviar_zap_async(phone_id, cliente_zap, "⚠️ Tô com instabilidade agora. Me manda a mensagem de novo em alguns segundos.")
        return
    except Exception as e:
        print(f"Erro IA: {e}")
        await enviar_zap_async(phone_id, cliente_zap, "⚠️ Tive um erro aqui. Pode repetir sua mensagem?")
        return


    try:
        await sb_exec(lambda: supabase.table("conversas").insert({
            "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "user", "mensagem": texto_completo
        }).execute())
    except Exception:
        pass


    if intencao == "cancelar":
        if pedido_ativo:
            await sb_exec(lambda: supabase.table("pedidos").update({"status": "cancelado"}).eq("id", pedido_ativo["id"]).execute())
        await enviar_zap_async(phone_id, cliente_zap, "Pedido cancelado. Se mudar de ideia, é só chamar! 👋")
        await sb_exec(lambda: set_estado(cliente_zap, phone_id, "INICIO"))
        return

    if intencao == "pedir_fechamento":
        if not pedido_ativo:
            await enviar_zap_async(phone_id, cliente_zap, "Seu carrinho está vazio. Escolha algo do cardápio primeiro! 🍕")
            return

        await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_ENDERECO"))
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
        await enviar_zap_async(phone_id, cliente_zap, msg)
        try:
            await sb_exec(lambda: supabase.table("conversas").insert({
                "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": msg
            }).execute())
        except Exception:
            pass
        return


    if intencao in ("adicionar_item", "fixar_item", "remover_item", "adicionar_observacao", "batch_update") and itens_ia:
        tabela_precos = dados_loja.get("precos_dict", {}) or {}
        nomes_oficiais = list(tabela_precos.keys())
        categorias_dict = dados_loja.get("categorias_dict", {}) or {}
        estoque_dict = (dados_loja or {}).get("estoque_dict", {}) or {}
        avisos_validacao = []
        need_clarify_item = False
        pending_size_info: dict | None = None
        pending_borda_info: dict | None = None

        nomes_norm_map = {}
        nomes_norm_list = []
        for k in nomes_oficiais:
            nk = normalizar_texto(k)
            if nk and nk not in nomes_norm_map:
                nomes_norm_map[nk] = k
                nomes_norm_list.append(nk)

        def _item_variants(raw: str) -> list[str]:
            base = str(raw or "")
            variants = set()

            def _add(s: str) -> None:
                ns = normalizar_texto(s)
                if ns:
                    variants.add(ns)

            _add(base)

            s = re.sub(r"\bretornavel\b|\bretornável\b", "vidro", base, flags=re.IGNORECASE)
            s = re.sub(r"\bgarrafa\b", "vidro", s, flags=re.IGNORECASE)
            s = re.sub(r"\b(\d+)\s*litro(s)?\b", r"\1l", s, flags=re.IGNORECASE)
            s = re.sub(r"\bmeio\s*litro\b", "500ml", s, flags=re.IGNORECASE)
            s = re.sub(r"\bcoca\s*cola\b", "coca-cola", s, flags=re.IGNORECASE)
            s = re.sub(r"\b(\d+)\s*l\b", r"\1l", s, flags=re.IGNORECASE)
            _add(s)

            return list(variants)
        
        def _match_item(nome: str) -> str | None:
            if not nome or not nomes_norm_list:
                return None
            best_match = None
            best_score = 0.0
            for termo in _item_variants(nome):
                if termo in nomes_norm_map:
                    return nomes_norm_map[termo]
                m = difflib.get_close_matches(termo, nomes_norm_list, n=1, cutoff=0.6)
                if m:
                    cand = m[0]
                    score = difflib.SequenceMatcher(None, termo, cand).ratio()
                    if score > best_score:
                        best_score = score
                        best_match = nomes_norm_map.get(cand, cand)
            return best_match
        

        def _tokens_menu(s: str) -> set[str]:
            s = normalizar_texto(s or "")
            s = re.sub(r"[^a-z0-9\s]", " ", s)
            stop = {"meio", "meia", "metade", "pizza", "sabor", "com", "e", "de", "da", "do", "a", "o"}
            return {p for p in s.split() if len(p) >= 3 and p not in stop}

        def _split_fallback_item(raw: str) -> tuple[str, str] | None:
            if not raw:
                return None
            txt = str(raw)
            m_fallback = re.search(
                r"(?P<base>.+?)\s*(?:,|\.)?\s*(?:se\s+n[aã]o\s+tiver|se\s+n[aã]o\s+tem|caso\s+n[aã]o\s+tenha)\s*(?:,|\.)?\s*(?:manda|envia|pode\s+ser|troca\s+por|substitui\s+por)?\s*(?P<alt>.+)$",
                txt,
                flags=re.IGNORECASE,
            )
            if m_fallback:
                base = (m_fallback.group("base") or "").strip(" .,")
                alt = (m_fallback.group("alt") or "").strip(" .,")
                if base and alt:
                    return base, alt
            if " ou " not in txt:
                return None
            parts = [p.strip(" .,") for p in re.split(r"\bou\b", txt, flags=re.IGNORECASE) if p.strip()]
            if len(parts) >= 2:
                return parts[0], parts[1]
            return None

        def _best_flavor_matches(query: str, oficiais: list[str], *, top_k: int = 3) -> list[str]:
            tq = _tokens_menu(query)
            scored = []
            for off in oficiais:
                to = _tokens_menu(off)
                inter = len(tq & to)
                # bônus simples pra casos tipo "calabresa" vs "pizza calabresa"
                bonus = 0
                for q in tq:
                    for o in to:
                        if o.startswith(q) or q.startswith(o):
                            bonus = max(bonus, 1)
                score = inter + bonus
                if score > 0:
                    scored.append((score, off))
            scored.sort(key=lambda x: x[0], reverse=True)
            return [off for _, off in scored[:top_k]]

        def _find_menu_candidates(query: str, oficiais: list[str], *, top_k: int = 5) -> list[str]:
            tq = _tokens_menu(query)
            if not tq:
                return []
            scored = []
            for off in oficiais:
                to = _tokens_menu(off)
                inter = len(tq & to)
                if inter <= 0:
                    continue
                # bônus por match de prefixo
                bonus = 0
                for q in tq:
                    for o in to:
                        if o.startswith(q) or q.startswith(o):
                            bonus = max(bonus, 1)
                score = inter + bonus
                scored.append((score, -len(to), off))
            scored.sort(reverse=True)
            return [off for _, __, off in scored[:top_k]]

        def _has_size_token(raw: str) -> bool:
            t = normalizar_texto(raw or "")
            if not t:
                return False
            if re.search(r"\b\d+\s*(l|ml)\b", t):
                return True
            return any(k in t for k in ("pequena", "media", "média", "grande", "gigante", "familia", "família", "brotinho"))

        def _strip_size_tokens(raw: str) -> str:
            t = normalizar_texto(raw or "")
            t = re.sub(r"\b\d+\s*(l|ml)\b", " ", t)
            t = re.sub(r"\b(pequena|media|média|grande|gigante|familia|família|brotinho)\b", " ", t)
            t = re.sub(r"\s+", " ", t).strip()
            return t

        def _size_ambiguous_options(query: str, candidatos: list[str]) -> list[str]:
            if _has_size_token(query):
                return []
            base_q = _strip_size_tokens(query)
            if not base_q:
                return []
            opts = []
            for c in (candidatos or []):
                if _strip_size_tokens(c) == base_q and _has_size_token(c):
                    opts.append(c)
            return opts if len(opts) >= 2 else []

        def _user_mentioned_size(texto_raw: str) -> bool:
            return _has_size_token(texto_raw)

        def _extract_size_tokens_simple(raw: str) -> list[str]:
            t = normalizar_texto(raw or "")
            if not t:
                return []
            tokens = []
            for k in ("pequena", "media", "média", "grande", "gigante", "familia", "família", "brotinho"):
                if k in t:
                    tokens.append(k)
            m = re.search(r"\b\d+(?:[\.,]\d+)?\s*(l|ml)\b", t)
            if m:
                tokens.append(m.group(0).replace(" ", ""))
            return tokens

        def _is_pizza_item_key(key: str) -> bool:
            if not key:
                return False
            cat = normalizar_texto(str(categorias_dict.get(key) or ""))
            k_norm = normalizar_texto(key)
            return k_norm.startswith("meio ") or ("pizza" in cat) or ("pizza" in k_norm)

        def _is_borda_item_key(key: str) -> bool:
            if not key:
                return False
            cat = normalizar_texto(str(categorias_dict.get(key) or ""))
            k_norm = normalizar_texto(key)
            return ("borda" in cat) or ("borda" in k_norm)

        def _pick_borda_option(raw_text: str, options: list[str], size_tokens: list[str]) -> str | None:
            if not raw_text or not options:
                return None
            q = re.sub(r"\bborda(s)?\b", " ", str(raw_text or ""), flags=re.IGNORECASE)
            q = re.sub(r"\s+", " ", q).strip()
            candidatos = _find_menu_candidates(q, options, top_k=6)
            if size_tokens and candidatos:
                sized = [c for c in candidatos if any(tok in normalizar_texto(c) for tok in size_tokens)]
                if sized:
                    candidatos = sized
            if candidatos:
                return candidatos[0]
            # fallback: tenta match por tokens simples
            t_norm = normalizar_texto(q)
            for opt in options:
                opt_norm = normalizar_texto(opt)
                if opt_norm and opt_norm in t_norm:
                    return opt
            m = difflib.get_close_matches(t_norm, [normalizar_texto(o) for o in options], n=1, cutoff=0.6)
            if m:
                for opt in options:
                    if normalizar_texto(opt) == m[0]:
                        return opt
            return None

        borda_oficiais: list[str] = []
        if categorias_dict:
            for k in nomes_oficiais:
                cat = normalizar_texto(str(categorias_dict.get(k) or ""))
                if "borda" in cat:
                    borda_oficiais.append(k)
        if not borda_oficiais:
            borda_oficiais = [k for k in nomes_oficiais if "borda" in normalizar_texto(k)]
        if borda_oficiais:
            seen = set()
            uniq = []
            for b in borda_oficiais:
                nb = normalizar_texto(b)
                if nb and nb not in seen:
                    seen.add(nb)
                    uniq.append(b)
            borda_oficiais = uniq

        

        carrinho_atual = _safe_dict((pedido_ativo or {}).get("carrinho_json")) if pedido_ativo else {}

        avisos_estoque = []
        bloquear_msg_ia = False
        last_item_key_candidate: str | None = None
        last_item_nome_candidate: str | None = None

        for item in itens_ia:
            nome_ia = (item.get("nome") or "").strip()
            if not nome_ia:
                nome_ia = ""

            item_op = str(item.get("_op") or intencao or "").strip().lower()
            if item_op not in ("adicionar_item", "remover_item", "fixar_item", "adicionar_observacao"):
                # Para batch_update sem _op, assume adicionar.
                item_op = "adicionar_item" if intencao == "batch_update" else str(intencao or "adicionar_item")

            try:
                qtd_ia = int(item.get("qtd", 1))
            except Exception:
                qtd_ia = 1
            qtd_ia = max(1, min(int(MAX_QTD_ITEM or 10), qtd_ia))

            obs_ia = item.get("observacao")
            obs_ia = (obs_ia or "").strip() if isinstance(obs_ia, str) else ""

            if item_op == "adicionar_observacao" and (not nome_ia) and obs_ia:
                target_key = None
                if len(carrinho_atual or {}) == 1:
                    target_key = next(iter(carrinho_atual))
                else:
                    last_key = str((dados_parciais or {}).get("last_item_key") or "").strip()
                    if last_key and last_key in (carrinho_atual or {}):
                        target_key = last_key
                if target_key:
                    nome_ia = target_key
                else:
                    bloquear_msg_ia = True
                    avisos_validacao.append("Qual item do seu carrinho devo ajustar? Ex.: *sem cebola na pizza de frango*.")
                    continue

            # fallback: "X ou Y" => adiciona X e registra substituição por Y
            fallback = _split_fallback_item(nome_ia) if item_op in ("adicionar_item", "fixar_item") else None
            if fallback:
                nome_ia, alt_nome = fallback
                if alt_nome:
                    fb_note = f"se não tiver, substituir por {alt_nome}"
                    obs_ia = f"{obs_ia}; {fb_note}".strip("; ") if obs_ia else fb_note

            nome_ia_norm = normalizar_texto(nome_ia)
            chave_item = ""
            preco_unitario = 0.0
            nome_exibicao = ""
            base_item_key = ""

            if item_op in ("adicionar_item", "fixar_item"):
                candidatos = _find_menu_candidates(nome_ia, nomes_oficiais, top_k=8)
                amb_opts = _size_ambiguous_options(nome_ia, candidatos)
                if amb_opts and not _user_mentioned_size(texto_completo):
                    need_clarify_item = True
                    bloquear_msg_ia = True
                    lista = ", ".join([str(c).title() for c in amb_opts[:3]])
                    avisos_validacao.append(f"Temos {lista}. Qual tamanho você deseja?")
                    if pending_size_info is None:
                        base_nome = _strip_size_tokens(nome_ia) or normalizar_texto(nome_ia) or nome_ia
                        pending_size_info = {
                            "base": base_nome,
                            "options": list(amb_opts or []),
                        }
                    continue
                if (not _user_mentioned_size(texto_completo)) and _has_size_token(nome_ia):
                    base_nome = _strip_size_tokens(nome_ia)
                    if base_nome:
                        candidatos_base = _find_menu_candidates(base_nome, nomes_oficiais, top_k=8)
                        amb_base = _size_ambiguous_options(base_nome, candidatos_base)
                        if amb_base:
                            need_clarify_item = True
                            bloquear_msg_ia = True
                            lista = ", ".join([str(c).title() for c in amb_base[:3]])
                            avisos_validacao.append(f"Temos {lista}. Qual tamanho você deseja?")
                            if pending_size_info is None:
                                pending_size_info = {
                                    "base": base_nome,
                                    "options": list(amb_base or []),
                                }
                            continue

            componentes_meio = []
            if ("meio" in nome_ia_norm) or ("meia" in nome_ia_norm) or ("metade" in nome_ia_norm) or ("/" in nome_ia):
                # Meio-a-meio é um recurso EXCLUSIVO de pizzas.
                # Se o cadastro de categorias não marcar pizzas corretamente, preferimos bloquear do que misturar pizza com bebida.
                def _is_pizza_cat(value: str) -> bool:
                    v = normalizar_texto(value or "")
                    return ("pizza" in v) or ("pizz" in v)

                oficiais_pizza = []
                if categorias_dict:
                    oficiais_pizza = [k for k in tabela_precos.keys() if _is_pizza_cat(str(categorias_dict.get(k) or ""))]

                if len(oficiais_pizza) < 2 and item_op in ("adicionar_item", "fixar_item"):
                    bloquear_msg_ia = True
                    avisos_validacao.append(
                        "⚠️ Meio a meio é válido apenas para *pizzas* (dois sabores).\n"
                        "Não dá pra fazer meia pizza e meia bebida.\n\n"
                        "Dica: cadastre suas pizzas com categoria contendo a palavra *pizza* (ex.: 'Pizzas') e peça assim: *meia calabresa e meia frango*."
                    )
                    continue

                melhores = _best_flavor_matches(nome_ia, oficiais_pizza, top_k=6)

                sabores_unicos = []

                # 1) Se o texto vier no formato "meio X e meio Y" (ou "meia X e meia Y"),
                # tenta casar CADA metade separadamente (prioriza o que o cliente escreveu).
                txt_user_norm = normalizar_texto(texto_completo)
                src_meio = txt_user_norm if (txt_user_norm.count("meio") + txt_user_norm.count("meia") + txt_user_norm.count("metade")) >= 2 else nome_ia_norm
                m_meio = re.search(
                    r"(?:meio|meia|metade)\s+(?P<a>.+?)\s*(?:(?:e|/|&)\s*)?(?:meio|meia|metade)\s+(?P<b>.+)",
                    src_meio,
                )
                if m_meio:
                    a_txt = (m_meio.group("a") or "").strip()
                    b_txt = (m_meio.group("b") or "").strip()

                    def _match_half(half_txt: str, oficiais: list[str]) -> tuple[str | None, list[str]]:
                        cleaned = normalizar_texto(half_txt)
                        cleaned = re.sub(r"\b(meio|meia|metade|pizza)\b", " ", cleaned).strip()
                        cleaned = re.sub(r"\s+", " ", cleaned).strip()
                        if not cleaned:
                            return None, []

                        if cleaned in oficiais:
                            return cleaned, [cleaned]

                        tq = _tokens_menu(cleaned)
                        if not tq:
                            return None, []

                        scored = []
                        for off in oficiais:
                            to = _tokens_menu(off)
                            inter = len(tq & to)

                            bonus = 0
                            for q in tq:
                                for o in to:
                                    if o.startswith(q) or q.startswith(o):
                                        bonus = max(bonus, 1)

                            subset = tq.issubset(to)
                            base = inter + bonus + (2 if subset else 0)

                            # IMPORTANTE: se não teve nenhum sinal “real” (token/prefixo/subset), ignora
                            if base <= 0:
                                continue

                            ratio = difflib.SequenceMatcher(None, cleaned, off).ratio()
                            score = base + (0.25 * ratio)
                            scored.append((score, -len(to), off))

                        scored.sort(reverse=True)
                        cand = [off for _, __, off in scored[:6]]
                        return (cand[0] if cand else None), cand

                    a_key, cand_a = _match_half(a_txt, oficiais_pizza)
                    b_key, cand_b = _match_half(b_txt, oficiais_pizza)

                    if a_key and b_key and b_key == a_key:
                        b_key = next((c for c in (cand_b or []) if c != a_key), None)

                    if a_key and b_key:
                        sabores_unicos = [a_key, b_key]
                if len(sabores_unicos) == 2 and sabores_unicos[0] == sabores_unicos[1]:
                    m_meio = re.search(r"(?:meio|meia|metade)\s+(?P<a>.+?)\s*(?:(?:e|/|&)\s*)?(?:meio|meia|metade)\s+(?P<b>.+)", txt_user_norm)
                    if m_meio:
                        a_txt = (m_meio.group("a") or "").strip()
                        b_txt = (m_meio.group("b") or "").strip()
                        # repete cand_a/cand_b e redefine sabores_unicos

                # 2) Fallback: se não deu pra extrair 2 metades, volta pro ranking geral
                if len(sabores_unicos) < 2:
                    for tok in sorted(_tokens_menu(texto_completo)):
                        for c in _best_flavor_matches(tok, oficiais_pizza, top_k=6):
                            if c not in sabores_unicos:
                                sabores_unicos.append(c)
                                break
                        if len(sabores_unicos) >= 2:
                            break

                componentes_meio = sabores_unicos[:2] if len(sabores_unicos) >= 2 else []

                # Se o cliente pediu meio-a-meio mas não achamos 2 sabores de pizza válidos, não cria item.
                if len(sabores_unicos) < 2 and item_op in ("adicionar_item", "fixar_item"):
                    bloquear_msg_ia = True
                    avisos_validacao.append(
                        "⚠️ Não encontrei 2 sabores de *pizza* válidos para esse meio a meio.\n"
                        "Escolha dois sabores que existam no cardápio de pizzas (ex: *meia calabresa e meia frango*)."
                    )
                    continue

                if len(sabores_unicos) >= 2:
                    precos = []
                    for s in sabores_unicos:
                        try:
                            precos.append(float(tabela_precos.get(s) or 0.0))
                        except Exception:
                            precos.append(0.0)

                    preco_unitario = max(precos) if precos else 0.0
                    if preco_unitario <= 0 and item_op in ("adicionar_item", "fixar_item"):
                        bloquear_msg_ia = True
                        avisos_validacao.append(
                            "⚠️ Não consegui precificar esse meio a meio. Pode escolher sabores do cardápio de pizzas?"
                        )
                        continue

                    sabores_titulo = [str(s).title() for s in sabores_unicos]
                    chave_item = normalizar_texto("Meio " + " e Meio ".join(sabores_titulo))
                    nome_exibicao = "Meio " + " / ".join(sabores_titulo)


            if not chave_item:
                if item_op in ("adicionar_item", "fixar_item"):
                    candidatos = _find_menu_candidates(nome_ia, nomes_oficiais, top_k=8)
                    amb_opts = _size_ambiguous_options(nome_ia, candidatos)
                    if amb_opts:
                        need_clarify_item = True
                        bloquear_msg_ia = True
                        lista = ", ".join([str(c).title() for c in amb_opts[:3]])
                        avisos_validacao.append(f"Temos {lista}. Qual tamanho você deseja?")
                        if pending_size_info is None:
                            base_nome = _strip_size_tokens(nome_ia) or normalizar_texto(nome_ia) or nome_ia
                            pending_size_info = {
                                "base": base_nome,
                                "options": list(amb_opts or []),
                            }
                        continue
                match_nome = _match_item(nome_ia)
                if match_nome:
                    chave_item = match_nome
                    try:
                        preco_unitario = float(tabela_precos[chave_item])
                    except Exception:
                        preco_unitario = 0.0
                    nome_exibicao = chave_item.title()
                else:
                    # Não adiciona itens fora do cardápio (evita "acerola", preço 0 etc)
                    if item_op in ("remover_item", "adicionar_observacao"):
                        # tenta casar com item já no carrinho
                        termo = normalizar_texto(nome_ia)
                        m = difflib.get_close_matches(termo, list((carrinho_atual or {}).keys()), n=1, cutoff=0.6)
                        if m:
                            chave_item = m[0]
                            nome_exibicao = (carrinho_atual.get(chave_item, {}) or {}).get("nome_exibicao", chave_item.title())
                        else:
                            bloquear_msg_ia = True
                            avisos_validacao.append(f"⚠️ Não achei esse item no seu carrinho: *{nome_ia}*.")
                            continue
                    else:
                        candidatos = _find_menu_candidates(nome_ia, nomes_oficiais, top_k=5)
                        if len(candidatos) >= 2:
                            need_clarify_item = True
                            bloquear_msg_ia = True
                            lista = " e ".join([str(c).title() for c in candidatos[:2]])
                            if len(candidatos) > 2:
                                lista = ", ".join([str(c).title() for c in candidatos[:3]])
                            avisos_validacao.append(
                                f"Temos {lista}. Qual você deseja?"
                            )
                            continue
                        bloquear_msg_ia = True
                        avisos_validacao.append(f"⚠️ Esse item não existe no cardápio: *{nome_ia}*.")
                        continue

            base_item_key = chave_item
            if item_op in ("adicionar_item", "fixar_item") and chave_item and _is_borda_item_key(chave_item):
                bloquear_msg_ia = True
                need_clarify_item = True
                lista = ", ".join([str(c).title() for c in (borda_oficiais or [])[:4]])
                if lista:
                    avisos_validacao.append(f"Essa borda vai em qual pizza? (Temos {lista})")
                else:
                    avisos_validacao.append("Essa borda vai em qual pizza?")
                if pending_borda_info is None:
                    pending_borda_info = {
                        "base": "",
                        "options": list(borda_oficiais or []),
                        "size": "",
                    }
                continue

            borda_sabor = ""
            borda_preco = 0.0
            if item_op in ("adicionar_item", "fixar_item") and chave_item and _is_pizza_item_key(base_item_key):
                borda_text = " ".join([str(texto_completo or ""), str(nome_ia or ""), str(obs_ia or "")]).strip()
                borda_norm = normalizar_texto(borda_text)
                wants_borda = ("borda" in borda_norm) and ("sem borda" not in borda_norm)
                if wants_borda and borda_oficiais:
                    size_tokens = _extract_size_tokens_simple(nome_ia or "")
                    if not size_tokens:
                        size_tokens = _extract_size_tokens_simple(chave_item)
                    borda_match = _pick_borda_option(borda_text, borda_oficiais, size_tokens)
                    if not borda_match:
                        bloquear_msg_ia = True
                        need_clarify_item = True
                        lista = ", ".join([str(c).title() for c in borda_oficiais[:4]])
                        avisos_validacao.append(f"Qual sabor de borda voce quer? Temos {lista}.")
                        if pending_borda_info is None:
                            pending_borda_info = {
                                "base": (nome_exibicao or chave_item.title()),
                                "options": list(borda_oficiais),
                                "size": " ".join(size_tokens) if size_tokens else "",
                            }
                        continue
                    borda_sabor = borda_match
                    try:
                        borda_preco = float(tabela_precos.get(borda_match) or 0.0)
                    except Exception:
                        borda_preco = 0.0
                    if borda_preco > 0:
                        preco_unitario += borda_preco
                    if borda_sabor:
                        label = f"borda {str(borda_sabor).title()}"
                        obs_ia = f"{obs_ia}; {label}".strip("; ") if obs_ia else label
                        nome_exibicao = f"{nome_exibicao} (Borda {str(borda_sabor).title()})" if nome_exibicao else f"{chave_item.title()} (Borda {str(borda_sabor).title()})"
                        borda_key = normalizar_texto(borda_sabor)
                        if borda_key:
                            chave_item = f"{base_item_key}__borda_{borda_key}"
            existed_before = chave_item in carrinho_atual
            is_meio_a_meio = bool(componentes_meio) and str(chave_item).startswith("meio ")

            if (not existed_before) and item_op == "fixar_item":
                if is_meio_a_meio:
                    carrinho_atual[chave_item] = {
                        "nome_exibicao": nome_exibicao,
                        "qtd": 0,
                        "preco_unitario": float(preco_unitario),
                        "observacao": "",
                        "componentes": componentes_meio,
                        "obs_componentes": {},
                    }
                    if base_item_key and base_item_key != chave_item:
                        carrinho_atual[chave_item]["item_base"] = base_item_key
                else:
                    carrinho_atual[chave_item] = {
                        "nome_exibicao": nome_exibicao,
                        "qtd": 0,
                        "preco_unitario": float(preco_unitario),
                        "observacao": "",
                    }
                    if base_item_key and base_item_key != chave_item:
                        carrinho_atual[chave_item]["item_base"] = base_item_key

            if item_op == "adicionar_item":
            

                # Meio-a-meio não existe como produto no estoque -> não tenta RPC de estoque.
                # (Se você quiser controlar estoque, precisa modelar isso no banco.)
                if is_meio_a_meio:
                    if chave_item not in carrinho_atual:
                        carrinho_atual[chave_item] = {
                            "nome_exibicao": nome_exibicao,
                            "qtd": 0,
                            "preco_unitario": float(preco_unitario),
                            "observacao": "",
                            "componentes": componentes_meio,
                            "obs_componentes": {},
                        }
                    if borda_sabor:
                        carrinho_atual[chave_item]["borda_sabor"] = borda_sabor
                        carrinho_atual[chave_item]["borda_preco"] = float(borda_preco or 0.0)
                        carrinho_atual[chave_item]["preco_unitario"] = float(preco_unitario)
                        if base_item_key and base_item_key != chave_item:
                            carrinho_atual[chave_item]["item_base"] = base_item_key
                    carrinho_atual[chave_item]["qtd"] += qtd_ia
                    if obs_ia:
                        obs_norm = normalizar_texto(obs_ia)

                        comps = carrinho_atual[chave_item].get("componentes") or []
                        if isinstance(comps, list) and comps:
                            # escolhe o componente mais citado na observação
                            best_comp = None
                            best_score = 0
                            for comp in comps:
                                comp_norm = normalizar_texto(comp)
                                score = 0

                                # match simples por substring + tokens
                                if comp_norm and comp_norm in obs_norm:
                                    score += 3
                                for tok in comp_norm.split():
                                    if len(tok) >= 3 and tok in obs_norm:
                                        score += 1

                                if score > best_score:
                                    best_score = score
                                    best_comp = comp

                            if best_comp and best_score > 0:
                                carrinho_atual[chave_item].setdefault("obs_componentes", {})
                                carrinho_atual[chave_item]["obs_componentes"][best_comp] = obs_ia
                            else:
                                # fallback: não conseguiu identificar metade -> obs geral
                                carrinho_atual[chave_item]["observacao"] = obs_ia
                        else:
                            carrinho_atual[chave_item]["observacao"] = obs_ia
                    last_item_key_candidate = chave_item
                    last_item_nome_candidate = nome_exibicao or chave_item.title()
                else:
                    def _is_unlimited_stock(k: str) -> bool:
                        if k not in estoque_dict:
                            return False
                        v = estoque_dict.get(k)
                        if v is None:
                            return True
                        if isinstance(v, str) and v.strip().lower() in ("none", "ilimitado", "infinito", "inf", "∞"):
                            return True
                        return False

                    estoque_key = base_item_key or chave_item
                    sucesso, dados_retorno = await atualizar_estoque_real_time_async(restaurante_db_id, estoque_key, -qtd_ia)
                    if sucesso:
                        if chave_item not in carrinho_atual:
                            carrinho_atual[chave_item] = {
                                "nome_exibicao": nome_exibicao,
                                "qtd": 0,
                                "preco_unitario": float(preco_unitario),
                                "observacao": "",
                            }

                        carrinho_atual[chave_item]["qtd"] += qtd_ia
                        if borda_sabor:
                            carrinho_atual[chave_item]["borda_sabor"] = borda_sabor
                            carrinho_atual[chave_item]["borda_preco"] = float(borda_preco or 0.0)
                            carrinho_atual[chave_item]["preco_unitario"] = float(preco_unitario)
                            if base_item_key and base_item_key != chave_item:
                                carrinho_atual[chave_item]["item_base"] = base_item_key
                        if obs_ia:
                            carrinho_atual[chave_item]["observacao"] = obs_ia
                        last_item_key_candidate = chave_item
                        last_item_nome_candidate = nome_exibicao or chave_item.title()
                        if (dados_retorno or {}).get("novo_estoque") == 0:
                            avisos_estoque.append(f"📦 Você garantiu a última unidade de *{nome_exibicao}*!")
                    else:
                        if _is_unlimited_stock(estoque_key):
                            if chave_item not in carrinho_atual:
                                carrinho_atual[chave_item] = {
                                    "nome_exibicao": nome_exibicao,
                                    "qtd": 0,
                                    "preco_unitario": float(preco_unitario),
                                    "observacao": "",
                                }
                            carrinho_atual[chave_item]["qtd"] += qtd_ia
                            if borda_sabor:
                                carrinho_atual[chave_item]["borda_sabor"] = borda_sabor
                                carrinho_atual[chave_item]["borda_preco"] = float(borda_preco or 0.0)
                                carrinho_atual[chave_item]["preco_unitario"] = float(preco_unitario)
                                if base_item_key and base_item_key != chave_item:
                                    carrinho_atual[chave_item]["item_base"] = base_item_key
                            if obs_ia:
                                carrinho_atual[chave_item]["observacao"] = obs_ia
                            last_item_key_candidate = chave_item
                            last_item_nome_candidate = nome_exibicao or chave_item.title()
                            continue
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
                            avisos_estoque.append(f"⚠️ Não consegui adicionar *{nome_exibicao}* agora. Tente novamente.")

            elif item_op == "remover_item":
                if chave_item not in carrinho_atual:
                    continue
                qtd_atual = int(carrinho_atual[chave_item].get("qtd") or 0)
                qtd_remover = min(qtd_ia, qtd_atual)
                if qtd_remover > 0:
                    dados_item = carrinho_atual.get(chave_item) or {}
                    if not _is_meio_a_meio_item(chave_item, dados_item):
                        estoque_key = str(dados_item.get("item_base") or chave_item)
                        await atualizar_estoque_real_time_async(restaurante_db_id, estoque_key, +qtd_remover)

                    carrinho_atual[chave_item]["qtd"] -= qtd_remover
                    if carrinho_atual[chave_item]["qtd"] <= 0:
                        carrinho_atual.pop(chave_item, None)

            elif item_op == "fixar_item":
                qtd_atual = int(carrinho_atual[chave_item].get("qtd") or 0)
                diferenca = qtd_ia - qtd_atual
                dados_item = carrinho_atual.get(chave_item) or {}
                is_meio = _is_meio_a_meio_item(chave_item, dados_item)

                if diferenca > 0:
                    if is_meio:
                        carrinho_atual[chave_item]["qtd"] = qtd_ia
                    else:
                        estoque_key = str(dados_item.get("item_base") or chave_item)
                        sucesso, _ = await atualizar_estoque_real_time_async(restaurante_db_id, estoque_key, -diferenca)
                        if sucesso:
                            carrinho_atual[chave_item]["qtd"] = qtd_ia
                            last_item_key_candidate = chave_item
                            last_item_nome_candidate = nome_exibicao or chave_item.title()
                        else:
                            bloquear_msg_ia = True
                            avisos_estoque.append(f"⚠️ Não há estoque suficiente de *{nome_exibicao}* para completar {qtd_ia}.")
                elif diferenca < 0:
                    qtd_devolver = abs(diferenca)
                    if not is_meio:
                        estoque_key = str(dados_item.get("item_base") or chave_item)
                        await atualizar_estoque_real_time_async(restaurante_db_id, estoque_key, +qtd_devolver)
                    carrinho_atual[chave_item]["qtd"] = qtd_ia

                if int(carrinho_atual.get(chave_item, {}).get("qtd", 0)) <= 0:
                    carrinho_atual.pop(chave_item, None)
                elif obs_ia:
                    carrinho_atual[chave_item]["observacao"] = obs_ia
                    last_item_key_candidate = chave_item
                    last_item_nome_candidate = nome_exibicao or chave_item.title()
                if borda_sabor and chave_item in carrinho_atual:
                    carrinho_atual[chave_item]["borda_sabor"] = borda_sabor
                    carrinho_atual[chave_item]["borda_preco"] = float(borda_preco or 0.0)
                    carrinho_atual[chave_item]["preco_unitario"] = float(preco_unitario)
                    if base_item_key and base_item_key != chave_item:
                        carrinho_atual[chave_item]["item_base"] = base_item_key

            elif item_op == "adicionar_observacao":
                if chave_item not in carrinho_atual:
                    continue
                if not obs_ia:
                    continue

                dados_item = carrinho_atual.get(chave_item) or {}
                comps = dados_item.get("componentes") or []

                # Se for meio-a-meio e a observação citar uma metade, salva em obs_componentes
                if isinstance(comps, list) and comps:
                    obs_norm = normalizar_texto(obs_ia)
                    best_comp = None
                    best_score = 0

                    for comp in comps:
                        comp_norm = normalizar_texto(comp)
                        score = 0

                        # match simples por substring + tokens (igual ao adicionar_item)
                        if comp_norm and comp_norm in obs_norm:
                            score += 3
                        for tok in comp_norm.split():
                            if len(tok) >= 3 and tok in obs_norm:
                                score += 1

                        if score > best_score:
                            best_score = score
                            best_comp = comp

                    if best_comp and best_score > 0:
                        carrinho_atual[chave_item].setdefault("obs_componentes", {})
                        carrinho_atual[chave_item]["obs_componentes"][best_comp] = obs_ia
                    else:
                        # fallback: não conseguiu identificar metade -> obs geral
                        carrinho_atual[chave_item]["observacao"] = obs_ia
                else:
                    carrinho_atual[chave_item]["observacao"] = obs_ia

                last_item_key_candidate = chave_item
                last_item_nome_candidate = (carrinho_atual.get(chave_item, {}) or {}).get("nome_exibicao") or chave_item.title()

        try:
            if last_item_key_candidate:
                merged = dict(dados_parciais or {})
                merged["last_item_key"] = last_item_key_candidate
                if last_item_nome_candidate:
                    merged["last_item_nome"] = last_item_nome_candidate
                if merged != (dados_parciais or {}):
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, estado_atual, merged))
                    dados_parciais = merged
        except Exception:
            pass

        handled_cart_checkout = await _persist_and_respond_carrinho_update(
            phone_id=phone_id,
            cliente_zap=cliente_zap,
            restaurante_db_id=int(restaurante_db_id),
            nome_cliente=str(nome_cliente or ""),
            pedido_ativo=pedido_ativo,
            carrinho_atual=carrinho_atual,
            mensagem_ia=str(mensagem_ia or ""),
            avisos_validacao=list(avisos_validacao or []),
            avisos_estoque=list(avisos_estoque or []),
            bloquear_msg_ia=bool(bloquear_msg_ia),
            intent_router_prefill=(None if need_clarify_item else intent_router_prefill),
            bairros_dict=bairros_dict or {},
            lista_bairros_txt=lista_bairros_txt,
            cardapio_txt=str((dados_loja or {}).get("cardapio") or ""),
            texto_completo=str(texto_completo or ""),
            txt_norm=str(txt_norm or ""),
            dados_parciais=(dados_parciais or {}),
            skip_post_prompt=(need_clarify_item or _slot_should_force_checkout(txt_norm, slot_obj)),
        )

        # Se a mensagem pediu checkout, tenta avançar imediatamente após salvar o carrinho.
        if handled_cart_checkout:
            return

        if need_clarify_item:
            try:
                if intent_router_prefill and isinstance(intent_router_prefill, dict):
                    merged = dict(dados_parciais or {})
                    merged.update(intent_router_prefill)
                    await sb_exec(lambda: set_estado(cliente_zap, phone_id, estado_atual, merged))
            except Exception:
                pass
            try:
                merged = dict(dados_parciais or {})
                if pending_size_info:
                    merged["pending_size"] = pending_size_info
                if pending_borda_info:
                    merged["pending_borda"] = pending_borda_info
                await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_MAIS_ALGO", merged))
            except Exception:
                await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_MAIS_ALGO", {}))
            return

        if _slot_should_force_checkout(txt_norm, slot_obj):
            try:
                pedido_ativo2 = await _run_blocking(lambda: get_pedido_ativo(cliente_zap, restaurante_db_id), timeout=SUPABASE_TIMEOUT_SECONDS)
            except Exception:
                pedido_ativo2 = pedido_ativo

            try:
                estado_data2 = await _run_blocking(lambda: get_estado(cliente_zap, phone_id), timeout=SUPABASE_TIMEOUT_SECONDS)
                dados_parciais2 = (estado_data2.get("dados_parciais") or {}) if estado_data2 else (dados_parciais or {})
            except Exception:
                dados_parciais2 = (dados_parciais or {})

            handled = await _slot_advance_checkout(
                phone_id=phone_id,
                cliente_zap=cliente_zap,
                restaurante_db_id=int(restaurante_db_id),
                pedido_ativo=pedido_ativo2,
                dados_parciais=dados_parciais2,
                bairros_dict=bairros_dict or {},
                lista_bairros_txt=lista_bairros_txt,
                now_iso=now_iso,
            )
            if handled:
                return

        return

    await enviar_zap_async(phone_id, cliente_zap, mensagem_ia)
    try:
        await sb_exec(lambda: supabase.table("conversas").insert({
            "cliente_zap": cliente_zap, "restaurante_id": phone_id, "role": "assistant", "mensagem": mensagem_ia
        }).execute())
    except Exception:
        pass

    # Se capturou endereço de entrega mas faltou bairro, pergunta agora (mesmo sem atualizar carrinho).
    try:
        dp = dados_parciais if isinstance(dados_parciais, dict) else {}
        tipo = str((dp or {}).get("tipo_entrega") or "").strip().lower()
        end = str((dp or {}).get("endereco_txt") or "").strip()
        bairro = str((dp or {}).get("bairro") or "").strip()

        if not tipo:
            if _is_retirada_text(txt_norm):
                tipo = "retirada"
            elif ("entrega" in (txt_norm or "")) or end:
                tipo = "entrega"

        if tipo == "entrega" and end and not bairro:
            await sb_exec(lambda: set_estado(cliente_zap, phone_id, "AGUARDANDO_ENDERECO", (dp or {})))
            await enviar_zap_async(phone_id, cliente_zap, "Qual é o *bairro*? Assim eu confirmo a taxa certinho.")
            return
    except Exception:
        pass



    # Gatilhos robustos para pedidos de resumo/carrinho
    palavras_gatilho = [
        "pedi", "pedido", "carrinho", "resumo", "lista", "conta", "total", "comprado",
        "meu pedido", "meu carrinho", "resumo do pedido", "o que eu pedi", "o que pedi", "o que eu pedi até agora", "o que pedi até agora",
        "mostrar pedido", "mostrar carrinho", "ver pedido", "ver carrinho", "quais itens", "quais produtos", "quais coisas", "o que tem no meu pedido", "o que tem no carrinho"
    ]
    if pedido_ativo and any(p in txt_norm for p in palavras_gatilho) or re.search(r"(o que.*pedi|resumo.*pedido|meu.*pedido|meu.*carrinho|mostrar.*pedido|mostrar.*carrinho|ver.*pedido|ver.*carrinho|quais.*itens|quais.*produtos|o que tem.*pedido|o que tem.*carrinho)", txt_norm):
        resumo_txt = str(pedido_ativo.get("resumo_pedido", "") or "").replace("|", "\n")
        try:
            total_val = float(pedido_ativo.get("total_valor") or 0.0)
        except Exception:
            total_val = 0.0
        if resumo_txt and resumo_txt != "Carrinho vazio":
            msg_resumo = f"🛒 *Seu Carrinho:*\n{resumo_txt}\n💰 Total: R$ {total_val:.2f}"
            await enviar_zap_async(phone_id, cliente_zap, msg_resumo)
