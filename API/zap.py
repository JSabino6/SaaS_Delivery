import os
import time
import json
import asyncio
import base64

import requests
import urllib3

from fastapi import Request
from fastapi.responses import JSONResponse

from banco import get_dados_restaurante, touch_estado_last_message, incrementar_metricas_restaurante
from utils import (
    UAZAPI_BASE_URL,
    UAZAPI_TIMEOUT,
    UAZAPI_PRESENCE_ENABLED,
    UAZAPI_PRESENCE_DELAY_MS,
    GROQ_API_KEY,
    GROQ_TIMEOUT_SECONDS,
    WEBHOOK_SECRET,
    HTTP_VERIFY_TLS,
    MAX_WEBHOOK_BODY_BYTES,
    MAX_INCOMING_TEXT_CHARS,
    MESSAGE_DEBOUNCE_SECONDS,
    WEBHOOK_RATE_LIMIT_PER_MIN,
    logger,
    _only_digits,
    _extract_cliente_zap,
    _stable_event_id,
    _dbg_webhook,
    _first_dict,
    _run_blocking,
    sb_exec,
    redis_client,
    redis_add_buffer,
    redis_get_clear_buffer,
    redis_claim_event_once,
    redis_rate_limit,
    redis_acquire_lock,
    redis_release_lock,
    SEND_MAX_CONCURRENCY,
)


if not HTTP_VERIFY_TLS:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


processing_tasks = {}
processing_followup_tasks = {}
processing_task_phase = {}  # conv_key -> "sleep" | "processing"

# Fallback buffer (quando REDIS_URL estiver vazio). Mantém o comportamento de "juntar" mensagens.
_mem_buffers: dict[str, list[str]] = {}
_audio_flags_by_conv: dict[tuple[str, str], bool] = {}
_audio_transcribed_by_conv: dict[tuple[str, str], bool] = {}


_send_sem = asyncio.Semaphore(max(1, SEND_MAX_CONCURRENCY))


async def enviar_zap_async(phone_id, numero, texto, *, timeout: float = 25):
    async with _send_sem:
        return await _run_blocking(lambda: enviar_zap(phone_id, numero, texto), timeout=timeout)


async def enviar_presenca_async(phone_id, numero, presence: str = "composing", *, delay_ms: int | None = None):
    async with _send_sem:
        return await _run_blocking(
            lambda: enviar_presenca(phone_id, numero, presence=presence, delay_ms=delay_ms),
            timeout=10,
        )


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

        if logger.isEnabledFor(10):
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


def enviar_presenca(phone_id, numero, presence: str = "composing", *, delay_ms: int | None = None):
    if not UAZAPI_PRESENCE_ENABLED:
        return

    dados = get_dados_restaurante(phone_id, tipo="phone_id")
    if not dados:
        logger.error("enviar_presenca: restaurante nao encontrado | phone_id=%s", phone_id)
        return

    token = (dados.get("instance_token") or "").strip()
    if not token:
        logger.warning("enviar_presenca: token vazio | phone_id=%s", phone_id)
        return

    try:
        delay = int(delay_ms or UAZAPI_PRESENCE_DELAY_MS or 0)
    except Exception:
        delay = int(UAZAPI_PRESENCE_DELAY_MS or 0)

    if delay <= 0:
        return

    payload = {
        "number": numero,
        "presence": str(presence or "composing"),
        "delay": delay,
    }
    try:
        requests.post(
            f"{UAZAPI_BASE_URL}/message/presence",
            json=payload,
            headers={"token": token, "Content-Type": "application/json"},
            verify=HTTP_VERIFY_TLS,
            timeout=(5, max(int(UAZAPI_TIMEOUT or 15), 10)),
        )
    except Exception:
        logger.exception("Erro Presenca Zap | numero=%s | phone_id=%s", numero, phone_id)


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


async def webhook(request: Request):
    phone_id = None
    cliente_zap = None
    restaurante_db_id = 0
    redis_ops_count = 0
    try:
        if WEBHOOK_SECRET:
            incoming_secret = (
                request.headers.get("x-webhook-secret")
                or request.headers.get("x-webhook-token")
                or request.query_params.get("token")
                or ""
            ).strip()
            if incoming_secret != WEBHOOK_SECRET:
                _dbg_webhook("skip:unauthorized_webhook")
                return JSONResponse(status_code=401, content={"ok": False, "error": "unauthorized"})

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
        if not isinstance(body, dict):
            logger.warning("webhook ignored: payload_root_not_dict | type=%s", type(body).__name__)
            return "ok"

        data = body.get("data") or {}
        if not isinstance(data, dict):
            data = {}

        # Normaliza msg_data para formatos comuns
        msg_data = _first_dict(body.get("message"))
        if not msg_data and isinstance(data, dict):
            msg_data = _first_dict(data.get("message"))
        if not msg_data:
            msg_data = _first_dict(body.get("messages"))
        if not msg_data and isinstance(data, dict):
            msg_data = _first_dict(data.get("messages"))
        if not msg_data and isinstance(data, dict):
            if any(k in data for k in ("text", "conversation", "fromMe", "sender", "remoteJid", "from", "chatid")):
                msg_data = data

        logger.info("webhook received | has_message=%s", bool(msg_data))
        _dbg_webhook("received", has_message=bool(msg_data), keys=list(body.keys())[:10])

        # 1. Validações Iniciais
        instance = (body.get("instanceName") or body.get("instance") or "").strip()
        if not instance and isinstance(data, dict):
            instance = (data.get("instanceName") or data.get("instance") or "").strip()
        if not instance and isinstance(msg_data, dict):
            instance = (msg_data.get("instanceName") or msg_data.get("instance") or "").strip()
        if not instance:
            _dbg_webhook("skip:no_instance")
            return "ok"

        if msg_data.get("fromMe"):
            _dbg_webhook("skip:from_me", instance=instance)
            return "ok"

        # 2. Obter dados do Restaurante e Cliente
        restaurante = await sb_exec(lambda: get_dados_restaurante(instance, tipo="instance_name"))
        if not restaurante:
            _dbg_webhook("skip:no_restaurante", instance=instance)
            return "ok"

        try:
            restaurante_db_id = int(restaurante.get("id") or 0)
        except Exception:
            restaurante_db_id = 0

        phone_id = restaurante.get("phone_id")
        if not phone_id:
            _dbg_webhook("skip:no_phone_id", instance=instance)
            return "ok"

        remote_jid = (msg_data.get("chatid") or msg_data.get("sender") or msg_data.get("remoteJid") or msg_data.get("from") or "")
        cliente_zap = _extract_cliente_zap(body, msg_data, phone_id) or _only_digits(remote_jid.split("@")[0]) or remote_jid.split("@")[0]

        if not cliente_zap:
            _dbg_webhook("skip:no_cliente", instance=instance, phone_id=phone_id)
            return "ok"

        # 3. Deduplicação (prioriza message_id real do provedor, quando existir)
        provider_msg_id = _extract_message_id_from_msg_data(msg_data, data)
        event_id = provider_msg_id or _stable_event_id(body)
        if event_id:
            dedup_key = f"dedup:webhook:{instance}:{event_id}"
            redis_ops_count += 1
            if not redis_claim_event_once(dedup_key, ttl_seconds=6 * 3600):
                _dbg_webhook("skip:dedup", instance=instance, event_id=event_id)
                return "ok"

        # 3.5 Atualizar última atividade
        try:
            await sb_exec(lambda: touch_estado_last_message(cliente_zap, phone_id))
        except Exception:
            pass

        # 4. Extração de Texto e Tratamento de Tipos
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
                            if restaurante_db_id > 0:
                                try:
                                    await sb_exec(lambda: incrementar_metricas_restaurante(restaurante_db_id, ia_audio_calls=1))
                                except Exception:
                                    pass
                            _dbg_webhook("audio_transcribed", instance=instance, phone_id=phone_id, cliente=cliente_zap, text_len=len(texto))
                except Exception as e:
                    _dbg_webhook("audio_transcribe_failed", instance=instance, phone_id=phone_id, cliente=cliente_zap, error=str(e))

        # Truncar texto se necessário
        if texto and MAX_INCOMING_TEXT_CHARS and len(texto) > MAX_INCOMING_TEXT_CHARS:
            texto = texto[:MAX_INCOMING_TEXT_CHARS]

        # 5. Lógica de Fallback (Se não houver texto)
        if not texto:
            if kind == "location":
                await enviar_zap_async(phone_id, cliente_zap, "Recebi sua localização. Agora me diga em texto: bairro/rua/número e ponto de referência, por favor.")
            elif kind == "media":
                await enviar_zap_async(phone_id, cliente_zap, "Recebi seu arquivo, mas aqui eu só consigo atender por texto. Me diga o pedido por mensagem, por favor.")
            elif kind == "interactive":
                pass
            else:
                await enviar_zap_async(phone_id, cliente_zap, "Não consegui ler sua mensagem. Pode me mandar em texto, por favor?")
            return "ok"

        # 6. Rate Limit (Só conta se for mensagem de texto válida)
        rl_key = f"ratelimit:webhook:{phone_id}:{cliente_zap}"
        redis_ops_count += 1
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
            redis_ops_count += 1
            redis_add_buffer(buffer_key, texto)
        else:
            _mem_buffers.setdefault(buffer_key, []).append(texto)

        if buffer_key in processing_tasks:
            try:
                processing_tasks[buffer_key].cancel()
            except Exception:
                pass

        processing_tasks[buffer_key] = asyncio.create_task(
            executar_ia_com_delay(buffer_key, cliente_zap, phone_id, nome, restaurante_db_id=restaurante_db_id)
        )

        if restaurante_db_id > 0 and redis_ops_count > 0:
            try:
                await sb_exec(lambda: incrementar_metricas_restaurante(restaurante_db_id, redis_ops=redis_ops_count))
            except Exception:
                pass

    except Exception:
        logger.exception("Erro Webhook")
        try:
            if phone_id and cliente_zap:
                await enviar_zap_async(
                    phone_id,
                    cliente_zap,
                    "Tive uma instabilidade aqui. Pode repetir sua mensagem em texto, por favor?",
                )
        except Exception:
            pass

    return "ok"


async def executar_ia_com_delay(buffer_key: str, cliente_zap: str, phone_id: str, nome: str, restaurante_db_id: int = 0):
    # 1. Pega a referência da tarefa atual para comparar depois
    this_task = asyncio.current_task()
    lock_key = f"lock:conv:{str(phone_id)}:{str(cliente_zap)}"
    lock_token = ""
    redis_ops_count = 0

    try:
        try:
            debounce_s = float(MESSAGE_DEBOUNCE_SECONDS or 3)
        except Exception:
            debounce_s = 3.0
        await asyncio.sleep(max(0.5, debounce_s))  # Espera configurável para juntar mensagens

        redis_ops_count += 1
        lock_token = redis_acquire_lock(lock_key, ttl_seconds=60)
        if not lock_token:
            _dbg_webhook("skip:conv_locked", phone_id=phone_id, cliente=cliente_zap)
            return

        if redis_client:
            redis_ops_count += 1
            texto_final = redis_get_clear_buffer(buffer_key)
        else:
            parts = _mem_buffers.pop(buffer_key, [])
            texto_final = " ".join([p for p in parts if p]).strip()
        if texto_final:
            from cerebro import processar_mensagem_final
            # Show "Digitando" while the AI is processing this message.
            try:
                await enviar_presenca_async(phone_id, cliente_zap, "composing")
            except Exception:
                pass
            await processar_mensagem_final(phone_id, cliente_zap, nome, texto_final)

    except asyncio.CancelledError:
        return
    except Exception:
        logger.exception("Erro executar_ia_com_delay | phone_id=%s | cliente=%s", phone_id, cliente_zap)
        try:
            await enviar_zap_async(
                phone_id,
                cliente_zap,
                "Tive uma instabilidade para processar sua mensagem agora. Pode me enviar novamente?",
            )
        except Exception:
            pass

    finally:
        try:
            redis_ops_count += 1
            redis_release_lock(lock_key, lock_token)

            if processing_tasks.get(buffer_key) is this_task:
                processing_tasks.pop(buffer_key, None)

            if "processing_task_phase" in globals():
                processing_task_phase.pop(buffer_key, None)

        except Exception:
            pass

        if restaurante_db_id > 0 and redis_ops_count > 0:
            try:
                await sb_exec(lambda: incrementar_metricas_restaurante(restaurante_db_id, redis_ops=redis_ops_count))
            except Exception:
                pass
#