import os
import re
import json
from pathlib import Path
from typing import Any, Dict

import requests
import httpx

# Load .env (same behavior as main.py)
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None

_WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if load_dotenv:
    load_dotenv(dotenv_path=str(_WORKSPACE_ROOT / ".env"), override=False)
    load_dotenv(dotenv_path=str(Path(__file__).resolve().parent / ".env"), override=False)
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


app = FastAPI(title="UAZAPI Audio Test", version="1.0.0")

UAZAPI_BASE_URL = os.getenv("UAZAPI_BASE_URL", "https://free.uazapi.com").rstrip("/")
UAZAPI_SEND_TOKEN = os.getenv("UAZAPI_SEND_TOKEN", "")
UAZAPI_TIMEOUT = float(os.getenv("UAZAPI_TIMEOUT", "15"))
UAZAPI_MEDIA_DOWNLOAD_URL = os.getenv("UAZAPI_MEDIA_DOWNLOAD_URL", "").strip()
WEBHOOK_SECRET = (os.getenv("WEBHOOK_SECRET") or "").strip()

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_TRANSCRIBE_MODEL = os.getenv("GROQ_TRANSCRIBE_MODEL", "whisper-large-v3")
GROQ_CHAT_MODEL = os.getenv("GROQ_CHAT_MODEL", "llama-3.3-70b-versatile")

SYSTEM_PROMPT = os.getenv(
    "AUDIO_TEST_SYSTEM_PROMPT",
    "Você é um atendente virtual de restaurante. Responda de forma curta e objetiva.",
)


def _only_digits(v: str) -> str:
    return re.sub(r"\D", "", str(v or "")).strip()


def _extract_uazapi_payload(body: Dict[str, Any]) -> Dict[str, Any]:
    data = body.get("data") or {}
    msg = data.get("message") or body.get("message") or {}
    return {
        "event": body.get("event") or data.get("event"),
        "message_type": data.get("messageType") or body.get("messageType"),
        "message": msg,
        "remote_jid": (data.get("key") or {}).get("remoteJid") or (body.get("key") or {}).get("remoteJid"),
        "from_me": (data.get("key") or {}).get("fromMe") if data.get("key") is not None else (body.get("key") or {}).get("fromMe"),
        "message_id": (data.get("key") or {}).get("id") or (body.get("key") or {}).get("id"),
        "instance": body.get("instance") or body.get("instanceName") or data.get("instance") or data.get("instanceName"),
    }


def _first_dict(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    if isinstance(v, list):
        for item in v:
            if isinstance(item, dict):
                return item
    return {}


def _extract_msg_data(body: Dict[str, Any]) -> Dict[str, Any]:
    data = body.get("data") or {}

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
    return msg_data or {}


async def _download_audio(url: str) -> tuple[bytes, str]:
    async with httpx.AsyncClient(timeout=UAZAPI_TIMEOUT) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type") or "audio/ogg"
        return resp.content, content_type


async def _download_audio_by_id(message_id: str) -> tuple[bytes, str]:
    download_url = UAZAPI_MEDIA_DOWNLOAD_URL or f"{UAZAPI_BASE_URL}/message/download"
    if not download_url:
        raise RuntimeError("UAZAPI_MEDIA_DOWNLOAD_URL não configurado")
    if not UAZAPI_SEND_TOKEN:
        raise RuntimeError("UAZAPI_SEND_TOKEN não configurado")

    headers = {"token": UAZAPI_SEND_TOKEN, "Content-Type": "application/json"}
    payload = {
        "id": str(message_id),
        "generate_mp3": True,
        "return_link": True,
        "return_base64": False,
        "transcribe": False,
    }
    async with httpx.AsyncClient(timeout=UAZAPI_TIMEOUT) as client:
        r = await client.post(download_url, headers=headers, json=payload)
        content_type = r.headers.get("content-type", "")
        if content_type.startswith("audio/"):
            return r.content, content_type or "audio/mpeg"

        r.raise_for_status()
        data = r.json()
        media_url = data.get("fileURL") or data.get("url") or data.get("media_url")
        if media_url:
            resp = await client.get(media_url)
            resp.raise_for_status()
            return resp.content, resp.headers.get("content-type") or "audio/mpeg"

        b64 = data.get("base64Data") or data.get("base64") or data.get("data")
        if b64:
            import base64
            return base64.b64decode(b64), "audio/mpeg"

    raise RuntimeError("Resposta de download inválida")


def _extract_audio_message(body: Dict[str, Any]) -> Dict[str, Any]:
    data = body.get("data") or {}
    msg = data.get("message") or body.get("message") or {}
    audio_obj = msg.get("audioMessage") or msg.get("voiceMessage")
    if audio_obj:
        return audio_obj
    nested = (data.get("message") or {}).get("audioMessage")
    if nested:
        return nested
    audio_root = data.get("audioMessage") or data.get("voiceMessage")
    if audio_root:
        return audio_root
    return {}


def _transcribe_audio(audio_bytes: bytes, mime_type: str) -> str:
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY não configurada")

    url = "https://api.groq.com/openai/v1/audio/transcriptions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}"}
    files = {"file": ("audio.ogg", audio_bytes, mime_type)}
    data = {"model": GROQ_TRANSCRIBE_MODEL, "response_format": "json"}

    r = requests.post(url, headers=headers, files=files, data=data, timeout=30)
    r.raise_for_status()
    payload = r.json()
    return (payload.get("text") or "").strip()


def _groq_chat_response(user_text: str) -> str:
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY não configurada")

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    body = {
        "model": GROQ_CHAT_MODEL,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_text},
        ],
    }

    r = requests.post(url, headers=headers, json=body, timeout=30)
    r.raise_for_status()
    data = r.json()
    return (
        (data.get("choices") or [{}])[0]
        .get("message", {})
        .get("content", "")
        .strip()
    )


def _send_text_uazapi(number: str, text: str) -> None:
    if not UAZAPI_SEND_TOKEN:
        raise RuntimeError("UAZAPI_SEND_TOKEN não configurado")

    url = f"{UAZAPI_BASE_URL}/send/text"
    headers = {"token": UAZAPI_SEND_TOKEN, "Content-Type": "application/json"}
    payload = {"number": number, "text": text}
    r = requests.post(url, json=payload, headers=headers, timeout=UAZAPI_TIMEOUT)
    r.raise_for_status()


def _check_webhook_secret(request: Request) -> bool:
    if not WEBHOOK_SECRET:
        return True
    got = request.query_params.get("token") or request.headers.get("x-webhook-secret")
    return bool(got and got == WEBHOOK_SECRET)


async def _handle_webhook(request: Request):
    body = await request.json()
    print("[audio-test] webhook recebido")
    payload = _extract_uazapi_payload(body or {})
    msg_data = _extract_msg_data(body or {})
    print(f"[audio-test] event={payload.get('event')} message_type={payload.get('message_type')}")
    if not payload.get("message_type"):
        data = body.get("data") or {}
        alt_type = (msg_data.get("messageType") if isinstance(msg_data, dict) else None) or msg_data.get("type") or data.get("messageType")
        print(f"[audio-test] messageType fallback={alt_type}")

    if payload.get("from_me"):
        print("[audio-test] ignorado: from_me")
        return JSONResponse({"ok": True, "ignored": True})

    msg_type = payload.get("message_type")
    if not msg_type:
        data = body.get("data") or {}
        msg_type = (msg_data.get("messageType") if isinstance(msg_data, dict) else None) or msg_data.get("type") or data.get("messageType")
        if msg_type:
            msg_type = str(msg_type)
    msg_type_norm = (msg_type or "").strip().lower()
    if msg_type_norm != "audiomessage":
        print(f"[audio-test] ignorado: message_type={msg_type}")
        return JSONResponse({"ok": True, "ignored": True, "reason": "not_audio"})

    audio_msg = _extract_audio_message(body or {})
    audio_url = audio_msg.get("url")
    message_id = payload.get("message_id")
    if not message_id and isinstance(msg_data, dict):
        key = msg_data.get("key") or {}
        message_id = key.get("id") or msg_data.get("id")
    if not audio_url and not message_id:
        print("[audio-test] erro: audio_url e message_id ausentes")
        return JSONResponse({"ok": False, "error": "missing_audio_url_or_id"}, status_code=400)

    remote_jid = payload.get("remote_jid") or (msg_data.get("remoteJid") if isinstance(msg_data, dict) else "") or ""
    if not remote_jid and isinstance(msg_data, dict):
        key = msg_data.get("key") or {}
        remote_jid = key.get("remoteJid") or ""
    if not remote_jid and isinstance(body.get("data"), dict):
        key2 = (body.get("data") or {}).get("key") or {}
        remote_jid = key2.get("remoteJid") or ""
    if not remote_jid and isinstance(msg_data, dict):
        remote_jid = msg_data.get("from") or msg_data.get("sender") or msg_data.get("chatid") or ""
    number = _only_digits(remote_jid.split("@")[0])
    if not number:
        print("[audio-test] erro: número ausente")
        return JSONResponse({"ok": False, "error": "missing_number"}, status_code=400)

    try:
        if audio_url:
            print(f"[audio-test] baixando áudio por URL: {audio_url}")
            audio_bytes, mime = await _download_audio(audio_url)
        else:
            print(f"[audio-test] baixando áudio por ID: {message_id}")
            audio_bytes, mime = await _download_audio_by_id(str(message_id))
        transcript = _transcribe_audio(audio_bytes, mime)
        print(f"[audio-test] transcrição: {transcript}")
        if not transcript:
            _send_text_uazapi(number, "Não consegui entender o áudio. Pode repetir em texto?")
            return JSONResponse({"ok": True, "transcript": ""})

        ai_reply = _groq_chat_response(transcript)
        if not ai_reply:
            ai_reply = "Consegui transcrever o áudio, mas não gerei resposta."

        _send_text_uazapi(number, ai_reply)
        return JSONResponse({"ok": True, "transcript": transcript, "reply": ai_reply})
    except Exception as e:
        print(f"[audio-test] erro: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/webhook")
async def webhook_root(request: Request):
    if not _check_webhook_secret(request):
        return JSONResponse({"ok": False, "error": "invalid_webhook_token"}, status_code=401)
    return await _handle_webhook(request)


@app.post("/webhook/uazapi")
async def webhook_uazapi(request: Request):
    if not _check_webhook_secret(request):
        return JSONResponse({"ok": False, "error": "invalid_webhook_token"}, status_code=401)
    return await _handle_webhook(request)
