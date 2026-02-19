import os
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from health_startup import startup_diagnostics
from utils import logger

import banco
import zap


app = FastAPI(
    title="AI Atendimento API",
    version=os.getenv("APP_VERSION", "1.0.0"),
    description="Backend do projeto AI Atendimento (webhooks, pedidos, estoque, Pix e rotinas cron).",
)


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


@app.get("/payments/qr/{payment_id}.png")
async def payment_qr_png(payment_id: str):
    return banco.payment_qr_png(payment_id)


@app.post("/webhook/mercadopago")
async def webhook_mercadopago(request: Request):
    return await banco.webhook_mercadopago(request)


@app.get("/health")
async def health():
    return banco.health_check()


@app.post("/admin/cache/invalidate")
async def admin_cache_invalidate(request: Request):
    return await banco.admin_cache_invalidate(request)


@app.post("/admin/chat/toggle_pause")
async def admin_chat_toggle_pause(request: Request):
    return await banco.admin_chat_toggle_pause(request)


@app.get("/cron/abandoned-carts")
async def cron_abandoned_carts(request: Request):
    return await banco.cron_abandoned_carts(request)


@app.get("/cron/reset-states")
async def cron_reset_states(request: Request):
    return await banco.cron_reset_states(request)


@app.get("/cron/avaliar")
async def cron_avaliar(request: Request):
    return await banco.cron_avaliar(request)


@app.post("/webhook")
async def webhook(request: Request):
    return await zap.webhook(request)
