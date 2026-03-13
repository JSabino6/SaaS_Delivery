"""
Snippet de portfolio: validacao defensiva de argumentos para tools de um agente.

Este exemplo e sanitizado para demonstrar o padrao tecnico sem expor logica proprietaria.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable


ALLOWED_TOOLS = {
    "add_item",
    "remove_item",
    "set_delivery_address",
    "set_payment_method",
    "confirm_order",
}


TOOL_REQUIRED_FIELDS = {
    "add_item": ("sku", "qty"),
    "remove_item": ("sku",),
    "set_delivery_address": ("street", "number", "district"),
    "set_payment_method": ("method",),
    "confirm_order": tuple(),
}


TOOL_ALLOWED_FIELDS = { 
    "add_item": {"sku", "qty", "note"},
    "remove_item": {"sku"},
    "set_delivery_address": {"street", "number", "district", "complement"},
    "set_payment_method": {"method", "change_for"},
    "confirm_order": set(),
}


def _missing_fields(payload: Dict[str, Any], required: Iterable[str]) -> list[str]:
    return [key for key in required if not payload.get(key)]


def guard_tool_args(tool_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Valida ferramenta, contrato de campos e tamanho de payload."""
    if tool_name not in ALLOWED_TOOLS:
        return {
            "ok": False,
            "error": "tool_not_allowed",
            "detail": f"Tool '{tool_name}' nao permitida.",
        }

    if not isinstance(payload, dict):
        return {
            "ok": False,
            "error": "invalid_payload",
            "detail": "Payload precisa ser um objeto JSON.",
        }

    raw_size = len(str(payload))
    if raw_size > 2000:
        return {
            "ok": False,
            "error": "payload_too_large",
            "detail": "Payload acima do limite de seguranca.",
        }

    required = TOOL_REQUIRED_FIELDS.get(tool_name, tuple())
    missing = _missing_fields(payload, required)
    if missing:
        return {
            "ok": False,
            "error": "missing_fields",
            "detail": f"Campos obrigatorios ausentes: {', '.join(missing)}",
        }

    allowed = TOOL_ALLOWED_FIELDS.get(tool_name, set())
    unknown = [k for k in payload.keys() if k not in allowed]
    if unknown:
        return {
            "ok": False,
            "error": "unknown_fields",
            "detail": f"Campos nao permitidos: {', '.join(unknown)}",
        }

    return {
        "ok": True,
        "tool": tool_name,
        "sanitized_args": payload,
    }
