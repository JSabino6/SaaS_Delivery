import os
import json
from datetime import datetime, timedelta, timezone

import requests
import urllib3

from fastapi import Request, Response
from fastapi.responses import JSONResponse

from supabase import create_client, Client

try:
    import qrcode  # type: ignore
except Exception:
    qrcode = None

from utils import (
    SUPABASE_URL,
    SUPABASE_KEY,
    SUPABASE_TIMEOUT_SECONDS,
    PUBLIC_BASE_URL,
    CRON_SECRET,
    GROQ_API_KEY,
    ALLOW_QUERY_TOKEN_AUTH,
    MP_WEBHOOK_TOKEN,
    CACHE_PREFIX,
    CACHE_INVALIDATE_TOKEN,
    HTTP_VERIFY_TLS,
    ALLOW_ABANDONED_CLEANUP_WITHOUT_REDIS,
    CART_ABANDONED_REMINDER_MIN,
    CART_ABANDONED_CANCEL_MIN,
    MAX_ABANDONED_SWEEP,
    STATE_STALE_RESET_MIN,
    MAX_STATE_RESET_SWEEP,
    REPEAT_ORDER_LOOKBACK_DAYS,
    AVALIACAO_DELAY_MIN,
    MAX_AVALIACAO_SWEEP,
    logger,
    decrypt_secret,
    _money_2,
    _format_brl,
    _format_carrinho_display,
    _only_digits,
    _is_meio_a_meio_item,
    _match_bairro_from_input,
    _texto_parece_endereco,
    _texto_parece_bairro,
    _texto_e_so_bairro,
    _extract_bairro_from_text,
    extrair_endereco_de_texto,
    normalizar_texto,
    redis_client,
    redis_set_cache,
    redis_get_cache,
    redis_del_cache,
    redis_acquire_lock,
    redis_release_lock,
    _redis_setnx_once,
    sb_exec,
    _run_blocking,
)


if not HTTP_VERIFY_TLS:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class _MissingSupabase:
    def __getattr__(self, name):
        raise RuntimeError(
            "Supabase não configurado. Defina SUPABASE_URL e SUPABASE_KEY no ambiente/.env "
            "(ex.: rode o uvicorn com --env-file ..\\.env)"
        )


supabase: Client | _MissingSupabase = _MissingSupabase()
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"❌ Erro Config: {e}")


def extrair_precos_do_cardapio(texto_cardapio):
    tabela = {}
    if not texto_cardapio:
        return tabela
    padrao = r"(?:^|\n)[ \t]*[-*]?[ \t]*(.+?)(?::| -| --|R\$)\s*R?\$?\s*(\d+[.,]\d{2})"
    for linha in texto_cardapio.split("\n"):
        match = __import__("re").search(padrao, linha)
        if match:
            item_nome = match.group(1).strip()
            tabela[normalizar_texto(item_nome)] = float(match.group(2).replace(",", "."))
            palavras = item_nome.split()
            if len(palavras) > 0:
                tabela[normalizar_texto(palavras[0])] = float(match.group(2).replace(",", "."))
    return tabela


def carregar_taxas_bairros(restaurante_db_id):
    """
    Busca bairros na tabela 'bairros' e retorna um dicionário {nome_norm: valor}
    """
    try:
        resp = (
            supabase.table("bairros").select("*")
            .eq("restaurante_id", restaurante_db_id)
            .eq("ativo", True)
            .execute()
        )
        bairros = resp.data or []
        dict_taxas = {}
        for b in bairros:
            nome_norm = normalizar_texto(b["nome"])
            dict_taxas[nome_norm] = float(b["taxa"])
            dict_taxas[b["nome"]] = float(b["taxa"])
        return dict_taxas

    except Exception as e:
        print(f"❌ Erro ao carregar bairros: {e}")
        return {}


def extrair_taxas_entrega(texto_taxas):
    taxas = {}
    if not texto_taxas:
        return taxas
    padrao = r"(.*?)(?::| -| --|R\$)\s*R?\$?\s*([\d,.]+)"
    for linha in texto_taxas.split("\n"):
        match = __import__("re").search(padrao, linha)
        if match:
            bairro = normalizar_texto(match.group(1).strip())
            try:
                taxas[bairro] = float(match.group(2).replace(",", "."))
            except Exception:
                continue
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
            "p_nome_produto": nome_exato,
            "p_delta_qtd": int(delta_qtd),
        }

        resp = supabase.rpc("movimentar_estoque_seguro", params).execute()
        resultado = resp.data

        if resultado.get("sucesso"):
            logger.info("Estoque atualizado | item=%s | novo=%s", nome_exato, resultado.get("novo_estoque"))
            return True, resultado
        else:
            msg = str(resultado.get("msg") or "")
            msg_norm = msg.strip().lower()
            esperado = (
                msg_norm == "estoque insuficiente"
                or "não encontrado" in msg_norm
                or "nao encontrado" in msg_norm
                or "not found" in msg_norm
            )
            if not esperado:
                logger.warning("Falha estoque | item=%s | msg=%s", nome_exato, msg)
            return False, resultado

    except Exception as e:
        print(f"❌ Erro RPC Estoque: {e}")
        return False, {"msg": str(e)}


def carregar_cardapio_estruturado(restaurante_db_id):
    try:
        resp = (
            supabase.table("produtos").select("*")
            .eq("restaurante_id", restaurante_db_id)
            .eq("disponivel", True)
            .execute()
        )

        produtos = resp.data or []

        texto_para_ia = ""
        dict_precos = {}
        dict_estoque = {}
        dict_categorias = {}
        dict_aliases = {}
        categorias = {}
        produto_id_para_nome_norm = {}

        for p in produtos:
            estoque = p.get("estoque")

            cat = p.get("categoria", "Outros")
            cat_norm = normalizar_texto(cat)
            if cat not in categorias:
                categorias[cat] = []

            nome_norm = normalizar_texto(p["nome"])
            preco = float(p["preco"])
            dict_precos[nome_norm] = preco
            dict_estoque[nome_norm] = estoque
            dict_categorias[nome_norm] = cat_norm
            try:
                produto_id_para_nome_norm[int(p.get("id") or 0)] = nome_norm
            except Exception:
                pass

            partes = p["nome"].split()
            if len(partes) > 1:
                curto = normalizar_texto(partes[0])
                dict_precos[curto] = preco
                dict_estoque[curto] = estoque
                dict_categorias[curto] = cat_norm

            nome_visual = p["nome"]
            if estoque is not None and estoque <= 0:
                nome_visual += " 🚫 (ESGOTADO)"

            p_visual = p.copy()
            p_visual["nome_display"] = nome_visual
            categorias[cat].append(p_visual)

        # Aliases opcionais por produto (melhora matching textual sem alterar nomes oficiais)
        try:
            if produto_id_para_nome_norm:
                r_alias = (
                    supabase.table("produtos_aliases")
                    .select("produto_id,alias")
                    .eq("restaurante_id", int(restaurante_db_id))
                    .execute()
                )
                for row in (r_alias.data or []):
                    try:
                        pid = int((row or {}).get("produto_id") or 0)
                    except Exception:
                        pid = 0
                    alias_raw = str((row or {}).get("alias") or "").strip()
                    alias_norm = normalizar_texto(alias_raw)
                    nome_norm = produto_id_para_nome_norm.get(pid)
                    if alias_norm and nome_norm and alias_norm != nome_norm:
                        dict_aliases[alias_norm] = nome_norm
        except Exception:
            dict_aliases = {}

        for cat, itens in categorias.items():
            texto_para_ia += f"\n--- {cat.upper()} ---\n"
            for item in itens:
                desc = f" ({item['descricao']})" if item.get("descricao") else ""
                texto_para_ia += f"- {item['nome_display']}{desc}: R$ {item['preco']:.2f}\n"

        return texto_para_ia, dict_precos, dict_estoque, dict_categorias, dict_aliases

    except Exception as e:
        print(f"❌ Erro ao carregar produtos: {e}")
        return "", {}, {}, {}, {}


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
            restaurante_db_id = dados["id"]

            txt_cardapio, dict_precos, dict_estoque, dict_categorias, dict_aliases = carregar_cardapio_estruturado(restaurante_db_id)

            if txt_cardapio:
                dados["cardapio"] = txt_cardapio
                dados["precos_dict"] = dict_precos
                dados["estoque_dict"] = dict_estoque
                dados["categorias_dict"] = dict_categorias
                dados["produtos_aliases_dict"] = dict_aliases or {}
            else:
                dados["precos_dict"] = extrair_precos_do_cardapio(dados.get("cardapio", ""))
                dados["estoque_dict"] = {}
                dados["categorias_dict"] = {}
                dados["produtos_aliases_dict"] = {}

            taxas_dict = carregar_taxas_bairros(restaurante_db_id)
            if not taxas_dict:
                taxas_dict = extrair_taxas_entrega(dados.get("taxas_entrega", "") or "")
            dados["taxas_dict"] = taxas_dict or {}

            redis_set_cache(f"{CACHE_PREFIX}:restaurante:phone_id:{dados['phone_id']}", dados, 600)
            redis_set_cache(f"{CACHE_PREFIX}:restaurante:instance_name:{dados['instance_name']}", dados, 600)

            return dados

    except Exception as e:
        print(f"❌ Erro Banco Rest: {e}")
        import traceback
        traceback.print_exc()

    return None


def get_pedido_ativo(cliente_zap, restaurante_db_id):
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
    except Exception:
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


def get_estado(cliente_zap, phone_id):
    try:
        resp = supabase.table("clientes_estado").select("*")\
            .eq("cliente_zap", cliente_zap).eq("restaurante_id", phone_id).execute()
        if resp.data:
            return resp.data[0]
    except Exception:
        pass
    return None


def set_estado(cliente_zap, phone_id, novo_estado, dados_extras=None):
    try:
        payload = {
            "cliente_zap": cliente_zap,
            "restaurante_id": phone_id,
            "estado_conversa": novo_estado,
            "ultima_mensagem_em": datetime.now(timezone.utc).isoformat(),
        }
        if dados_extras:
            payload["dados_parciais"] = dados_extras

        supabase.table("clientes_estado").upsert(payload).execute()
    except Exception as e:
        print(f"❌ Erro Set Estado: {e}")


def incrementar_metricas_restaurante(
    restaurante_db_id: int,
    *,
    pedidos_total: int = 0,
    ia_calls: int = 0,
    ia_prompt_tokens: int = 0,
    ia_completion_tokens: int = 0,
    ia_audio_calls: int = 0,
    redis_ops: int = 0,
) -> bool:
    """Incrementa métricas diárias por restaurante (best-effort)."""
    try:
        rid = int(restaurante_db_id)
        if rid <= 0:
            return False

        inc = {
            "pedidos_total": int(pedidos_total or 0),
            "ia_calls": int(ia_calls or 0),
            "ia_prompt_tokens": int(ia_prompt_tokens or 0),
            "ia_completion_tokens": int(ia_completion_tokens or 0),
            "ia_audio_calls": int(ia_audio_calls or 0),
            "redis_ops": int(redis_ops or 0),
        }
        if not any(v != 0 for v in inc.values()):
            return True

        today = datetime.now(timezone.utc).date().isoformat()
        lock_key = f"lock:metricas:{rid}:{today}"
        lock_token = ""
        if redis_client:
            lock_token = redis_acquire_lock(lock_key, ttl_seconds=10)
            if not lock_token:
                return False

        try:
            resp = (
                supabase.table("metricas_gastos_restaurante")
                .select("*")
                .eq("restaurante_id", rid)
                .eq("periodo", today)
                .limit(1)
                .execute()
            )

            if resp.data:
                row = resp.data[0] or {}
                payload = {
                    "pedidos_total": int(row.get("pedidos_total") or 0) + inc["pedidos_total"],
                    "ia_calls": int(row.get("ia_calls") or 0) + inc["ia_calls"],
                    "ia_prompt_tokens": int(row.get("ia_prompt_tokens") or 0) + inc["ia_prompt_tokens"],
                    "ia_completion_tokens": int(row.get("ia_completion_tokens") or 0) + inc["ia_completion_tokens"],
                    "ia_audio_calls": int(row.get("ia_audio_calls") or 0) + inc["ia_audio_calls"],
                    "redis_ops": int(row.get("redis_ops") or 0) + inc["redis_ops"],
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                (
                    supabase.table("metricas_gastos_restaurante")
                    .update(payload)
                    .eq("id", int(row.get("id") or 0))
                    .execute()
                )
                return True

            payload = {
                "restaurante_id": rid,
                "periodo": today,
                "pedidos_total": inc["pedidos_total"],
                "ia_calls": inc["ia_calls"],
                "ia_prompt_tokens": inc["ia_prompt_tokens"],
                "ia_completion_tokens": inc["ia_completion_tokens"],
                "ia_audio_calls": inc["ia_audio_calls"],
                "redis_ops": inc["redis_ops"],
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            supabase.table("metricas_gastos_restaurante").insert(payload).execute()
            return True
        finally:
            if lock_token:
                redis_release_lock(lock_key, lock_token)
    except Exception:
        return False


def get_cliente_profile(restaurante_db_id: int, cliente_zap: str) -> dict | None:
    try:
        resp = (
            supabase.table("clientes_perfil")
            .select("*")
            .eq("restaurante_id", int(restaurante_db_id))
            .eq("cliente_zap", str(cliente_zap or "").strip())
            .limit(1)
            .execute()
        )
        if resp.data:
            return resp.data[0]
    except Exception:
        pass
    return None


def upsert_cliente_profile(
    restaurante_db_id: int,
    cliente_zap: str,
    *,
    tipo_entrega: str | None = None,
    endereco_txt: str | None = None,
    bairro: str | None = None,
    forma_pagamento: str | None = None,
) -> bool:
    try:
        cliente = str(cliente_zap or "").strip()
        if not cliente:
            return False

        te = str(tipo_entrega or "").strip().lower()
        if te not in ("entrega", "retirada"):
            te = ""

        fp = str(forma_pagamento or "").strip().lower()
        if fp not in ("pix", "dinheiro", "cartao"):
            fp = ""

        payload = {
            "restaurante_id": int(restaurante_db_id),
            "cliente_zap": cliente,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        if te:
            payload["tipo_entrega_favorita"] = te
        if fp:
            payload["forma_pagamento_favorita"] = fp

        end = str(endereco_txt or "").strip()
        bai = str(bairro or "").strip()
        if te == "entrega" and end:
            payload["endereco_favorito"] = end
            if bai:
                payload["bairro_favorito"] = bai

        supabase.table("clientes_perfil").upsert(payload, on_conflict="restaurante_id,cliente_zap").execute()
        return True
    except Exception:
        return False


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
    from zap import enviar_zap_async

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

            # Se for meio a meio, pula a reserva de estoque
            if _is_meio_a_meio_item(str(chave_item), dados_item):
                continue

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
    got = (
        request.headers.get("x-cron-secret")
        or request.headers.get("x-cron-token")
        or (request.query_params.get("token") if ALLOW_QUERY_TOKEN_AUTH else None)
    )
    return bool(got and got == CRON_SECRET)


def _abandoned_cleanup_pedido(restaurante_db_id: int, phone_id: str, cliente_zap: str, pedido: dict) -> bool:
    """Cancela + limpa carrinho + devolve estoque (idempotência via Redis lock/dedup)."""
    from zap import enviar_zap

    if not redis_client:
        return False
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


def _abandoned_send_reminder(phone_id: str, cliente_zap: str, pedido: dict, minutes_left: int) -> bool:
    """Envia lembrete 1x (dedup via Redis)."""
    from zap import enviar_zap

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
    from zap import enviar_zap

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
                        from zap import enviar_zap_async
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


def _cache_invalidate_authed(request: Request) -> bool:
    """Auth for cache invalidation endpoint (shared secret)."""
    if not CACHE_INVALIDATE_TOKEN:
        return False
    got = (
        request.headers.get("x-cache-invalidate-token")
        or request.headers.get("x-cache-token")
        or request.headers.get("x-admin-token")
        or (request.query_params.get("token") if ALLOW_QUERY_TOKEN_AUTH else None)
    )
    return bool(got and got == CACHE_INVALIDATE_TOKEN)


def _admin_authed(request: Request) -> bool:
    """Auth for admin endpoints (shared secret)."""
    return _cache_invalidate_authed(request)


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
        phone_id = row.get("restaurante_id")
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
            continue

        minutes_left = CART_ABANDONED_CANCEL_MIN - minutes_inactive
        if await _run_blocking(lambda: _abandoned_send_reminder(phone_id, cliente_zap, pedido, minutes_left=minutes_left), timeout=SUPABASE_TIMEOUT_SECONDS):
            reminded += 1

    return {"status": "ok", "scanned": scanned, "reminded": reminded, "cleaned": cleaned}


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


def _should_reset_state_by_inactivity(estado: str) -> bool:
    if not estado:
        return False
    est = estado.strip().upper()
    if est in ("INICIO",):
        return False
    if est in ("AGUARDANDO_PAGAMENTO_PIX",):
        return False
    return True


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


def health_check():
    details = {
        "supabase": {"status": "ok"},
        "redis": {"status": "ok" if redis_client else "disabled"},
        "groq": {"status": "configured" if GROQ_API_KEY else "not_configured"},
    }

    status = "ok"

    try:
        supabase.table("restaurantes").select("id").limit(1).execute()
    except Exception as e:
        details["supabase"] = {"status": "error", "detail": str(e)[:240]}
        status = "degraded"

    if redis_client:
        try:
            redis_client.ping()
        except Exception as e:
            details["redis"] = {"status": "error", "detail": str(e)[:240]}
            status = "degraded"

    return {"status": status, "dependencies": details}
