import streamlit as st
import os
import sys
from pathlib import Path
import pandas as pd
import altair as alt
import time
from supabase import create_client
try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None


_WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
if str(_WORKSPACE_ROOT) not in sys.path:
    sys.path.append(str(_WORKSPACE_ROOT))
from logging_setup import setup_logging

logger = setup_logging("dashboard", log_dir=os.getenv("LOG_DIR") or str(_WORKSPACE_ROOT / "logs"))
from dotenv import load_dotenv
import os
import base64
from PIL import Image
from io import BytesIO
import requests
import re
from urllib.parse import quote
import secrets
from datetime import datetime, timezone
from datetime import timedelta

try:
    from cryptography.fernet import Fernet
except Exception:
    Fernet = None



st.set_page_config(page_title="Gestor Uazapi SaaS", layout="wide", page_icon="🍕")

# Garante carregar o .env da raiz do workspace (mesmo se o Streamlit for iniciado de outra pasta)
load_dotenv(dotenv_path=str(_WORKSPACE_ROOT / ".env"))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
UAZAPI_BASE_URL = "https://free.uazapi.com"


HTTP_VERIFY_TLS = os.getenv("HTTP_VERIFY_TLS", "true").strip().lower() in ("1", "true", "yes")


CRED_ENCRYPTION_KEY = os.getenv("CRED_ENCRYPTION_KEY")
PUBLIC_BASE_URL = (os.getenv("PUBLIC_BASE_URL") or "").strip().rstrip("/")
MP_WEBHOOK_TOKEN = os.getenv("MP_WEBHOOK_TOKEN")

CACHE_INVALIDATE_TOKEN = os.getenv("CACHE_INVALIDATE_TOKEN")
CACHE_INVALIDATE_URL = (os.getenv("CACHE_INVALIDATE_URL") or PUBLIC_BASE_URL).rstrip("/")

ADMIN_USER = (os.getenv("ADMIN_USER") or "").strip()
ADMIN_PASS = (os.getenv("ADMIN_PASS") or "").strip()

def _fernet():
    if not CRED_ENCRYPTION_KEY or not Fernet:
        return None
    try:
        key = CRED_ENCRYPTION_KEY.encode() if isinstance(CRED_ENCRYPTION_KEY, str) else CRED_ENCRYPTION_KEY
        return Fernet(key)
    except Exception:
        return None

def encrypt_secret(raw: str) -> str:
    raw = (raw or "").strip()
    f = _fernet()
    if not raw or not f:
        return ""
    return f.encrypt(raw.encode()).decode()

@st.cache_resource
def init_connection():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = init_connection()

if "user_role" not in st.session_state:
    st.session_state.user_role = None
if "user_data" not in st.session_state:
    st.session_state.user_data = None
if "motoboy_data" not in st.session_state:
    st.session_state.motoboy_data = None




def enviar_mensagem_ativa(dados_restaurante, numero, texto):
    try:
        url = f"{UAZAPI_BASE_URL}/send/text"
        headers = {
            "token": dados_restaurante['instance_token'].strip(),
            "Content-Type": "application/json"
        }
        requests.post(url, json={"number": numero, "text": texto}, headers=headers, verify=HTTP_VERIFY_TLS, timeout=15)
    except:
        pass


def invalidate_api_cache(phone_id: str, instance_name: str | None = None) -> bool:
    """Ask API to invalidate Redis cache for this restaurant (best-effort)."""
    if not CACHE_INVALIDATE_URL or not CACHE_INVALIDATE_TOKEN:
        return False
    try:
        url = f"{CACHE_INVALIDATE_URL}/admin/cache/invalidate"
        headers = {"x-cache-invalidate-token": str(CACHE_INVALIDATE_TOKEN)}
        payload = {
            "phone_id": str(phone_id or "").strip(),
            "instance_name": str(instance_name or "").strip(),
        }
        r = requests.post(url, json=payload, headers=headers, verify=HTTP_VERIFY_TLS, timeout=8)
        return r.status_code == 200
    except Exception:
        return False


def toggle_chat_pause(phone_id: str, cliente_zap: str, minutes: int) -> dict | None:
    """Pause/resume AI for a given customer (best-effort). Returns JSON dict on success."""
    if not CACHE_INVALIDATE_URL or not CACHE_INVALIDATE_TOKEN:
        return None
    try:
        url = f"{CACHE_INVALIDATE_URL}/admin/chat/toggle_pause"
        headers = {"x-cache-invalidate-token": str(CACHE_INVALIDATE_TOKEN)}
        payload = {
            "phone_id": str(phone_id or "").strip(),
            "cliente_zap": str(cliente_zap or "").strip(),
            "minutes": int(minutes or 0),
        }
        r = requests.post(url, json=payload, headers=headers, verify=HTTP_VERIFY_TLS, timeout=8)
        if r.status_code != 200:
            return None
        out = r.json()
        return out if isinstance(out, dict) else None
    except Exception:
        return None


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

def _only_digits(v: str) -> str:
    return re.sub(r"\D", "", str(v or "")).strip()

def _maps_link(endereco: str) -> str:
    q = quote((endereco or "").strip())
    return f"https://www.google.com/maps/search/?api=1&query={q}" if q else ""

def _waze_link(endereco: str) -> str:
    q = quote((endereco or "").strip())
    return f"https://waze.com/ul?q={q}&navigate=yes" if q else ""

def _generate_pin(length: int = 4) -> str:

    length = 4 if not isinstance(length, int) or length < 4 else length
    digits = "0123456789"
    return "".join(secrets.choice(digits) for _ in range(length))


def _normalize_product_text(raw: str) -> str:
    txt = str(raw or "")
    txt = txt.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _normalize_name_key(raw: str) -> str:
    txt = _normalize_product_text(raw).lower()
    txt = re.sub(r"[^a-z0-9à-ÿ ]", "", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _is_pizza_categoria(cat: str) -> bool:
    c = _normalize_name_key(cat)
    return "pizza" in c


def _has_size_token(nome: str) -> bool:
    n = _normalize_name_key(nome)
    return bool(re.search(r"\b(pequena|media|média|grande|gigante|familia|família|brotinho|p|m|g)\b", n))


def _has_volume_token(nome: str, descricao: str) -> bool:
    txt = f"{_normalize_product_text(nome)} {_normalize_product_text(descricao)}".lower()
    return bool(re.search(r"\b\d+(?:[\.,]\d+)?\s?(ml|l)\b", txt))


@st.cache_data(ttl=120)
def carregar_metricas_periodo(data_ini_iso: str, data_fim_iso: str):
    try:
        resp = (
            supabase.table("metricas_gastos_restaurante")
            .select("restaurante_id,periodo,pedidos_total,ia_calls,ia_prompt_tokens,ia_completion_tokens,ia_audio_calls,redis_ops")
            .gte("periodo", data_ini_iso)
            .lte("periodo", data_fim_iso)
            .execute()
        )
        return resp.data or []
    except Exception:
        return []


@st.cache_data(ttl=120)
def carregar_pedidos_periodo(data_ini_iso: str, data_fim_iso: str):
    try:
        dt_ini = f"{data_ini_iso}T00:00:00+00:00"
        dt_fim = f"{data_fim_iso}T23:59:59+00:00"
        resp = (
            supabase.table("pedidos")
            .select("restaurante_id,created_at")
            .gte("created_at", dt_ini)
            .lte("created_at", dt_fim)
            .limit(50000)
            .execute()
        )
        return resp.data or []
    except Exception:
        return []




@st.cache_data(ttl=30)
def verificar_status_whatsapp(instance_name, token):
    headers = {"Authorization": f"Bearer {token}"}
    try:

        r = requests.get(
            f"{UAZAPI_BASE_URL}/instance/status/{instance_name}", 
            headers={"token": token},
            timeout=5,
            verify=HTTP_VERIFY_TLS
        )
        return r.json()
    except:
        return {}

def widget_conexao_whatsapp(instance_name, token):
    try:


        if st.sidebar.button("🔄 Checar Conexão"):
            st.cache_data.clear() # Limpa cache para forçar verificação
            st.rerun()
            
        st.sidebar.info(f"Instância: {instance_name}")

    except Exception as e:
        st.sidebar.warning("Verificando conexão...")




def login_page():
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.title("🍕 Portal do Parceiro")
        with st.form("login_form"):
            usuario = st.text_input("Usuário")
            senha = st.text_input("Senha", type="password")
            if st.form_submit_button("ENTRAR", use_container_width=True):
                admin_ok = bool(ADMIN_USER and ADMIN_PASS)
                if admin_ok and usuario == ADMIN_USER and senha == ADMIN_PASS:
                    st.session_state.user_role = "admin"
                    st.session_state.user_data = None
                    st.session_state.motoboy_data = None
                    st.rerun()
                else:
                    res = supabase.table("restaurantes").select("*").eq("usuario", usuario).eq("senha", senha).execute()
                    if res.data:
                        st.session_state.user_role = "client"
                        st.session_state.user_data = res.data[0]
                        st.session_state.motoboy_data = None
                        st.rerun()
                    else:

                        tel = _only_digits(usuario)
                        try:
                            r_m = supabase.table("motoboys").select("*")\
                                .eq("telefone", tel)\
                                .eq("senha", senha)\
                                .eq("ativo", True)\
                                .execute()
                            if r_m.data:
                                st.session_state.user_role = "motoboy"
                                st.session_state.motoboy_data = r_m.data[0]
                                st.session_state.user_data = None
                                st.rerun()
                        except Exception:
                            pass

                        st.error("Usuário/telefone ou senha incorretos.")


def motoboy_page():
    m = st.session_state.motoboy_data or {}
    motoboy_id = m.get("id")
    restaurante_id = m.get("restaurante_id")

    st.sidebar.header("🛵 Painel do Motoboy")
    if st.sidebar.button("Sair"):
        st.session_state.user_role = None
        st.session_state.motoboy_data = None
        st.rerun()

    st.title(f"🛵 {m.get('nome', 'Motoboy')}")
    st.caption(f"Telefone: {m.get('telefone', '-')}")


    try:
        r = supabase.table("restaurantes").select("id,nome").eq("id", restaurante_id).execute().data
        if r:
            st.info(f"Restaurante: {r[0].get('nome','-')}")
    except Exception:
        pass


    try:
        entregas_abertas = supabase.table("entregas").select("*")\
            .eq("motoboy_id", motoboy_id)\
            .eq("status", "encaminhado")\
            .order("encaminhado_em", desc=True)\
            .execute().data or []
    except Exception:
        st.error("A tabela 'entregas' ainda não existe no Supabase (ou a permissão não permite acesso).")
        st.caption("Rode o SQL em supabase_entregas.sql e recarregue.")
        return

    st.subheader("📦 Entregas pendentes")
    if not entregas_abertas:
        st.info("Nenhuma entrega pendente no momento.")
    else:
        for e in entregas_abertas:
            with st.container(border=True):
                st.markdown(f"### Pedido #{e.get('pedido_id')}")
                end = (e.get("endereco") or "").strip()
                if end:
                    st.write(f"📍 {end}")
                if e.get("maps_url"):
                    st.markdown(f"🗺️ [Google Maps]({e.get('maps_url')})")
                if e.get("waze_url"):
                    st.markdown(f"🚗 [Waze]({e.get('waze_url')})")

                c1, c2 = st.columns([2, 1])
                with c1:
                    if st.button("✅ Marcar como entregue", key=f"delivered_{e.get('id')}", type="primary", use_container_width=True):
                        try:
                            now = datetime.now(timezone.utc).isoformat()
                            supabase.table("entregas").update({"status": "entregue", "entregue_em": now}).eq("id", e["id"]).execute()
                            st.success("Entrega marcada como entregue.")
                            time.sleep(0.4)
                            st.rerun()
                        except Exception as ex:
                            st.error(f"Erro ao marcar entregue: {ex}")
                with c2:
                    st.caption(str(e.get("encaminhado_em") or ""))

    st.markdown("---")
    st.subheader("📜 Histórico (últimas entregas)")
    try:
        hist = supabase.table("entregas").select("pedido_id,endereco,entregue_em")\
            .eq("motoboy_id", motoboy_id)\
            .eq("status", "entregue")\
            .order("entregue_em", desc=True)\
            .limit(30)\
            .execute().data or []
    except Exception:
        hist = []

    if not hist:
        st.info("Sem entregas finalizadas ainda.")
    else:
        for h in hist:
            st.write(f"Pedido #{h.get('pedido_id')} — {str(h.get('entregue_em') or '')[:16].replace('T',' ')}")







def restaurant_page():
    dados = st.session_state.user_data

    restaurante_db_id = dados.get('id') 
    phone_id = dados['phone_id']

    st.sidebar.title(f"🏠 {dados['nome']}")
    



    res_notas = supabase.table("pedidos").select("avaliacao")\
        .eq("restaurante_id", restaurante_db_id)\
        .not_.is_("avaliacao", "null")\
        .execute()
    
    notas = [n['avaliacao'] for n in res_notas.data]
    
    if notas:
        media = sum(notas) / len(notas)
        qtd = len(notas)
        st.sidebar.metric(label="⭐ Nota Média", value=f"{media:.1f}/5.0", delta=f"{qtd} votos")
    else:
        st.sidebar.info("Sem avaliações ainda.")


    if dados.get("instance_name") and dados.get("instance_token"):
        widget_conexao_whatsapp(dados["instance_name"], dados["instance_token"])

    if st.sidebar.button("Sair"):
        st.session_state.user_role = None
        st.rerun()

    aba = st.sidebar.radio("Menu", ["Pedidos (Live)", "Métricas", "Gestão de Cardápio", "Configurações", "Motoboys"])


    if aba == "Pedidos (Live)":
        st.header("🔥 Gestão de Pedidos")

        if st_autorefresh:
            st_autorefresh(interval=10_000, key="pedidos_live_autorefresh")
        else:
            if st.button("Atualizar agora"):
                st.rerun()
            st.caption("Auto-refresh indisponível (instale streamlit-autorefresh para atualizar automaticamente).")


        try:
            pend = supabase.table("pedidos").select("id,payment_status,forma_pagamento")\
                .eq("restaurante_id", restaurante_db_id)\
                .in_("payment_status", ["pending", "in_process"])\
                .execute().data or []
            pend = [p for p in pend if 'whatsapp' in str(p.get('forma_pagamento') or '').lower()]
            if pend:
                st.warning(f"💠 {len(pend)} pedido(s) aguardando pagamento Pix no WhatsApp")
        except Exception:
            pass
        res_prods = supabase.table("produtos").select("nome, preco")\
            .eq("restaurante_id", restaurante_db_id)\
            .eq("disponivel", True)\
            .order("nome")\
            .execute()
        lista_produtos_raw = res_prods.data or []
        lista_produtos_nomes = [p['nome'] for p in lista_produtos_raw]
        dict_precos_produtos = {p['nome']: p['preco'] for p in lista_produtos_raw}


        status_filter = st.multiselect(
            "Filtrar status",
            ["novo", "confirmado", "em preparo", "saiu para entrega", "finalizado", "cancelado"],
            default=["novo", "confirmado", "em preparo"]
        )


        query = supabase.table("pedidos").select("*").eq("restaurante_id", restaurante_db_id).order("created_at", desc=True)
        if status_filter:
            query = query.in_("status", status_filter)

        pedidos = query.execute().data or []

        if not pedidos:
            st.info("Nenhum pedido encontrado com os filtros atuais.")

        for p in pedidos:
            with st.container(border=True):
                c1, c2, c3 = st.columns([3, 2, 2])

                with c1:
                    st.markdown(f"### #{p['id']} - {p['cliente_nome']}")
                    

                    if p.get('avaliacao'):
                        estrelas = "⭐" * int(p['avaliacao'])
                        st.markdown(f"**Avaliação:** {estrelas} ({p['avaliacao']}/5)")


                    st.caption(f"📞 {p['cliente_zap']} | 🕒 {p['created_at'][11:16]}")
                    
                    resumo = p.get("resumo_pedido", "")
                    
                    if "|" in resumo:
                        linhas = resumo.split("|")
                        for linha in linhas:
                            st.markdown(f"- {linha.strip()}")
                    else:
                        st.text(resumo)

                    val = float(p.get('total_valor') or 0)
                    st.markdown(f"**Total: R$ {val:.2f}** ({p.get('forma_pagamento', '-')})")

                    pay_status = (p.get('payment_status') or "").lower()
                    if pay_status:
                        paid_amount = p.get('payment_amount')
                        if pay_status == 'approved':
                            st.success(f"Pago via Pix: R$ {float(paid_amount or val):.2f}")
                        elif pay_status in ('pending', 'in_process'):
                            st.error("💠 AGUARDANDO PAGAMENTO PIX")
                        else:
                            st.info(f"Pagamento: {pay_status}")
                    
                    if p.get('endereco_completo'):
                        tipo = p.get('tipo_entrega', 'entrega').upper()
                        st.info(f"📍 {tipo}: {p['endereco_completo']}")

                with c2:
                    st.write(f"Status: `{p['status'].upper()}`")

                    # Indicadores de atendimento do bot
                    bot_finalizado_raw = p.get("bot_finalizado", None)
                    bot_finalizado = bool(bot_finalizado_raw) if bot_finalizado_raw is not None else False

                    # Fallback: se a coluna ainda não existe/está nula, infere por campos preenchidos
                    if bot_finalizado_raw is None:
                        tipo_ent = str(p.get("tipo_entrega") or "").strip().lower()
                        has_end = bool((p.get("endereco_completo") or "").strip()) or (tipo_ent == "retirada")
                        has_pay = bool((p.get("forma_pagamento") or "").strip())
                        try:
                            has_total = float(p.get("total_valor") or 0) > 0
                        except Exception:
                            has_total = False
                        bot_finalizado = (str(p.get("status") or "").strip().lower() in ("confirmado",)) and has_end and has_pay and has_total
                    if not bot_finalizado:
                        st.info("🤖 Em atendimento (bot ainda não finalizou)")

                        # Abandono: se o cliente não fala há um tempo
                        last = p.get("last_cliente_msg_at") or p.get("updated_at") or p.get("created_at")
                        dt_last = _parse_dt_utc(last)
                        if dt_last:
                            mins = int((datetime.now(timezone.utc) - dt_last).total_seconds() // 60)
                            if mins >= 5:
                                st.warning(f"⏳ Abandonado há {mins} min")

                    if bool(p.get("needs_human", False)):
                        st.error("🧑 Atendimento humano solicitado")
                        if st.button("✅ Marcar como resolvido", key=f"nh_{p['id']}", use_container_width=True):
                            try:
                                supabase.table("pedidos").update({
                                    "needs_human": False,
                                    "needs_human_resolved_at": datetime.now(timezone.utc).isoformat(),
                                }).eq("id", p["id"]).execute()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Erro ao resolver: {e}")

                    pay_status = (p.get('payment_status') or "").lower()
                    pix_pendente = pay_status in ('pending', 'in_process') and 'whatsapp' in str(p.get('forma_pagamento') or '').lower()

                    if pix_pendente:
                        st.error("Bloqueado até o Pix ser aprovado")

                    can_accept = bot_finalizado and (p.get('status') in ["novo", "confirmado"]) and (not pix_pendente)

                    if p['status'] in ["novo", "confirmado"]:
                        if st.button("✅ ACEITAR", key=f"a_{p['id']}", use_container_width=True, disabled=(not can_accept)):
                            supabase.table("pedidos").update({"status": "em preparo"}).eq("id", p['id']).execute()
                            enviar_mensagem_ativa(dados, p['cliente_zap'], f"✅ Pedido #{p['id']} aceito! Em instantes iniciaremos o preparo.")
                            st.rerun()

                    elif p['status'] == "em preparo":
                        if st.button("🛵 SAIU P/ ENTREGA", key=f"s_{p['id']}", use_container_width=True, disabled=pix_pendente):
                            supabase.table("pedidos").update({"status": "saiu para entrega"}).eq("id", p['id']).execute()
                            extra = ""
                            try:

                                e = supabase.table("entregas").select("motoboy_id,motoboy_nome")\
                                    .eq("restaurante_id", restaurante_db_id)\
                                    .eq("pedido_id", p["id"])\
                                    .eq("status", "encaminhado")\
                                    .order("encaminhado_em", desc=True)\
                                    .limit(1)\
                                    .execute().data
                                e = (e or [None])[0]
                                if e and e.get("motoboy_id"):
                                    m = supabase.table("motoboys").select("nome,placa")\
                                        .eq("id", e["motoboy_id"])\
                                        .limit(1)\
                                        .execute().data
                                    m = (m or [None])[0]
                                    nome_m = (m or {}).get("nome") or e.get("motoboy_nome")
                                    placa_m = (m or {}).get("placa")
                                    if nome_m and placa_m:
                                        extra = f"\n\nO entregador {nome_m} (Placa {placa_m}) está a caminho!"
                                    elif nome_m:
                                        extra = f"\n\nO entregador {nome_m} está a caminho!"
                            except Exception:
                                pass

                            enviar_mensagem_ativa(dados, p['cliente_zap'], f"🛵 Pedido #{p['id']} saiu para entrega! Fique atento.{extra}")
                            st.rerun()

                    elif p['status'] == "saiu para entrega":
                        if st.button("🏁 ENTREGUE (Finalizar)", key=f"f_{p['id']}", type="primary", use_container_width=True, disabled=pix_pendente):

                            agora = datetime.now(timezone.utc).isoformat()
                            
                            supabase.table("pedidos").update({
                                "status": "finalizado",
                                "finalizado_em": agora
                            }).eq("id", p['id']).execute()
                            
                            enviar_mensagem_ativa(dados, p['cliente_zap'], f"✅ Pedido #{p['id']} entregue. Bom apetite! 🍕")
                            st.rerun()
                    
                    elif p['status'] == "finalizado":
                        st.success("Concluído")

                    if p['status'] not in ["cancelado", "finalizado"]:
                        if st.button("❌ CANCELAR", key=f"c_{p['id']}", use_container_width=True):
                            supabase.table("pedidos").update({"status": "cancelado"}).eq("id", p['id']).execute()
                            enviar_mensagem_ativa(dados, p['cliente_zap'], f"⚠️ Pedido #{p['id']} cancelado pelo restaurante.")
                            st.rerun()

                with c3:
                    with st.expander("💬 Chat"):
                        # ---- Pause AI per customer (10/15min) ----
                        try:
                            phone_id_for_pause = str(dados.get("phone_id") or "").strip()
                            cliente_for_pause = str(p.get("cliente_zap") or "").strip()
                            paused_until = None
                            if phone_id_for_pause and cliente_for_pause:
                                row = supabase.table("clientes_estado")\
                                    .select("ai_paused_until")\
                                    .eq("cliente_zap", cliente_for_pause)\
                                    .eq("restaurante_id", phone_id_for_pause)\
                                    .limit(1)\
                                    .execute().data
                                row = (row or [None])[0] or {}
                                paused_until = _parse_dt_utc(row.get("ai_paused_until"))

                            is_paused = bool(paused_until and paused_until > datetime.now(timezone.utc))
                            if is_paused:
                                st.error(f"🔴 IA Pausada (até {paused_until.astimezone(timezone.utc).strftime('%H:%M')} UTC)")
                                if st.button("Retomar Agora", key=f"resume_ai_{p['id']}", use_container_width=True):
                                    ok = toggle_chat_pause(phone_id_for_pause, cliente_for_pause, 0)
                                    if ok:
                                        st.success("IA retomada!")
                                        time.sleep(0.3)
                                        st.rerun()
                                    else:
                                        st.warning("Não consegui retomar via API. Verifique CACHE_INVALIDATE_URL/TOKEN.")
                            else:
                                st.success("🟢 IA Ativa")
                                if st.button("Pausar por 15min", key=f"pause_ai_{p['id']}", use_container_width=True):
                                    ok = toggle_chat_pause(phone_id_for_pause, cliente_for_pause, 15)
                                    if ok:
                                        st.success("IA pausada por 15 min.")
                                        time.sleep(0.3)
                                        st.rerun()
                                    else:
                                        st.warning("Não consegui pausar via API. Verifique CACHE_INVALIDATE_URL/TOKEN.")
                        except Exception:
                            pass

                        msg = st.text_input("Escrever...", key=f"m_{p['id']}")
                        if st.button("Enviar", key=f"e_{p['id']}"):
                            enviar_mensagem_ativa(dados, p['cliente_zap'], msg)
                            st.success("Enviada!")


                with st.expander("✏️ Editar / Alterar Itens do Pedido"):
                    carrinho_atual = p.get('carrinho_json') or {}
                    

                    lista_itens_edit = []
                    for k, v in carrinho_atual.items():
                        lista_itens_edit.append({
                            "chave": k,
                            "Produto": v.get("nome_exibicao", k),
                            "Qtd": int(v.get("qtd", 1)),
                            "Preço Unit (R$)": float(v.get("preco_unitario", 0.0)),
                            "Obs": v.get("observacao", "")
                        })
                    
                    if not lista_itens_edit:
                        lista_itens_edit = [{"chave": "novo", "Produto": "", "Qtd": 1, "Preço Unit (R$)": 0.0, "Obs": ""}]

                    df_itens = pd.DataFrame(lista_itens_edit)

                    st.caption("Edite a quantidade (0 remove), preço ou obs.")
                    edited_df = st.data_editor(
                        df_itens,
                        key=f"editor_{p['id']}",
                        use_container_width=True,
                        num_rows="dynamic",
                        column_config={
                            "chave": None,
                            "Produto": st.column_config.TextColumn("Item", disabled=True),
                            "Qtd": st.column_config.NumberColumn("Qtd", min_value=0, step=1),
                            "Preço Unit (R$)": st.column_config.NumberColumn("Valor Unit.", format="R$ %.2f"),
                            "Obs": st.column_config.TextColumn("Observação")
                        }
                    )


                    c_add1, c_add2 = st.columns([3, 1])
                    with c_add1:

                        item_to_add = st.selectbox("Adicionar Produto", [""] + lista_produtos_nomes, key=f"sel_{p['id']}")
                    with c_add2:
                        qtd_to_add = st.number_input("Qtd Add", min_value=1, value=1, key=f"qtd_{p['id']}")


                    if st.button("💾 Salvar Alterações", key=f"save_edit_{p['id']}", type="primary"):
                        novo_carrinho = {}
                        novo_total_itens = 0.0
                        novo_resumo_list = []


                        for index, row in edited_df.iterrows():
                            qtd = int(row['Qtd'])
                            if qtd > 0:
                                chave = row.get('chave')
                                nome = row['Produto']
                                if not chave or pd.isna(chave): chave = nome.lower().replace(" ", "")
                                
                                preco = float(row['Preço Unit (R$)'])
                                obs = row['Obs']
                                
                                novo_carrinho[chave] = {
                                    "nome_exibicao": nome, "qtd": qtd, "preco_unitario": preco, "observacao": obs
                                }
                                total_item = qtd * preco
                                novo_total_itens += total_item
                                txt_obs = f" ({obs})" if obs else ""
                                novo_resumo_list.append(f"{qtd}x {nome}{txt_obs} (R$ {total_item:.2f})")


                        if item_to_add:
                            preco_add = dict_precos_produtos.get(item_to_add, 0.0)
                            chave_add = item_to_add.lower().replace(" ", "")
                            
                            if chave_add in novo_carrinho:
                                novo_carrinho[chave_add]['qtd'] += qtd_to_add
                            else:
                                novo_carrinho[chave_add] = {
                                    "nome_exibicao": item_to_add, "qtd": qtd_to_add, 
                                    "preco_unitario": preco_add, "observacao": "Adicionado Manualmente"
                                }

                            novo_total_itens += (qtd_to_add * preco_add)
                            

                            novo_resumo_list.append(f"{qtd_to_add}x {item_to_add} (R$ {qtd_to_add*preco_add:.2f})")



                        resumo_final_list = []
                        total_calculado_novo = 0.0
                        for k, v in novo_carrinho.items():
                            sub = v['qtd'] * v['preco_unitario']
                            total_calculado_novo += sub
                            obs_t = f" ({v['observacao']})" if v['observacao'] else ""
                            resumo_final_list.append(f"{v['qtd']}x {v['nome_exibicao']}{obs_t} (R$ {sub:.2f})")
                        
                        novo_resumo_str = " | ".join(resumo_final_list)


                        total_antigo_db = float(p.get('total_valor', 0.0))
                        

                        total_itens_velho = 0.0
                        carrinho_velho = p.get('carrinho_json') or {}
                        for k, v in carrinho_velho.items():
                            total_itens_velho += (v.get('qtd',0) * v.get('preco_unitario',0))
                        
                        taxa_estimada = total_antigo_db - total_itens_velho
                        if taxa_estimada < 0: taxa_estimada = 0.0
                        
                        novo_total_final = total_calculado_novo + taxa_estimada


                        supabase.table("pedidos").update({
                            "carrinho_json": novo_carrinho,
                            "resumo_pedido": novo_resumo_str,
                            "total_valor": novo_total_final
                        }).eq("id", p['id']).execute()

                        st.success("Atualizado!")

                        enviar_mensagem_ativa(dados, p['cliente_zap'], f"📝 Pedido #{p['id']} atualizado:\n\n{novo_resumo_str.replace('|', '\n')}\n💰 Novo Total: R$ {novo_total_final:.2f}")
                        time.sleep(1)
                        st.rerun()








    elif aba == "Métricas":
        st.header("📈 Métricas")
        st.caption("Painel completo de performance, produtos, horários, bairros, pagamentos e entregas.")
        st.markdown(
            """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@500;700&family=IBM+Plex+Sans:wght@400;600&display=swap');
    .bi-banner {padding:14px 16px; border-radius:16px; background: radial-gradient(circle at 10% 20%, #fff1df 0%, #f8f4ea 35%, #f7fbff 100%); border:1px solid #efe3d5; margin:6px 0 12px 0; animation: biRise 0.6s ease;}
    .bi-banner h3 {margin:0; font-family: 'Space Grotesk', sans-serif; font-weight:700; color:#2a2017;}
    .bi-banner p {margin:6px 0 0 0; font-family: 'IBM Plex Sans', sans-serif; color:#6c5b4a;}
    .bi-kpi-grid {display:grid; grid-template-columns: repeat(4, minmax(150px, 1fr)); gap:12px; margin:10px 0 16px 0;}
    .bi-kpi {background: linear-gradient(135deg, #f9f7f2 0%, #fff6e9 100%); border:1px solid #f0e7d8; border-radius:14px; padding:14px 16px; box-shadow: 0 8px 20px rgba(25, 25, 25, 0.06);} 
    .bi-kpi h4 {margin:0; font-size:12px; letter-spacing:0.08em; text-transform:uppercase; color:#8a6f52; font-weight:700; font-family: 'IBM Plex Sans', sans-serif;}
    .bi-kpi .val {font-size:22px; color:#1f1a17; font-weight:700; margin-top:6px; font-family: 'Space Grotesk', sans-serif;}
    .bi-section {margin-top:12px; padding:10px 12px; background: #fffaf3; border:1px solid #f3e5d2; border-radius:12px;}
    .bi-title {font-size:16px; font-weight:700; color:#2a2017; margin-bottom:6px; font-family: 'Space Grotesk', sans-serif;}
    .bi-sub {color:#7b6958; font-size:12px; margin-bottom:8px; font-family: 'IBM Plex Sans', sans-serif;}
    @keyframes biRise {from {opacity:0; transform: translateY(6px);} to {opacity:1; transform: translateY(0);} }
</style>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            """
<div class="bi-banner">
    <h3>Resumo Executivo</h3>
    <p>Visao sintetica do desempenho com foco em receita, mix de pagamento e itens mais vendidos.</p>
</div>
            """,
            unsafe_allow_html=True,
        )

        @st.cache_data(ttl=60)
        def _fetch_pedidos_metrics(restaurante_id: int):
            try:
                return (
                    supabase.table("pedidos")
                    .select(
                        "id,cliente_nome,created_at,finalizado_em,status,total_valor,forma_pagamento,"
                        "payment_status,tipo_entrega,endereco_completo,carrinho_json"
                    )
                    .eq("restaurante_id", restaurante_id)
                    .order("created_at", desc=True)
                    .execute()
                    .data
                    or []
                )
            except Exception:
                return []

        @st.cache_data(ttl=60)
        def _fetch_entregas_metrics(restaurante_id: int):
            try:
                return (
                    supabase.table("entregas")
                    .select("id,pedido_id,motoboy_nome,motoboy_telefone,status,encaminhado_em,entregue_em")
                    .eq("restaurante_id", restaurante_id)
                    .order("encaminhado_em", desc=True)
                    .execute()
                    .data
                    or []
                )
            except Exception:
                return []

        def _norm_pagamento(v: str) -> str:
            t = (v or "").strip().lower()
            if not t:
                return "Indefinido"
            if "pix" in t:
                return "Pix"
            if "dinheiro" in t or "espécie" in t or "especie" in t:
                return "Dinheiro"
            if "cartao" in t or "cartão" in t or "credito" in t or "crédito" in t or "debito" in t or "débito" in t:
                return "Cartão"
            return "Outro"

        def _extract_bairro(endereco: str) -> str:
            txt = (endereco or "").strip()
            if not txt:
                return ""
            m = re.search(r"\(([^)]+)\)\s*$", txt)
            if m:
                return (m.group(1) or "").strip().title()
            return ""

        hoje = datetime.now().date()
        st.subheader("🔎 Filtros")
        c1, c2 = st.columns([2, 2])
        with c1:
            dias = st.selectbox(
                "Período",
                ["Últimos 7 dias", "Últimos 30 dias", "Últimos 90 dias", "Tudo"],
                index=1,
            )
        with c2:
            st.caption("Dica: use 'Tudo' para histórico completo.")

        if dias == "Últimos 7 dias":
            dt_ini = hoje - pd.Timedelta(days=7)
        elif dias == "Últimos 30 dias":
            dt_ini = hoje - pd.Timedelta(days=30)
        elif dias == "Últimos 90 dias":
            dt_ini = hoje - pd.Timedelta(days=90)
        else:
            dt_ini = None

        rows = _fetch_pedidos_metrics(restaurante_db_id)
        if not rows:
            st.info("Ainda não há pedidos para gerar métricas.")
        else:
            df = pd.DataFrame(rows)
            if df.empty:
                st.info("Ainda não há pedidos para gerar métricas.")
            else:
                df["total_valor"] = pd.to_numeric(df.get("total_valor"), errors="coerce").fillna(0.0)

                dt_base = df.get("finalizado_em")
                dt_fallback = df.get("created_at")
                df["_dt"] = pd.to_datetime(dt_base, errors="coerce", utc=True)
                df.loc[df["_dt"].isna(), "_dt"] = pd.to_datetime(dt_fallback, errors="coerce", utc=True)
                df = df.dropna(subset=["_dt"])

                df["forma_pagamento_norm"] = df.get("forma_pagamento").apply(_norm_pagamento)
                df["payment_status_norm"] = df.get("payment_status").fillna("Indefinido").astype(str).str.strip().str.lower()
                df["tipo_entrega"] = df.get("tipo_entrega").fillna("entrega").astype(str).str.strip().str.lower()
                df["bairro"] = df.get("endereco_completo").apply(_extract_bairro)

                if dt_ini is not None:
                    df = df[df["_dt"].dt.date >= dt_ini]

                status_vals = sorted([s for s in df.get("status").dropna().astype(str).unique().tolist() if s])
                tipo_vals = sorted([s for s in df.get("tipo_entrega").dropna().astype(str).unique().tolist() if s])
                pagamento_vals = sorted([s for s in df.get("forma_pagamento_norm").dropna().astype(str).unique().tolist() if s])
                payment_status_vals = sorted([s for s in df.get("payment_status_norm").dropna().astype(str).unique().tolist() if s])
                bairro_vals = sorted([b for b in df.get("bairro").dropna().astype(str).unique().tolist() if b])

                c5, c6, c7, c8 = st.columns([2, 2, 2, 2])
                with c5:
                    status_filter = st.multiselect("Status do pedido", status_vals, default=["finalizado"] if "finalizado" in status_vals else None)
                with c6:
                    tipo_filter = st.multiselect("Tipo de entrega", tipo_vals)
                with c7:
                    pagamento_filter = st.multiselect("Forma de pagamento", pagamento_vals)
                with c8:
                    payment_status_filter = st.multiselect("Status do pagamento", payment_status_vals)

                c9, c10 = st.columns([2, 2])
                with c9:
                    bairro_filter = st.multiselect("Bairros", bairro_vals)
                with c10:
                    st.caption("Filtros aplicam a todos os gráficos abaixo.")

                if status_filter:
                    df = df[df.get("status").isin(status_filter)]
                if tipo_filter:
                    df = df[df.get("tipo_entrega").isin(tipo_filter)]
                if pagamento_filter:
                    df = df[df.get("forma_pagamento_norm").isin(pagamento_filter)]
                if payment_status_filter:
                    df = df[df.get("payment_status_norm").isin(payment_status_filter)]
                if bairro_filter:
                    df = df[df.get("bairro").isin(bairro_filter)]

                if df.empty:
                    st.info("Sem pedidos no período selecionado com os filtros atuais.")
                else:
                    total_periodo = float(df["total_valor"].sum())
                    pedidos_count = int(df.shape[0])
                    ticket_medio = (total_periodo / pedidos_count) if pedidos_count else 0.0
                    pedidos_pix = int((df["forma_pagamento_norm"] == "Pix").sum())
                    taxa_pix = (pedidos_pix / pedidos_count * 100.0) if pedidos_count else 0.0

                    st.subheader("✨ Visão Geral")
                    st.markdown(
                        f"""
<div class="bi-kpi-grid">
    <div class="bi-kpi"><h4>Faturamento</h4><div class="val">R$ {total_periodo:.2f}</div></div>
    <div class="bi-kpi"><h4>Pedidos</h4><div class="val">{pedidos_count}</div></div>
    <div class="bi-kpi"><h4>Ticket Médio</h4><div class="val">R$ {ticket_medio:.2f}</div></div>
    <div class="bi-kpi"><h4>Pix</h4><div class="val">{taxa_pix:.1f}%</div></div>
</div>
                        """,
                        unsafe_allow_html=True,
                    )

                    st.divider()

                    st.subheader("📊 Vendas por dia da semana")
                    st.caption("Soma do total vendido (R$) em pedidos do período.")

                    dias_pt = {
                        6: "Domingo",
                        0: "Segunda",
                        1: "Terça",
                        2: "Quarta",
                        3: "Quinta",
                        4: "Sexta",
                        5: "Sábado",
                    }
                    ordem = ["Domingo", "Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado"]

                    df["_dow"] = df["_dt"].dt.dayofweek
                    df["dia_semana"] = df["_dow"].map(dias_pt)
                    agrupado = (
                        df.groupby("dia_semana", dropna=False)["total_valor"]
                        .sum()
                        .reindex(ordem)
                        .fillna(0.0)
                    )
                    df_chart = pd.DataFrame({"dia": agrupado.index, "valor": agrupado.values})
                    chart_dow = (
                        alt.Chart(df_chart)
                        .mark_bar(color="#d97745", cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                        .encode(
                            x=alt.X("dia:N", sort=ordem, title=None),
                            y=alt.Y("valor:Q", title="R$"),
                            tooltip=["dia", alt.Tooltip("valor:Q", format=".2f")],
                        )
                        .properties(height=280)
                    )
                    st.altair_chart(chart_dow, use_container_width=True)

                    st.subheader("⏰ Horários de maior venda")
                    df["hora"] = df["_dt"].dt.hour
                    hora_agg = df.groupby("hora")["total_valor"].sum().reindex(range(24)).fillna(0.0)
                    df_hora = pd.DataFrame({"hora": hora_agg.index, "valor": hora_agg.values})
                    chart_hora = (
                        alt.Chart(df_hora)
                        .mark_area(color="#2c7a7b", line=True, opacity=0.45)
                        .encode(
                            x=alt.X("hora:O", title="Hora"),
                            y=alt.Y("valor:Q", title="R$"),
                            tooltip=["hora", alt.Tooltip("valor:Q", format=".2f")],
                        )
                        .properties(height=260)
                    )
                    st.altair_chart(chart_hora, use_container_width=True)

                    st.divider()

                    st.subheader("🍕 Produtos e Itens")
                    itens = []
                    pedidos_por_item = {}
                    for _, row in df.iterrows():
                        carrinho = row.get("carrinho_json") or {}
                        if not isinstance(carrinho, dict):
                            continue
                        itens_no_pedido = set()
                        for k, v in carrinho.items():
                            if not isinstance(v, dict):
                                continue
                            nome = (v.get("nome_exibicao") or k or "").strip()
                            if not nome:
                                continue
                            try:
                                qtd = int(v.get("qtd") or 0)
                            except Exception:
                                qtd = 0
                            try:
                                preco_u = float(v.get("preco_unitario") or 0.0)
                            except Exception:
                                preco_u = 0.0
                            if qtd <= 0:
                                continue
                            itens.append({"produto": nome, "qtd": qtd, "receita": qtd * preco_u})
                            itens_no_pedido.add(nome)
                        for nome in itens_no_pedido:
                            pedidos_por_item[nome] = pedidos_por_item.get(nome, 0) + 1

                    if itens:
                        df_itens = pd.DataFrame(itens)
                        agg_itens = df_itens.groupby("produto").agg({"qtd": "sum", "receita": "sum"}).reset_index()
                        agg_itens["pedidos"] = agg_itens["produto"].map(pedidos_por_item).fillna(0).astype(int)
                        agg_itens = agg_itens.sort_values("qtd", ascending=False)

                        c_prod1, c_prod2 = st.columns([2, 2])
                        top_qtd = agg_itens.head(10)
                        with c_prod1:
                            st.caption("Top 10 itens mais vendidos (quantidade)")
                            chart_qtd = (
                                alt.Chart(top_qtd)
                                .mark_bar(color="#bb6c3f", cornerRadiusTopRight=6, cornerRadiusBottomRight=6)
                                .encode(
                                    y=alt.Y("produto:N", sort="-x", title=None),
                                    x=alt.X("qtd:Q", title="Unidades"),
                                    tooltip=["produto", "qtd"],
                                )
                                .properties(height=280)
                            )
                            st.altair_chart(chart_qtd, use_container_width=True)
                        with c_prod2:
                            st.caption("Top 10 itens por receita")
                            chart_rec = (
                                alt.Chart(top_qtd)
                                .mark_bar(color="#2f855a", cornerRadiusTopRight=6, cornerRadiusBottomRight=6)
                                .encode(
                                    y=alt.Y("produto:N", sort="-x", title=None),
                                    x=alt.X("receita:Q", title="R$"),
                                    tooltip=["produto", alt.Tooltip("receita:Q", format=".2f")],
                                )
                                .properties(height=280)
                            )
                            st.altair_chart(chart_rec, use_container_width=True)

                        st.caption("Tabela completa de produtos")
                        st.dataframe(
                            agg_itens.rename(columns={"qtd": "unidades", "receita": "receita_total"}).sort_values("unidades", ascending=False),
                            use_container_width=True,
                        )

                        st.caption("Menos pedidos (itens com menor demanda)")
                        st.dataframe(
                            agg_itens.sort_values("pedidos", ascending=True).head(10),
                            use_container_width=True,
                        )
                    else:
                        st.info("Sem dados de itens no período selecionado.")

                    st.divider()

                    st.subheader("📍 Bairros e Entregas")
                    bairros_agg = df.groupby("bairro")["total_valor"].sum().sort_values(ascending=False)
                    if not bairros_agg.empty:
                        df_bairros = bairros_agg.head(12).reset_index()
                        chart_bairros = (
                            alt.Chart(df_bairros)
                            .mark_bar(color="#3b8c7b", cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                            .encode(
                                x=alt.X("bairro:N", sort="-y", title=None),
                                y=alt.Y("total_valor:Q", title="R$"),
                                tooltip=["bairro", alt.Tooltip("total_valor:Q", format=".2f")],
                            )
                            .properties(height=280)
                        )
                        st.altair_chart(chart_bairros, use_container_width=True)
                        st.caption("Tabela de faturamento por bairro")
                        st.dataframe(
                            bairros_agg.reset_index().rename(columns={"bairro": "bairro", "total_valor": "faturamento"}),
                            use_container_width=True,
                        )
                    else:
                        st.info("Sem bairros identificados no período.")

                    st.divider()

                    st.subheader("💳 Pagamentos")
                    pg_agg = df.groupby("forma_pagamento_norm")["total_valor"].sum().sort_values(ascending=False)
                    df_pg = pg_agg.reset_index()
                    chart_pg = (
                        alt.Chart(df_pg)
                        .mark_bar(color="#1f7a8c", cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                        .encode(
                            x=alt.X("forma_pagamento_norm:N", sort="-y", title=None),
                            y=alt.Y("total_valor:Q", title="R$"),
                            tooltip=["forma_pagamento_norm", alt.Tooltip("total_valor:Q", format=".2f")],
                        )
                        .properties(height=240)
                    )
                    st.altair_chart(chart_pg, use_container_width=True)
                    st.caption("Tabela de pagamentos")
                    st.dataframe(
                        pg_agg.reset_index().rename(columns={"forma_pagamento_norm": "forma_pagamento", "total_valor": "faturamento"}),
                        use_container_width=True,
                    )

                    st.divider()

                    st.subheader("🛵 Motoboys")
                    entregas_rows = _fetch_entregas_metrics(restaurante_db_id)
                    if entregas_rows:
                        dfe = pd.DataFrame(entregas_rows)
                        dfe["_dt"] = pd.to_datetime(dfe.get("entregue_em"), errors="coerce", utc=True)
                        dfe.loc[dfe["_dt"].isna(), "_dt"] = pd.to_datetime(dfe.get("encaminhado_em"), errors="coerce", utc=True)
                        dfe = dfe.dropna(subset=["_dt"])
                        if dt_ini is not None:
                            dfe = dfe[dfe["_dt"].dt.date >= dt_ini]

                        motoboy_vals = sorted([m for m in dfe.get("motoboy_nome").dropna().astype(str).unique().tolist() if m])
                        motoboy_filter = st.multiselect("Filtrar motoboys", motoboy_vals)
                        if motoboy_filter:
                            dfe = dfe[dfe.get("motoboy_nome").isin(motoboy_filter)]

                        if not dfe.empty:
                            entregas_por_motoboy = dfe.groupby("motoboy_nome")["id"].count().sort_values(ascending=False)
                            df_moto = entregas_por_motoboy.reset_index().rename(columns={"id": "entregas"})
                            chart_moto = (
                                alt.Chart(df_moto)
                                .mark_bar(color="#e89f59", cornerRadiusTopRight=6, cornerRadiusBottomRight=6)
                                .encode(
                                    y=alt.Y("motoboy_nome:N", sort="-x", title=None),
                                    x=alt.X("entregas:Q", title="Entregas"),
                                    tooltip=["motoboy_nome", "entregas"],
                                )
                                .properties(height=260)
                            )
                            st.altair_chart(chart_moto, use_container_width=True)

                            dfe["tempo_entrega_min"] = (
                                pd.to_datetime(dfe.get("entregue_em"), errors="coerce", utc=True)
                                - pd.to_datetime(dfe.get("encaminhado_em"), errors="coerce", utc=True)
                            ).dt.total_seconds() / 60.0
                            tempo_medio = dfe.groupby("motoboy_nome")["tempo_entrega_min"].mean().sort_values()

                            st.caption("Tempo médio de entrega (min) por motoboy")
                            st.dataframe(
                                tempo_medio.reset_index().rename(columns={"tempo_entrega_min": "minutos"}),
                                use_container_width=True,
                            )

                            st.caption("Entregas detalhadas")
                            st.dataframe(dfe[["motoboy_nome", "pedido_id", "status", "encaminhado_em", "entregue_em"]], use_container_width=True)
                        else:
                            st.info("Sem entregas no período selecionado com os filtros atuais.")
                    else:
                        st.info("Sem dados de motoboys/entregas.")


    elif aba == "Gestão de Cardápio":
        st.header("📦 Cardápio Geral (Modo Planilha)")
        st.caption("Edite tudo na tabela abaixo. Deixe o estoque VAZIO para ser INFINITO.")
        

        res = supabase.table("produtos").select("*").eq("restaurante_id", restaurante_db_id).order("categoria", desc=False).order("nome").execute()
        produtos = res.data or []


        if produtos:
            df = pd.DataFrame(produtos)
        else:
            df = pd.DataFrame(columns=["id", "restaurante_id", "nome", "descricao", "preco", "categoria", "estoque", "disponivel"])


        if "estoque" not in df.columns:
            df["estoque"] = None # Cria a coluna vazia se ela não vier do banco
        

        df["estoque"] = pd.to_numeric(df["estoque"], errors='coerce')


        default_cats = ["Bebidas", "Promoções", "Hambúrgueres", "Borda"]
        existing_cats = df["categoria"].unique().tolist() if not df.empty else []
        cats_existentes = list(set(existing_cats + default_cats))
        cats_existentes = sorted([str(c) for c in cats_existentes if c]) 


        edited_df = st.data_editor(
            df,
            key="editor_cardapio",
            num_rows="dynamic",
            use_container_width=True,
            height=600,
            hide_index=True,
            column_config={
                "id": None,
                "restaurante_id": None,
                "created_at": None,
                "nome": st.column_config.TextColumn("Nome do Item", required=True),
                "descricao": st.column_config.TextColumn("Descrição", width="large"),
                "preco": st.column_config.NumberColumn("Preço (R$)", min_value=0.0, step=0.50, format="R$ %.2f", required=True),
                "categoria": st.column_config.SelectboxColumn("Categoria", options=cats_existentes, required=True),
                

                "estoque": st.column_config.NumberColumn(
                    "Estoque (Qtd)",
                    help="🔢 Digite um número para limitar.\n♾️ Apague o número (deixe vazio) para Estoque Infinito.",
                    step=1,
                    min_value=0
                ),
                "disponivel": st.column_config.CheckboxColumn("Ativo?", default=True)
            }
        )

        st.info("💡 **Dica de Estoque:** Para tornar um item **Infinito**, clique na célula de estoque e aperte **DELETE/BACKSPACE** até ela ficar vazia (`<NA>`).")



        c_save1, c_save2 = st.columns([1, 4])
        with c_save1:
            if st.button("💾 SALVAR ALTERAÇÕES", type="primary", use_container_width=True):
                with st.spinner("Sincronizando..."):
                    try:

                        ids_originais = set([p['id'] for p in produtos])

                        ids_finais = set([int(row['id']) for i, row in edited_df.iterrows() if pd.notna(row.get('id'))])
                        
                        ids_para_deletar = ids_originais - ids_finais
                        if ids_para_deletar:
                            supabase.table("produtos").delete().in_("id", list(ids_para_deletar)).execute()


                        registros = []
                        validation_errors = []
                        nomes_seen = set()

                        next_id = None
                        try:
                            r_max = supabase.table("produtos").select("id").order("id", desc=True).limit(1).execute()
                            if r_max.data:
                                next_id = int(r_max.data[0].get("id") or 0) + 1
                        except Exception:
                            next_id = None
                        
                        for index, row in edited_df.iterrows():
                            linha = int(index) + 1
                            nome_raw = _normalize_product_text(row.get('nome'))
                            categoria_raw = _normalize_product_text(row.get('categoria'))
                            descricao_raw = _normalize_product_text(row.get('descricao'))

                            if not nome_raw:
                                validation_errors.append(f"Linha {linha}: nome do item é obrigatório.")
                                continue

                            if not categoria_raw:
                                validation_errors.append(f"Linha {linha}: categoria é obrigatória.")
                                continue

                            nome_key = _normalize_name_key(nome_raw)
                            if nome_key in nomes_seen:
                                validation_errors.append(f"Linha {linha}: item duplicado '{nome_raw}'.")
                                continue
                            nomes_seen.add(nome_key)

                            try:
                                preco_val = float(row.get('preco') or 0.0)
                            except Exception:
                                preco_val = 0.0
                            if preco_val <= 0:
                                validation_errors.append(f"Linha {linha}: preço inválido para '{nome_raw}'.")
                                continue

                            if _is_pizza_categoria(categoria_raw) and (not _has_size_token(nome_raw)):
                                validation_errors.append(
                                    f"Linha {linha}: pizza sem tamanho em '{nome_raw}'. Use Pequena/Média/Grande no nome."
                                )
                                continue

                            if _normalize_name_key(categoria_raw) == "bebidas" and (not _has_volume_token(nome_raw, descricao_raw)):
                                validation_errors.append(
                                    f"Linha {linha}: bebida sem volume em '{nome_raw}'. Ex.: 350ml, 600ml, 1L, 2L."
                                )
                                continue

                            val_estoque = row['estoque']
                            

                            if pd.isna(val_estoque) or val_estoque == "":
                                estoque_final = None
                            else:

                                estoque_final = int(float(val_estoque))

                            item = {
                                "restaurante_id": restaurante_db_id,
                                "nome": nome_raw,
                                "descricao": descricao_raw,
                                "preco": float(preco_val),
                                "categoria": categoria_raw,
                                "estoque": estoque_final,
                                "disponivel": bool(row['disponivel'])
                            }
                            

                            if pd.notna(row.get('id')):
                                item['id'] = int(row['id'])
                            else:
                                if next_id is None:
                                    next_id = 1
                                item['id'] = int(next_id)
                                next_id += 1
                                
                            registros.append(item)

                        if validation_errors:
                            st.error("Não foi possível salvar. Corrija os itens abaixo:")
                            for msg in validation_errors[:12]:
                                st.write(f"- {msg}")
                            if len(validation_errors) > 12:
                                st.write(f"- ... e mais {len(validation_errors) - 12} erro(s)")
                            return

                        if registros:
                            supabase.table("produtos").upsert(registros).execute()

                        st.success("✅ Cardápio salvo com sucesso!")
                        time.sleep(1)
                        st.rerun()

                    except Exception as e:
                        st.error(f"Erro ao salvar: {e}")


    elif aba == "Motoboys":
        st.header("🛵 Motoboys")
        st.caption("Cadastre motoboys, encaminhe pedidos e acompanhe histórico por entregador.")


        try:
            motoboys = supabase.table("motoboys").select("*")\
                .eq("restaurante_id", restaurante_db_id)\
                .order("created_at", desc=True)\
                .execute().data or []
        except Exception:
            st.error("A tabela 'motoboys' ainda não existe no Supabase (ou a permissão não permite acesso).")
            st.caption("Crie a tabela e recarregue a página:")
            st.code(
                "CREATE TABLE IF NOT EXISTS public.motoboys (\n"
                "  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,\n"
                "  restaurante_id bigint NOT NULL REFERENCES public.restaurantes(id),\n"
                "  nome text NOT NULL,\n"
                "  telefone text NOT NULL,\n"
                "  placa text,\n"
                "  chave_pix text,\n"
                "  cpf text,\n"
                "  modelo text,\n"
                "  senha text,\n"
                "  ativo boolean DEFAULT true,\n"
                "  created_at timestamptz DEFAULT timezone('utc'::text, now())\n"
                ");"
            )
            return

        with st.expander("➕ Cadastrar motoboy", expanded=True):
            with st.form("form_add_motoboy"):
                st.caption("Obrigatórios: Nome e Telefone")
                c1, c2 = st.columns([2, 2])
                with c1:
                    nome_m = st.text_input("Nome *")
                with c2:
                    tel_m = st.text_input("Telefone (WhatsApp) *", help="Use com DDD e país. Ex.: 5585...")

                st.caption("Desejáveis: Placa e Chave Pix")
                c3, c4 = st.columns([2, 2])
                with c3:
                    placa_m = st.text_input("Placa")
                with c4:
                    pix_m = st.text_input("Chave Pix")

                st.caption("Opcionais: CPF e Modelo")
                c5, c6, c7 = st.columns([2, 2, 1])
                with c5:
                    cpf_m = st.text_input("CPF", help="Somente números ou com pontuação. Opcional.")
                with c6:
                    modelo_m = st.text_input("Modelo")
                with c7:
                    pin_m = st.text_input("PIN", help="PIN para o motoboy acessar a própria tela. Se vazio, gera automático.")

                if st.form_submit_button("Cadastrar", use_container_width=True):
                    nome_m = (nome_m or "").strip()
                    tel_digits = _only_digits(tel_m)
                    placa_final = (placa_m or "").strip().upper()
                    pix_final = (pix_m or "").strip()
                    modelo_final = (modelo_m or "").strip()
                    cpf_digits = _only_digits(cpf_m)

                    if not nome_m or not tel_digits:
                        st.warning("Preencha Nome e Telefone.")
                    elif len(tel_digits) < 10:
                        st.warning("Telefone parece inválido. Use com DDD (e país, se possível).")
                    elif cpf_digits and len(cpf_digits) != 11:
                        st.warning("CPF inválido. Informe 11 dígitos (ou deixe em branco).")
                    else:
                        try:
                            pin_final = (pin_m or "").strip() or _generate_pin(4)
                            supabase.table("motoboys").insert({
                                "restaurante_id": restaurante_db_id,
                                "nome": nome_m,
                                "telefone": tel_digits,
                                "placa": placa_final or None,
                                "chave_pix": pix_final or None,
                                "cpf": cpf_digits or None,
                                "modelo": modelo_final or None,
                                "senha": pin_final,
                                "ativo": True,
                            }).execute()
                            st.success("Motoboy cadastrado.")
                            st.info(f"PIN de acesso: {pin_final}")
                            time.sleep(0.5)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao cadastrar: {e}")

        st.markdown("---")
        st.subheader("📋 Motoboys cadastrados")


        delivered_count = {}
        try:
            delivered = supabase.table("entregas").select("motoboy_id")\
                .eq("restaurante_id", restaurante_db_id)\
                .eq("status", "entregue")\
                .execute().data or []
            for row in delivered:
                mid = row.get("motoboy_id")
                if mid is not None:
                    delivered_count[mid] = delivered_count.get(mid, 0) + 1
        except Exception:
            delivered_count = {}

        if not motoboys:
            st.info("Nenhum motoboy cadastrado ainda.")
        else:
            for m in motoboys:
                with st.container(border=True):
                    c1, c2, c3 = st.columns([3, 4, 1])
                    with c1:
                        st.write(f"**{m.get('nome','-')}**")
                        st.caption(f"Entregas concluídas: {delivered_count.get(m.get('id'), 0)}")
                    with c2:
                        st.write(f"📞 {m.get('telefone','')}")
                        placa_v = (m.get("placa") or "").strip()
                        pix_v = (m.get("chave_pix") or "").strip()
                        if placa_v:
                            st.caption(f"Placa: {placa_v}")
                        if pix_v:
                            st.caption(f"Pix: {pix_v}")
                        if m.get("senha"):
                            st.caption(f"PIN: {m.get('senha')}")
                    with c3:
                        if st.button("🗑️", key=f"del_motoboy_{m.get('id')}", help="Remover motoboy"):
                            try:
                                supabase.table("motoboys").delete().eq("id", m["id"]).execute()
                                st.success("Removido.")
                                time.sleep(0.5)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Erro ao remover: {e}")

        st.markdown("---")
        st.subheader("📦 Encaminhar pedido para entrega")
        st.caption("Selecione um motoboy e encaminhe um pedido com endereço + links de mapa.")

        motoboys_ativos = [m for m in motoboys if bool(m.get("ativo", True))]
        if not motoboys_ativos:
            st.info("Cadastre pelo menos 1 motoboy para encaminhar pedidos.")
            return

        motoboy_opcoes = {f"{m['nome']} - {m['telefone']}": m for m in motoboys_ativos}
        escolhido = st.selectbox("Motoboy", list(motoboy_opcoes.keys()))
        motoboy_sel = motoboy_opcoes[escolhido]

        try:
            pedidos_abertos = supabase.table("pedidos").select("id,cliente_nome,cliente_zap,endereco_completo,tipo_entrega,total_valor,status,created_at")\
                .eq("restaurante_id", restaurante_db_id)\
                .in_("status", ["novo", "confirmado", "em preparo", "saiu para entrega"])\
                .order("created_at", desc=True)\
                .execute().data or []
        except Exception as e:
            st.error(f"Erro ao carregar pedidos: {e}")
            return

        if not pedidos_abertos:
            st.info("Nenhum pedido em aberto no momento.")
            return


        entrega_por_pedido = {}
        try:
            pedido_ids = [p.get("id") for p in pedidos_abertos if p.get("id") is not None]
            if pedido_ids:
                entregas_abertas = supabase.table("entregas").select("id,pedido_id,motoboy_nome,motoboy_telefone,status,encaminhado_em")\
                    .eq("restaurante_id", restaurante_db_id)\
                    .eq("status", "encaminhado")\
                    .in_("pedido_id", pedido_ids)\
                    .execute().data or []
                for e in entregas_abertas:
                    entrega_por_pedido[e.get("pedido_id")] = e
        except Exception:
            entrega_por_pedido = {}

        for p in pedidos_abertos:
            with st.container(border=True):
                c1, c2 = st.columns([3, 2])
                with c1:
                    st.markdown(f"### Pedido #{p['id']}")
                    st.caption(f"Cliente: {p.get('cliente_nome','-')} | {p.get('cliente_zap','-')}")
                    st.write(f"Status: {str(p.get('status') or '').upper()}")

                    e_atual = entrega_por_pedido.get(p.get("id"))
                    if e_atual:
                        st.info(f"Encaminhado para: {e_atual.get('motoboy_nome','-')} ({e_atual.get('motoboy_telefone','-')})")
                    end = (p.get("endereco_completo") or "").strip()
                    if end:
                        st.write(f"📍 {end}")
                    else:
                        st.warning("Pedido sem endereço cadastrado.")

                with c2:
                    disabled = not bool((p.get("endereco_completo") or "").strip())
                    if st.button(
                        "📨 Encaminhar p/ motoboy",
                        key=f"fw_{p['id']}_{motoboy_sel['id']}",
                        use_container_width=True,
                        disabled=disabled,
                    ):
                        try:
                            end = (p.get("endereco_completo") or "").strip()
                            maps = _maps_link(end)
                            waze = _waze_link(end)

                            total = p.get("total_valor")
                            try:
                                total_txt = f"R$ {float(total or 0):.2f}"
                            except Exception:
                                total_txt = str(total or "-")

                            msg = (
                                "🛵 *ENTREGA NOVA*\n"
                                f"Pedido: *#{p['id']}*\n"
                                f"Cliente: {p.get('cliente_nome','-')}\n"
                                f"Contato cliente: {p.get('cliente_zap','-')}\n"
                                f"Total: {total_txt}\n\n"
                                f"📍 Endereço: {end}\n\n"
                                f"🗺️ Google Maps: {maps}\n"
                                f"🚗 Waze: {waze}"
                            )

                            enviar_mensagem_ativa(dados, motoboy_sel["telefone"], msg)


                            try:
                                existing = supabase.table("entregas").select("id")\
                                    .eq("restaurante_id", restaurante_db_id)\
                                    .eq("pedido_id", p["id"])\
                                    .eq("status", "encaminhado")\
                                    .limit(1)\
                                    .execute().data or []
                                payload_e = {
                                    "restaurante_id": restaurante_db_id,
                                    "pedido_id": p["id"],
                                    "motoboy_id": motoboy_sel["id"],
                                    "motoboy_nome": motoboy_sel.get("nome") or "Motoboy",
                                    "motoboy_telefone": motoboy_sel.get("telefone") or "",
                                    "endereco": end,
                                    "maps_url": maps,
                                    "waze_url": waze,
                                    "status": "encaminhado",
                                }
                                if existing:
                                    supabase.table("entregas").update(payload_e).eq("id", existing[0]["id"]).execute()
                                else:
                                    supabase.table("entregas").insert(payload_e).execute()
                            except Exception:
                                pass

                            st.success(f"Encaminhado para {motoboy_sel['nome']}.")
                            time.sleep(0.4)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro ao encaminhar: {e}")

        st.markdown("---")
        st.subheader("📜 Histórico de entregas (por motoboy)")
        try:
            hist = supabase.table("entregas").select("motoboy_nome,motoboy_telefone,pedido_id,entregue_em")\
                .eq("restaurante_id", restaurante_db_id)\
                .eq("status", "entregue")\
                .order("entregue_em", desc=True)\
                .limit(50)\
                .execute().data or []
        except Exception:
            hist = []

        if not hist:
            st.info("Ainda não há entregas concluídas.")
        else:
            for h in hist:
                when = str(h.get("entregue_em") or "")
                st.write(f"{h.get('motoboy_nome','-')} ({h.get('motoboy_telefone','-')}) — Pedido #{h.get('pedido_id')} — {when[:16].replace('T',' ')}")
    

    elif aba == "Meu Cardápio":
        st.header("🍔 Gestão de Cardápio")
        st.caption("Adicione, edite preços ou desative itens. A IA lê isso automaticamente.")


        res_prod = supabase.table("produtos").select("*").eq("restaurante_id", restaurante_db_id).order("categoria").order("nome").execute()
        produtos = res_prod.data


        with st.expander("➕ Adicionar Novo Produto", expanded=False):
            with st.form("add_prod_form"):
                c_a, c_b = st.columns([2, 1])
                with c_a:
                    new_nome = st.text_input("Nome do Produto (ex: Pizza Calabresa)")
                    new_desc = st.text_input("Descrição (ex: Cebola, azeitona e orégano)")
                with c_b:
                    new_cat = st.text_input("Categoria (ex: Pizzas Salgadas)")
                    new_preco = st.number_input("Preço (R$)", min_value=0.0, step=1.0)
                
                if st.form_submit_button("Cadastrar Produto"):
                    if new_nome and new_preco > 0:
                        supabase.table("produtos").insert({
                            "restaurante_id": restaurante_db_id,
                            "nome": new_nome,
                            "descricao": new_desc,
                            "categoria": new_cat,
                            "preco": new_preco,
                            "disponivel": True
                        }).execute()
                        # Invalida cache da API para refletir cardápio instantaneamente
                        invalidate_api_cache(phone_id, dados.get("instance_name"))
                        st.success("Produto adicionado!")
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.warning("Preencha pelo menos Nome e Preço.")

        st.divider()


        if produtos:
            df = pd.DataFrame(produtos)
            

            edited_df = st.data_editor(
                df,
                key="editor_produtos",
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic", # Permite adicionar/remover linhas direto na tabela
                column_config={
                    "id": None, # Esconde ID
                    "restaurante_id": None, # Esconde ID restaurante
                    "created_at": None,
                    "nome": st.column_config.TextColumn("Nome", required=True),
                    "descricao": st.column_config.TextColumn("Descrição"),
                    "categoria": st.column_config.TextColumn("Categoria", required=True),
                    "preco": st.column_config.NumberColumn("Preço (R$)", min_value=0.0, format="R$ %.2f", required=True),
                    "disponivel": st.column_config.CheckboxColumn("Disponível?", default=True)
                },
                disabled=["id", "restaurante_id", "created_at"] # Colunas que não podem mudar ID
            )


            if st.button("💾 Salvar Alterações na Tabela", type="primary"):

                

                ids_originais = set(p['id'] for p in produtos)
                ids_editados = set(row['id'] for index, row in edited_df.iterrows() if pd.notna(row['id']))
                ids_para_deletar = ids_originais - ids_editados
                
                if ids_para_deletar:
                    supabase.table("produtos").delete().in_("id", list(ids_para_deletar)).execute()
                    st.toast(f"🗑️ {len(ids_para_deletar)} itens removidos.")



                count_upsert = 0
                for index, row in edited_df.iterrows():

                    payload = {
                        "restaurante_id": restaurante_db_id,
                        "nome": row['nome'],
                        "descricao": row['descricao'],
                        "categoria": row['categoria'],
                        "preco": row['preco'],
                        "disponivel": row['disponivel']
                    }
                    

                    if pd.notna(row['id']):

                        payload['id'] = row['id']
                    


                    supabase.table("produtos").upsert(payload).execute()
                    count_upsert += 1

                # Invalida cache da API para refletir cardápio instantaneamente
                invalidate_api_cache(phone_id, dados.get("instance_name"))
                
                st.success("Cardápio atualizado com sucesso!")
                time.sleep(1)
                st.rerun()

        else:
            st.info("Seu cardápio está vazio. Adicione o primeiro item acima!")



    elif aba == "Configurações":
        st.header("⚙️ Configurações do Bot")
        



        st.subheader("📍 Taxas de Entrega por Bairro")
        st.caption("Cadastre aqui os bairros atendidos. O sistema isola seus dados dos outros restaurantes.")


        res_bairros = supabase.table("bairros").select("*").eq("restaurante_id", restaurante_db_id).order("nome").execute()
        lista_bairros = res_bairros.data


        with st.expander("➕ Adicionar Novo Bairro"):
            with st.form("add_bairro_form"):
                cb1, cb2 = st.columns([3, 1])
                new_bairro_nome = cb1.text_input("Nome do Bairro")
                new_bairro_taxa = cb2.number_input("Taxa (R$)", min_value=0.0, step=1.0, value=5.0)
                
                if st.form_submit_button("Adicionar"):
                    if new_bairro_nome:
                        supabase.table("bairros").insert({
                            "restaurante_id": restaurante_db_id, # Vínculo de segurança
                            "nome": new_bairro_nome, 
                            "taxa": new_bairro_taxa,
                            "ativo": True
                        }).execute()
                        st.success(f"Bairro {new_bairro_nome} adicionado!")
                        time.sleep(0.5)
                        st.rerun()


        if lista_bairros:
            df_bairros = pd.DataFrame(lista_bairros)
            
            edited_bairros = st.data_editor(
                df_bairros,
                key="editor_bairros_final",
                use_container_width=True,
                hide_index=True,
                num_rows="dynamic", # Permite adicionar linhas direto na tabela
                column_config={
                    "id": None, "restaurante_id": None, "created_at": None, # Esconde colunas técnicas
                    "nome": st.column_config.TextColumn("Bairro", required=True),
                    "taxa": st.column_config.NumberColumn("Taxa (R$)", format="R$ %.2f", required=True),
                    "ativo": st.column_config.CheckboxColumn("Ativo?", default=True)
                },
                disabled=["id", "restaurante_id"]
            )


            if st.button("💾 Salvar Alterações de Taxas"):

                ids_orig = set(b['id'] for b in lista_bairros)
                ids_edit = set(row['id'] for i, row in edited_bairros.iterrows() if pd.notna(row['id']))
                to_delete = ids_orig - ids_edit
                
                if to_delete:
                    supabase.table("bairros").delete().in_("id", list(to_delete)).execute()


                for i, row in edited_bairros.iterrows():
                    payload = {
                        "restaurante_id": restaurante_db_id,
                        "nome": row['nome'], 
                        "taxa": row['taxa'], 
                        "ativo": row['ativo']
                    }
                    if pd.notna(row['id']): payload['id'] = row['id']
                    
                    supabase.table("bairros").upsert(payload).execute()
                
                st.success("Taxas atualizadas com sucesso!")
                time.sleep(1)
                st.rerun()
        else:
            st.info("Nenhum bairro cadastrado. Adicione o primeiro acima.")
            
        st.divider()




        c1, c2 = st.columns(2)
        with c1:
            prompt = st.text_area("Personalidade da IA", value=dados.get("system_prompt", ""), height=150)

            st.subheader("📌 Informações da Loja")
            endereco_loja = st.text_area("Endereço da Loja", value=dados.get("endereco_loja", ""), height=80)
            telefone_loja = st.text_input("Telefone da Loja", value=dados.get("telefone_loja", ""))
            horario_loja = st.text_input("Horário de Funcionamento", value=dados.get("horario_loja", ""))
        
        with c2:
            st.subheader("🛵 Entrega Padrão")
            taxa_padrao = st.number_input("Taxa Padrão (R$)", value=float(dados.get('taxa_entrega_padrao', 0.0)), step=0.5, format="%.2f")
            taxa_unica_ativa = st.toggle(
                "Taxa única de entrega",
                value=bool(dados.get("taxa_unica_ativa", False)),
                help="Quando ativo, a taxa padrão é aplicada para todos os pedidos e o bairro deixa de ser obrigatório.",
            )
            st.caption("Quando ativo, usa a taxa padrão para todos os bairros.")
        
        bot_ativo = st.toggle("🤖 Bot Ligado", value=dados.get("bot_ativo", True))
        msg_fechado = st.text_input("Mensagem de Fechado", value=dados.get("mensagem_fechado", "Estamos fechados no momento."))

        if st.button("💾 Salvar Configurações Gerais"):
            supabase.table("restaurantes").update({
                "system_prompt": prompt,
                "taxa_entrega_padrao": taxa_padrao,
                "taxa_unica_ativa": bool(taxa_unica_ativa),
                "bot_ativo": bot_ativo,
                "mensagem_fechado": msg_fechado,
                "endereco_loja": endereco_loja,
                "telefone_loja": telefone_loja,
                "horario_loja": horario_loja,
            }).eq("id", dados["id"]).execute()


            dados["system_prompt"] = prompt
            dados["taxa_entrega_padrao"] = taxa_padrao
            dados["taxa_unica_ativa"] = bool(taxa_unica_ativa)
            dados["bot_ativo"] = bot_ativo
            dados["mensagem_fechado"] = msg_fechado
            dados["endereco_loja"] = endereco_loja
            dados["telefone_loja"] = telefone_loja
            dados["horario_loja"] = horario_loja

            try:
                invalidate_api_cache(dados.get("phone_id"), dados.get("instance_name"))
            except Exception:
                pass

            st.success("Configurações atualizadas!")

        st.divider()




        st.subheader("💠 Pagamentos - Pix no WhatsApp (Mercado Pago)")

        f = _fernet()
        if not f:
            st.info("Para habilitar Pix no WhatsApp, configure `CRED_ENCRYPTION_KEY` no `.env` do Dashboard e da API.")

        pix_enabled = bool(dados.get("pix_whatsapp_enabled", False))
        if pix_enabled:
            st.success("Pix no WhatsApp: ATIVADO pelo admin")
        else:
            st.info("Pix no WhatsApp: DESATIVADO pelo admin")

        token_configurado = bool(dados.get("mp_access_token_enc"))
        if token_configurado:
            st.caption("Token do Mercado Pago: configurado")

        mp_token_input = st.text_input(
            "Access Token do Mercado Pago (Pix)",
            value="",
            type="password",
            help="Cole o access token da conta do próprio restaurante. Ele será salvo criptografado."
        )

        if PUBLIC_BASE_URL and MP_WEBHOOK_TOKEN:
            st.code(f"{PUBLIC_BASE_URL}/webhook/mercadopago?token={MP_WEBHOOK_TOKEN}")
            st.caption("Cole essa URL no painel do Mercado Pago como Webhook/Notificações.")

        if st.button("💾 Salvar Token do Mercado Pago"):
            try:
                if not mp_token_input.strip():
                    st.warning("Cole o Access Token para salvar.")
                else:
                    payload = {"pix_provider": "mercadopago", "mp_access_token_enc": encrypt_secret(mp_token_input.strip())}
                    supabase.table("restaurantes").update(payload).eq("id", dados["id"]).execute()
                    dados["mp_access_token_enc"] = payload.get("mp_access_token_enc")
                    st.success("Token salvo com sucesso.")
            except Exception as e:
                st.error(f"Erro ao salvar token: {e}")




def admin_page():
    st.sidebar.header("🛠️ Painel Master")
    if st.sidebar.button("Sair do Admin"):
        st.session_state.user_role = None
        st.rerun()

    st.title("🍕 Gestão de Parceiros")
    
    res = supabase.table("restaurantes").select("*").order("id").execute()
    restaurantes = res.data or []

    tab1, tab2, tab3 = st.tabs(["✏️ Editar Cliente", "➕ Cadastrar Novo", "📊 Métricas/Gastos"])


    with tab1:
        if not restaurantes:
            st.warning("Nenhum restaurante cadastrado.")
        else:
            opcoes = {f"{r['id']} - {r['nome']}": r for r in restaurantes}
            escolha = st.selectbox("Selecione o Restaurante:", list(opcoes.keys()))
            dados_r = opcoes[escolha]

            st.markdown("---")
            
            with st.form("form_editar_restaurante"):
                c1, c2 = st.columns(2)
                
                with c1:
                    st.subheader("🏢 Dados Básicos")
                    nome = st.text_input("Nome Fantasia", value=dados_r.get('nome', ''))
                    usuario = st.text_input("Usuário (Login)", value=dados_r.get('usuario', ''))
                    senha = st.text_input("Senha", value=dados_r.get('senha', ''))
                    telefone_dono = st.text_input("Telefone Dono", value=dados_r.get('telefone_dono', ''))

                    st.subheader("📌 Informações da Loja")
                    endereco_loja = st.text_area("Endereço da Loja", value=dados_r.get('endereco_loja', ''), height=80)
                    telefone_loja = st.text_input("Telefone da Loja", value=dados_r.get('telefone_loja', ''))
                    horario_loja = st.text_input("Horário de Funcionamento", value=dados_r.get('horario_loja', ''))

                with c2:
                    st.subheader("🤖 Conexão Uazapi")
                    phone_id = st.text_input("Phone ID (Núm Bot)", value=dados_r.get('phone_id', ''))
                    instance_name = st.text_input("Nome da Instância", value=dados_r.get('instance_name', ''))
                    instance_token = st.text_input("Token da Instância", value=dados_r.get('instance_token', ''))

                    st.subheader("💠 Pagamentos")
                    pix_enabled_admin = st.checkbox(
                        "Ativar Pix no WhatsApp (admin)",
                        value=bool(dados_r.get('pix_whatsapp_enabled', False))
                    )
                    st.caption("O restaurante configura o token na conta dele; você controla ativar/desativar.")

                col_salvar, col_delete = st.columns([4, 1])
                
                with col_salvar:
                    if st.form_submit_button("💾 SALVAR ALTERAÇÕES", type="primary", use_container_width=True):
                        try:
                            old_phone_id = (dados_r.get("phone_id") or "").strip()
                            old_instance_name = (dados_r.get("instance_name") or "").strip()
                            supabase.table("restaurantes").update({
                                "nome": nome, "usuario": usuario, "senha": senha,
                                "telefone_dono": telefone_dono, "phone_id": phone_id,
                                "instance_name": instance_name, "instance_token": instance_token,
                                "pix_whatsapp_enabled": bool(pix_enabled_admin),
                                "pix_provider": "mercadopago",
                                "endereco_loja": endereco_loja,
                                "telefone_loja": telefone_loja,
                                "horario_loja": horario_loja,
                            }).eq("id", dados_r['id']).execute()

                            # Evita esperar o TTL do Redis para validar/usar o novo token.
                            new_phone_id = (phone_id or "").strip()
                            new_instance_name = (instance_name or "").strip()
                            invalidated = False
                            if old_phone_id:
                                invalidated = invalidate_api_cache(old_phone_id, old_instance_name) or invalidated
                            if new_phone_id and (new_phone_id != old_phone_id or new_instance_name != old_instance_name):
                                invalidated = invalidate_api_cache(new_phone_id, new_instance_name) or invalidated
                            if new_phone_id:
                                invalidated = invalidate_api_cache(new_phone_id, new_instance_name) or invalidated
                            if not invalidated:
                                st.warning("Atualizado, mas não consegui invalidar o cache da API. Verifique CACHE_INVALIDATE_URL/TOKEN e se a API está ligada.")
                            st.success("Atualizado!")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro: {e}")

            with st.expander(f"🗑️ Zona de Perigo"):
                st.warning("Ações destrutivas (sem volta).")

                st.subheader("🧹 Limpar dados do restaurante")
                st.caption("Apaga TODAS as conversas e estados de clientes, e remove todos os pedidos desse restaurante.")
                st.caption("Tabelas afetadas: pedidos, clientes_estado, conversas")

                confirm_clear = st.text_input(
                    "Digite LIMPAR para confirmar",
                    key=f"confirm_clear_{dados_r['id']}",
                    help="Isso vai zerar o histórico/estado desse restaurante."
                )

                if st.button(
                    "🧹 Limpar tudo deste restaurante",
                    key=f"btn_clear_{dados_r['id']}",
                    type="secondary",
                    disabled=(confirm_clear.strip().upper() != "LIMPAR"),
                    use_container_width=True,
                ):
                    try:
                        with st.spinner("Limpando dados..."):

                            supabase.table("pedidos").delete().eq("restaurante_id", dados_r["id"]).execute()


                            supabase.table("clientes_estado").delete().eq("restaurante_id", dados_r["phone_id"]).execute()
                            supabase.table("conversas").delete().eq("restaurante_id", dados_r["phone_id"]).execute()

                        st.success("✅ Limpeza concluída. O restaurante começa do zero nas conversas/pedidos.")
                        st.cache_data.clear()
                        time.sleep(0.5)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro ao limpar: {e}")

                st.markdown("---")
                st.subheader("🗑️ Excluir restaurante")
                st.caption("Além de apagar os dados, remove o cadastro do restaurante.")

                confirm_delete = st.text_input(
                    f"Digite EXCLUIR {dados_r['id']} para confirmar",
                    key=f"confirm_delete_{dados_r['id']}",
                )

                if st.button(
                    f"Excluir {dados_r['nome']}",
                    key=f"btn_delete_{dados_r['id']}",
                    type="primary",
                    disabled=(confirm_delete.strip().upper() != f"EXCLUIR {dados_r['id']}"),
                    use_container_width=True,
                ):
                    try:
                        with st.spinner("Excluindo restaurante..."):

                            try:
                                supabase.table("entregas").delete().eq("restaurante_id", dados_r["id"]).execute()
                            except Exception:
                                pass
                            try:
                                supabase.table("motoboys").delete().eq("restaurante_id", dados_r["id"]).execute()
                            except Exception:
                                pass

                            supabase.table("pedidos").delete().eq("restaurante_id", dados_r["id"]).execute()
                            supabase.table("clientes_estado").delete().eq("restaurante_id", dados_r["phone_id"]).execute()
                            supabase.table("conversas").delete().eq("restaurante_id", dados_r["phone_id"]).execute()
                            supabase.table("restaurantes").delete().eq("id", dados_r["id"]).execute()
                        st.success("Removido!")
                        st.cache_data.clear()
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro ao excluir: {e}")


    with tab2:
        st.subheader("Novo Parceiro")
        with st.form("form_novo_restaurante"):
            c1, c2 = st.columns(2)
            with c1:
                new_nome = st.text_input("Nome Fantasia")
                new_user = st.text_input("Usuário Login")
                new_pass = st.text_input("Senha Login")
            with c2:
                new_phone = st.text_input("Phone ID (ex: 5585...)")
                new_inst = st.text_input("Instância Uazapi")
                new_token = st.text_input("Token Uazapi")
            
            if st.form_submit_button("CADASTRAR", use_container_width=True):
                if not new_nome or not new_phone or not new_user:
                    st.warning("Preencha Nome, Phone ID e Usuário.")
                else:
                    try:
                        supabase.table("restaurantes").insert({
                            "nome": new_nome, "phone_id": new_phone,
                            "usuario": new_user, "senha": new_pass,
                            "instance_name": new_inst, "instance_token": new_token,
                            "cardapio": "", 
                            "system_prompt": "Você é um atendente simpático.",
                            "endereco_loja": "",
                            "telefone_loja": "",
                            "horario_loja": "",
                        }).execute()
                        st.success("Criado com sucesso!")
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro ao criar: {e}")

    with tab3:
        st.subheader("📊 Métricas e estimativa de gastos")

        hoje = datetime.now(timezone.utc).date()
        inicio_padrao = hoje - timedelta(days=6)

        col_d1, col_d2 = st.columns(2)
        with col_d1:
            data_ini = st.date_input("Data inicial", value=inicio_padrao, key="metrica_data_ini")
        with col_d2:
            data_fim = st.date_input("Data final", value=hoje, key="metrica_data_fim")

        if data_ini > data_fim:
            st.warning("Período inválido: a data inicial deve ser menor ou igual à data final.")
            return

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            custo_prompt_1k = st.number_input("Custo prompt (R$/1k)", min_value=0.0, value=0.005, step=0.001, format="%.4f")
        with c2:
            custo_completion_1k = st.number_input("Custo resposta (R$/1k)", min_value=0.0, value=0.010, step=0.001, format="%.4f")
        with c3:
            custo_audio_call = st.number_input("Custo áudio IA (R$/chamada)", min_value=0.0, value=0.006, step=0.001, format="%.4f")
        with c4:
            custo_redis_1k = st.number_input("Custo Redis (R$/1k ops)", min_value=0.0, value=0.001, step=0.001, format="%.4f")

        data_ini_iso = data_ini.isoformat()
        data_fim_iso = data_fim.isoformat()

        metricas = carregar_metricas_periodo(data_ini_iso, data_fim_iso)
        pedidos = carregar_pedidos_periodo(data_ini_iso, data_fim_iso)

        if not metricas and not pedidos:
            st.info("Sem dados no período selecionado.")
            return

        df_m = pd.DataFrame(metricas)
        if df_m.empty:
            df_m = pd.DataFrame(columns=[
                "restaurante_id", "pedidos_total", "ia_calls", "ia_prompt_tokens",
                "ia_completion_tokens", "ia_audio_calls", "redis_ops"
            ])

        for col in ["pedidos_total", "ia_calls", "ia_prompt_tokens", "ia_completion_tokens", "ia_audio_calls", "redis_ops"]:
            if col not in df_m.columns:
                df_m[col] = 0

        agg_m = df_m.groupby("restaurante_id", as_index=False)[[
            "pedidos_total", "ia_calls", "ia_prompt_tokens", "ia_completion_tokens", "ia_audio_calls", "redis_ops"
        ]].sum() if not df_m.empty else pd.DataFrame(columns=[
            "restaurante_id", "pedidos_total", "ia_calls", "ia_prompt_tokens", "ia_completion_tokens", "ia_audio_calls", "redis_ops"
        ])

        df_p = pd.DataFrame(pedidos)
        if not df_p.empty and "restaurante_id" in df_p.columns:
            agg_p = df_p.groupby("restaurante_id", as_index=False).size().rename(columns={"size": "pedidos_periodo"})
        else:
            agg_p = pd.DataFrame(columns=["restaurante_id", "pedidos_periodo"])

        df = agg_m.merge(agg_p, on="restaurante_id", how="outer").fillna(0)
        if df.empty:
            st.info("Sem dados agregados no período.")
            return

        map_rest = {int(r["id"]): str(r.get("nome") or f"Restaurante {r['id']}") for r in restaurantes if r.get("id") is not None}
        df["restaurante_id"] = df["restaurante_id"].astype(int)
        df["Restaurante"] = df["restaurante_id"].map(lambda x: map_rest.get(int(x), f"Restaurante {int(x)}"))

        df["Pedidos"] = df["pedidos_periodo"].astype(int)
        df["IA Calls"] = df["ia_calls"].astype(int)
        df["Prompt Tokens"] = df["ia_prompt_tokens"].astype(int)
        df["Completion Tokens"] = df["ia_completion_tokens"].astype(int)
        df["Áudio IA"] = df["ia_audio_calls"].astype(int)
        df["Redis Ops"] = df["redis_ops"].astype(int)

        df["Custo IA (R$)"] = (df["Prompt Tokens"] / 1000.0) * float(custo_prompt_1k) + (df["Completion Tokens"] / 1000.0) * float(custo_completion_1k)
        df["Custo Áudio (R$)"] = df["Áudio IA"] * float(custo_audio_call)
        df["Custo Redis (R$)"] = (df["Redis Ops"] / 1000.0) * float(custo_redis_1k)
        df["Custo Total (R$)"] = df["Custo IA (R$)"] + df["Custo Áudio (R$)"] + df["Custo Redis (R$)"]

        exibir = df[[
            "Restaurante", "Pedidos", "IA Calls", "Prompt Tokens", "Completion Tokens", "Áudio IA", "Redis Ops",
            "Custo IA (R$)", "Custo Áudio (R$)", "Custo Redis (R$)", "Custo Total (R$)"
        ]].sort_values("Custo Total (R$)", ascending=False)

        total_pedidos = int(exibir["Pedidos"].sum())
        total_custo = float(exibir["Custo Total (R$)"].sum())
        total_ia = int(exibir["IA Calls"].sum())
        total_redis = int(exibir["Redis Ops"].sum())

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Pedidos no período", f"{total_pedidos}")
        m2.metric("IA calls", f"{total_ia}")
        m3.metric("Redis ops", f"{total_redis}")
        m4.metric("Custo total estimado", f"R$ {total_custo:.2f}")

        st.dataframe(exibir, use_container_width=True, hide_index=True)




if st.session_state.user_role == "admin":
    admin_page()
elif st.session_state.user_role == "client":
    restaurant_page()
elif st.session_state.user_role == "motoboy":
    motoboy_page()
else:
    login_page()