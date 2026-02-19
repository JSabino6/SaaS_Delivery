import os
import time
import requests
from datetime import datetime, timezone



try:
    import redis
except Exception:
    redis = None

try:
    from supabase import create_client
except Exception:
    create_client = None

try:
    from groq import Groq
except Exception:
    Groq = None


def _ok(label: str, detail: str = ""):
    msg = f"✅ {label} OK"
    if detail:
        msg += f" — {detail}"
    print(msg)


def _warn(label: str, detail: str = ""):
    msg = f"⚠️ {label} ATENÇÃO"
    if detail:
        msg += f" — {detail}"
    print(msg)


def _fail(label: str, detail: str = ""):
    msg = f"❌ {label} FALHOU"
    if detail:
        msg += f" — {detail}"
    print(msg)


def check_redis(redis_url: str, timeout_s: int = 2) -> bool:
    if not redis_url:
        _warn("Redis", "REDIS_URL vazio (funciona, mas sem dedup/rate-limit/buffer)")
        return False
    if not redis:
        _fail("Redis", "lib 'redis' não instalada")
        return False
    try:
        r = redis.from_url(redis_url, decode_responses=True, socket_timeout=timeout_s)
        pong = r.ping()
        if pong is True:
            _ok("Redis", "ping")
            return True
        _fail("Redis", f"ping retornou {pong!r}")
        return False
    except Exception as e:
        _fail("Redis", str(e))
        return False


def check_supabase(url: str, key: str, timeout_s: int = 4) -> bool:
    if not url or not key:
        _fail("Supabase", "SUPABASE_URL/SUPABASE_KEY não configurados")
        return False
    if not create_client:
        _fail("Supabase", "lib 'supabase' não instalada")
        return False
    try:
        sb = create_client(url, key)



        t0 = time.time()
        resp = sb.table("restaurantes").select("id").limit(1).execute()
        ms = int((time.time() - t0) * 1000)

        if resp.data is not None:
            _ok("Supabase", f"query restaurantes ok ({ms}ms)")
            return True

        _fail("Supabase", "resposta sem data (ver permissões/URL/KEY)")
        return False
    except Exception as e:
        _fail("Supabase", str(e))
        return False


def check_groq(groq_key: str, timeout_s: int = 6) -> bool:
    if not groq_key:
        _warn("Groq", "GROQ_API_KEY vazio (IA não funcionará)")
        return False
    if not Groq:
        _fail("Groq", "lib 'groq' não instalada")
        return False

    try:
        client = Groq(api_key=groq_key)



        t0 = time.time()
        models = client.models.list()
        ms = int((time.time() - t0) * 1000)


        _ok("Groq", f"models.list ok ({ms}ms)")
        return True
    except Exception as e:
        _fail("Groq", str(e))
        return False


def check_public_base_url(public_base_url: str) -> bool:
    if not public_base_url:
        _warn("PUBLIC_BASE_URL", "vazio (links de QR/webhooks podem ficar errados)")
        return False
    _ok("PUBLIC_BASE_URL", public_base_url)
    return True


def check_cron_config(cron_secret: str, public_base_url: str) -> bool:


    if not cron_secret:
        _warn("Cron", "CRON_SECRET vazio (endpoints /cron ficam desprotegidos)")
        return False
    if not public_base_url:
        _warn("Cron", "sem PUBLIC_BASE_URL (não consigo montar URLs de cron)")
        return False


    base = public_base_url.rstrip("/")
    _ok("Cron", "config presente")
    print("   URLs sugeridas:")
    print(f"   - {base}/cron/abandoned-carts?token=CRON_SECRET")
    print(f"   - {base}/cron/reset-states?token=CRON_SECRET")
    print(f"   - {base}/cron/avaliar?token=CRON_SECRET")
    return True


def check_local_api_health(local_api_url: str = "http://127.0.0.1:8000/health", timeout_s: int = 2) -> bool:

    try:
        r = requests.get(local_api_url, timeout=timeout_s)
        if r.status_code == 200:
            _ok("API /health", local_api_url)
            return True
        _warn("API /health", f"status={r.status_code} url={local_api_url}")
        return False
    except Exception as e:
        _warn("API /health", f"não acessível ainda ({e})")
        return False


def startup_diagnostics():
    print("==================================================")
    print("Startup Diagnostics — SaaS WhatsApp")
    print("UTC:", datetime.now(timezone.utc).isoformat())
    print("==================================================")

    redis_url = (os.getenv("REDIS_URL") or "").strip()
    supabase_url = (os.getenv("SUPABASE_URL") or "").strip()
    supabase_key = (os.getenv("SUPABASE_KEY") or "").strip()
    groq_key = (os.getenv("GROQ_API_KEY") or "").strip()
    public_base_url = (os.getenv("PUBLIC_BASE_URL") or "").strip()
    cron_secret = (os.getenv("CRON_SECRET") or "").strip()

    check_public_base_url(public_base_url)
    check_redis(redis_url)
    check_supabase(supabase_url, supabase_key)
    check_groq(groq_key)
    check_cron_config(cron_secret, public_base_url)


    check_local_api_health()

    print("==================================================")
    print("Fim do diagnóstico.")
    print("==================================================")


if __name__ == "__main__":
    startup_diagnostics()