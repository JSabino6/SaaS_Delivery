import os
import requests
import json
from supabase import create_client, Client
from groq import Groq
from dotenv import load_dotenv
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


load_dotenv()

print("--- 🕵️ INICIANDO DIAGNÓSTICO DO SISTEMA ---")


print("\n[1/4] Verificando Variáveis de Ambiente...")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
UAZAPI_BASE_URL = "https://free.uazapi.com" 

if not all([SUPABASE_URL, SUPABASE_KEY, GROQ_API_KEY]):
    print("❌ ERRO: Faltam variáveis no arquivo .env!")
    exit()
else:
    print("✅ Variáveis carregadas.")


print("\n[2/4] Testando Conexão com Supabase e Buscando Credenciais...")
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    response = supabase.table("restaurantes").select("*").limit(1).execute()
    
    if not response.data:
        print("❌ ERRO: Conectou no Supabase, mas a tabela 'restaurantes' está vazia.")
        exit()
    
    dados_loja = response.data[0]
    instance_name = dados_loja.get('instance_name')
    instance_token = dados_loja.get('instance_token')
    
    print(f"✅ Sucesso! Usando loja de teste: {instance_name}")
    print(f"   Token encontrado: {instance_token[:5]}...*****")

except Exception as e:
    print(f"❌ ERRO CRÍTICO NO BANCO: {e}")
    exit()


print("\n[3/4] Testando Envio de Mensagem (API UAZAPI)...")
numero_destino = input("Digite seu número com DDD (ex: 5511999999999) para receber o teste: ").strip()

try:
    url = f"{UAZAPI_BASE_URL}/send/text"
    headers = {
        "token": instance_token.strip(),
        "Content-Type": "application/json"
    }
    payload = {
        "number": numero_destino, 
        "text": "🤖 Teste de Diagnóstico: Se você ler isso, a API está enviando!"
    }
    
    print(f"   Enviando requisição para: {url}")
    resp = requests.post(url, json=payload, headers=headers, verify=False, timeout=15)
    
    print(f"   Status Code: {resp.status_code}")
    print(f"   Resposta API: {resp.text}")
    
    if resp.status_code in [200, 201]:
        print("✅ WHATSAPP OK: Mensagem enviada com sucesso.")
    else:
        print("❌ WHATSAPP FALHOU: O erro está na API de envio ou no Token.")

except Exception as e:
    print(f"❌ ERRO DE CONEXÃO: {e}")


print("\n[4/4] Testando Inteligência Artificial (Groq)...")
try:
    client = Groq(api_key=GROQ_API_KEY)
    
    print("   Enviando prompt de teste para Llama-3...")
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": "Responda apenas com um JSON: {\"status\": \"funcionando\", \"mensagem\": \"IA Operacional\"}"
            },
            {
                "role": "user",
                "content": "Teste de sistema."
            }
        ],
        model="llama-3.3-70b-versatile",
        response_format={"type": "json_object"}
    )
    
    resposta_ia = chat_completion.choices[0].message.content
    print(f"   Resposta Crua da IA: {resposta_ia}")
    

    json_ia = json.loads(resposta_ia)
    print("✅ IA OK: Resposta gerada e JSON válido.")

except Exception as e:
    print(f"❌ IA FALHOU: {e}")
    print("   Verifique sua API KEY da Groq ou se o modelo está indisponível.")

print("\n--- 🏁 FIM DO DIAGNÓSTICO ---")