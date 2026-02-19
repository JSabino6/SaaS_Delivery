import psycopg2
from dotenv import load_dotenv
import os

# Carrega as variáveis do arquivo .env
load_dotenv()

# Pega as variáveis
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

print(f"Tentando conectar em: {HOST}:{PORT}")

# Conecta ao banco
try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    print("Conexão realizada com sucesso! (Connection successful!)")
    
    # Cria um cursor para executar comandos SQL
    cursor = connection.cursor()
    
    # Exemplo de query
    cursor.execute("SELECT NOW();")
    result = cursor.fetchone()
    print("Hora atual no banco:", result)

    # Fecha tudo
    cursor.close()
    connection.close()
    print("Conexão fechada.")

except Exception as e:
    print(f"Falha ao conectar: {e}")