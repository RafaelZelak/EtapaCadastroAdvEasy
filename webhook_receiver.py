"""
Aplicação Flask com segurança refinada para o webhook HTTP.
"""

from flask import Flask, request, jsonify, abort
import logging
import os
from dotenv import load_dotenv  # Certifique-se de instalar a biblioteca com `pip install python-dotenv`
from VerifyAndCreateData import process_webhook_data  # Corrigido!
from MovePipeline import exibir_info_pipeline

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

app = Flask(__name__)

# Configura o logging para exibir informações detalhadas
logging.basicConfig(level=logging.INFO)

# Configuração do token de autorização a partir do arquivo .env
AUTH_TOKEN = os.environ.get("WEBHOOK_AUTH_TOKEN")
if not AUTH_TOKEN:
    raise ValueError("WEBHOOK_AUTH_TOKEN não está definido nas variáveis de ambiente.")

def verify_auth_token():
    """
    Verifica se o token de autorização enviado no header é válido.
    Caso não seja fornecido ou seja inválido, interrompe a requisição com erro 401.
    """
    token = request.headers.get('Authorization')
    if not token:
        logging.warning("Tentativa de acesso sem token de autorização.")
        abort(401, description="Token de autorização não fornecido")
    if token != AUTH_TOKEN:
        logging.warning("Token de autorização inválido: %s", token)
        abort(401, description="Token de autorização inválido")

@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Endpoint para receber dados do webhook.
    Espera um JSON com os dados do contato, endereço e card.
    """
    # Verifica o token de autorização
    verify_auth_token()

    # Garante que o conteúdo seja do tipo JSON
    if not request.is_json:
        logging.error("Conteúdo enviado não é JSON.")
        return jsonify({"error": "Content-Type inválido. Esperado application/json"}), 400

    data = request.get_json()
    if not data:
        return jsonify({"error": "Nenhum dado recebido"}), 400

    logging.info("Dados recebidos no webhook: %s", data)

    # Processa os dados e executa as ações necessárias
    resultado = process_webhook_data(data, LOG=True)

    # Executa a função do pipeline com os dados processados
    response_data = exibir_info_pipeline(
        resultado["etapa"],
        resultado["contact_id"],
        resultado.get("card_id")
    )

    return jsonify(response_data), 200

if __name__ == '__main__':
    # Inicia a aplicação na porta 5001
    app.run(port=5001)
