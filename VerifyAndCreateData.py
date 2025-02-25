import sys
import os
import logging
import json
import time
from dotenv import load_dotenv
load_dotenv()

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from utils import bitrixUtils
# Variáveis do Bitrix24
BITRIX_WEBHOOK_URL = os.getenv("BITRIX_WEBHOOK_URL")
CPF_FIELD = os.getenv("CPF_FIELD")
CARD_ENTITY_TYPE_ID = int(os.getenv("CARD_ENTITY_TYPE_ID", 128))
CARD_STAGE_ID = os.getenv("CARD_STAGE_ID")
CARD_CATEGORY_ID = int(os.getenv("CARD_CATEGORY_ID", 5))
CARD_ASSIGNED_BY_ID = int(os.getenv("CARD_ASSIGNED_BY_ID", 1))


def is_endereco_vazio(endereco):
    """ Verifica se o endereço está vazio ou contém apenas uma vírgula. """
    return not endereco or endereco.strip() == ","

def process_webhook_data(data, LOG=False):
    """
    Processa os dados recebidos e executa a lógica de criação no Bitrix24.

    Fluxo:
        1. Verifica se o CPF já existe no Bitrix.
        2. Se existir, busca o card vinculado e enriquece com dados faltantes.
        3. Se não existir um card, cria um novo e adiciona as informações recebidas.
        4. Se o contato não existir, cria o contato e endereço, depois cria o card.
        5. Movimenta o card para a nova etapa caso necessário.

    Retorno:
        dict: IDs do contato e card criados ou mensagem de erro.
    """
    cpf = data.get("cpf")
    if not cpf:
        return {"error": "CPF não informado"}

    # 🔍 Verifica se o contato já existe no Bitrix24
    contact_id = bitrixUtils.verificarContato(cpf, CPF_FIELD, BITRIX_WEBHOOK_URL, LOG=LOG)

    if contact_id:
        logging.info(f"[INFO] Contato já existe no Bitrix24. ID: {contact_id}")

        # 🔎 Busca se já existe um card vinculado a esse contato
        card_id = bitrixUtils.obterCardPorContato(contact_id, CARD_ENTITY_TYPE_ID, BITRIX_WEBHOOK_URL, LOG=LOG)

        if card_id:
            logging.info(f"[INFO] Card já existe para o contato ID {contact_id}. ID do Card: {card_id}")

            # 🔍 Obtém os campos do card existente
            card_info_str = bitrixUtils.obterCampos(CARD_ENTITY_TYPE_ID, card_id, BITRIX_WEBHOOK_URL, LOG=LOG)
            card_info = json.loads(card_info_str) if isinstance(card_info_str, str) else card_info_str

            # 🔥 Identifica os campos que precisam ser atualizados no card
            campos_para_atualizar = {}
            campos_bitrix = {
                "numOAB": "ufCrm41_1737980095947",
                "UFdaOAB": "ufCrm41_1737980514688",
                "pacote": "ufCrm41_1739881889472"
            }

            for campo, bitrix_field in campos_bitrix.items():
                if not card_info.get(bitrix_field) and data.get(campo):
                    campos_para_atualizar[bitrix_field] = data[campo]

            # 🔄 Verifica se UFdaOAB precisa ser convertido para ID antes de atualizar
            if "ufCrm41_1737980514688" in campos_para_atualizar:
                valores_uf = bitrixUtils.obterCampoEspecifico("ufCrm41_1737980514688", CARD_ENTITY_TYPE_ID, BITRIX_WEBHOOK_URL)
                if valores_uf and isinstance(valores_uf, dict):
                    items = valores_uf.get("ufCrm41_1737980514688", {}).get("items", [])
                    for item in items:
                        if item.get("VALUE") == data["UFdaOAB"]:
                            campos_para_atualizar["ufCrm41_1737980514688"] = item.get("ID")
                            break

            # 🔄 Atualiza o card se houver novos dados
            if campos_para_atualizar:
                sucesso = bitrixUtils.atualizarCard(card_id, campos_para_atualizar, CARD_ENTITY_TYPE_ID, BITRIX_WEBHOOK_URL, LOG=LOG)
                if sucesso:
                    logging.info(f"[ATUALIZAR CARD] Card ID {card_id} atualizado com novos dados.")
                else:
                    logging.error(f"[ATUALIZAR CARD] Falha ao atualizar Card ID {card_id}.")

            # 🔎 Verifica se o contato tem um endereço vinculado e adiciona se necessário
            endereco_atual = bitrixUtils.obterEndereco(contact_id, BITRIX_WEBHOOK_URL, LOG=LOG)

            if not endereco_atual or is_endereco_vazio(endereco_atual.get("ADDRESS_1", "")):
                logging.info(f"[INFO] Contato ID {contact_id} não tem endereço válido. Criando novo...")
                address_data = {
                    "rua": data.get("rua", ""),
                    "numero": data.get("numCasa", ""),
                    "cidade": data.get("cidade", ""),
                    "cep": data.get("CEP", ""),
                    "estado": data.get("estado", ""),
                    "bairro": data.get("bairro", ""),
                    "complemento": data.get("complemento", "")
                }
                bitrixUtils.criarEndereco(contact_id, address_data, BITRIX_WEBHOOK_URL, LOG=LOG)
            else:
                logging.info(f"[INFO] Contato ID {contact_id} já tem endereço válido. Nenhuma ação necessária.")

            return {"etapa": data.get("etapa"), "contact_id": contact_id, "card_id": card_id}

    # 🔥 Se o contato não existe, cria o contato e o endereço
    logging.info(f"[CRIAR CONTATO] Criando novo contato para CPF: {cpf}")

    contact_data = {
        "cpf": cpf,
        "name": data.get("name", ""),
        "email": data.get("email", ""),
        "celular": data.get("celular", "")
    }

    contact_id = bitrixUtils.criarContato(contact_data, CPF_FIELD, BITRIX_WEBHOOK_URL, LOG=LOG)
    if not contact_id:
        return {"error": "Falha ao criar contato"}

    # Criar endereço associado ao contato
    address_data = {
        "rua": data.get("rua", ""),
        "numero": data.get("numCasa", ""),
        "cidade": data.get("cidade", ""),
        "cep": data.get("CEP", ""),
        "estado": data.get("estado", ""),
        "bairro": data.get("bairro", ""),
        "complemento": data.get("complemento", "")
    }

    bitrixUtils.criarEndereco(contact_id, address_data, BITRIX_WEBHOOK_URL, LOG=LOG)

    # Criar um novo Card para esse novo contato
    logging.info(f"[CRIAR CARD] Criando novo Card para o contato ID: {contact_id}")

    extra_fields = {}
    if data.get("UFdaOAB"):
        valores_uf = bitrixUtils.obterCampoEspecifico("ufCrm41_1737980514688", CARD_ENTITY_TYPE_ID, BITRIX_WEBHOOK_URL)
        if valores_uf and isinstance(valores_uf, dict):
            items = valores_uf.get("ufCrm41_1737980514688", {}).get("items", [])
            for item in items:
                if item.get("VALUE") == data["UFdaOAB"]:
                    extra_fields["ufCrm41_1737980514688"] = item.get("ID")
                    break

    if data.get("numOAB"):
        extra_fields["ufCrm41_1737980095947"] = data["numOAB"]

    card_title = f"{data.get('name')}"
    card_id = bitrixUtils.criarCardContato(
        card_title, CARD_STAGE_ID, CARD_CATEGORY_ID, CARD_ASSIGNED_BY_ID, contact_id,
        BITRIX_WEBHOOK_URL, extra_fields=extra_fields, LOG=LOG
    )

    return {"etapa": data.get("etapa"), "contact_id": contact_id, "card_id": card_id}
