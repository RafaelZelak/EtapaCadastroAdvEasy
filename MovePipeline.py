import logging
import json
import os
from utils import bitrixUtils
from dotenv import load_dotenv

# Carrega as variáveis do .env
load_dotenv()

# Obtém as configurações do .env
BITRIX_WEBHOOK_URL = os.getenv("BITRIX_WEBHOOK_URL")
CARD_ENTITY_TYPE_ID = int(os.getenv("CARD_ENTITY_TYPE_ID", 128))  # Padrão: 128

# Mapeamento das etapas para os respectivos Stage IDs
ETAPAS_STAGE_IDS = {
    1: "DT128_203:UC_XANPBC",
    2: "DT128_203:PREPARATION",
    3: "DT128_203:NEW",
    4: "DT128_203:UC_3UTEIE",
    5: "DT128_203:CLIENT"
}

def exibir_info_pipeline(etapa, contact_id, card_id):
    """
    Exibe as informações necessárias para o pipeline no Bitrix24 e move o card para a nova etapa.

    :param etapa: Etapa do card vinda do JSON.
    :param contact_id: ID do contato no Bitrix24.
    :param card_id: ID do card associado ao contato.
    """
    category_id = None
    stage_id = None

    # 🔍 Se houver um card associado, busca os detalhes do card
    if card_id:
        card_info_str = bitrixUtils.obterCampos(CARD_ENTITY_TYPE_ID, card_id, BITRIX_WEBHOOK_URL, LOG=False)

        try:
            # 🔥 Converte JSON string em dicionário
            card_info = json.loads(card_info_str)

            # ✅ Agora acessamos diretamente `categoryId` e `stageId`
            category_id = card_info.get("categoryId")
            stage_id = card_info.get("stageId")

            print(f"[DEBUG] Category ID: {category_id}")
            print(f"[DEBUG] Stage ID: {stage_id}")

        except json.JSONDecodeError:
            logging.error("[ERRO] Falha ao converter a resposta do Bitrix para JSON.")

    # 🔥 Exibe as informações no log
    logging.info(f"[PIPELINE INFO] Etapa: {etapa}")
    logging.info(f"[PIPELINE INFO] ID do Usuário: {contact_id}")
    logging.info(f"[PIPELINE INFO] ID do Card: {card_id}")
    logging.info(f"[PIPELINE INFO] categoryId: {category_id}")
    logging.info(f"[PIPELINE INFO] stageId: {stage_id}")

    # ✅ Move o card para a etapa correspondente, se necessário
    novo_stage_id = ETAPAS_STAGE_IDS.get(etapa)

    if novo_stage_id and novo_stage_id != stage_id:
        sucesso = bitrixUtils.moverEtapaCard(novo_stage_id, card_id, CARD_ENTITY_TYPE_ID, BITRIX_WEBHOOK_URL, LOG=False)

        if sucesso:
            logging.info(f"[MOVER ETAPA] Card {card_id} movido para {novo_stage_id}.")
        else:
            logging.error(f"[MOVER ETAPA] Falha ao mover card {card_id} para {novo_stage_id}.")

    return {
        "etapa": etapa,
        "contact_id": contact_id,
        "card_id": card_id,
        "categoryId": category_id,
        "stageId": stage_id
    }
