import requests
import logging
import json
import time

logging.basicConfig(
    level=logging.INFO,
    format="\n%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()  # Exibe logs no console
    ]
)

def executar_requisicao_com_retry(endpoint, payload, max_tentativas=5, LOG=False):
    """
    Executa uma requisição HTTP com retries automáticos e backoff exponencial em caso de erro 503 (Too Many Requests).

    :param endpoint: URL do Bitrix24 a ser chamada.
    :param payload: Dicionário contendo os dados da requisição.
    :param max_tentativas: Número máximo de tentativas antes de desistir.
    :param LOG: Se True, ativa logs detalhados.
    :return: Resposta JSON da API ou None em caso de falha.
    """
    tentativa = 0
    delay = 2  # Começa com 2 segundos de espera

    while tentativa < max_tentativas:
        try:
            if LOG:
                logging.info(f"[REQUISIÇÃO] Tentativa {tentativa + 1}/{max_tentativas} para {endpoint}")
                logging.info(f"[REQUISIÇÃO] Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")

            response = requests.post(endpoint, json=payload)

            if LOG:
                logging.info(f"[REQUISIÇÃO] Resposta: {response.status_code} - {response.text}")

            if response.status_code == 200:
                return response.json()

            elif response.status_code == 503:  # Caso seja erro "Too Many Requests"
                logging.warning(f"[REQUISIÇÃO] Erro 503: Too Many Requests. Tentando novamente em {delay} segundos...")
                time.sleep(delay)
                delay *= 2  # Aumenta o tempo de espera exponencialmente
                tentativa += 1
            else:
                logging.error(f"[REQUISIÇÃO] Falha na requisição: {response.status_code} - {response.text}")
                return None

        except requests.RequestException as e:
            logging.error(f"[REQUISIÇÃO] Erro ao conectar com Bitrix24: {str(e)}")
            return None

    logging.error("[REQUISIÇÃO] Número máximo de tentativas atingido. Requisição falhou.")
    return None

def log_detalhado(mensagem, tag_log=False):
    """
    Função para gerar logs detalhados se a tag LOG estiver ativa.

    :param mensagem: Mensagem a ser logada.
    :param tag_log: Se True, exibe/loga a mensagem.
    """
    if tag_log:
        logging.info(mensagem)

def verificarContato(cpf, cpf_field, bitrix_webhook_url, LOG=False):
    """
    Verifica se um contato com o CPF fornecido já existe no Bitrix24.

    :param cpf: CPF do contato a ser verificado (string no formato "123.456.789-00").
    :param cpf_field: Campo personalizado do Bitrix24 que armazena o CPF (string).
    :param bitrix_webhook_url: URL do webhook do Bitrix24 (string).
    :param LOG: Se True, ativa logs detalhados.
    :return: ID do contato se existir, None caso contrário.
    """
    endpoint = f"{bitrix_webhook_url}crm.contact.list"
    payload = {
        "filter": {cpf_field: cpf},
        "select": ["ID"]
    }

    log_detalhado(f"[VERIFICAR CONTATO] Verificando contato com CPF: {cpf}", LOG)
    log_detalhado(f"[VERIFICAR CONTATO] Endpoint: {endpoint}", LOG)
    log_detalhado(f"[VERIFICAR CONTATO] Payload: {payload}", LOG)

    max_tentativas = 5
    delay = 2  # Tempo inicial de espera

    for tentativa in range(max_tentativas):
        try:
            response = requests.post(endpoint, json=payload, timeout=10)
            log_detalhado(f"[VERIFICAR CONTATO] Resposta do Bitrix24: {response.status_code} - {response.text}", LOG)

            if response.status_code == 200:
                data = response.json()
                if data.get("result"):
                    contact_id = data["result"][0]["ID"]
                    log_detalhado(f"[VERIFICAR CONTATO] Contato encontrado. ID: {contact_id}", LOG)
                    return contact_id
                else:
                    log_detalhado("[VERIFICAR CONTATO] Nenhum contato encontrado com esse CPF.", LOG)
                    return None
            elif response.status_code == 503:
                log_detalhado(f"[VERIFICAR CONTATO] Erro 503: Too Many Requests. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
                time.sleep(delay)
                delay *= 2  # Aumenta o tempo de espera exponencialmente
            else:
                log_detalhado(f"[VERIFICAR CONTATO] Erro na requisição: {response.status_code} - {response.text}", LOG)
                return None

        except requests.Timeout:
            log_detalhado(f"[VERIFICAR CONTATO] Timeout na requisição. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
            time.sleep(delay)
            delay *= 2  # Aumenta o tempo de espera exponencialmente
        except requests.RequestException as e:
            log_detalhado(f"[VERIFICAR CONTATO] Erro ao conectar com Bitrix24: {str(e)}", LOG)
            return None

    log_detalhado("[VERIFICAR CONTATO] Número máximo de tentativas atingido. Requisição falhou.", LOG)
    return None

def criarContato(contact_data, cpf_field, bitrix_webhook_url, extra_fields=None, LOG=False):
    """
    Cria um novo contato no Bitrix24 com os dados fornecidos.

    :param contact_data: Dicionário contendo os dados do contato:
        - cpf: CPF do contato (string no formato "123.456.789-00").
        - name: Nome do contato (string).
        - email: E-mail do contato (string).
        - celular: Número de telefone do contato (string no formato "(11) 98765-4321").
    :param cpf_field: Campo personalizado do Bitrix24 que armazena o CPF (string).
    :param bitrix_webhook_url: URL do webhook do Bitrix24 (string).
    :param extra_fields: Dicionário com campos extras a serem incluídos no contato (opcional).
    :param LOG: Se True, ativa logs detalhados.
    :return: ID do contato criado ou None em caso de falha.
    """
    endpoint = f"{bitrix_webhook_url}crm.contact.add"
    payload = {
        "fields": {
            cpf_field: contact_data["cpf"],
            "NAME": contact_data["name"],
            "EMAIL": [{"VALUE": contact_data["email"], "VALUE_TYPE": "WORK"}],
            "PHONE": [{"VALUE": contact_data["celular"], "VALUE_TYPE": "WORK"}]
        }
    }

    # Adiciona campos extras, se fornecidos
    if extra_fields and isinstance(extra_fields, dict):
        payload["fields"].update(extra_fields)

    log_detalhado(f"[CRIAR CONTATO] Criando novo contato com dados: {contact_data}", LOG)
    log_detalhado(f"[CRIAR CONTATO] Endpoint: {endpoint}", LOG)
    log_detalhado(f"[CRIAR CONTATO] Payload: {payload}", LOG)

    max_tentativas = 5
    delay = 2  # Tempo inicial de espera

    for tentativa in range(max_tentativas):
        try:
            response = requests.post(endpoint, json=payload, timeout=10)
            log_detalhado(f"[CRIAR CONTATO] Resposta do Bitrix24: {response.status_code} - {response.text}", LOG)

            if response.status_code == 200:
                data = response.json()
                contact_id = data.get("result")
                if contact_id:
                    log_detalhado(f"[CRIAR CONTATO] Contato criado com sucesso. ID: {contact_id}", LOG)
                    return contact_id
                else:
                    log_detalhado("[CRIAR CONTATO] Falha ao obter o ID do contato criado.", LOG)
                    return None
            elif response.status_code == 503:
                log_detalhado(f"[CRIAR CONTATO] Erro 503: Too Many Requests. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
                time.sleep(delay)
                delay *= 2  # Aumenta o tempo de espera exponencialmente
            else:
                log_detalhado(f"[CRIAR CONTATO] Erro ao criar contato: {response.status_code} - {response.text}", LOG)
                return None

        except requests.Timeout:
            log_detalhado(f"[CRIAR CONTATO] Timeout na requisição. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
            time.sleep(delay)
            delay *= 2  # Aumenta o tempo de espera exponencialmente
        except requests.RequestException as e:
            log_detalhado(f"[CRIAR CONTATO] Erro ao conectar com Bitrix24: {str(e)}", LOG)
            return None

    log_detalhado("[CRIAR CONTATO] Número máximo de tentativas atingido. Requisição falhou.", LOG)
    return None

def criarEndereco(contact_id, address_data, bitrix_webhook_url, extra_fields=None, LOG=False):
    """
    Cria um endereço no Bitrix24 e vincula ao contato especificado.

    :param contact_id: ID do contato ao qual o endereço será vinculado (int ou string).
    :param address_data: Dicionário contendo os dados do endereço:
        - rua: Nome da rua (string).
        - numero: Número do endereço (string).
        - cidade: Nome da cidade (string).
        - cep: CEP do endereço (string no formato "01234-567").
        - estado: Sigla do estado (string no formato "SP").
        - bairro: Nome do bairro (string).
        - complemento: Complemento do endereço (string).
    :param bitrix_webhook_url: URL do webhook do Bitrix24 (string).
    :param extra_fields: Dicionário com campos extras a serem incluídos no endereço (opcional).
    :param LOG: Se True, ativa logs detalhados.
    :return: ID do endereço criado ou None em caso de falha.
    """
    endpoint = f"{bitrix_webhook_url}crm.address.add"
    payload = {
        "fields": {
            "ENTITY_ID": contact_id,
            "ENTITY_TYPE_ID": 3,
            "TYPE_ID": 1,
            "ADDRESS_1": f"{address_data['rua']}, {address_data['numero']}",
            "CITY": address_data["cidade"],
            "POSTAL_CODE": address_data["cep"],
            "COUNTRY": "Brasil",
            "PROVINCE": address_data["estado"],
            "ADDRESS_2": f"{address_data['bairro']}, {address_data['complemento']}"
        }
    }

    # Adiciona campos extras, se fornecidos
    if extra_fields and isinstance(extra_fields, dict):
        payload["fields"].update(extra_fields)

    log_detalhado(f"[CRIAR ENDEREÇO] Criando endereço para o contato ID: {contact_id}", LOG)
    log_detalhado(f"[CRIAR ENDEREÇO] Endpoint: {endpoint}", LOG)
    log_detalhado(f"[CRIAR ENDEREÇO] Payload: {payload}", LOG)

    max_tentativas = 5
    delay = 2  # Tempo inicial de espera

    for tentativa in range(max_tentativas):
        try:
            response = requests.post(endpoint, json=payload, timeout=10)
            log_detalhado(f"[CRIAR ENDEREÇO] Resposta do Bitrix24: {response.status_code} - {response.text}", LOG)

            if response.status_code == 200:
                data = response.json()
                address_id = data.get("result")
                if address_id:
                    log_detalhado(f"[CRIAR ENDEREÇO] Endereço criado com sucesso. ID: {address_id}", LOG)
                    return address_id
                else:
                    log_detalhado("[CRIAR ENDEREÇO] Falha ao obter o ID do endereço criado.", LOG)
                    return None
            elif response.status_code == 503:
                log_detalhado(f"[CRIAR ENDEREÇO] Erro 503: Too Many Requests. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
                time.sleep(delay)
                delay *= 2  # Aumenta o tempo de espera exponencialmente
            else:
                log_detalhado(f"[CRIAR ENDEREÇO] Erro ao criar endereço: {response.status_code} - {response.text}", LOG)
                return None

        except requests.Timeout:
            log_detalhado(f"[CRIAR ENDEREÇO] Timeout na requisição. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
            time.sleep(delay)
            delay *= 2  # Aumenta o tempo de espera exponencialmente
        except requests.RequestException as e:
            log_detalhado(f"[CRIAR ENDEREÇO] Erro ao conectar com Bitrix24: {str(e)}", LOG)
            return None

    log_detalhado("[CRIAR ENDEREÇO] Número máximo de tentativas atingido. Requisição falhou.", LOG)
    return None

def criarCard(title, stage_id, category_id, assigned_by_id, bitrix_webhook_url, contact_id=None, extra_fields=None, LOG=False):
    """
    Cria um novo card no pipeline do Bitrix24.

    :param title: Título do card (string).
    :param stage_id: ID do estágio inicial do card (string).
    :param category_id: ID da categoria do card (int ou string).
    :param assigned_by_id: ID do responsável pelo card (int ou string).
    :param bitrix_webhook_url: URL do webhook do Bitrix24 (string).
    :param contact_id: ID do contato a ser vinculado ao card (opcional, int ou string).
    :param extra_fields: Dicionário com campos extras a serem incluídos no card (opcional).
    :param LOG: Se True, ativa logs detalhados.
    :return: ID do card criado ou None em caso de falha.
    """
    endpoint = f"{bitrix_webhook_url}crm.item.add"
    payload = {
        "entityTypeId": 128,
        "fields": {
            "title": title,
            "stageId": stage_id,
            "categoryId": category_id,
            "assignedById": assigned_by_id,
            "contactId": contact_id
        }
    }

    # Adiciona campos extras, se fornecidos
    if extra_fields and isinstance(extra_fields, dict):
        payload["fields"].update(extra_fields)

    log_detalhado(f"[CRIAR CARD] Criando card com título: {title}", LOG)
    log_detalhado(f"[CRIAR CARD] Endpoint: {endpoint}", LOG)
    log_detalhado(f"[CRIAR CARD] Payload: {payload}", LOG)

    max_tentativas = 5
    delay = 2  # Tempo inicial de espera

    for tentativa in range(max_tentativas):
        try:
            response = requests.post(endpoint, json=payload, timeout=10)
            log_detalhado(f"[CRIAR CARD] Resposta do Bitrix24: {response.status_code} - {response.text}", LOG)

            if response.status_code == 200:
                data = response.json()
                log_detalhado(f"[CRIAR CARD] Resposta completa do Bitrix24: {data}", LOG)  # Adiciona log da resposta completa
                card_id = data.get("result", {}).get("item", {}).get("id", None)  # Correção aqui

                if card_id:
                    log_detalhado(f"[CRIAR CARD] Card criado com sucesso. ID: {card_id}", LOG)
                    return card_id
                else:
                    log_detalhado("[CRIAR CARD] Falha ao obter o ID do card criado.", LOG)
                    return None
            elif response.status_code == 503:
                log_detalhado(f"[CRIAR CARD] Erro 503: Too Many Requests. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
                time.sleep(delay)
                delay *= 2  # Aumenta o tempo de espera exponencialmente
            else:
                log_detalhado(f"[CRIAR CARD] Erro ao criar card: {response.status_code} - {response.text}", LOG)
                return None

        except requests.Timeout:
            log_detalhado(f"[CRIAR CARD] Timeout na requisição. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
            time.sleep(delay)
            delay *= 2  # Aumenta o tempo de espera exponencialmente
        except requests.RequestException as e:
            log_detalhado(f"[CRIAR CARD] Erro ao conectar com Bitrix24: {str(e)}", LOG)
            return None

    log_detalhado("[CRIAR CARD] Número máximo de tentativas atingido. Requisição falhou.", LOG)
    return None

def criarCardContato(title, stage_id, category_id, assigned_by_id, contact_id, bitrix_webhook_url, extra_fields=None, LOG=False):
    """
    Cria um novo card no pipeline do Bitrix24 e vincula a um contato.

    :param title: Título do card (string).
    :param stage_id: ID do estágio inicial do card (string).
    :param category_id: ID da categoria do card (int ou string).
    :param assigned_by_id: ID do responsável pelo card (int ou string).
    :param contact_id: ID do contato a ser vinculado ao card (int ou string).
    :param bitrix_webhook_url: URL do webhook do Bitrix24 (string).
    :param extra_fields: Dicionário com campos extras a serem incluídos no card (opcional).
    :param LOG: Se True, ativa logs detalhados.
    :return: ID do card criado ou None em caso de falha.
    """
    return criarCard(title, stage_id, category_id, assigned_by_id, bitrix_webhook_url, contact_id, extra_fields, LOG)

def obterCamposPersonalizados(entity_type_id, bitrix_webhook_url, LOG=False):
    """
    Obtém os metadados dos campos personalizados da entidade, incluindo os valores das listas de seleção.

    :param entity_type_id: ID da entidade no Bitrix24.
    :param bitrix_webhook_url: URL do webhook do Bitrix24.
    :param LOG: Se True, ativa logs detalhados.
    :return: Dicionário com os metadados dos campos personalizados.
    """
    endpoint = f"{bitrix_webhook_url}crm.item.fields"
    payload = {"entityTypeId": entity_type_id}

    log_detalhado(f"[OBTER CAMPOS PERSONALIZADOS] Buscando metadados para entityTypeId: {entity_type_id}", LOG)

    max_tentativas = 5
    delay = 2  # Tempo inicial de espera

    for tentativa in range(max_tentativas):
        try:
            response = requests.post(endpoint, json=payload, timeout=10)
            log_detalhado(f"[OBTER CAMPOS PERSONALIZADOS] Resposta: {response.status_code} - {response.text}", LOG)

            if response.status_code == 200:
                return response.json().get("result", {})
            elif response.status_code == 503:
                log_detalhado(f"[OBTER CAMPOS PERSONALIZADOS] Erro 503: Too Many Requests. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
                time.sleep(delay)
                delay *= 2  # Aumenta o tempo de espera exponencialmente
            else:
                log_detalhado(f"[OBTER CAMPOS PERSONALIZADOS] Erro na requisição: {response.status_code} - {response.text}", LOG)
                return None

        except requests.Timeout:
            log_detalhado(f"[OBTER CAMPOS PERSONALIZADOS] Timeout na requisição. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
            time.sleep(delay)
            delay *= 2  # Aumenta o tempo de espera exponencialmente
        except requests.RequestException as e:
            log_detalhado(f"[OBTER CAMPOS PERSONALIZADOS] Erro ao conectar com Bitrix24: {str(e)}", LOG)
            return None

    log_detalhado("[OBTER CAMPOS PERSONALIZADOS] Número máximo de tentativas atingido. Requisição falhou.", LOG)
    return None

def mapearCampos(campos, metadados):
    """
    Converte IDs de campos personalizados para seus valores reais.

    :param campos: Dicionário com os campos do item obtido.
    :param metadados: Dicionário com os metadados dos campos personalizados.
    :return: Dicionário com os campos formatados.
    """
    if not isinstance(campos, dict) or not isinstance(metadados, dict):
        return campos  # Retorna inalterado se os parâmetros não forem dicionários válidos

    for campo, valor in campos.items():
        if campo in metadados and metadados[campo].get("type") == "enumeration":
            opcoes = {str(item["ID"]): item["VALUE"] for item in metadados[campo].get("items", [])}

            if isinstance(valor, list):  # Se for múltipla escolha
                campos[campo] = [opcoes.get(str(v), v) for v in valor if v is not None]
            else:
                campos[campo] = opcoes.get(str(valor), valor) if valor is not None else valor

    return campos

def obterCampos(entity_type_id, item_id, bitrix_webhook_url, LOG=False):
    """
    Obtém todos os campos de um item específico no Bitrix24 e mapeia valores de listas de seleção.

    :param entity_type_id: Tipo da entidade (ex: 128 para AdvEasy, 158 para Sittax, etc.).
    :param item_id: ID do item a ser consultado.
    :param bitrix_webhook_url: URL do webhook do Bitrix24.
    :param LOG: Se True, ativa logs detalhados.
    :return: Dicionário com os campos formatados ou None em caso de erro.
    """
    endpoint = f"{bitrix_webhook_url}crm.item.get"
    payload = {"entityTypeId": entity_type_id, "id": item_id}

    log_detalhado(f"[OBTER CAMPOS] Buscando campos do item ID: {item_id} (entityTypeId: {entity_type_id})", LOG)

    max_tentativas = 5
    delay = 2  # Tempo inicial de espera

    for tentativa in range(max_tentativas):
        try:
            response = requests.post(endpoint, json=payload, timeout=10)
            log_detalhado(f"[OBTER CAMPOS] Resposta do Bitrix24: {response.status_code} - {response.text}", LOG)

            if response.status_code == 200:
                data = response.json()
                if "result" in data and "item" in data["result"]:
                    campos = data["result"]["item"]
                    log_detalhado(f"[OBTER CAMPOS] Campos obtidos com sucesso.", LOG)

                    # Obtém os metadados dos campos personalizados
                    metadados = obterCamposPersonalizados(entity_type_id, bitrix_webhook_url, LOG)
                    if metadados:
                        campos = mapearCampos(campos, metadados)

                    return json.dumps(campos, indent=4, ensure_ascii=False)
                else:
                    log_detalhado("[OBTER CAMPOS] Nenhum campo encontrado ou erro na resposta.", LOG)
                    return None
            elif response.status_code == 503:
                log_detalhado(f"[OBTER CAMPOS] Erro 503: Too Many Requests. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
                time.sleep(delay)
                delay *= 2  # Aumenta o tempo de espera exponencialmente
            else:
                log_detalhado(f"[OBTER CAMPOS] Erro na requisição: {response.status_code} - {response.text}", LOG)
                return None

        except requests.Timeout:
            log_detalhado(f"[OBTER CAMPOS] Timeout na requisição. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
            time.sleep(delay)
            delay *= 2  # Aumenta o tempo de espera exponencialmente
        except requests.RequestException as e:
            log_detalhado(f"[OBTER CAMPOS] Erro ao conectar com Bitrix24: {str(e)}", LOG)
            return None

    log_detalhado("[OBTER CAMPOS] Número máximo de tentativas atingido. Requisição falhou.", LOG)
    return None

def obterCampoEspecifico(campo_personalizado, entity_type_id, bitrix_webhook_url, LOG=False):
    """
    Obtém os metadados de um campo personalizado específico no Bitrix24.

    :param campo_personalizado: Nome do campo personalizado a ser consultado (ex: ufCrm41_1737980514688).
    :param entity_type_id: ID da entidade no Bitrix24.
    :param bitrix_webhook_url: URL do webhook do Bitrix24.
    :param LOG: Se True, ativa logs detalhados.
    :return: Dicionário com os metadados do campo específico ou None em caso de erro.
    """
    endpoint = f"{bitrix_webhook_url}crm.item.fields"
    payload = {"entityTypeId": entity_type_id}

    max_tentativas = 5
    delay = 2  # Tempo inicial de espera

    for tentativa in range(max_tentativas):
        try:
            response = requests.post(endpoint, json=payload, timeout=10)

            if response.status_code == 200:
                campos = response.json().get("result", {}).get("fields", {})
                if campo_personalizado in campos:
                    return {campo_personalizado: campos[campo_personalizado]}
                return None
            elif response.status_code == 503:
                if LOG:
                    logging.warning(f"[OBTER CAMPO ESPECÍFICO] Erro 503: Too Many Requests. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...")
                time.sleep(delay)
                delay *= 2  # Aumenta o tempo de espera exponencialmente
            else:
                if LOG:
                    logging.error(f"[OBTER CAMPO ESPECÍFICO] Erro na requisição: {response.status_code} - {response.text}")
                return None

        except requests.Timeout:
            if LOG:
                logging.warning(f"[OBTER CAMPO ESPECÍFICO] Timeout na requisição. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...")
            time.sleep(delay)
            delay *= 2  # Aumenta o tempo de espera exponencialmente
        except requests.RequestException as e:
            if LOG:
                logging.error(f"[OBTER CAMPO ESPECÍFICO] Erro ao conectar com Bitrix24: {str(e)}")
            return None

    if LOG:
        logging.error("[OBTER CAMPO ESPECÍFICO] Número máximo de tentativas atingido. Requisição falhou.")
    return None

def obterCardPorContato(contact_id, entity_type_id, bitrix_webhook_url, LOG=False):
    """
    Verifica se existe um Card associado ao contato informado.

    :param contact_id: ID do contato no Bitrix24.
    :param entity_type_id: Tipo de entidade no Bitrix24 (ex: 128 para AdvEasy, 158 para outra entidade).
    :param bitrix_webhook_url: URL do webhook do Bitrix24.
    :param LOG: Se True, ativa logs detalhados.
    :return: ID do card encontrado ou None se nenhum card estiver vinculado ao contato.
    """
    endpoint = f"{bitrix_webhook_url}crm.item.list"
    payload = {
        "entityTypeId": entity_type_id,  # Agora `entityTypeId` é passado como argumento!
        "filter": {
            "contactId": contact_id
        },
        "select": ["id"]
    }

    log_detalhado(f"[OBTER CARD] Buscando card para o contato ID: {contact_id}, EntityType: {entity_type_id}", LOG)

    max_tentativas = 5
    delay = 2  # Tempo inicial de espera

    for tentativa in range(max_tentativas):
        try:
            response = requests.post(endpoint, json=payload, timeout=10)
            log_detalhado(f"[OBTER CARD] Resposta do Bitrix24: {response.status_code} - {response.text}", LOG)

            if response.status_code == 200:
                data = response.json()
                items = data.get("result", {}).get("items", [])
                if items:
                    card_id = items[0]["id"]  # Pega o primeiro card associado ao contato
                    log_detalhado(f"[OBTER CARD] Card encontrado. ID: {card_id}", LOG)
                    return card_id
                else:
                    log_detalhado("[OBTER CARD] Nenhum card encontrado para o contato.", LOG)
                    return None
            elif response.status_code == 503:
                log_detalhado(f"[OBTER CARD] Erro 503: Too Many Requests. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
                time.sleep(delay)
                delay *= 2  # Aumenta o tempo de espera exponencialmente
            else:
                log_detalhado(f"[OBTER CARD] Erro na requisição: {response.status_code} - {response.text}", LOG)
                return None

        except requests.Timeout:
            log_detalhado(f"[OBTER CARD] Timeout na requisição. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...", LOG)
            time.sleep(delay)
            delay *= 2  # Aumenta o tempo de espera exponencialmente
        except requests.RequestException as e:
            log_detalhado(f"[OBTER CARD] Erro ao conectar com Bitrix24: {str(e)}", LOG)
            return None

    log_detalhado("[OBTER CARD] Número máximo de tentativas atingido. Requisição falhou.", LOG)
    return None

def moverEtapaCard(stage_id, card_id, entity_type_id, bitrix_webhook_url, LOG=False):
    """
    Move um Card para uma nova etapa no Bitrix24.

    :param stage_id: Novo stageId para onde o card será movido.
    :param card_id: ID do card que será movido.
    :param entity_type_id: Tipo de entidade no Bitrix24 (ex: 128 para negócios).
    :param bitrix_webhook_url: URL do webhook do Bitrix24.
    :param LOG: Se True, ativa logs detalhados.
    :return: True se a movimentação for bem-sucedida, False caso contrário.
    """
    endpoint = f"{bitrix_webhook_url}crm.item.update"
    payload = {
        "entityTypeId": entity_type_id,
        "id": card_id,
        "fields": {
            "stageId": stage_id
        }
    }

    max_tentativas = 5
    delay = 2  # Tempo inicial de espera

    for tentativa in range(max_tentativas):
        if LOG:
            logging.info(f"[MOVER ETAPA] Tentativa {tentativa + 1}/{max_tentativas} - Movendo card ID {card_id} para stageId {stage_id} (EntityType: {entity_type_id})")
            logging.info(f"[MOVER ETAPA] Endpoint: {endpoint}")
            logging.info(f"[MOVER ETAPA] Payload: {payload}")

        try:
            response = requests.post(endpoint, json=payload, timeout=10)

            if LOG:
                logging.info(f"[MOVER ETAPA] Resposta do Bitrix24: {response.status_code} - {response.text}")

            if response.status_code == 200:
                data = response.json()
                if "result" in data:
                    logging.info(f"[MOVER ETAPA] Card ID {card_id} movido com sucesso para stageId {stage_id}")
                    return True
            elif response.status_code == 503:
                logging.warning(f"[MOVER ETAPA] Erro 503: Too Many Requests. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...")
                time.sleep(delay)
                delay *= 2  # Aumenta o tempo de espera exponencialmente
            else:
                logging.error(f"[MOVER ETAPA] Falha ao mover card ID {card_id}: {response.text}")
                return False

        except requests.Timeout:
            logging.warning(f"[MOVER ETAPA] Timeout na requisição. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...")
            time.sleep(delay)
            delay *= 2  # Aumenta o tempo de espera exponencialmente
        except requests.RequestException as e:
            logging.error(f"[MOVER ETAPA] Erro na requisição para mover card: {str(e)}")
            return False

    logging.error("[MOVER ETAPA] Número máximo de tentativas atingido. Requisição falhou.")
    return False

def atualizarCard(card_id, campos_para_atualizar, entity_type_id, bitrix_webhook_url, LOG=False):
    """
    Atualiza um Card existente no Bitrix24.

    :param card_id: ID do card que será atualizado.
    :param campos_para_atualizar: Dicionário com os campos e valores a serem atualizados.
    :param entity_type_id: Tipo de entidade no Bitrix24 (ex: 128 para negócios).
    :param bitrix_webhook_url: URL do webhook do Bitrix24.
    :param LOG: Se True, ativa logs detalhados.
    :return: True se a atualização for bem-sucedida, False caso contrário.
    """
    if not campos_para_atualizar:
        logging.info(f"[ATUALIZAR CARD] Nenhuma atualização necessária para o Card ID {card_id}.")
        return False

    endpoint = f"{bitrix_webhook_url}crm.item.update"
    payload = {
        "entityTypeId": entity_type_id,
        "id": card_id,
        "fields": campos_para_atualizar
    }

    max_tentativas = 5
    delay = 2  # Tempo inicial de espera

    for tentativa in range(max_tentativas):
        if LOG:
            logging.info(f"[ATUALIZAR CARD] Tentativa {tentativa + 1}/{max_tentativas} - Atualizando card ID {card_id} com novos dados: {campos_para_atualizar}")
            logging.info(f"[ATUALIZAR CARD] Endpoint: {endpoint}")
            logging.info(f"[ATUALIZAR CARD] Payload: {payload}")

        try:
            response = requests.post(endpoint, json=payload, timeout=10)

            if LOG:
                logging.info(f"[ATUALIZAR CARD] Resposta do Bitrix24: {response.status_code} - {response.text}")

            if response.status_code == 200:
                data = response.json()
                if "result" in data:
                    logging.info(f"[ATUALIZAR CARD] Card ID {card_id} atualizado com sucesso.")
                    return True
            elif response.status_code == 503:
                logging.warning(f"[ATUALIZAR CARD] Erro 503: Too Many Requests. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...")
                time.sleep(delay)
                delay *= 2  # Aumenta o tempo de espera exponencialmente
            else:
                logging.error(f"[ATUALIZAR CARD] Falha ao atualizar Card ID {card_id}: {response.text}")
                return False

        except requests.Timeout:
            logging.warning(f"[ATUALIZAR CARD] Timeout na requisição. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...")
            time.sleep(delay)
            delay *= 2  # Aumenta o tempo de espera exponencialmente
        except requests.RequestException as e:
            logging.error(f"[ATUALIZAR CARD] Erro ao conectar ao Bitrix24: {str(e)}")
            return False

    logging.error("[ATUALIZAR CARD] Número máximo de tentativas atingido. Requisição falhou.")
    return False

def obterEndereco(contact_id, bitrix_webhook_url, LOG=False):
    """
    Obtém o endereço vinculado a um contato no Bitrix24.

    :param contact_id: ID do contato no Bitrix24.
    :param bitrix_webhook_url: URL do webhook do Bitrix24.
    :param LOG: Se True, ativa logs detalhados.
    :return: Dicionário com os dados do endereço ou None se não existir.
    """
    endpoint = f"{bitrix_webhook_url}crm.address.list"
    payload = {
        "filter": {
            "ENTITY_ID": contact_id,
            "ENTITY_TYPE_ID": 3  # Tipo de entidade para contatos no Bitrix24
        }
    }

    max_tentativas = 5
    delay = 2  # Tempo inicial de espera

    for tentativa in range(max_tentativas):
        if LOG:
            logging.info(f"[OBTER ENDEREÇO] Tentativa {tentativa + 1}/{max_tentativas} - Buscando endereço para o contato ID {contact_id}")
            logging.info(f"[OBTER ENDEREÇO] Endpoint: {endpoint}")
            logging.info(f"[OBTER ENDEREÇO] Payload: {payload}")

        try:
            response = requests.post(endpoint, json=payload, timeout=10)

            if LOG:
                logging.info(f"[OBTER ENDEREÇO] Resposta do Bitrix24: {response.status_code} - {response.text}")

            if response.status_code == 200:
                data = response.json()
                result = data.get("result", [])
                if result:
                    logging.info(f"[OBTER ENDEREÇO] Endereço encontrado para contato ID {contact_id}: {result[0]}")
                    return result[0]  # Retorna o primeiro endereço encontrado
                else:
                    logging.info(f"[OBTER ENDEREÇO] Nenhum endereço encontrado para contato ID {contact_id}.")
                    return None
            elif response.status_code == 503:
                logging.warning(f"[OBTER ENDEREÇO] Erro 503: Too Many Requests. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...")
                time.sleep(delay)
                delay *= 2  # Aumenta o tempo de espera exponencialmente
            else:
                logging.error(f"[OBTER ENDEREÇO] Erro ao obter endereço: {response.status_code} - {response.text}")
                return None

        except requests.Timeout:
            logging.warning(f"[OBTER ENDEREÇO] Timeout na requisição. Tentativa {tentativa + 1}/{max_tentativas}. Retentando em {delay} segundos...")
            time.sleep(delay)
            delay *= 2  # Aumenta o tempo de espera exponencialmente
        except requests.RequestException as e:
            logging.error(f"[OBTER ENDEREÇO] Erro ao conectar com Bitrix24: {str(e)}")
            return None

    logging.error("[OBTER ENDEREÇO] Número máximo de tentativas atingido. Requisição falhou.")
    return None

def listar_cards_spa(bitrix_webhook_url, entity_type_id, category_id=None, stage_id=None, LOG=False):
    """
    Lista todos os cards de um SPA no Bitrix24, aplicando filtros opcionais e lidando corretamente com paginação.

    :param bitrix_webhook_url: URL do webhook do Bitrix24.
    :param entity_type_id: ID do tipo de entidade (SPA).
    :param category_id: ID da categoria (opcional).
    :param stage_id: ID do estágio (opcional).
    :param LOG: Se True, ativa logs detalhados.
    :return: Lista de dicionários contendo ID e title de cada card encontrado.
    """
    endpoint = f"{bitrix_webhook_url}crm.item.list"
    cards = []
    start = 0  # Início da paginação
    max_tentativas = 5
    delay = 2  # Tempo inicial de espera para backoff exponencial
    total_itens_recebidos = 0  # Para rastrear quantos itens recebemos

    # Configuração do filtro
    filtro = {}
    if category_id:
        filtro["categoryId"] = category_id
    if stage_id:
        filtro["stageId"] = stage_id

    while True:
        payload = {
            "entityTypeId": entity_type_id,
            "filter": filtro,
            "select": ["id", "title"],
            "start": start
        }

        for tentativa in range(max_tentativas):
            try:
                if LOG:
                    logging.info(f"[LISTAR CARDS] Tentativa {tentativa + 1}/{max_tentativas}. Start: {start}")
                    logging.info(f"[LISTAR CARDS] Payload: {payload}")

                response = requests.post(endpoint, json=payload, timeout=10)

                if LOG:
                    logging.info(f"[LISTAR CARDS] Resposta: {response.status_code}")

                if response.status_code == 200:
                    data = response.json()
                    items = data.get("result", {}).get("items", [])

                    if not items:
                        if LOG:
                            logging.info("[LISTAR CARDS] Nenhum item retornado. Paginação encerrada.")
                        return cards if LOG else [{"id": item["id"], "title": item["title"]} for item in cards]

                    cards.extend(items)
                    total_itens_recebidos += len(items)

                    # Atualiza paginação
                    next_start = data.get("result", {}).get("next")
                    if next_start:
                        start = next_start  # Avança para a próxima página
                    else:
                        start += 50  # Se não tiver "next", forçamos a paginação

                    if LOG:
                        logging.info(f"[LISTAR CARDS] Total de cards coletados até agora: {total_itens_recebidos}")

                    break  # Sai da tentativa e continua a paginação

                elif response.status_code == 503:
                    logging.warning(f"[LISTAR CARDS] Erro 503: Too Many Requests. Retentando em {delay} segundos...")
                    time.sleep(delay)
                    delay *= 2  # Aumenta o tempo de espera exponencialmente

                else:
                    logging.error(f"[LISTAR CARDS] Falha na requisição: {response.status_code} - {response.text}")
                    return []

            except requests.RequestException as e:
                logging.error(f"[LISTAR CARDS] Erro ao conectar com Bitrix24: {str(e)}")
                return []

    return cards
