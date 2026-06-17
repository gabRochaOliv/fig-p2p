"""
protocol.py — Constantes de tipos e builders de mensagens para o protocolo P2P de figurinhas.

Todos os campos seguem o protocolo definido pelo professor para garantir
interoperabilidade com outros grupos da turma.
"""

import uuid
import json

# ---------------------------------------------------------------------------
# Constantes de tipo de mensagem
# ---------------------------------------------------------------------------

HELLO = "HELLO"
SEARCH = "SEARCH"
SEARCH_HIT = "SEARCH_HIT"
SEARCH_MISS = "SEARCH_MISS"
TRADE_OFFER = "TRADE_OFFER"
TRADE_ACCEPT = "TRADE_ACCEPT"
TRADE_REJECT = "TRADE_REJECT"
TRANSFER_CONFIRM = "TRANSFER_CONFIRM"


# ---------------------------------------------------------------------------
# Builders de mensagens
# ---------------------------------------------------------------------------

def build_hello(sender_peer_id, known_peers=None):
    """
    Constrói mensagem HELLO.

    Args:
        sender_peer_id (str): ID do peer remetente (ex: "ALUNO-09").
        known_peers (list, optional): Lista de IPs de peers conhecidos. Padrão: [].

    Returns:
        dict: Mensagem HELLO pronta para serialização.
    """
    return {
        "type": HELLO,
        "message_id": str(uuid.uuid4()),
        "sender_peer_id": sender_peer_id,
        "peers": known_peers if known_peers is not None else [],
    }


def build_search(origin_peer_id, origin_peer_ip, sender_peer_id, receiver_peer_id,
                 sticker_id, query_id=None, ttl=7):
    """
    Constrói mensagem SEARCH.

    Args:
        origin_peer_id (str): Peer que originou a busca.
        origin_peer_ip (str): IP do peer de origem.
        sender_peer_id (str): Peer que está enviando esta mensagem (pode ser relay).
        receiver_peer_id (str): Peer destinatário desta mensagem.
        sticker_id (str): Figurinha sendo buscada.
        query_id (str, optional): UUID v4 da busca. Gerado automaticamente se None.
        ttl (int): Time-to-live. Padrão: 7.

    Returns:
        dict: Mensagem SEARCH.
    """
    return {
        "type": SEARCH,
        "message_id": str(uuid.uuid4()),
        "origin_peer_id": origin_peer_id,
        "origin_peer_ip": origin_peer_ip,
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "query_id": query_id if query_id is not None else str(uuid.uuid4()),
        "ttl": ttl,
        "sticker_id": sticker_id,
    }


def build_search_hit(sender_peer_id, receiver_peer_id, query_id, sticker_id, origin_peer_id):
    """
    Constrói mensagem SEARCH_HIT — indica que o peer possui a figurinha buscada.

    Returns:
        dict: Mensagem SEARCH_HIT.
    """
    return {
        "type": SEARCH_HIT,
        "message_id": str(uuid.uuid4()),
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "query_id": query_id,
        "sticker_id": sticker_id,
        "origin_peer_id": origin_peer_id,
    }


def build_search_miss(sender_peer_id, receiver_peer_id, query_id, sticker_id, origin_peer_id):
    """
    Constrói mensagem SEARCH_MISS — indica que o peer não possui a figurinha.

    Returns:
        dict: Mensagem SEARCH_MISS.
    """
    return {
        "type": SEARCH_MISS,
        "message_id": str(uuid.uuid4()),
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "query_id": query_id,
        "sticker_id": sticker_id,
        "origin_peer_id": origin_peer_id,
    }


def build_trade_offer(sender_peer_id, receiver_peer_id, offer_sticker_id, want_sticker_id):
    """
    Constrói mensagem TRADE_OFFER.

    Args:
        sender_peer_id (str): Quem está oferecendo.
        receiver_peer_id (str): Quem vai receber a oferta.
        offer_sticker_id (str): Figurinha oferecida pelo remetente.
        want_sticker_id (str): Figurinha desejada em troca.

    Returns:
        dict: Mensagem TRADE_OFFER.
    """
    return {
        "type": TRADE_OFFER,
        "message_id": str(uuid.uuid4()),
        "origin_peer_id": sender_peer_id,
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "offer_sticker_id": offer_sticker_id,
        "want_sticker_id": want_sticker_id,
    }


def build_trade_accept(sender_peer_id, receiver_peer_id, message_id,
                       offer_sticker_id, want_sticker_id):
    """
    Constrói mensagem TRADE_ACCEPT em resposta a um TRADE_OFFER.

    Args:
        message_id (str): Mesmo message_id do TRADE_OFFER original.

    Returns:
        dict: Mensagem TRADE_ACCEPT.
    """
    return {
        "type": TRADE_ACCEPT,
        "message_id": message_id,
        "origin_peer_id": sender_peer_id,
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "offer_sticker_id": offer_sticker_id,
        "want_sticker_id": want_sticker_id,
    }


def build_trade_reject(sender_peer_id, receiver_peer_id, message_id,
                       offer_sticker_id, want_sticker_id):
    """
    Constrói mensagem TRADE_REJECT em resposta a um TRADE_OFFER.

    Returns:
        dict: Mensagem TRADE_REJECT.
    """
    return {
        "type": TRADE_REJECT,
        "message_id": message_id,
        "origin_peer_id": sender_peer_id,
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "offer_sticker_id": offer_sticker_id,
        "want_sticker_id": want_sticker_id,
    }


def build_transfer_confirm(sender_peer_id, receiver_peer_id, message_id,
                           offer_sticker_id, want_sticker_id):
    """
    Constrói mensagem TRANSFER_CONFIRM — confirma que os itens foram transferidos.

    offer_sticker_id: figurinha que o remetente transferiu (enviou ao peer).
    want_sticker_id:  figurinha que o remetente recebeu do peer.

    Returns:
        dict: Mensagem TRANSFER_CONFIRM.
    """
    return {
        "type": TRANSFER_CONFIRM,
        "message_id": message_id,
        "origin_peer_id": sender_peer_id,
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "offer_sticker_id": offer_sticker_id,
        "want_sticker_id": want_sticker_id,
    }


# ---------------------------------------------------------------------------
# Serialização / Deserialização
# ---------------------------------------------------------------------------

def encode(msg_dict):
    """
    Serializa um dict de mensagem para string JSON UTF-8.

    Args:
        msg_dict (dict): Mensagem do protocolo.

    Returns:
        str: String JSON.
    """
    return json.dumps(msg_dict, ensure_ascii=False)


def decode(raw_str):
    """
    Deserializa uma string JSON para dict.

    Args:
        raw_str (str): Mensagem JSON recebida via WebSocket.

    Returns:
        dict: Mensagem do protocolo.

    Raises:
        json.JSONDecodeError: Se a string não for JSON válido.
    """
    return json.loads(raw_str)
