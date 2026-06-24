import uuid
import json

# constantes que evitam erros de digitação
HELLO = "HELLO"
SEARCH = "SEARCH"
SEARCH_HIT = "SEARCH_HIT"
SEARCH_MISS = "SEARCH_MISS"
TRADE_OFFER = "TRADE_OFFER"
TRADE_ACCEPT = "TRADE_ACCEPT"
TRADE_REJECT = "TRADE_REJECT"
TRANSFER_CONFIRM = "TRANSFER_CONFIRM"


# Anuncia presença na rede; carrega lista de peers conhecidos e figurinhas do inventário
def build_hello(sender_peer_id, known_peers=None, stickers=None):
    return {
        "type": HELLO,
        "message_id": str(uuid.uuid4()),
        "sender_peer_id": sender_peer_id,
        "peers": known_peers if known_peers is not None else [],
        "stickers": stickers if stickers is not None else [],
    }


# Monta busca por inundação com TTL, query_id único e IP de origem
def build_search(origin_peer_id, origin_peer_ip, sender_peer_id, receiver_peer_id,
                 sticker_id, query_id=None, ttl=7):
    return {
        "type": SEARCH,
        "message_id": str(uuid.uuid4()),
        "origin_peer_id": origin_peer_id,
        "origin_peer_ip": origin_peer_ip,
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "query_id": query_id if query_id is not None else str(uuid.uuid4()), #deduplicar a mensagem na rede
        "ttl": ttl,
        "sticker_id": sticker_id,
    }


# Resposta positiva: este nó possui a figurinha buscada
def build_search_hit(sender_peer_id, receiver_peer_id, query_id, sticker_id, origin_peer_id):
    return {
        "type": SEARCH_HIT,
        "message_id": str(uuid.uuid4()),
        "origin_peer_id": origin_peer_id,
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "query_id": query_id,
        "sticker_id": sticker_id,
    }


# Resposta negativa opcional: este nó não possui a figurinha
def build_search_miss(sender_peer_id, receiver_peer_id, query_id, sticker_id, origin_peer_id):
    return {
        "type": SEARCH_MISS,
        "message_id": str(uuid.uuid4()),
        "origin_peer_id": origin_peer_id,
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "query_id": query_id,
        "sticker_id": sticker_id,
    }


# Proposta de troca: ofereço offer_sticker_id, quero want_sticker_id
def build_trade_offer(sender_peer_id, receiver_peer_id, offer_sticker_id, want_sticker_id):
    return {
        "type": TRADE_OFFER,
        "message_id": str(uuid.uuid4()),
        "origin_peer_id": sender_peer_id,
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "offer_sticker_id": offer_sticker_id,
        "want_sticker_id": want_sticker_id,
    }


# Aceite da troca; reutiliza o message_id do TRADE_OFFER para correlação
def build_trade_accept(sender_peer_id, receiver_peer_id, message_id,
                       offer_sticker_id, want_sticker_id):
    return {
        "type": TRADE_ACCEPT,
        "message_id": message_id,
        "origin_peer_id": sender_peer_id,
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "offer_sticker_id": offer_sticker_id,
        "want_sticker_id": want_sticker_id,
    }


# Rejeição da troca; nenhum inventário é alterado
def build_trade_reject(sender_peer_id, receiver_peer_id, message_id,
                       offer_sticker_id, want_sticker_id):
    return {
        "type": TRADE_REJECT,
        "message_id": message_id,
        "origin_peer_id": sender_peer_id,
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "offer_sticker_id": offer_sticker_id,
        "want_sticker_id": want_sticker_id,
    }


# Confirmação de transferência: ambos os nós devem atualizar o inventário ao receber
def build_transfer_confirm(sender_peer_id, receiver_peer_id, message_id,
                           offer_sticker_id, want_sticker_id):
    return {
        "type": TRANSFER_CONFIRM,
        "message_id": message_id,
        "origin_peer_id": sender_peer_id,
        "sender_peer_id": sender_peer_id,
        "receiver_peer_id": receiver_peer_id,
        "offer_sticker_id": offer_sticker_id,
        "want_sticker_id": want_sticker_id,
    }


# Serializa dict para string JSON UTF-8
def encode(msg_dict):
    return json.dumps(msg_dict, ensure_ascii=False)


# Deserializa string JSON para dict
def decode(raw_str):
    return json.loads(raw_str)
