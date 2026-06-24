import asyncio
import json
import socket
import sys
import uuid

import websockets
import websockets.exceptions

import http_server

from protocol import (
    encode, decode,
    build_hello, build_search, build_search_hit, build_search_miss,
    build_trade_offer, build_trade_accept, build_trade_reject, build_transfer_confirm,
)
from inventory import Inventory

# Constantes fixadas, lista de chamada
PEER_ID = "ALUNO-09"
OWN_STICKER = "FIG-09"
INITIAL_COUNT = 28
PORT = 8080

# Estado global compartilhado entre todas as coroutines
inventory = Inventory(PEER_ID, OWN_STICKER, INITIAL_COUNT)
connected_peers = {}    # peer_id -> websocket
query_history = set()   # query_ids já processados (dedup de SEARCH)
own_searches = {}       # query_id -> sticker_id das buscas iniciadas por este nó
trade_pending = {}      # message_id -> dados da troca aguardando resposta
search_results = []     # resultados de busca exibidos na UI
trade_history = []      # histórico de trocas exibido na UI
incoming_offers = {}    # message_id -> proposta recebida aguardando decisão do usuário
hit_history = set()     # (query_id, sender_peer_id) processados — dedup de SEARCH_HIT
peer_inventories = {}   # peer_id -> lista de figurinhas descobertas via HELLO/SEARCH_HIT
peer_neighbors = {}     # peer_id -> peers que este vizinho reportou no seu HELLO
outbound_peers = set()  # peer_ids de conexões que NÓS iniciamos (vs inbound)
_outbound_ws = set()    # websockets de conexões de saída (para identificar peer_id no HELLO)
_outbound_ws_info = {}  # websocket -> (host, port) das conexões de saída
peer_ip_map = {}        # "host:port" -> peer_id resolvido após HELLO


# Obtém o IP local para preencher origin_peer_ip nas mensagens SEARCH
def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# Envia mensagem via websocket ignorando erros de conexão fechada
async def _safe_send(websocket, msg_dict):
    try:
        await websocket.send(encode(msg_dict))
    except Exception as e:
        print(f"[SEND ERROR] {e}")


# Lê peers.json e retorna lista de {"host": ..., "port": ...}
def load_peers():
    try:
        with open("peers.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


# Salva lista de peers em peers.json
def save_peers(peers_list):
    try:
        with open("peers.json", "w", encoding="utf-8") as f:
            json.dump(peers_list, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[PEERS] Erro ao salvar peers.json: {e}")


# Adiciona um peer a peers.json se ainda não estiver na lista
def add_configured_peer(host, port):
    peers = load_peers()
    already = any(p["host"] == host and int(p.get("port", 8080)) == int(port) for p in peers)
    if not already:
        peers.append({"host": host, "port": int(port)})
        save_peers(peers)
        print(f"[PEERS] {host}:{port} adicionado a peers.json")


# Remove um peer de peers.json pelo host e porta
def remove_configured_peer(host, port):
    peers = load_peers()
    new_peers = [p for p in peers if not (p["host"] == host and int(p.get("port", 8080)) == int(port))]
    save_peers(new_peers)
    print(f"[PEERS] {host}:{port} removido de peers.json")


# Inicia busca por inundação: gera query_id UUID, registra no histórico e envia SEARCH com TTL=7
async def initiate_search(sticker_id):
    query_id = str(uuid.uuid4())
    own_searches[query_id] = sticker_id
    query_history.add(query_id)

    local_ip = get_local_ip()
    for peer_id, peer_ws in list(connected_peers.items()):
        msg = build_search(
            origin_peer_id=PEER_ID,
            origin_peer_ip=local_ip,
            sender_peer_id=PEER_ID,
            receiver_peer_id=peer_id,
            sticker_id=sticker_id,
            query_id=query_id,
            ttl=7,
        )
        await _safe_send(peer_ws, msg)

    print(f"[SEARCH] Buscando {sticker_id} | query_id={query_id} | vizinhos={len(connected_peers)}")


# Envia TRADE_OFFER ao peer; retorna (True, "") ou (False, motivo_do_erro)
async def initiate_trade_offer(target_peer_id, want_sticker_id, offer_sticker_id=None):
    offer_sid = offer_sticker_id if offer_sticker_id else OWN_STICKER
    if not inventory.has(offer_sid):
        msg = f"Sem {offer_sid} no inventário para oferecer"
        print(f"[TRADE_OFFER] {msg}")
        return False, msg
    peer_ws = connected_peers.get(target_peer_id)
    if not peer_ws:
        msg = f"{target_peer_id} não está conectado diretamente"
        print(f"[TRADE_OFFER] {msg}")
        return False, msg

    offer = build_trade_offer(
        sender_peer_id=PEER_ID,
        receiver_peer_id=target_peer_id,
        offer_sticker_id=offer_sid,
        want_sticker_id=want_sticker_id,
    )
    trade_pending[offer["message_id"]] = {
        "offer_sticker_id": offer_sid,
        "want_sticker_id": want_sticker_id,
        "counterparty": target_peer_id,
    }
    await _safe_send(peer_ws, offer)
    print(f"[TRADE_OFFER] Ofertando {offer_sid} por {want_sticker_id} → {target_peer_id}")
    return True, ""


# Processa HELLO: registra o peer, armazena inventário e vizinhos reportados, responde com HELLO
async def handle_hello(websocket, msg):
    sender_peer_id = msg.get("sender_peer_id", "UNKNOWN")
    already_known = sender_peer_id in connected_peers
    connected_peers[sender_peer_id] = websocket
    if websocket in _outbound_ws:
        outbound_peers.add(sender_peer_id)
        info = _outbound_ws_info.get(websocket)
        if info:
            peer_ip_map[f"{info[0]}:{info[1]}"] = sender_peer_id
    else:
        try:
            remote_ip = websocket.remote_address[0]
            peer_ip_map[f"{remote_ip}:8080"] = sender_peer_id
        except Exception:
            pass

    reported_peers = msg.get("peers", [])
    if reported_peers:
        peer_neighbors[sender_peer_id] = list(reported_peers)

    stickers_from_hello = msg.get("stickers", [])
    if stickers_from_hello:
        peer_inventories[sender_peer_id] = list(stickers_from_hello)
        print(f"[HELLO] {sender_peer_id} possui: {stickers_from_hello} | peers: {reported_peers}")
    else:
        print(f"[HELLO] Conexão de {sender_peer_id} | peers: {reported_peers}")

    if not already_known:
        own_stickers = list(inventory.items.keys())
        own_peers = list(connected_peers.keys())
        await websocket.send(encode(build_hello(PEER_ID, known_peers=own_peers, stickers=own_stickers)))


# Processa SEARCH: dedup por query_id, responde HIT/MISS, repassa com TTL-1 aos demais vizinhos
async def handle_search(websocket, msg):
    query_id = msg.get("query_id", "")
    sticker_id = msg.get("sticker_id", "").replace(".PNG", "").replace(".png", "")
    sender_peer_id = msg.get("sender_peer_id", "")
    origin_peer_id = msg.get("origin_peer_id", "")
    origin_peer_ip = msg.get("origin_peer_ip", "")
    ttl = msg.get("ttl", 0)

    if query_id in query_history:
        return
    query_history.add(query_id)

    if inventory.has(sticker_id):
        hit = build_search_hit(
            sender_peer_id=PEER_ID,
            receiver_peer_id=origin_peer_id,
            query_id=query_id,
            sticker_id=sticker_id,
            origin_peer_id=PEER_ID,
        )
        target_ws = connected_peers.get(origin_peer_id, websocket)
        await _safe_send(target_ws, hit)
        print(f"[SEARCH_HIT] Tenho {sticker_id} | enviando HIT para {origin_peer_id}")

    if ttl > 0:
        for peer_id, peer_ws in list(connected_peers.items()):
            if peer_id != sender_peer_id:
                relay = build_search(
                    origin_peer_id=origin_peer_id,
                    origin_peer_ip=origin_peer_ip,
                    sender_peer_id=PEER_ID,
                    receiver_peer_id=peer_id,
                    sticker_id=sticker_id,
                    query_id=query_id,
                    ttl=ttl - 1,
                )
                await _safe_send(peer_ws, relay)
    elif not inventory.has(sticker_id):
        miss = build_search_miss(
            sender_peer_id=PEER_ID,
            receiver_peer_id=origin_peer_id,
            query_id=query_id,
            sticker_id=sticker_id,
            origin_peer_id=PEER_ID,
        )
        target_ws = connected_peers.get(origin_peer_id, websocket)
        await _safe_send(target_ws, miss)


# Processa SEARCH_HIT: registra o peer que possui a figurinha e exibe na UI
async def handle_search_hit(websocket, msg):
    query_id = msg.get("query_id", "")
    sticker_id = msg.get("sticker_id", "").replace(".PNG", "").replace(".png", "")
    sender_peer_id = msg.get("sender_peer_id", "")

    dedup_key = (query_id, sender_peer_id)
    if dedup_key in hit_history:
        return
    hit_history.add(dedup_key)

    print(f"[SEARCH_HIT] {sender_peer_id} tem {sticker_id} | query={query_id}")

    known = peer_inventories.setdefault(sender_peer_id, [])
    if sticker_id not in known:
        known.append(sticker_id)

    search_results.append({
        "query_id": query_id,
        "sticker_id": sticker_id,
        "from_peer": sender_peer_id,
    })


# Processa TRADE_OFFER recebido: rejeita se não tiver a figurinha, caso contrário aguarda decisão do usuário
async def handle_trade_offer(websocket, msg):
    sender = msg.get("sender_peer_id", "")
    message_id = msg.get("message_id", "")
    offer_sticker_id = msg.get("offer_sticker_id", "")
    want_sticker_id = msg.get("want_sticker_id", "")

    peer_ws = connected_peers.get(sender, websocket)

    if not inventory.has(want_sticker_id):
        reject = build_trade_reject(
            sender_peer_id=PEER_ID,
            receiver_peer_id=sender,
            message_id=message_id,
            offer_sticker_id=offer_sticker_id,
            want_sticker_id=want_sticker_id,
        )
        await _safe_send(peer_ws, reject)
        print(f"[TRADE_REJECT] Sem {want_sticker_id} para oferecer a {sender}")
        return

    incoming_offers[message_id] = {
        "message_id": message_id,
        "from_peer": sender,
        "offer_sticker_id": offer_sticker_id,
        "want_sticker_id": want_sticker_id,
        "ws": peer_ws,
    }
    print(f"[TRADE_OFFER] Proposta de {sender}: oferecem {offer_sticker_id}, querem {want_sticker_id}")


# Aceita proposta pendente: envia TRADE_ACCEPT, atualiza inventário e envia TRANSFER_CONFIRM
async def accept_incoming_offer(message_id):
    offer = incoming_offers.pop(message_id, None)
    if not offer:
        return False

    sender = offer["from_peer"]
    offer_sticker_id = offer["offer_sticker_id"]
    want_sticker_id = offer["want_sticker_id"]
    peer_ws = offer["ws"]

    accept = build_trade_accept(
        sender_peer_id=PEER_ID,
        receiver_peer_id=sender,
        message_id=message_id,
        offer_sticker_id=want_sticker_id,
        want_sticker_id=offer_sticker_id,
    )
    await _safe_send(peer_ws, accept)

    inventory.remove(want_sticker_id)
    inventory.add(offer_sticker_id)

    trade_history.append({
        "status": "aceita",
        "type": "recebida",
        "gave": want_sticker_id,
        "got": offer_sticker_id,
        "counterparty": sender,
    })

    confirm = build_transfer_confirm(
        sender_peer_id=PEER_ID,
        receiver_peer_id=sender,
        message_id=message_id,
        offer_sticker_id=want_sticker_id,
        want_sticker_id=offer_sticker_id,
    )
    await _safe_send(peer_ws, confirm)
    print(f"[TRADE_ACCEPT] Troca com {sender}: dei {want_sticker_id}, recebi {offer_sticker_id}")
    print(f"[INVENTÁRIO] {inventory}")
    return True


# Rejeita proposta pendente: envia TRADE_REJECT sem alterar inventário
async def reject_incoming_offer(message_id):
    offer = incoming_offers.pop(message_id, None)
    if not offer:
        return False

    sender = offer["from_peer"]
    offer_sticker_id = offer["offer_sticker_id"]
    want_sticker_id = offer["want_sticker_id"]
    peer_ws = offer["ws"]

    reject = build_trade_reject(
        sender_peer_id=PEER_ID,
        receiver_peer_id=sender,
        message_id=message_id,
        offer_sticker_id=offer_sticker_id,
        want_sticker_id=want_sticker_id,
    )
    await _safe_send(peer_ws, reject)
    trade_history.append({
        "status": "rejeitada",
        "type": "recebida",
        "gave": "—",
        "got": "—",
        "counterparty": sender,
    })
    print(f"[TRADE_REJECT] Proposta de {sender} rejeitada pelo usuário")
    return True


# Processa TRADE_ACCEPT: localiza a troca pendente, atualiza inventário
async def handle_trade_accept(websocket, msg):
    message_id = msg.get("message_id", "")
    sender = msg.get("sender_peer_id", "")
    pending = trade_pending.pop(message_id, None)

    # Fallback: peers com implementações diferentes podem usar message_id diferente no ACCEPT
    if not pending:
        for mid, p in list(trade_pending.items()):
            if p.get("counterparty") == sender:
                pending = trade_pending.pop(mid, None)
                print(f"[TRADE_ACCEPT] message_id não casou, recuperado via counterparty={sender}")
                break

    if pending:
        gave = pending.get("offer_sticker_id", "")
        got = pending.get("want_sticker_id", "")
        if gave:
            inventory.remove(gave)
        if got:
            inventory.add(got)
        trade_history.append({
            "status": "aceita",
            "type": "enviada",
            "gave": gave,
            "got": got,
            "counterparty": sender,
        })
        print(f"[TRADE_ACCEPT] Troca com {sender}: dei {gave}, recebi {got}")
        print(f"[INVENTÁRIO] {inventory}")
    else:
        print(f"[TRADE_ACCEPT] Aceite de {sender} sem trade pendente (id={message_id})")


# Processa TRADE_REJECT: remove da fila de pendentes sem alterar inventário
async def handle_trade_reject(websocket, msg):
    message_id = msg.get("message_id", "")
    sender = msg.get("sender_peer_id", "")
    pending = trade_pending.pop(message_id, {})
    trade_history.append({
        "status": "rejeitada",
        "type": "enviada",
        "gave": pending.get("offer_sticker_id", "?"),
        "got": pending.get("want_sticker_id", "?"),
        "counterparty": sender,
    })
    print(f"[TRADE_REJECT] Oferta rejeitada por {sender}")


# Processa TRANSFER_CONFIRM: atualiza inventário se a troca ainda não foi processada no TRADE_ACCEPT
async def handle_transfer_confirm(websocket, msg):
    message_id = msg.get("message_id", "")
    sender = msg.get("sender_peer_id", "")
    offer_sticker_id = (msg.get("offer_sticker_id") or msg.get("sent_sticker_id") or "").strip()
    want_sticker_id = (msg.get("want_sticker_id") or msg.get("received_sticker_id") or "").strip()

    if message_id not in trade_pending:
        # Fallback para peers com message_id diferente
        fallback_mid = next(
            (mid for mid, p in trade_pending.items() if p.get("counterparty") == sender),
            None,
        )
        if fallback_mid:
            message_id = fallback_mid
        else:
            print(f"[TRANSFER_CONFIRM] Confirmação de {sender} (troca já processada no TRADE_ACCEPT)")
            return

    if not offer_sticker_id or not want_sticker_id:
        pending = trade_pending[message_id]
        offer_sticker_id = offer_sticker_id or pending.get("offer_sticker_id", "")
        want_sticker_id = want_sticker_id or pending.get("want_sticker_id", "")

    if not offer_sticker_id or not want_sticker_id:
        print(f"[TRANSFER_CONFIRM] Campos ausentes de {sender}, troca ignorada")
        trade_pending.pop(message_id, None)
        return

    inventory.remove(want_sticker_id)
    inventory.add(offer_sticker_id)

    trade_pending.pop(message_id, None)
    trade_history.append({
        "status": "aceita",
        "type": "enviada",
        "gave": want_sticker_id,
        "got": offer_sticker_id,
        "counterparty": sender,
    })
    print(f"[TRANSFER_CONFIRM] Troca concluída com {sender}: dei {want_sticker_id}, recebi {offer_sticker_id}")
    print(f"[INVENTÁRIO] {inventory}")


# Roteador central: decodifica JSON e despacha para o handler correto
async def handle_message(websocket, raw_msg):
    try:
        msg = decode(raw_msg)
    except json.JSONDecodeError as e:
        print(f"[{PEER_ID}] JSON inválido, ignorando: {e}")
        return

    msg_type = msg.get("type", "")

    if msg_type == "HELLO":
        await handle_hello(websocket, msg)
    elif msg_type == "SEARCH":
        await handle_search(websocket, msg)
    elif msg_type == "SEARCH_HIT":
        await handle_search_hit(websocket, msg)
    elif msg_type == "SEARCH_MISS":
        print(f"[SEARCH_MISS] de {msg.get('sender_peer_id', '?')} | sticker={msg.get('sticker_id', '?')}")
    elif msg_type == "TRADE_OFFER":
        await handle_trade_offer(websocket, msg)
    elif msg_type == "TRADE_ACCEPT":
        await handle_trade_accept(websocket, msg)
    elif msg_type == "TRADE_REJECT":
        await handle_trade_reject(websocket, msg)
    elif msg_type == "TRANSFER_CONFIRM":
        await handle_transfer_confirm(websocket, msg)
    else:
        print(f"[{PEER_ID}] Tipo desconhecido: {msg_type}")


# Handler do servidor WebSocket: mantém conexão ativa e remove peer ao desconectar
async def server_handler(websocket):
    remote = websocket.remote_address
    print(f"[SERVER] Conexão recebida de {remote}")
    try:
        async for raw_msg in websocket:
            await handle_message(websocket, raw_msg)
    except websockets.exceptions.ConnectionClosed:
        print(f"[SERVER] Conexão fechada com {remote}")
    finally:
        to_remove = [pid for pid, ws in connected_peers.items() if ws is websocket]
        for pid in to_remove:
            del connected_peers[pid]
            print(f"[SERVER] Peer {pid} removido de connected_peers")


# Conecta a um peer e mantém reconexão automática a cada 5s se cair
async def connect_to_peer(host, port):
    uri = f"ws://{host}:{port}"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                _outbound_ws.add(ws)
                _outbound_ws_info[ws] = (host, port)
                try:
                    own_stickers = list(inventory.items.keys())
                    own_peers = list(connected_peers.keys())
                    await ws.send(encode(build_hello(PEER_ID, known_peers=own_peers, stickers=own_stickers)))
                    print(f"[CLIENT] Conectado a {uri}, HELLO enviado")
                    async for raw_msg in ws:
                        await handle_message(ws, raw_msg)
                finally:
                    _outbound_ws.discard(ws)
                    _outbound_ws_info.pop(ws, None)
        except websockets.exceptions.ConnectionClosed:
            print(f"[CLIENT] Desconectado de {uri}, reconectando em 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[CLIENT] Erro em {uri}, reconectando em 5s... ({e})")
            await asyncio.sleep(5)


# Cria tasks de conexão para todos os peers listados em peers.json
async def connect_to_all_peers():
    peers = load_peers()
    if not peers:
        print(f"[{PEER_ID}] Nenhum vizinho em peers.json — iniciando isolado")
        return
    for peer in peers:
        asyncio.create_task(connect_to_peer(peer["host"], peer["port"]))
        print(f"[{PEER_ID}] Conectando a {peer['host']}:{peer['port']}...")


# Ponto de entrada: sobe servidor WebSocket, servidor HTTP e conecta aos vizinhos
async def main():
    async with websockets.serve(server_handler, "0.0.0.0", PORT) as server:
        print(f"[{PEER_ID}] Servidor P2P ouvindo em 0.0.0.0:{PORT}")
        print(f"[{PEER_ID}] Inventário inicial: {inventory}")
        http_server.set_node(sys.modules[__name__])
        await http_server.start()
        asyncio.create_task(connect_to_all_peers())
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
