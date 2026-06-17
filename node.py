"""
node.py — Nó P2P de troca de figurinhas.

Serve como ponto de entrada do sistema. Gerencia:
- Servidor WebSocket na porta 8080 (aceita conexões de outros nós)
- Cliente WebSocket para conectar a vizinhos em peers.json
- Protocolo HELLO (troca de IDs ao conectar)
- Protocolo SEARCH / SEARCH_HIT / SEARCH_MISS (busca por inundação)
- Protocolo TRADE_OFFER / TRADE_ACCEPT / TRADE_REJECT / TRANSFER_CONFIRM (troca)

Aluno: Gabriel Rocha | peer_id: ALUNO-09 | Figurinha: FIG-09 | Porta: 8080
"""

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

# ---------------------------------------------------------------------------
# Constantes do nó (NÃO alterar — definidos pelo professor e lista de chamada)
# ---------------------------------------------------------------------------

PEER_ID = "ALUNO-09"
OWN_STICKER = "FIG-09"
INITIAL_COUNT = 28
PORT = 8080

# ---------------------------------------------------------------------------
# Estado global (módulo-nível) — compartilhado entre coroutines
# ---------------------------------------------------------------------------

inventory = Inventory(PEER_ID, OWN_STICKER, INITIAL_COUNT)
connected_peers = {}   # peer_id -> websocket object
query_history = set()  # query_ids já processados (dedup de SEARCH)
trade_pending = {}     # message_id -> dict com detalhes da troca em andamento
own_searches = {}      # query_id -> sticker_id (buscas iniciadas por este nó)
search_results = []    # lista de {query_id, sticker_id, from_peer} para a UI
trade_history = []     # lista de dicts com histórico de trocas para a UI
incoming_offers = {}   # message_id -> dict com proposta recebida aguardando decisão (inclui 'ws')
hit_history = set()    # (query_id, sender_peer_id) já processados — dedup de SEARCH_HIT
trades_initiated = set()  # query_ids que já dispararam TRADE_OFFER — evita múltiplas ofertas


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def get_local_ip():
    """Retorna o IP local do nó para preencher origin_peer_ip em mensagens SEARCH."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


async def _safe_send(websocket, msg_dict):
    """Envia msg_dict via websocket, ignorando erros de conexão fechada."""
    try:
        await websocket.send(encode(msg_dict))
    except Exception as e:
        print(f"[SEND ERROR] {e}")


# ---------------------------------------------------------------------------
# Carregamento de configuração
# ---------------------------------------------------------------------------

def load_peers():
    """
    Lê peers.json e retorna lista de dicts [{"host": ..., "port": ...}].

    Retorna [] se o arquivo não existir ou contiver JSON inválido.
    Nunca levanta exceção — o nó pode iniciar sem vizinhos configurados.

    Returns:
        list[dict]: Lista de peers configurados.
    """
    try:
        with open("peers.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


# ---------------------------------------------------------------------------
# Busca por inundação — iniciação
# ---------------------------------------------------------------------------

async def initiate_search(sticker_id):
    """
    Inicia uma busca por inundação para sticker_id.

    Gera query_id único, registra em own_searches e query_history,
    depois envia SEARCH com TTL=7 a todos os vizinhos conectados.
    """
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


# ---------------------------------------------------------------------------
# Troca — iniciação
# ---------------------------------------------------------------------------

async def initiate_trade_offer(target_peer_id, want_sticker_id):
    """
    Envia TRADE_OFFER ao peer que respondeu SEARCH_HIT.

    Oferece OWN_STICKER (FIG-09) em troca de want_sticker_id.
    Registra em trade_pending para correlacionar com TRADE_ACCEPT/REJECT futuros.
    """
    if not inventory.has(OWN_STICKER):
        print(f"[TRADE_OFFER] Sem {OWN_STICKER} disponível para oferecer, trade cancelado")
        return
    peer_ws = connected_peers.get(target_peer_id)
    if not peer_ws:
        print(f"[TRADE_OFFER] {target_peer_id} não está em connected_peers, trade cancelado")
        return

    offer = build_trade_offer(
        sender_peer_id=PEER_ID,
        receiver_peer_id=target_peer_id,
        offer_sticker_id=OWN_STICKER,
        want_sticker_id=want_sticker_id,
    )
    trade_pending[offer["message_id"]] = {
        "offer_sticker_id": OWN_STICKER,
        "want_sticker_id": want_sticker_id,
        "counterparty": target_peer_id,
    }
    await _safe_send(peer_ws, offer)
    print(f"[TRADE_OFFER] Ofertando {OWN_STICKER} por {want_sticker_id} → {target_peer_id}")


# ---------------------------------------------------------------------------
# Handlers de mensagens
# ---------------------------------------------------------------------------

async def handle_hello(websocket, msg):
    """
    Processa mensagem HELLO recebida de outro nó.

    - Registra o peer em connected_peers usando o sender_peer_id como chave.
    - Envia HELLO de volta para confirmar a troca de identidade.
    - Ao conectar com um peer novo, busca automaticamente a figurinha dele.

    Args:
        websocket: Conexão WebSocket ativa.
        msg (dict): Mensagem HELLO já decodificada.
    """
    sender_peer_id = msg.get("sender_peer_id", "UNKNOWN")
    already_known = sender_peer_id in connected_peers
    connected_peers[sender_peer_id] = websocket
    print(f"[HELLO] Conexão de {sender_peer_id}")
    if not already_known:
        await websocket.send(encode(build_hello(PEER_ID)))
        # Busca automática pela figurinha do peer recém conectado
        if sender_peer_id.startswith("ALUNO-"):
            num = sender_peer_id.replace("ALUNO-", "")
            asyncio.create_task(initiate_search(f"FIG-{num}"))


async def handle_search(websocket, msg):
    """
    Processa mensagem SEARCH recebida de outro nó.

    Fluxo:
    1. Dedup: se query_id já processado, descarta silenciosamente.
    2. Registra query_id no histórico.
    3. Verifica inventário local — responde SEARCH_HIT se possui a figurinha.
    4. Repassa a mensagem com TTL-1 para todos os vizinhos exceto o remetente.
    5. Se TTL=0 e não possui a figurinha, envia SEARCH_MISS ao origin (opcional).
    """
    query_id = msg.get("query_id", "")
    sticker_id = msg.get("sticker_id", "")
    sender_peer_id = msg.get("sender_peer_id", "")
    origin_peer_id = msg.get("origin_peer_id", "")
    origin_peer_ip = msg.get("origin_peer_ip", "")
    ttl = msg.get("ttl", 0)

    # Dedup (SRCH-02, SRCH-03)
    if query_id in query_history:
        return
    query_history.add(query_id)

    # Verificar inventário local (SRCH-04)
    if inventory.has(sticker_id):
        hit = build_search_hit(
            sender_peer_id=PEER_ID,
            receiver_peer_id=origin_peer_id,
            query_id=query_id,
            sticker_id=sticker_id,
            origin_peer_id=origin_peer_id,
        )
        target_ws = connected_peers.get(origin_peer_id, websocket)
        await _safe_send(target_ws, hit)
        print(f"[SEARCH_HIT] Tenho {sticker_id} | enviando HIT para {origin_peer_id}")

    # Repassar com TTL-1 (SRCH-05)
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
        # Fim de propagação sem resultado — SEARCH_MISS (SRCH-06, opcional)
        miss = build_search_miss(
            sender_peer_id=PEER_ID,
            receiver_peer_id=origin_peer_id,
            query_id=query_id,
            sticker_id=sticker_id,
            origin_peer_id=origin_peer_id,
        )
        target_ws = connected_peers.get(origin_peer_id, websocket)
        await _safe_send(target_ws, miss)


async def handle_search_hit(websocket, msg):
    """
    Processa SEARCH_HIT recebido — outro nó possui a figurinha buscada.

    Se a busca foi iniciada por este nó (query_id em own_searches),
    dispara automaticamente TRADE_OFFER para o peer que respondeu.
    Armazena o resultado em search_results para a UI (Fase 3).
    """
    query_id = msg.get("query_id", "")
    sticker_id = msg.get("sticker_id", "")
    sender_peer_id = msg.get("sender_peer_id", "")

    dedup_key = (query_id, sender_peer_id)
    if dedup_key in hit_history:
        return
    hit_history.add(dedup_key)

    print(f"[SEARCH_HIT] {sender_peer_id} tem {sticker_id} | query={query_id}")

    search_results.append({
        "query_id": query_id,
        "sticker_id": sticker_id,
        "from_peer": sender_peer_id,
    })

    # Se foi nossa busca, disparar TRADE_OFFER (TRADE-01)
    if query_id in own_searches:
        await initiate_trade_offer(sender_peer_id, sticker_id)


async def handle_trade_offer(websocket, msg):
    """
    Processa TRADE_OFFER recebido de outro nó.

    Se não temos a figurinha pedida: rejeita imediatamente.
    Se temos: armazena em incoming_offers para aprovação manual via UI.
    """
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
    print(f"[TRADE_OFFER] Proposta de {sender}: oferecem {offer_sticker_id}, querem {want_sticker_id} — aguardando decisao")


async def accept_incoming_offer(message_id):
    """Aceita uma proposta de troca pendente em incoming_offers."""
    offer = incoming_offers.pop(message_id, None)
    if not offer:
        return False

    sender = offer["from_peer"]
    offer_sticker_id = offer["offer_sticker_id"]
    want_sticker_id = offer["want_sticker_id"]
    peer_ws = offer["ws"]

    # Do aceitante (nós): oferecemos want_sticker_id (o que eles queriam de nós)
    #                     e queremos offer_sticker_id (o que eles nos ofereceram)
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


async def reject_incoming_offer(message_id):
    """Rejeita uma proposta de troca pendente em incoming_offers."""
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


async def handle_trade_accept(websocket, msg):
    """
    Processa TRADE_ACCEPT recebido — nosso TRADE_OFFER foi aceito pelo peer.

    Atualiza o inventário aqui usando trade_pending, sem depender dos campos
    do TRANSFER_CONFIRM (que pode ter nomes diferentes em outras implementações).
    """
    message_id = msg.get("message_id", "")
    sender = msg.get("sender_peer_id", "")
    pending = trade_pending.pop(message_id, None)
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
        print(f"[TRADE_ACCEPT] Oferta aceita por {sender} | id={message_id}")


async def handle_trade_reject(websocket, msg):
    """
    Processa TRADE_REJECT recebido — nosso TRADE_OFFER foi recusado.

    Remove da fila de pendentes sem alterar inventário (TRADE-04).
    """
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
    print(f"[TRADE_REJECT] Oferta rejeitada por {sender} | id={message_id}")


async def handle_transfer_confirm(websocket, msg):
    """
    Processa TRANSFER_CONFIRM recebido — o peer confirmou que a transferência ocorreu.

    Atualiza inventário do nó ofertante (este nó):
    - Remove o que enviamos ao peer (received_sticker_id do ponto de vista deles)
    - Adiciona o que recebemos do peer (sent_sticker_id do ponto de vista deles)
    """
    message_id = msg.get("message_id", "")
    sender = msg.get("sender_peer_id", "")
    # Prioriza campos do protocolo (offer/want); aceita sent/received como fallback legado
    offer_sticker_id = (msg.get("offer_sticker_id") or msg.get("sent_sticker_id") or "").strip()
    want_sticker_id = (msg.get("want_sticker_id") or msg.get("received_sticker_id") or "").strip()

    # trade_pending já foi consumido no TRADE_ACCEPT — apenas loga a confirmação
    if message_id not in trade_pending:
        print(f"[TRANSFER_CONFIRM] Confirmação de {sender} (troca já processada no TRADE_ACCEPT)")
        return

    # Fallback para implementações que não enviam TRADE_ACCEPT com dados completos
    if not offer_sticker_id or not want_sticker_id:
        pending = trade_pending[message_id]
        offer_sticker_id = offer_sticker_id or pending.get("offer_sticker_id", "")
        want_sticker_id = want_sticker_id or pending.get("want_sticker_id", "")

    if not offer_sticker_id or not want_sticker_id:
        print(f"[TRANSFER_CONFIRM] Campos ausentes de {sender}, troca ignorada")
        trade_pending.pop(message_id, None)
        return

    # offer_sticker_id = o que o peer nos enviou (= o que ganhamos)
    # want_sticker_id  = o que o peer recebeu de nós (= o que demos)
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


async def handle_message(websocket, raw_msg):
    """
    Roteador central de mensagens.

    Decodifica o JSON recebido e despacha para o handler correto.
    JSON inválido é logado e descartado sem derrubar o processo.

    Args:
        websocket: Conexão WebSocket ativa.
        raw_msg (str): Mensagem bruta recebida via WebSocket.
    """
    try:
        msg = decode(raw_msg)
    except json.JSONDecodeError as e:
        print(f"[{PEER_ID}] JSON inválido recebido, ignorando: {e}")
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
        print(f"[{PEER_ID}] Tipo desconhecido ignorado: {msg_type}")


# ---------------------------------------------------------------------------
# Servidor WebSocket (aceita conexões de outros nós)
# ---------------------------------------------------------------------------

async def server_handler(websocket):
    """
    Handler do servidor WebSocket — chamado para cada nova conexão recebida.

    Mantém a conexão viva lendo mensagens em loop. Remove o peer de
    connected_peers quando a conexão fechar.

    Args:
        websocket: Conexão WebSocket ativa.
    """
    remote = websocket.remote_address
    print(f"[SERVER] Conexão recebida de {remote}")
    try:
        async for raw_msg in websocket:
            await handle_message(websocket, raw_msg)
    except websockets.exceptions.ConnectionClosed:
        print(f"[SERVER] Conexão fechada com {remote}")
    finally:
        # Remove o peer da lista de conectados ao fechar a conexão
        to_remove = [pid for pid, ws in connected_peers.items() if ws is websocket]
        for pid in to_remove:
            del connected_peers[pid]
            print(f"[SERVER] Peer {pid} removido de connected_peers")


# ---------------------------------------------------------------------------
# Cliente WebSocket (conecta a vizinhos configurados)
# ---------------------------------------------------------------------------

async def connect_to_peer(host, port):
    """
    Conecta a um peer vizinho e mantém a conexão com reconexão automática.

    Ao conectar, envia HELLO imediatamente. Se a conexão cair por qualquer
    motivo, aguarda 5 segundos e tenta novamente — sem bloquear o event loop.

    Args:
        host (str): Endereço IP ou hostname do peer vizinho.
        port (int): Porta WebSocket do peer vizinho.
    """
    uri = f"ws://{host}:{port}"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                await ws.send(encode(build_hello(PEER_ID)))
                print(f"[CLIENT] Conectado a {uri}, HELLO enviado")
                async for raw_msg in ws:
                    await handle_message(ws, raw_msg)
        except websockets.exceptions.ConnectionClosed:
            print(f"[CLIENT] Desconectado de {uri}, reconectando em 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[CLIENT] Desconectado de {uri}, reconectando em 5s... ({e})")
            await asyncio.sleep(5)


async def connect_to_all_peers():
    """
    Cria tasks de conexão para todos os peers em peers.json.

    Cada peer recebe sua própria task assíncrona com reconexão automática.
    """
    peers = load_peers()
    if not peers:
        print(f"[{PEER_ID}] Nenhum vizinho configurado em peers.json — iniciando isolado")
        return
    for peer in peers:
        asyncio.create_task(connect_to_peer(peer["host"], peer["port"]))
        print(f"[{PEER_ID}] Tentando conectar a {peer['host']}:{peer['port']}...")


# ---------------------------------------------------------------------------
# CLI interativa para testes sem UI
# ---------------------------------------------------------------------------

async def stdin_reader():
    """
    Lê comandos da stdin para testes interativos sem a UI.

    Comando disponível:
      buscar <sticker_id>  — inicia busca por inundação para a figurinha
      inventario           — imprime o inventário atual
    """
    loop = asyncio.get_event_loop()
    print(f"[CLI] Comandos disponíveis: 'buscar <sticker_id>', 'inventario'")
    while True:
        try:
            line = await loop.run_in_executor(None, sys.stdin.readline)
        except EOFError:
            break
        cmd = line.strip()
        if cmd.startswith("buscar "):
            sticker_id = cmd[7:].strip()
            if sticker_id:
                await initiate_search(sticker_id)
        elif cmd == "inventario":
            print(f"[INVENTÁRIO] {inventory}")
        elif cmd:
            print(f"[CLI] Comando desconhecido: {cmd}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    """
    Coroutine principal: sobe o servidor e conecta aos vizinhos.

    O servidor fica rodando indefinidamente até o processo ser interrompido
    (Ctrl+C ou sinal do SO).
    """
    async with websockets.serve(server_handler, "0.0.0.0", PORT) as server:
        print(f"[{PEER_ID}] Servidor P2P ouvindo em 0.0.0.0:{PORT}")
        print(f"[{PEER_ID}] Inventário inicial: {inventory}")
        http_server.set_node(sys.modules[__name__])
        await http_server.start()
        asyncio.create_task(connect_to_all_peers())
        asyncio.create_task(stdin_reader())
        await asyncio.Future()  # Aguarda indefinidamente


if __name__ == "__main__":
    asyncio.run(main())
