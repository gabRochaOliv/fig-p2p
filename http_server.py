"""
http_server.py — Servidor HTTP para a Web UI (porta 8081).

Usa asyncio.start_server — integra no mesmo event loop do node.py.
Endpoints:
  GET  /          → serve ui/index.html
  GET  /api/state → JSON com estado atual do nó
  POST /api/search → inicia busca por inundação
  POST /api/trade  → envia TRADE_OFFER
"""

import asyncio
import json
import os

_node = None
HTTP_PORT = 8081
_UI_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ui")


def set_node(node_module):
    """Injeta referência ao módulo node para acesso ao estado compartilhado."""
    global _node
    _node = node_module


async def _handle_http(reader, writer):
    try:
        raw = await asyncio.wait_for(reader.read(8192), timeout=10.0)
    except asyncio.TimeoutError:
        writer.close()
        return

    text = raw.decode("utf-8", errors="replace")
    head, _, body = text.partition("\r\n\r\n")
    lines = head.split("\r\n")
    if not lines:
        writer.close()
        return

    parts = lines[0].split(" ")
    method = parts[0] if len(parts) > 0 else "GET"
    path = (parts[1] if len(parts) > 1 else "/").split("?")[0]

    try:
        if method == "OPTIONS":
            await _send_response(writer, 204, b"", "text/plain")
        elif method == "GET" and path == "/":
            await _serve_file(writer, os.path.join(_UI_DIR, "index.html"), "text/html; charset=utf-8")
        elif method == "GET" and path == "/api/state":
            await _serve_json(writer, _get_state())
        elif method == "POST" and path == "/api/search":
            await _handle_search(writer, body)
        elif method == "POST" and path == "/api/trade":
            await _handle_trade(writer, body)
        elif method == "POST" and path == "/api/connect":
            await _handle_connect(writer, body)
        elif method == "POST" and path == "/api/trade/accept":
            await _handle_trade_decision(writer, body, accept=True)
        elif method == "POST" and path == "/api/trade/reject":
            await _handle_trade_decision(writer, body, accept=False)
        else:
            await _send_response(writer, 404, b"Not Found", "text/plain")
    except Exception as e:
        print(f"[HTTP] Erro handler: {e}")
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def _serve_file(writer, filepath, content_type):
    try:
        with open(filepath, "rb") as f:
            content = f.read()
        await _send_response(writer, 200, content, content_type)
    except FileNotFoundError:
        await _send_response(writer, 404, b"Not Found", "text/plain")


async def _serve_json(writer, data):
    content = json.dumps(data, ensure_ascii=False).encode("utf-8")
    await _send_response(writer, 200, content, "application/json")


async def _send_response(writer, status, body, content_type):
    status_text = {
        200: "OK", 204: "No Content", 404: "Not Found", 400: "Bad Request"
    }.get(status, "OK")
    headers = (
        f"HTTP/1.1 {status} {status_text}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Access-Control-Allow-Origin: *\r\n"
        f"Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
        f"Access-Control-Allow-Headers: Content-Type\r\n"
        f"Connection: close\r\n\r\n"
    ).encode()
    writer.write(headers + body)
    await writer.drain()


def _get_state():
    if not _node:
        return {"error": "node not initialized"}
    return {
        "peer_id": _node.PEER_ID,
        "inventory": _node.inventory.to_dict(),
        "connected_peers": list(_node.connected_peers.keys()),
        "search_results": _node.search_results[-50:],
        "trade_history": _node.trade_history[-50:],
        "incoming_offers": [
            {k: v for k, v in offer.items() if k != "ws"}
            for offer in _node.incoming_offers.values()
        ],
    }


async def _handle_search(writer, body):
    try:
        data = json.loads(body) if body.strip() else {}
        sticker_id = str(data.get("sticker_id", "")).strip()
        if not sticker_id:
            await _serve_json(writer, {"ok": False, "error": "sticker_id required"})
            return
        if _node:
            asyncio.create_task(_node.initiate_search(sticker_id))
        await _serve_json(writer, {"ok": True, "sticker_id": sticker_id})
    except Exception as e:
        await _serve_json(writer, {"ok": False, "error": str(e)})


async def _handle_trade(writer, body):
    try:
        data = json.loads(body) if body.strip() else {}
        target_peer_id = str(data.get("target_peer_id", "")).strip()
        want_sticker_id = str(data.get("want_sticker_id", "")).strip()
        if not target_peer_id or not want_sticker_id:
            await _serve_json(writer, {"ok": False, "error": "target_peer_id and want_sticker_id required"})
            return
        if _node:
            asyncio.create_task(_node.initiate_trade_offer(target_peer_id, want_sticker_id))
        await _serve_json(writer, {"ok": True})
    except Exception as e:
        await _serve_json(writer, {"ok": False, "error": str(e)})


async def _handle_trade_decision(writer, body, accept):
    try:
        data = json.loads(body) if body.strip() else {}
        message_id = str(data.get("message_id", "")).strip()
        if not message_id:
            await _serve_json(writer, {"ok": False, "error": "message_id required"})
            return
        if _node:
            if accept:
                ok = await _node.accept_incoming_offer(message_id)
            else:
                ok = await _node.reject_incoming_offer(message_id)
            await _serve_json(writer, {"ok": ok})
        else:
            await _serve_json(writer, {"ok": False, "error": "node not initialized"})
    except Exception as e:
        await _serve_json(writer, {"ok": False, "error": str(e)})


async def _handle_connect(writer, body):
    try:
        data = json.loads(body) if body.strip() else {}
        host = str(data.get("host", "")).strip()
        port = int(data.get("port", 8080))
        if not host:
            await _serve_json(writer, {"ok": False, "error": "host required"})
            return
        if port < 1 or port > 65535:
            await _serve_json(writer, {"ok": False, "error": "porta invalida"})
            return
        if _node:
            asyncio.create_task(_node.connect_to_peer(host, port))
        await _serve_json(writer, {"ok": True, "host": host, "port": port})
    except Exception as e:
        await _serve_json(writer, {"ok": False, "error": str(e)})


async def start(port=HTTP_PORT):
    """Inicia o servidor HTTP e retorna o objeto server."""
    server = await asyncio.start_server(_handle_http, "0.0.0.0", port)
    print(f"[HTTP] Servidor UI em http://localhost:{port}")
    return server
