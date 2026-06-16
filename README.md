# Sistema de Figurinhas P2P

Trabalho prático da disciplina de Sistemas Distribuídos — UENP.

Rede P2P de troca de figurinhas onde cada nó é um aluno. Os nós se comunicam via WebSocket, buscam figurinhas por inundação (flood search) e trocam entre si usando um protocolo de mensagens definido pelo professor.

**Aluno:** Gabriel Rocha | **Peer:** `ALUNO-09` | **Figurinha:** `FIG-09` | **Porta:** `8080`

---

## Stack

- Python 3.7+ com `websockets` e `asyncio`
- Servidor HTTP próprio com `asyncio.start_server` (sem frameworks)
- Frontend em HTML/JS puro com polling a cada 2s

## Como rodar

```bash
pip install -r requirements.txt
python node.py
```

Abre `http://localhost:8081` no navegador.

Para conectar a outro nó, edite `peers.json` com o IP do colega:

```json
[
  {"host": "IP_DO_COLEGA", "port": 8080}
]
```

---

## Arquitetura

```
node.py          — núcleo do nó: servidor WebSocket P2P + handlers de protocolo
protocol.py      — builders e serialização das 8 mensagens do protocolo
inventory.py     — gerenciamento de inventário com persistência em inventory.json
http_server.py   — servidor HTTP na porta 8081 (serve UI + API REST)
ui/index.html    — interface web com inventário, busca e histórico de trocas
peers.json       — lista de vizinhos (não versionado)
inventory.json   — estado do inventário (gerado automaticamente após primeira troca)
```

## Protocolo

8 tipos de mensagem (JSON via WebSocket):

| Mensagem | Descrição |
|----------|-----------|
| `HELLO` | Troca de identidade ao conectar |
| `SEARCH` | Busca por inundação com TTL=7 e query_id UUID |
| `SEARCH_HIT` | Resposta positiva: nó possui a figurinha |
| `SEARCH_MISS` | Resposta negativa: fim de propagação sem resultado |
| `TRADE_OFFER` | Proposta de troca |
| `TRADE_ACCEPT` | Aceitação da troca |
| `TRADE_REJECT` | Rejeição da troca |
| `TRANSFER_CONFIRM` | Confirmação de transferência + atualização de inventário |

## API REST (porta 8081)

| Endpoint | Descrição |
|----------|-----------|
| `GET /` | Interface web |
| `GET /api/state` | Estado do nó (inventário, vizinhos, resultados, histórico) |
| `POST /api/search` | Inicia busca: `{"sticker_id": "FIG-01"}` |
| `POST /api/trade` | Envia oferta: `{"target_peer_id": "ALUNO-01", "want_sticker_id": "FIG-01"}` |

---

## Desenvolvimento

O projeto foi desenvolvido em sessões guiadas com o [Claude Code](https://claude.ai/code) usando o workflow **GSD** (Get Shit Done), que organiza o desenvolvimento em fases planejadas e executadas iterativamente:

- **Fase 1** — Core: servidor WebSocket, HELLO, inventário
- **Fase 2** — Protocolo: flood search (SEARCH/HIT/MISS) + fluxo de troca completo
- **Fase 3** — Web UI: HTTP server + interface + busca e troca pelo browser

As imagens das figurinhas são carregadas do repositório do professor: `github.com/rgcoelho01/album`.
