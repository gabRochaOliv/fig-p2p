# Sistema de Figurinhas P2P — Project Guide

## Project Context

Sistema P2P de troca de figurinhas para a disciplina de Sistemas Distribuídos.  
Aluno: Gabriel Rocha → `ALUNO-09` | Figurinha: `FIG-09` | Porta: `8080`

See `.planning/PROJECT.md` for full context.

## GSD Workflow

This project uses GSD for structured development:

```
/gsd:plan-phase <N>     → criar plano detalhado para uma fase
/gsd:execute-phase <N>  → executar todos os planos da fase
/gsd:progress           → ver status atual e próximo passo
/gsd:resume-work        → retomar trabalho de sessão anterior
```

**Current phase:** 1 — Core Node + Rede  
**Roadmap:** `.planning/ROADMAP.md`  
**Requirements:** `.planning/REQUIREMENTS.md`

## Key Constraints (Never Deviate)

- **Porta WebSocket P2P:** 8080 (obrigatório pelo professor)
- **peer_id:** `ALUNO-09` (fixo — lista de chamada)
- **sticker_id autoral:** `FIG-09`
- **TTL padrão:** 7 para SEARCH
- **query_id:** UUID v4 aleatório por busca
- **Dedup:** ignorar SEARCH com query_id já visto
- **Formato JSON:** todos os campos das mensagens conforme protocolo
- **Stack:** Python + websockets + asyncio + HTML/JS puro

## Stack

- `websockets` — servidor e cliente WebSocket (porta 8080)
- `asyncio` — concorrência assíncrona
- HTTP simples (porta 8081) — servidor da Web UI
- HTML/JS puro — sem frameworks frontend

## Protocol Reference

Mensagens implementadas:
`HELLO` | `SEARCH` | `SEARCH_HIT` | `SEARCH_MISS` | `TRADE_OFFER` | `TRADE_ACCEPT` | `TRADE_REJECT` | `TRANSFER_CONFIRM`

Detalhes completos: `.planning/PROJECT.md` → Constraints
