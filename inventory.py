"""
inventory.py — Gerenciamento de inventário de figurinhas do nó P2P.

Controla quais figurinhas o nó possui e em que quantidade,
impedindo qualquer operação que resulte em inventário negativo.
"""

import json
import os


class Inventory:
    """
    Inventário de figurinhas de um nó P2P.

    O inventário é um dicionário {sticker_id: quantidade}. A figurinha autoral
    do nó é inicializada com 28 cópias. Trocas recebidas incrementam, trocas
    enviadas decrementam — remove() impede que a quantidade fique negativa.
    """

    SAVE_FILE = "inventory.json"

    def __init__(self, owner_peer_id, owner_sticker_id, initial_count=28):
        """
        Inicializa o inventário com a figurinha autoral do nó.

        Carrega de inventory.json se existir; caso contrário usa o estado inicial.

        Args:
            owner_peer_id (str): peer_id do nó dono deste inventário (ex: "ALUNO-09").
            owner_sticker_id (str): sticker_id da figurinha autoral (ex: "FIG-09").
            initial_count (int): Quantidade inicial da figurinha autoral. Padrão: 28.
        """
        self.owner_peer_id = owner_peer_id
        self.owner_sticker_id = owner_sticker_id
        if os.path.exists(self.SAVE_FILE):
            try:
                with open(self.SAVE_FILE, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                self.items = {k: v for k, v in loaded.items() if k and k.strip()}
                print(f"[INVENTÁRIO] Carregado de {self.SAVE_FILE}")
                return
            except Exception:
                pass
        self.items = {owner_sticker_id: initial_count}

    def _save(self):
        """Persiste o inventário atual em inventory.json."""
        try:
            with open(self.SAVE_FILE, "w", encoding="utf-8") as f:
                json.dump(self.items, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[INVENTÁRIO] Erro ao salvar: {e}")

    def has(self, sticker_id, qty=1):
        """
        Verifica se o inventário possui pelo menos `qty` cópias de `sticker_id`.

        Args:
            sticker_id (str): ID da figurinha.
            qty (int): Quantidade mínima necessária. Padrão: 1.

        Returns:
            bool: True se possui quantidade suficiente.
        """
        return self.items.get(sticker_id, 0) >= qty

    def add(self, sticker_id, qty=1):
        """
        Adiciona `qty` cópias de `sticker_id` ao inventário.

        Args:
            sticker_id (str): ID da figurinha a adicionar.
            qty (int): Quantidade a adicionar. Padrão: 1.
        """
        if not sticker_id or not sticker_id.strip():
            return
        self.items[sticker_id] = self.items.get(sticker_id, 0) + qty
        self._save()

    def remove(self, sticker_id, qty=1):
        """
        Remove `qty` cópias de `sticker_id` do inventário.

        Impede quantidade negativa: retorna False sem alterar o inventário
        se não houver quantidade suficiente.

        Args:
            sticker_id (str): ID da figurinha a remover.
            qty (int): Quantidade a remover. Padrão: 1.

        Returns:
            bool: True se a remoção foi realizada; False se insuficiente.
        """
        if not self.has(sticker_id, qty):
            return False
        self.items[sticker_id] -= qty
        self._save()
        return True

    def to_dict(self):
        """
        Retorna uma cópia do inventário como dicionário.

        Useful para serialização JSON (ex: enviar inventário via WebSocket).

        Returns:
            dict: Cópia de {sticker_id: quantidade}.
        """
        return dict(self.items)

    def __str__(self):
        """
        Representação legível do inventário (para logs e debug).

        Returns:
            str: Ex: "FIG-09: 28, FIG-01: 1"
        """
        if not self.items:
            return "(inventário vazio)"
        return ", ".join(f"{sid}: {qty}" for sid, qty in sorted(self.items.items()))
