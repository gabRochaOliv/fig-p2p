import json
import os


class Inventory:
    SAVE_FILE = "inventory.json"

    # Inicializa com 28 cópias da figurinha autoral; carrega inventory.json se existir
    def __init__(self, owner_peer_id, owner_sticker_id, initial_count=28):
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

    # Persiste o inventário em inventory.json
    def _save(self):
        try:
            with open(self.SAVE_FILE, "w", encoding="utf-8") as f:
                json.dump(self.items, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[INVENTÁRIO] Erro ao salvar: {e}")

    # Retorna True se possui pelo menos qty cópias da figurinha
    def has(self, sticker_id, qty=1):
        return self.items.get(sticker_id, 0) >= qty

    # Adiciona qty cópias e salva (atualiza o inventário)
    def add(self, sticker_id, qty=1):
        if not sticker_id or not sticker_id.strip():
            return
        self.items[sticker_id] = self.items.get(sticker_id, 0) + qty
        self._save()

    # Remove qty cópias; retorna False se não houver quantidade suficiente (impede negativo)
    def remove(self, sticker_id, qty=1):
        if not self.has(sticker_id, qty):
            return False
        self.items[sticker_id] -= qty
        self._save()
        return True

    # Retorna cópia do inventário como dict {sticker_id: quantidade}
    def to_dict(self):
        return dict(self.items)

    def __str__(self):
        if not self.items:
            return "(inventário vazio)"
        return ", ".join(f"{sid}: {qty}" for sid, qty in sorted(self.items.items()))
