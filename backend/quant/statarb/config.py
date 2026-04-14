from dataclasses import dataclass
from typing import List, Dict, Any
import yaml

@dataclass
class Cfg:
    raw: Dict[str, Any]

    @property
    def exchange(self) -> str: return self.raw["exchange"]
    @property
    def paper(self) -> bool: return bool(self.raw.get("paper", True))
    @property
    def symbols(self) -> List[str]: return list(self.raw["symbols"])
    @property
    def timeframe(self) -> str: return self.raw["timeframe"]
    @property
    def lookback_hours(self) -> int: return int(self.raw["lookback_hours"])
    @property
    def rebalance_every_minutes(self) -> int: return int(self.raw["rebalance_every_minutes"])

def load_config(path: str) -> Cfg:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return Cfg(raw)
