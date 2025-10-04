import csv
from pathlib import Path
from typing import Iterable

def console(lines: Iterable[str]) -> None:
    for ln in lines:
        print(ln)

def to_csv(lines: Iterable[str], path: str = "queue/alerts/alerts.csv") -> str:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        for ln in lines:
            w.writerow([ln])
    return str(p)
