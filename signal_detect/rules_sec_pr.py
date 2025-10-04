from typing import List

# Minimal, high-signal keywords. Tune later.
KEYWORDS = {
    "buyback": [
        "repurchase", "share repurchase", "buyback", "authorization to repurchase",
        "repurchase program", "share buyback"
    ],
    "dividend": [
        "dividend", "increases dividend", "raises dividend", "special dividend"
    ],
    "guidance_up": [
        "raises guidance", "updates guidance upward", "upward revision",
        "increase guidance", "guidance raised"
    ],
    "guidance_down": [
        "lowers guidance", "downward revision", "cuts guidance",
        "reduce guidance", "guidance lowered"
    ],
    "cfo_resign": [
        "chief financial officer", "cfo", "resigns", "resignation", "steps down"
    ],
    "ceo_resign": [
        "chief executive officer", "ceo", "resigns", "resignation", "steps down"
    ],
}

def hit_tags(text: str) -> List[str]:
    """
    Return list of rule tags that matched the provided text.
    Case-insensitive substring matching; dedup by tag.
    """
    if not text:
        return []
    t = text.lower()
    hits: List[str] = []
    for tag, keys in KEYWORDS.items():
        for k in keys:
            if k in t:
                hits.append(tag)
                break  # one hit per tag is enough
    return hits
