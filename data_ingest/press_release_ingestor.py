import os
import re
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
import urllib.request

OUT_DIR = Path(os.getenv("RAW_QUEUE_DIR", "queue/raw_events"))

def fetch(url: str, ua: str) -> str:
    if url.startswith("file:"):
        path = url[5:]
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    req = urllib.request.Request(url, headers={"User-Agent": ua})
    with urllib.request.urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8", "ignore")

def parse_naive_rss(xml_text: str):
    # Ultra-naive <item> parser (works for many RSS feeds)
    items = re.findall(r"<item>(.*?)</item>", xml_text, flags=re.S|re.I)
    if items:
        for chunk in items:
            title = re.search(r"<title>(.*?)</title>", chunk, re.S|re.I)
            link  = re.search(r"<link>(.*?)</link>", chunk, re.S|re.I)
            date  = re.search(r"<pubDate>(.*?)</pubDate>", chunk, re.S|re.I)
            yield {
                "source": "PR",
                "title": title.group(1).strip() if title else None,
                "body": None,
                "ts": (date.group(1).strip() if date else None),
                "meta": {
                    "source_name": "NewsroomRSS",
                    "doc_type": "PR",
                    "urls": [link.group(1).strip()] if link else [],
                },
            }
        return

    # Minimal Atom fallback (<entry>)
    entries = re.findall(r"<entry>(.*?)</entry>", xml_text, flags=re.S|re.I)
    for chunk in entries:
        title = re.search(r"<title[^>]*>(.*?)</title>", chunk, re.S|re.I)
        link  = re.search(r'<link[^>]*href="([^"]+)"', chunk, re.S|re.I)
        date  = re.search(r"<updated>(.*?)</updated>", chunk, re.S|re.I)
        yield {
            "source": "PR",
            "title": title.group(1).strip() if title else None,
            "body": None,
            "ts": (date.group(1).strip() if date else None),
            "meta": {
                "source_name": "NewsroomRSS",
                "doc_type": "PR",
                "urls": [link.group(1).strip()] if link else [],
            },
        }

def write_ndjson(items, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"pr_{now}.jsonl"
    count = 0
    with out_path.open("w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")
            count += 1
    print(f"[PR] wrote: {out_path} ({count} items)")
    return out_path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", default=os.getenv("PR_FEED_LIST", "ref/newsroom_rss_list_offline.txt"))
    ap.add_argument("--user-agent", default=os.getenv("PR_USER_AGENT", "supply-signals/phase1"))
    args = ap.parse_args()

    with open(args.list, "r", encoding="utf-8") as fh:
        urls = [u.strip() for u in fh if u.strip() and not u.strip().startswith("#")]

    out = []
    for u in urls:
        try:
            xml = fetch(u, args.user_agent)
            out.extend(list(parse_naive_rss(xml)))
        except Exception as e:
            print(f"[PR] WARN {u}: {e}")

    write_ndjson(out, OUT_DIR)

if __name__ == "__main__":
    main()
