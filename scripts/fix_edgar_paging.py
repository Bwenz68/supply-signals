#!/usr/bin/env python3
"""
Normalize sec_atom_page1.xml for offline paging:
- Remove any xml:base on root/descendants
- Ensure exactly one <link rel="next"> with absolute file:// to page2
- Print the final href
"""
from pathlib import Path
import sys
import xml.etree.ElementTree as ET

NS_XML = "{http://www.w3.org/XML/1998/namespace}"

def strip_xml_base(node):
    if NS_XML + "base" in node.attrib:
        del node.attrib[NS_XML + "base"]
    for ch in list(node):
        strip_xml_base(ch)

def main(p1: Path, p2: Path):
    if not p1.is_file() or not p2.is_file():
        sys.exit(f"Missing file: {p1 if not p1.is_file() else p2}")
    tree = ET.parse(p1)
    root = tree.getroot()

    # Remove any xml:base that could rewrite relative links
    strip_xml_base(root)

    # Prepare absolute file:// URI for page2
    p2_uri = p2.resolve().as_uri()

    # Ensure single rel="next"
    seen = False
    for parent in list(root.iter()):
        for child in list(parent):
            if child.tag.endswith("link") and child.attrib.get("rel") == "next":
                if not seen:
                    child.set("href", p2_uri)
                    seen = True
                else:
                    parent.remove(child)

    if not seen:
        sys.exit("No <link rel='next'> found in page1")

    # Write .bak once, then overwrite original
    bak = p1.with_suffix(p1.suffix + ".bak")
    if not bak.exists():
        bak.write_bytes(p1.read_bytes())
    tree.write(p1, encoding="utf-8", xml_declaration=True)

    # Re-parse to show final state
    href = None
    for el in ET.parse(p1).getroot().iter():
        if el.tag.endswith("link") and el.attrib.get("rel") == "next":
            href = el.attrib.get("href")
            break
    print("FINAL rel=next href:", href)

if __name__ == "__main__":
    repo = Path.cwd()
    # Allow explicit args, else default to tests/fixtures names
    if len(sys.argv) >= 3:
        p1 = Path(sys.argv[1])
        p2 = Path(sys.argv[2])
    else:
        p1 = repo / "tests" / "fixtures" / "sec_atom_page1.xml"
        p2 = repo / "tests" / "fixtures" / "sec_atom_page2.xml"
    main(p1, p2)
