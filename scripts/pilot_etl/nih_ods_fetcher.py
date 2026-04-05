"""
NIH ODS (Office of Dietary Supplements) Fact Sheet Fetcher
-----------------------------------------------------------
Scrapes authoritative vitamin/mineral information from NIH ODS fact sheets.

Source: https://ods.od.nih.gov/factsheets/{NUTRIENT}-HealthProfessional/
No API key required. Content is public domain (U.S. government).

Extracts:
  - Nutrient name + aliases
  - Recommended Dietary Allowance (RDA) / Adequate Intake (AI) by age/sex
  - Tolerable Upper Intake Level (UL)
  - Food sources
  - Deficiency symptoms / health effects
  - Drug interactions
"""
import re
import json
import time
import urllib.request
import urllib.error
from html.parser import HTMLParser
from typing import Dict, List, Optional

USER_AGENT = "OPLE-ETL/1.0 (contact: admin@ople.com)"

# NIH ODS nutrient → URL slug mapping (Health Professional versions are more detailed)
NIH_NUTRIENTS = {
    "vitamin_d": "VitaminD-HealthProfessional",
    "vitamin_c": "VitaminC-HealthProfessional",
    "vitamin_b12": "VitaminB12-HealthProfessional",
    "vitamin_a": "VitaminA-HealthProfessional",
    "vitamin_e": "VitaminE-HealthProfessional",
    "vitamin_k": "VitaminK-HealthProfessional",
    "folate": "Folate-HealthProfessional",
    "calcium": "Calcium-HealthProfessional",
    "iron": "Iron-HealthProfessional",
    "magnesium": "Magnesium-HealthProfessional",
    "zinc": "Zinc-HealthProfessional",
    "omega_3": "Omega3FattyAcids-HealthProfessional",
    "probiotics": "Probiotics-HealthProfessional",
}


class _TextExtractor(HTMLParser):
    """Strips HTML to get plain text blocks, keeping heading markers."""

    def __init__(self):
        super().__init__()
        self.text_parts: List[str] = []
        self._skip = False
        self._in_heading = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "nav", "footer", "header"):
            self._skip = True
        if tag in ("h1", "h2", "h3", "h4"):
            self._in_heading = True
            self.text_parts.append("\n### ")
        if tag in ("p", "li", "td", "br"):
            self.text_parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style", "nav", "footer", "header"):
            self._skip = False
        if tag in ("h1", "h2", "h3", "h4"):
            self._in_heading = False
            self.text_parts.append("\n")

    def handle_data(self, data):
        if self._skip:
            return
        txt = data.strip()
        if txt:
            self.text_parts.append(txt + " ")

    def get_text(self) -> str:
        return "".join(self.text_parts)


def _fetch_html(url: str, timeout: int = 20, retries: int = 3) -> Optional[str]:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                return raw.decode("utf-8", errors="replace")
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"  [NIH] fetch failed after {retries} tries: {e}")
                return None


def _extract_sections(text: str) -> Dict[str, str]:
    """Split plain text into sections by '### ' heading markers."""
    sections: Dict[str, str] = {}
    current = "intro"
    buf: List[str] = []
    for line in text.split("\n"):
        line = re.sub(r"\s+", " ", line).strip()
        if not line:
            continue
        if line.startswith("### "):
            if buf:
                sections[current] = " ".join(buf).strip()
            current = line[4:].lower().strip()[:80]
            buf = []
        else:
            buf.append(line)
    if buf:
        sections[current] = " ".join(buf).strip()
    return sections


def _extract_dosage_info(text: str) -> Dict[str, Optional[str]]:
    """Regex for RDA/AI/UL mentions."""
    info: Dict[str, Optional[str]] = {"rda": None, "ai": None, "ul": None}
    # Common patterns: "RDA is 600 IU", "UL of 4,000 IU/day", "AI: 15 mcg"
    rda_m = re.search(r"(RDA|Recommended Dietary Allowance).{0,80}?(\d[\d,]*\s*(?:IU|mcg|mg|g|µg)[^\.]{0,40})", text, re.I)
    ai_m = re.search(r"(AI|Adequate Intake).{0,80}?(\d[\d,]*\s*(?:IU|mcg|mg|g|µg)[^\.]{0,40})", text, re.I)
    ul_m = re.search(r"(UL|Tolerable Upper Intake Level|upper limit).{0,80}?(\d[\d,]*\s*(?:IU|mcg|mg|g|µg)[^\.]{0,40})", text, re.I)
    if rda_m:
        info["rda"] = rda_m.group(2).strip()
    if ai_m:
        info["ai"] = ai_m.group(2).strip()
    if ul_m:
        info["ul"] = ul_m.group(2).strip()
    return info


def fetch_nutrient(key: str) -> Dict:
    """Fetch and parse a NIH ODS fact sheet for a given nutrient key."""
    slug = NIH_NUTRIENTS.get(key)
    if not slug:
        return {"error": f"unknown nutrient: {key}"}

    url = f"https://ods.od.nih.gov/factsheets/{slug}/"
    html = _fetch_html(url)
    if not html:
        return {"nutrient": key, "source_url": url, "error": "fetch_failed"}

    parser = _TextExtractor()
    parser.feed(html)
    text = parser.get_text()
    sections = _extract_sections(text)
    dosage = _extract_dosage_info(text)

    # Pick useful sections (first 500 chars each)
    def _pick(keywords: List[str]) -> Optional[str]:
        for k in sections:
            if any(kw in k for kw in keywords):
                return sections[k][:600]
        return None

    return {
        "nutrient": key,
        "source": "NIH_ODS",
        "source_url": url,
        "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "dosage": dosage,
        "introduction": sections.get("introduction", "")[:600] or sections.get("intro", "")[:600],
        "sources": _pick(["food sources", "sources of"]),
        "deficiency": _pick(["deficiency", "inadequacy"]),
        "health_effects": _pick(["health effects", "and health"]),
        "interactions": _pick(["interactions", "medications"]),
        "safety": _pick(["safety", "excess", "toxicity"]),
        "section_count": len(sections),
    }


def fetch_all(keys: Optional[List[str]] = None, delay: float = 1.0) -> List[Dict]:
    keys = keys or list(NIH_NUTRIENTS.keys())
    out = []
    for k in keys:
        print(f"  [NIH] fetching {k}...")
        out.append(fetch_nutrient(k))
        time.sleep(delay)  # be polite
    return out


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else "vitamin_d"
    result = fetch_nutrient(target)
    print(json.dumps(result, indent=2, ensure_ascii=False))
