#!/usr/bin/env python3
"""Congbao collector for the KeToanAI Legal Intelligence MVP.

Discovers Vietnamese accounting/tax documents on Cong bao dien tu
(https://congbao.chinhphu.vn), downloads the full text as PDF from the
links published on each detail page, parses the text into provisions
(Chuong -> Muc -> Dieu -> Khoan -> Diem) and stores everything in a local
SQLite database with the same ``documents`` schema as the MVP.

Discovery mechanisms (all verified live 2026-11-08, see README.md):
  * Known detail URLs from the target list (primary path).
  * The site's own HTML-fragment listing endpoint
    ``/api-searchvanban/c{agency}/l{type}/d0/h0/0-0/s{size}/trang-{page}.htm``
    (reverse-engineered from the site's public JS bundle
    static.mediacdn.vn/CongBao/min/main-06042026v1.min.js). It returns 20
    documents per page, newest first, with detail-page links. Agency ids:
    Bo Tai chinh = 10, Quoc hoi = 31, Chinh phu = 1. Type ids:
    Nghi dinh = 1, Quyet dinh = 2, Thong tu = 3, Nghi quyet = 6, Luat = 13.
  * The keyword search API (https://api-searchcongbao.chinhphu.vn/search)
    is NOT used: outbound POSTs time out from this VM and the endpoint
    could not be verified live. It is documented in README.md as unverified.

Full text lives in PDFs, NOT in the detail-page HTML. Each detail page
publishes one PDF per Cong bao issue the document spans (TT99 spans 10
issues). The collector takes the PDF URLs from the detail page HTML:
prefer the direct links on congbaocdn.chinhphu.vn (plain, public URLs),
fall back to the signed g7.cdnchinhphu.vn download links. It NEVER
guesses a PDF URL.

Politeness: max 1 request/second, identifiable User-Agent, 3 retries with
exponential backoff. Robots: Cong bao allows / (robots.txt verified
2026-11-08). The VBPL blocklist in check_robots_policy() exists because
this collector must never request a disallowed VBPL path if a VBPL URL is
ever passed in by mistake (VBPL disallows /api/ and /Pages/).

Usage:
    python congbao_collector.py                      # ingest the 5 verified docs
    python congbao_collector.py --only 99/2025/TT-BTC,67/2025/QH15
    python congbao_collector.py --limit 20           # newest 20 Bo Tai chinh docs
    python congbao_collector.py --dry-run            # discover only, no writes
    python congbao_collector.py --only 99/2025/TT-BTC --force   # re-parse after a parser fix
    python congbao_collector.py --db path/to.db --target-csv path/to.csv
    python congbao_collector.py --local-pdfs pdfs/   # ingest PDFs downloaded
                                                     # by another route (e.g. a
                                                     # browser that can reach g7)
    python congbao_collector.py --pdf 88-2015-QH13.pdf  # one local PDF
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import httpx
import pymupdf
from bs4 import BeautifulSoup

BASE = "https://congbao.chinhphu.vn"
USER_AGENT = "KeToanAI-Research-Collector/0.1 (contact: KeToanAI research MVP)"
RATE_LIMIT_SECONDS = 1.0
MAX_RETRIES = 3
MAX_PDF_BYTES = 50_000_000
PAGE_SIZE = 20

# Agency / document-type ids used by /api-searchvanban (from the site's
# public JS bundle and the agency listing pages, verified live 2026-11-08).
AGENCY_IDS = {
    "CHÍNH PHỦ": 1,
    "THỦ TƯỚNG CHÍNH PHỦ": 2,
    "BỘ TÀI CHÍNH": 10,
    "QUỐC HỘI": 31,
    "ỦY BAN THƯỜNG VỤ QUỐC HỘI": 28,
    "CHỦ TỊCH NƯỚC": 12,
}
TYPE_IDS = {
    "NGHỊ ĐỊNH": 1,
    "QUYẾT ĐỊNH": 2,
    "THÔNG TƯ": 3,
    "NGHỊ QUYẾT": 6,
    "LUẬT": 13,
}

# Hosts this collector is allowed to request. Everything else is refused
# before any network call (see check_robots_policy).
ALLOWED_HOSTS = {
    "congbao.chinhphu.vn",
    "congbaocdn.chinhphu.vn",   # direct PDF files linked from detail pages
    "g7.cdnchinhphu.vn",        # signed PDF/DOC download links (fallback)
    "api-searchcongbao.chinhphu.vn",  # keyword search API (unverified, unused)
    "vbpl.vn",                   # allowed only outside /api/ and /Pages/
}

# VBPL robots.txt (verified 2026-11-08): Allow / ; Disallow /api/ ; Disallow
# /Pages/ ; Sitemap: https://vbpl.vn/sitemap.xml
VBPL_DISALLOWED_PREFIXES = ("/api/", "/pages/")
VBPL_DISALLOWED_EXACT = ("/api", "/pages")


# --------------------------------------------------------------------------
# Robots / allowlist policy
# --------------------------------------------------------------------------
def check_robots_policy(url: str) -> str:
    """Validate a URL against the collector's robots/allowlist policy.

    Returns the normalised URL if it may be fetched, otherwise raises
    ValueError. This runs BEFORE any network request, so a disallowed
    VBPL path (or an unknown host) can never be requested, even by
    accident. Rules:
      * scheme must be http/https;
      * host must be in ALLOWED_HOSTS;
      * on vbpl.vn, paths starting with /api/ or /Pages/ (and the bare
        /api or /Pages paths) are refused — VBPL robots.txt disallows
        them. The vbpl.vn sitemap (https://vbpl.vn/sitemap.xml) is allowed.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Refused (scheme not http/https): {url}")
    host = (parsed.hostname or "").lower()
    if host not in ALLOWED_HOSTS:
        raise ValueError(f"Refused (host not in allowlist): {url}")
    path = parsed.path or "/"
    if host == "vbpl.vn":
        low = path.lower()
        if low in VBPL_DISALLOWED_EXACT or low.startswith(VBPL_DISALLOWED_PREFIXES):
            raise ValueError(f"Refused (VBPL robots.txt disallows {path}): {url}")
    return url


def sanitize_no_proxy() -> None:
    """Remove bracketed IPv6 entries (e.g. [::1]) from NO_PROXY.

    httpx crashes with ``InvalidURL: ':1]'`` when NO_PROXY contains an
    IPv6 literal in brackets (same bug found in the MVP's /api/fetch).
    Stripping those entries keeps proxy use working and fixes the crash.
    """
    for key in ("NO_PROXY", "no_proxy"):
        val = os.environ.get(key)
        if not val:
            continue
        kept = [p for p in val.split(",") if "[" not in p]
        os.environ[key] = ",".join(kept)


# --------------------------------------------------------------------------
# HTTP client (rate-limited, retrying)
# --------------------------------------------------------------------------
class Collector:
    def __init__(self, db_path: Path, dry_run: bool = False):
        sanitize_no_proxy()
        self.db_path = db_path
        self.dry_run = dry_run
        self.client = httpx.Client(
            headers={"User-Agent": USER_AGENT},
            timeout=60,
            follow_redirects=True,
        )
        self._last_request_at = 0.0
        self.stats = {"fetched_pages": 0, "fetched_pdfs": 0, "retries": 0}

    def _polite_wait(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < RATE_LIMIT_SECONDS:
            time.sleep(RATE_LIMIT_SECONDS - elapsed)
        self._last_request_at = time.monotonic()

    def get(self, url: str, *, expect: str = "html") -> httpx.Response:
        """GET with policy check, 1 req/s pacing and retry/backoff."""
        check_robots_policy(url)
        last_exc: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            self._polite_wait()
            try:
                resp = self.client.get(url)
                resp.raise_for_status()
                self.stats["fetched_pages"] += 1
                return resp
            except (httpx.HTTPError, httpx.RemoteProtocolError) as exc:
                last_exc = exc
                self.stats["retries"] += 1
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"GET failed after {MAX_RETRIES} attempts: {url}: {last_exc}")

    def get_bytes(self, url: str, referer: str) -> bytes:
        """Download a binary file (PDF) with policy check and retries."""
        check_robots_policy(url)
        last_exc: Exception | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            self._polite_wait()
            try:
                with self.client.stream(
                    "GET", url, headers={"Referer": referer, "Accept": "application/pdf,*/*"}
                ) as resp:
                    resp.raise_for_status()
                    size = int(resp.headers.get("content-length", "0") or 0)
                    if size and size > MAX_PDF_BYTES:
                        raise RuntimeError(f"PDF too large ({size} bytes): {url}")
                    chunks, total = [], 0
                    for chunk in resp.iter_bytes(chunk_size=65536):
                        total += len(chunk)
                        if total > MAX_PDF_BYTES:
                            raise RuntimeError(f"PDF too large (> {MAX_PDF_BYTES} bytes): {url}")
                        chunks.append(chunk)
                self.stats["fetched_pdfs"] += 1
                return b"".join(chunks)
            except (httpx.HTTPError, httpx.RemoteProtocolError) as exc:
                last_exc = exc
                self.stats["retries"] += 1
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"PDF download failed after {MAX_RETRIES} attempts: {url}: {last_exc}")

    def close(self) -> None:
        self.client.close()


# --------------------------------------------------------------------------
# Detail page parsing
# --------------------------------------------------------------------------
VI_DATE_RE = re.compile(r"(\d{2})/(\d{2})/(\d{4})")
NUMBER_RE = re.compile(r"\b\d{1,4}/\d{4}/(?:TT|NĐ|QĐ|QH|VBHN|NQ|CT|PL)[-\w]*", re.I)


def _vi_to_iso(date_str: str | None) -> str | None:
    if not date_str:
        return None
    m = VI_DATE_RE.search(date_str)
    if not m:
        return None
    return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"


def _is_direct_pdf_link(href: str) -> bool:
    """True for a direct congbaocdn PDF link.

    Most gazette PDFs end in ".pdf", but older CongBaoCP files are served
    from /CongBaoCP/VanBan/... with a trailing "...pdf" and no dot
    (e.g. .../12961-1-882015qh1313010pdf — Luật Kế toán 88/2015).
    The path segment pins the match to gazette documents, not the .png
    agency logos that also live on congbaocdn.
    """
    low = href.lower()
    if "congbaocdn.chinhphu.vn" not in low:
        return False
    if low.endswith(".pdf"):
        return True
    return "/congbaocp/vanban/" in low and low.endswith("pdf")


def parse_detail_page(html: str, url: str) -> dict:
    """Extract metadata + PDF links from a Cong bao detail page.

    PDF links: prefer the direct congbaocdn.chinhphu.vn URLs embedded
    in the page (".pdf" or dotless /CongBaoCP/VanBan/...pdf); fall back
    to the signed g7.cdnchinhphu.vn download links.
    Links are returned in page order (== gazette-issue order).
    """
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    text = soup.get_text("\n", strip=True)

    number = ""
    m = re.search(r"Số, ký hiệu\s*\n\s*([^\n]+)", text)
    if m:
        number = m.group(1).strip()
    if not number:
        m = NUMBER_RE.search(title + " " + text[:2000])
        number = m.group(0) if m else ""

    issued = effective = None
    m = re.search(r"Ban hành:\s*(\d{2}/\d{2}/\d{4})\s*-\s*Hiệu lực:\s*(\d{2}/\d{2}/\d{4})", text)
    if m:
        issued, effective = _vi_to_iso(m.group(1)), _vi_to_iso(m.group(2))
    if not issued:
        m = re.search(r"Ngày ban hành\s*\n\s*(\d{2}/\d{2}/\d{4})", text)
        issued = _vi_to_iso(m.group(1)) if m else None
    if not effective:
        m = re.search(r"Ngày hiệu lực[^\d]*(\d{2}/\d{2}/\d{4})", text)
        effective = _vi_to_iso(m.group(1)) if m else None

    agency = ""
    # "Cơ quan ban hành" also appears as a nav label; take the match whose
    # value looks like a real agency name (all caps, e.g. BỘ TÀI CHÍNH).
    for m in re.finditer(r"Cơ quan ban hành\s*\n\s*([^\n]+)", text):
        val = m.group(1).strip()
        if len(val) > 3 and val == val.upper() and "Hệ thống" not in val:
            agency = val
            break

    doc_type = ""
    m = re.search(r"Loại văn bản\s*\n\s*([^\n]+)", text)
    if m:
        doc_type = m.group(1).strip()

    direct_pdfs: list[tuple[str, str]] = []
    signed_pdfs: list[tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        label = a.get_text(" ", strip=True)
        if _is_direct_pdf_link(href):
            direct_pdfs.append((label, href))
        elif href.startswith("https://g7.cdnchinhphu.vn/api/download/stream") and href.lower().endswith(".pdf"):
            signed_pdfs.append((label, href))
    # The direct PDF URLs also live in the issue-tab buttons as data-href
    # (<a class="itemtep" data-tepid="59631"
    #      data-href="https://congbaocdn.chinhphu.vn/...pdf">) — these are
    # the primary source on most detail pages. Older CongBaoCP files use
    # a dotless "...pdf" ending (see _is_direct_pdf_link).
    for tag in soup.find_all(attrs={"data-href": True}):
        href = tag["data-href"]
        label = tag.get("title", "") or tag.get_text(" ", strip=True)
        if _is_direct_pdf_link(href):
            direct_pdfs.append((label, href))
        elif href.startswith("https://g7.cdnchinhphu.vn/api/download/stream") and href.lower().endswith(".pdf"):
            signed_pdfs.append((label, href))

    def _dedupe(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
        seen, out = set(), []
        for label, href in pairs:
            if href not in seen:
                seen.add(href)
                out.append((label, href))
        return out

    pdfs = _dedupe(direct_pdfs)
    pdfs_fallback = _dedupe(signed_pdfs)
    return {
        "number": number,
        "title": title,
        "agency": agency,
        "doc_type": doc_type,
        "issued_date": issued,
        "effective_date": effective,
        "pdf_links": pdfs,  # [(file_name, url)] in issue order (direct CDN)
        "pdf_links_fallback": pdfs_fallback,  # signed g7 download links
        "source_url": url,
    }


# --------------------------------------------------------------------------
# Discovery via the site's listing fragment endpoint
# --------------------------------------------------------------------------
def normalize_number(s: str | None) -> str:
    """Normalise a document number for matching (case/space-insensitive)."""
    return re.sub(r"\s+", "", (s or "").strip().upper())


KYHIEU_RE = re.compile(r"Ký hiệu:\s*(\S+)")


def parse_listing_items(html: str) -> list[dict]:
    """Parse one /api-searchvanban fragment page into listing items.

    Each ``.item-newspaper`` block carries the document number in its
    "Ký hiệu:" label and the detail page in its ``a.sapo`` link. This
    is more reliable than the old NUMBER_RE title match: it also matches
    numbers the regex cannot express (e.g. ``954/2020/UBTVQH14``,
    ``66.7/2025/NQ-CP``). Returns [{ky_hieu, number_norm, title, url,
    issued_date}] in page order (newest first).
    """
    soup = BeautifulSoup(html, "html.parser")
    items: list[dict] = []
    for block in soup.select(".item-newspaper"):
        text = block.get_text(" ", strip=True)
        m = KYHIEU_RE.search(text)
        if not m:
            continue
        ky = m.group(1)
        a = block.select_one("a.sapo[href]")
        if not a:
            continue
        href = a["href"]
        if href.startswith("#") or "/van-ban/" not in href:
            continue
        url = href if href.startswith("http") else BASE + href
        d = re.search(r"Ban hành:\s*(\d{2}/\d{2}/\d{4})", text)
        issued = _vi_to_iso(d.group(1)) if d else None
        items.append({
            "ky_hieu": ky,
            "number_norm": normalize_number(ky),
            "title": a.get_text(" ", strip=True),
            "url": url,
            "issued_date": issued,
        })
    return items


def scan_agency_items(collector: Collector, agency_id: int,
                       cutoff_iso: str,
                       max_pages: int = 600) -> tuple[list[dict], int]:
    """Scan one agency's listing, newest-first, returning all items found.

    Stops early on an empty page or when the page's newest item is older
    than ``cutoff_iso`` (a date floor with gazette-lag buffer) — later
    pages can only be older. Each item: {ky_hieu, number_norm, title,
    url, issued_date}. NOTE: the endpoint ignores the type id, so the
    scan is agency-only and covers every document type of that agency.
    """
    items: list[dict] = []
    pages = 0
    for page in range(1, max_pages + 1):
        url = (f"{BASE}/api-searchvanban/c{agency_id}/l0/d0/h0/0-0"
               f"/s{PAGE_SIZE}/trang-{page}.htm")
        resp = collector.get(url)
        pages += 1
        page_items = parse_listing_items(resp.text)
        if not page_items:
            break
        items.extend(page_items)
        newest = page_items[0].get("issued_date")
        if newest and newest < cutoff_iso:
            break
    return items, pages


def scan_agency(collector: Collector, agency_id: int,
                wanted_norms: set[str], cutoff_iso: str,
                max_pages: int = 600) -> tuple[dict[str, str], int]:
    """Scan one agency's listing until all wanted docs are found.

    Pages are newest-first, 20 docs each. Stops early on an empty page,
    when every wanted number is found, or when the page's newest item is
    older than ``cutoff_iso`` (oldest wanted issue date minus a 180-day
    gazette-lag buffer) — later pages can only be older. Returns
    ({number_norm: detail_url}, pages_scanned).
    """
    items, pages = scan_agency_items(collector, agency_id, cutoff_iso,
                                     max_pages)
    found: dict[str, str] = {}
    for it in items:
        if it["number_norm"] in wanted_norms and it["number_norm"] not in found:
            found[it["number_norm"]] = it["url"]
        if len(found) == len(wanted_norms):
            break
    return found, pages


def fuzzy_match(row: dict, items: list[dict]) -> dict | None:
    """Fallback match for a doc whose exact number is not in the listing.

    Accepts an item only when ALL of these hold: same issue date as the
    CSV row, and the item title shares >= 60% of the CSV title's
    significant words. Returns the single matching item, or None when
    there are zero or several candidates. This exists because Cong bao
    itself can file a document under a mistyped number — e.g. the PIT
    deduction resolution is filed as 110/2023/UBTVQH15 although it was
    adopted 17/10/2025 and every other source calls it 110/2025/UBTVQH15.
    A fuzzy match is NEVER silently relabelled: callers must record the
    number discrepancy.
    """
    stop = {"về", "của", "và", "số", "quy", "định", "hướng", "dẫn",
            "thi", "hành", "một", "sửa", "đổi", "bổ", "sung", "cho",
            "các", "việc", "ban", "công", "bố", "luật", "thông", "tư",
            "nghị", "quyết", "nghị", "định"}
    def words(title: str) -> set[str]:
        out = set()
        for w in re.findall(r"[A-Za-zÀ-ỹ]+", title.lower()):
            if len(w) > 3 and w not in stop:
                out.add(w)
        return out
    target_words = words(row.get("ten_van_ban", ""))
    if not target_words:
        return None
    target_date = (row.get("ngay_ban_hanh") or "").strip()
    cands = []
    for it in items:
        if target_date and it.get("issued_date") != target_date:
            continue
        iw = words(it.get("title", ""))
        if not iw:
            continue
        if len(target_words & iw) / len(target_words) >= 0.6:
            cands.append(it)
    return cands[0] if len(cands) == 1 else None


def search_listing(collector: Collector, agency_id: int = 0, type_id: int = 0,
                   max_pages: int = 10) -> list[dict]:
    """Scan /api-searchvanban listing pages; return [{number, title, url}].

    Pages are newest-first, 20 documents each. Stops early on an empty
    page. Matching against a target number is done by the caller.
    NOTE: the endpoint ignores the type id (verified 2026-11-08: l13 and
    l6 return identical results for the same agency) — listings are
    agency-only, so callers should scan by agency and match on the
    "Ký hiệu" label via parse_listing_items / scan_agency instead.
    """
    found: list[dict] = []
    for page in range(1, max_pages + 1):
        url = (f"{BASE}/api-searchvanban/c{agency_id}/l{type_id}/d0/h0/0-0"
               f"/s{PAGE_SIZE}/trang-{page}.htm")
        resp = collector.get(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        page_docs: list[dict] = []
        seen_urls = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/van-ban/" not in href or href in seen_urls:
                continue
            if href.startswith("#") or "box-comment" in href:
                continue
            seen_urls.add(href)
            title = a.get_text(" ", strip=True)
            m = NUMBER_RE.search(title)
            if not m:
                continue
            full = href if href.startswith("http") else BASE + href
            page_docs.append({"number": m.group(0), "title": title, "url": full})
        if not page_docs:
            break
        found.extend(page_docs)
    return found


def resolve_detail_url(collector: Collector, number: str, agency: str,
                       doc_type: str, max_pages: int = 10) -> str | None:
    """Find a document's real detail page by scanning its agency listing.

    Used when the target list only has a listing/index URL for a document
    (e.g. Luật 108/2025 and 109/2025 were sourced from a 'newest documents'
    index page, not a detail page). Scans the agency listing (the endpoint
    ignores the type id) until the number is found or the pages run out.
    """
    agency_id = AGENCY_IDS.get((agency or "").strip().upper(), 0)
    wanted = {normalize_number(number)}
    found, _ = scan_agency(collector, agency_id, wanted, "0000-01-01",
                           max_pages=max_pages)
    return found.get(normalize_number(number))


# --------------------------------------------------------------------------
# PDF text extraction + provision parsing
# --------------------------------------------------------------------------
def extract_pdf_text(pdf_bytes: bytes) -> tuple[str, int, int]:
    """Return (text, page_count, scanned_page_count) for one PDF."""
    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    pages = len(doc)
    scanned = 0
    parts = []
    for page in doc:
        txt = page.get_text()
        if not txt.strip():
            scanned += 1
        parts.append(txt)
    return "\n".join(parts), pages, scanned


def extract_vbpl_html_text(html: str) -> str:
    """Extract the rendered full text of a VBPL document detail page.

    VBPL is a Next.js app: the document body (``body_content.html``) is
    not in the static HTML but in a ``self.__next_f.push`` RSC chunk whose
    decoded content is a complete HTML document. This finds that chunk
    (the longest ``<!DOCTYPE html>`` payload), strips its tags to text
    lines and drops the page-chrome line "Document Content".

    Raises ValueError if no document-body chunk is found. Never invents
    text: what is returned is exactly what VBPL rendered for the page.
    """
    parts = html.split('self.__next_f.push([1,"')
    candidates = []
    for part in parts[1:]:
        end = part.find('"])</script>')
        seg = part[:end] if end != -1 else ""
        if "DOCTYPE html" not in seg:
            continue
        try:
            decoded = json.loads('"' + seg + '"')
        except json.JSONDecodeError:
            continue
        # The chunk content is \\u-escaped in the page source, so only the
        # *decoded* string contains the literal "<!DOCTYPE html>".
        if "<!DOCTYPE html>" in decoded:
            candidates.append(decoded)
    if not candidates:
        # Fallback (VBPL format change, thấy 09/11/2026 ở TT200/TT133):
        # body về dạng mảnh <html>...<body>...<table> trong RSC chunk chứ
        # không phải tài liệu <!DOCTYPE html> đầy đủ. Lấy chunk đã decode
        # dài nhất có chứa thẻ <body> (chunk thân văn bản dài hàng MB, các
        # chunk khác chỉ vài KB) rồi xử lý như trên.
        for part in parts[1:]:
            end = part.find('"])</script>')
            seg = part[:end] if end != -1 else ""
            if "body" not in seg:
                continue
            try:
                decoded = json.loads('"' + seg + '"')
            except json.JSONDecodeError:
                continue
            if "<body" in decoded and len(decoded) > 10000:
                candidates.append(decoded)
    if not candidates:
        raise ValueError("no VBPL document-body (DOCTYPE) RSC chunk found")
    inner = max(candidates, key=len)
    soup = BeautifulSoup(inner, "html.parser")
    lines = [ln.strip() for ln in soup.get_text("\n").splitlines()]
    lines = [ln for ln in lines if ln]
    if lines and lines[0] == "Document Content":
        lines = lines[1:]
    return "\n".join(lines)


HEADER_RE = re.compile(r"^\s*\d+\s+CÔNG BÁO/Số\s+\d+\s*\+\s*\d+/Ngày\s+\d+-\d+-\d+\s*$", re.M)
HEADER2_RE = re.compile(r"^CÔNG BÁO/Số\s+\d+\s*\+\s*\d+/Ngày\s+\d+-\d+-\d+.*$", re.M)
CHUONG_RE = re.compile(r"^Chương\s+([IVXLCDM\d]+)\s*$", re.M)
MUC_RE = re.compile(r"^Mục\s+(\d+)\.?\s*(.*)$", re.M)
DIEU_RE = re.compile(r"^Điều\s+(\d+)\.\s*(.*)$", re.M)
KHOAN_RE = re.compile(r"^(\d+)\.\s+(.*)$")
DIEM_RE = re.compile(r"^([a-zđ])\)\s+(.*)$")
PHULUC_RE = re.compile(r"^(?:PHỤ LỤC\b.*|Phụ lục\s+[IVXLCDM\d]+\s*(?:[-–.].{0,60})?)$")


def clean_text(text: str) -> str:
    """Drop repeated gazette running headers/footers; normalise whitespace."""
    text = HEADER_RE.sub("", text)
    text = HEADER2_RE.sub("", text)
    # Lone page numbers on their own line (gazette pagination).
    lines = [ln for ln in text.splitlines() if not re.fullmatch(r"\s*\d{1,4}\s*", ln)]
    return "\n".join(lines)


def parse_provisions(text: str, document_number: str) -> list[dict]:
    """Split full text into provisions: Chuong -> Muc -> Dieu -> Khoan -> Diem.

    One row per smallest unit found (Diem > Khoan > Dieu). Rows carry a
    confidence: high = clean Dieu heading with real content; medium =
    heading matched but thin content, or the preamble chunk; low = fallback
    chunks (Phu luc, unparsed remainder). Never invents text.
    """
    text = clean_text(text)
    lines = [ln.rstrip() for ln in text.splitlines()]
    # Attach a following-line title to bare "Chương X" headings.
    provisions: list[dict] = []
    chuong: str | None = None
    muc: str | None = None
    cur_dieu: int | None = None
    cur_title = ""
    cur_body: list[str] = []
    preamble: list[str] = []
    seen_dieu = False

    def flush_dieu() -> None:
        nonlocal cur_dieu, cur_title, cur_body
        if cur_dieu is None:
            return
        body = "\n".join(cur_body).strip()
        if body:
            provisions.extend(_split_dieu_body(
                document_number, chuong, muc, cur_dieu, cur_title, body))
        cur_dieu, cur_title, cur_body = None, "", []

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue
        m = CHUONG_RE.match(line)
        if m:
            flush_dieu()
            chuong = m.group(1)
            # Title is usually the next non-empty line in caps.
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines) and lines[j].strip().isupper() and len(lines[j].strip()) > 3:
                chuong = f"{m.group(1)}: {lines[j].strip()}"
                i = j
            i += 1
            continue
        m = MUC_RE.match(line)
        if m and cur_dieu is None and len(line) < 120:
            # A Muc heading only counts before any Dieu in the current
            # Chuong (later "Mục ..." lines are body text, e.g. in TT99's
            # annexes). Short-line guard avoids matching long sentences.
            flush_dieu()
            muc = m.group(1)
            i += 1
            continue
        m = DIEU_RE.match(line)
        if m:
            flush_dieu()
            seen_dieu = True
            cur_dieu = int(m.group(1))
            cur_title = m.group(2).strip()
            cur_body = []
            i += 1
            continue
        m = PHULUC_RE.match(line)
        if m and seen_dieu and len(line) < 90:
            # Strict heading only: "Phụ lục I" / "PHỤ LỤC ..." as a short
            # heading line. A sentence that merely mentions an appendix
            # (e.g. "… tại Phụ lục I ban hành kèm theo Thông tư này.")
            # must NOT truncate the document — that bug cut TT99 off
            # after Điều 9 in the first run.
            flush_dieu()
            # The rest (appendices, forms, tables) is kept as one low-
            # confidence chunk; it is not legal articles.
            rest = "\n".join(lines[i:]).strip()
            if rest:
                provisions.append({
                    "document_number": document_number, "chuong": "Phụ lục",
                    "muc": None, "dieu": None, "khoan": None, "diem": None,
                    "noi_dung": rest[:200000], "confidence": "low",
                })
            break
        if cur_dieu is None:
            preamble.append(line)
        else:
            cur_body.append(line)
        i += 1
    flush_dieu()

    if preamble:
        pre = "\n".join(preamble).strip()
        if pre:
            # Split the preamble the same way a Dieu body is split.
            # VAS standards / guidance circulars (105/2003, 161/2007,
            # 165/2002, 20/2006, 21/2006) have NO Dieu at all, so without
            # this their whole text lands in ONE row capped at 200k chars
            # (161/2007 loses ~13k) and is unsearchable as units. Splitting
            # on the numbered paragraphs (dieu=0 keeps the masthead
            # convention) makes each paragraph a searchable provision.
            # For a short plain preamble _split_dieu_body returns exactly
            # one row, so normal docs are unaffected.
            provisions[:0] = _split_dieu_body(
                document_number, None, None, 0, "", pre)
    return provisions


def _split_dieu_body(number: str, chuong, muc, dieu: int, title: str,
                     body: str) -> list[dict]:
    """Split one Dieu's body into Khoan rows, then Diem rows where present."""
    lines = body.splitlines()
    # Group lines into khoan chunks: a line starting with "N. " starts a
    # new khoan; everything before the first such line belongs to khoan None
    # (usually the Dieu's opening sentence).
    chunks: list[tuple[int | None, list[str]]] = []
    cur_num: int | None = None
    cur_lines: list[str] = []
    for ln in lines:
        m = KHOAN_RE.match(ln.strip())
        if m and int(m.group(1)) <= 50 and len(ln.strip()) < 2000:
            if cur_lines or cur_num is not None:
                chunks.append((cur_num, cur_lines))
            cur_num = int(m.group(1))
            cur_lines = [ln.strip()]
        else:
            cur_lines.append(ln)
    if cur_lines or cur_num is not None:
        chunks.append((cur_num, cur_lines))

    rows: list[dict] = []
    for khoan, chunk_lines in chunks:
        chunk_text = "\n".join(chunk_lines).strip()
        if not chunk_text:
            continue
        # Split a khoan into diem rows on "a) ", "b) " ... line starts.
        diem_chunks: list[tuple[str | None, list[str]]] = []
        cur_d: str | None = None
        cur_dl: list[str] = []
        for ln in chunk_text.splitlines():
            m = DIEM_RE.match(ln.strip())
            if m and len(ln.strip()) < 2000:
                if cur_dl or cur_d is not None:
                    diem_chunks.append((cur_d, cur_dl))
                cur_d = m.group(1)
                cur_dl = [ln.strip()]
            else:
                cur_dl.append(ln)
        if cur_dl or cur_d is not None:
            diem_chunks.append((cur_d, cur_dl))
        # Only keep the diem split if it found >= 2 diem; otherwise the
        # khoan is a single row (avoids splitting on stray "a)" in text).
        real_diem = [d for d in diem_chunks if d[0] is not None]
        if len(real_diem) >= 2:
            for d, dl in diem_chunks:
                t = "\n".join(dl).strip()
                if t:
                    rows.append({
                        "document_number": number, "chuong": chuong, "muc": muc,
                        "dieu": dieu, "khoan": khoan, "diem": d,
                        "noi_dung": t[:200000], "confidence": "high",
                    })
        else:
            conf = "high" if len(chunk_text) > 50 else "medium"
            rows.append({
                "document_number": number, "chuong": chuong, "muc": muc,
                "dieu": dieu, "khoan": khoan, "diem": None,
                "noi_dung": chunk_text[:200000], "confidence": conf,
            })
    if not rows:  # Dieu with no body text at all
        rows.append({
            "document_number": number, "chuong": chuong, "muc": muc,
            "dieu": dieu, "khoan": None, "diem": None,
            "noi_dung": title, "confidence": "low",
        })
    return rows


# --------------------------------------------------------------------------
# Storage (MVP-compatible documents table + provisions table)
# --------------------------------------------------------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
  id INTEGER PRIMARY KEY, number TEXT, title TEXT NOT NULL,
  category TEXT NOT NULL, source_url TEXT NOT NULL UNIQUE,
  issued_date TEXT, effective_date TEXT,
  extracted_text TEXT NOT NULL DEFAULT '', sha256 TEXT NOT NULL DEFAULT '',
  fetched_at TEXT, review_status TEXT NOT NULL DEFAULT 'unverified'
);
CREATE INDEX IF NOT EXISTS idx_docs_number ON documents(number);
CREATE TABLE IF NOT EXISTS provisions (
  id INTEGER PRIMARY KEY, document_number TEXT NOT NULL,
  chuong TEXT, muc TEXT, dieu INTEGER, khoan INTEGER, diem TEXT,
  noi_dung TEXT NOT NULL, confidence TEXT NOT NULL DEFAULT 'medium',
  ordinal INTEGER NOT NULL DEFAULT 0,
  UNIQUE(document_number, dieu, khoan, diem, ordinal)
);
CREATE INDEX IF NOT EXISTS idx_prov_doc ON provisions(document_number);
CREATE TABLE IF NOT EXISTS fetch_log (
  id INTEGER PRIMARY KEY, document_number TEXT, source_url TEXT,
  pdf_url TEXT, pages INTEGER, chars INTEGER, status TEXT,
  error TEXT, fetched_at TEXT
);
"""


def migrate_provisions_schema(conn: sqlite3.Connection) -> bool:
    """Add the `ordinal` column to provisions on pre-fix databases.

    The old UNIQUE(document_number, dieu, khoan, diem) collapsed repeated
    article keys in amending decrees (111 provisions lost in 4 docs).
    The new key is UNIQUE(document_number, dieu, khoan, diem, ordinal),
    where ordinal is the provision's order of appearance in the document.
    Existing rows keep their insertion order as ordinal (ROW_NUMBER over
    id, partitioned by document). Returns True if a migration ran.
    """
    cols = [r[1] for r in conn.execute("PRAGMA table_info(provisions)")]
    if "ordinal" in cols:
        return False
    conn.execute("""
        CREATE TABLE provisions_new (
          id INTEGER PRIMARY KEY, document_number TEXT NOT NULL,
          chuong TEXT, muc TEXT, dieu INTEGER, khoan INTEGER, diem TEXT,
          noi_dung TEXT NOT NULL, confidence TEXT NOT NULL DEFAULT 'medium',
          ordinal INTEGER NOT NULL DEFAULT 0,
          UNIQUE(document_number, dieu, khoan, diem, ordinal)
        )""")
    conn.execute("""
        INSERT INTO provisions_new
          (id, document_number, chuong, muc, dieu, khoan, diem, noi_dung,
           confidence, ordinal)
        SELECT id, document_number, chuong, muc, dieu, khoan, diem,
               noi_dung, confidence,
               ROW_NUMBER() OVER (PARTITION BY document_number ORDER BY id)
        FROM provisions""")
    conn.execute("DROP TABLE provisions")
    conn.execute("ALTER TABLE provisions_new RENAME TO provisions")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_prov_doc ON provisions(document_number)")
    conn.commit()
    return True


def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)
    conn.commit()
    if migrate_provisions_schema(conn):
        print("[migrate] provisions table: added ordinal column, "
              "key is now UNIQUE(document_number, dieu, khoan, diem, ordinal)",
              flush=True)
    return conn


def store_document(conn: sqlite3.Connection, meta: dict, text: str,
                   category: str, force: bool = False) -> tuple[str, int]:
    """Idempotent upsert keyed by source_url + sha256.

    Returns (action, doc_id): 'inserted' | 'updated' | 'unchanged'.
    With force=True a matching sha is still re-stored (needed after a
    parser fix — otherwise the old provisions would survive).
    """
    sha = hashlib.sha256(text.encode()).hexdigest() if text else ""
    now = datetime.now(timezone.utc).isoformat()
    row = conn.execute(
        "SELECT id, sha256 FROM documents WHERE source_url = ?",
        (meta["source_url"],)).fetchone()
    if row and row[1] == sha and not force:
        return "unchanged", row[0]
    if row:
        conn.execute(
            """UPDATE documents SET number=?, title=?, category=?,
               issued_date=COALESCE(?, issued_date),
               effective_date=COALESCE(?, effective_date),
               extracted_text=?, sha256=?, fetched_at=?, review_status='unverified'
               WHERE id=?""",
            (meta["number"], meta["title"], category, meta["issued_date"],
             meta["effective_date"], text[:300000], sha, now, row[0]))
        conn.execute("DELETE FROM provisions WHERE document_number = ?", (meta["number"],))
        conn.commit()
        return "updated", row[0]
    cur = conn.execute(
        """INSERT INTO documents(number, title, category, source_url,
           issued_date, effective_date, extracted_text, sha256, fetched_at,
           review_status) VALUES(?,?,?,?,?,?,?,?,?, 'unverified')""",
        (meta["number"], meta["title"], category, meta["source_url"],
         meta["issued_date"], meta["effective_date"], text[:300000], sha, now))
    conn.commit()
    return "inserted", cur.lastrowid


def store_provisions(conn: sqlite3.Connection, provisions: list[dict]) -> int:
    """Store provisions in appearance order.

    ordinal is 1-based position in the parsed list, so repeated
    (dieu, khoan, diem) keys in amending decrees no longer overwrite
    each other under the UNIQUE(document_number, dieu, khoan, diem,
    ordinal) key.
    """
    n = 0
    for ordinal, p in enumerate(provisions, start=1):
        conn.execute(
            """INSERT OR REPLACE INTO provisions
               (document_number, chuong, muc, dieu, khoan, diem, noi_dung,
                confidence, ordinal)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (p["document_number"], p["chuong"], p["muc"], p["dieu"],
             p["khoan"], p["diem"], p["noi_dung"], p["confidence"], ordinal))
        n += 1
    conn.commit()
    return n


def log_fetch(conn: sqlite3.Connection, number: str, source_url: str,
              pdf_url: str, pages: int, chars: int, status: str,
              error: str | None = None) -> None:
    conn.execute(
        """INSERT INTO fetch_log(document_number, source_url, pdf_url, pages,
           chars, status, error, fetched_at) VALUES(?,?,?,?,?,?,?,?)""",
        (number, source_url, pdf_url, pages, chars, status, error,
         datetime.now(timezone.utc).isoformat()))
    conn.commit()


# --------------------------------------------------------------------------
# Hiệu chỉnh thủ công sau kiểm toán (giai đoạn 2, 09/11/2026)
# --------------------------------------------------------------------------
# Những lỗi mà collector tự động không sửa được, vì nguồn chính thức cũng
# sai (32/2025) hoặc vì PDF là bản scan không có lớp text (204/2025).
# apply_manual_corrections() áp dụng chúng một cách có ghi chú nguồn gốc
# (fetch_log status 'corrected'); chạy cuối mỗi lần --all và qua cờ
# --apply-manual. Hàm này idempotent — chạy lại không đổi gì thêm.

EFFECTIVE_DATE_OVERRIDES = {
    # Công báo ghi sai metadata của chính nó: header + tab Thuộc tính đều
    # ghi "Ban hành: 31/05/2025 – Hiệu lực: 01/06/2021" (trước cả ngày ban
    # hành). VBPL xác nhận 01/06/2025: header "Ngày có hiệu lực:
    # 01/06/2025" và Điều 12.1 "có hiệu lực thi hành kể từ ngày 01 tháng
    # 6 năm 2025".
    "32/2025/TT-BTC": "2025-06-01",
}

# Source URL cho --local-pdfs khi hàng CSV của văn bản không có Công báo
# detail URL. 4 văn bản này không có trong danh mục Công báo (chỉ có file
# trên VBPL gateway, đã tải bằng trình duyệt 09/11/2026, .doc convert sang
# PDF bằng LibreOffice) và VBPL cũng không có toàn văn HTML đầy đủ cho
# chúng (234/2003 chỉ có 1.982 ký tự phần quyết định, 12/2005 chỉ có bản
# tiếng Anh ngắn, 100/2005 và 15/2006 không có body HTML). Trang chi tiết
# VBPL là nguồn gốc (provenance) — cùng quy ước với vbpl_html_ingest.py
# (source_url = URL trang VBPL). File PDF đã convert nằm ở
# collector/local-pdfs/ trong repo; workflow chạy --local-pdfs trên đó.
LOCAL_PDF_SOURCE_URLS = {
    "234/2003/QĐ-BTC": "https://vbpl.vn/van-ban/chi-tiet/quyet-dinh-so-234-2003-qd-btc-cua-bo-truong-bo-tai-chinh-ve-viec-ban-hanh-va-cong-bo-sau-06-chuan-muc-ke-toan-viet-nam-dot-3--17841",
    "12/2005/QĐ-BTC": "https://vbpl.vn/van-ban/chi-tiet/quyet-dinh-so-12-2005-qd-btc-ve-viec-ban-hanh-va-cong-bo-sau-06-chuan-muc-ke-toan-viet-nam-dot-4--18466",
    "100/2005/QĐ-BTC": "https://vbpl.vn/van-ban/chi-tiet/quyet-dinh-so-100-2005-qd-btc-ve-viec-ban-hanh-va-cong-bo-bon-04-chuan-muc-ke-toan-viet-nam-dot-5--16846",
    "15/2006/QĐ-BTC": "https://vbpl.vn/van-ban/chi-tiet/quyet-dinh-so-15-2006-qd-btc-ve-viec-ban-hanh-che-do-ke-toan-doanh-nghiep--16280",
}

MANUAL_TEXTS = {
    # Nghị quyết 204/2025/QH15: PDF Công báo (849+850, 01/07/2025) là bản
    # scan 1 trang, không có lớp text — PyMuPDF chỉ trích được 306 ký tự
    # (masthead + chữ ký). Toàn văn dưới đây được đọc trực tiếp từ ảnh
    # trang scan của Công báo và đối chiếu với một lần đọc độc lập khác
    # của cùng file PDF (nội dung 2 Điều khớp nhau).
    "204/2025/QH15": {
        "provenance": (
            "Manual transcription from the official Cong bao PDF image "
            "(Cong bao 849+850, 01/07/2025): the PDF is a scan with no "
            "text layer (PyMuPDF extracts 306 chars: masthead + signature "
            "only). Cross-checked against an independent reading of the "
            "same PDF."),
        "text": (
            "NGHỊ QUYẾT\n"
            "Về giảm thuế giá trị gia tăng\n"
            "\n"
            "QUỐC HỘI\n"
            "\n"
            "Căn cứ Hiến pháp nước Cộng hòa xã hội chủ nghĩa Việt Nam đã được sửa đổi, bổ sung một số điều theo Nghị quyết số 203/2025/QH15;\n"
            "Căn cứ Luật Tổ chức Quốc hội số 57/2014/QH13 đã được sửa đổi, bổ sung một số điều theo Luật số 65/2020/QH14 và Luật số 62/2025/QH15;\n"
            "Căn cứ Luật Thuế giá trị gia tăng số 48/2024/QH15;\n"
            "Căn cứ Luật Ban hành văn bản quy phạm pháp luật số 64/2025/QH15;\n"
            "\n"
            "QUYẾT NGHỊ:\n"
            "\n"
            "Điều 1. Phạm vi áp dụng\n"
            "1. Giảm 2% thuế suất thuế giá trị gia tăng, áp dụng đối với các nhóm hàng hóa, dịch vụ quy định tại khoản 3 Điều 9 của Luật Thuế giá trị gia tăng số 48/2024/QH15 (còn 8%), trừ một số nhóm hàng hóa, dịch vụ sau: viễn thông, hoạt động tài chính, ngân hàng, chứng khoán, bảo hiểm, kinh doanh bất động sản, sản phẩm kim loại, sản phẩm khai khoáng (trừ than), sản phẩm hàng hóa và dịch vụ chịu thuế tiêu thụ đặc biệt (trừ xăng).\n"
            "2. Chính phủ quy định chi tiết khoản 1 Điều này.\n"
            "\n"
            "Điều 2. Hiệu lực thi hành\n"
            "Nghị quyết này có hiệu lực thi hành từ ngày 01 tháng 7 năm 2025 đến hết ngày 31 tháng 12 năm 2026.\n"
            "\n"
            "Nghị quyết này được Quốc hội nước Cộng hòa xã hội chủ nghĩa Việt Nam khóa XV, kỳ họp thứ 9 thông qua ngày 17 tháng 6 năm 2025.\n"
            "CHỦ TỊCH QUỐC HỘI\n"
            "Trần Thanh Mẫn"
        ),
        "provisions": [
            {
                "dieu": 1, "khoan": 1, "diem": None,
                "noi_dung": "Giảm 2% thuế suất thuế giá trị gia tăng, áp dụng đối với các nhóm hàng hóa, dịch vụ quy định tại khoản 3 Điều 9 của Luật Thuế giá trị gia tăng số 48/2024/QH15 (còn 8%), trừ một số nhóm hàng hóa, dịch vụ sau: viễn thông, hoạt động tài chính, ngân hàng, chứng khoán, bảo hiểm, kinh doanh bất động sản, sản phẩm kim loại, sản phẩm khai khoáng (trừ than), sản phẩm hàng hóa và dịch vụ chịu thuế tiêu thụ đặc biệt (trừ xăng).",
            },
            {
                "dieu": 1, "khoan": 2, "diem": None,
                "noi_dung": "Chính phủ quy định chi tiết khoản 1 Điều này.",
            },
            {
                "dieu": 2, "khoan": None, "diem": None,
                "noi_dung": "Nghị quyết này có hiệu lực thi hành từ ngày 01 tháng 7 năm 2025 đến hết ngày 31 tháng 12 năm 2026.",
            },
        ],
    },
}


def apply_manual_corrections(conn: sqlite3.Connection) -> list[str]:
    """Apply EFFECTIVE_DATE_OVERRIDES + MANUAL_TEXTS to the DB.

    Idempotent: a correction already in place is skipped (no duplicate
    fetch_log rows). Every applied correction is recorded in fetch_log
    with status 'corrected' and its provenance. Returns log lines.
    """
    logs: list[str] = []
    for number, eff in EFFECTIVE_DATE_OVERRIDES.items():
        row = conn.execute(
            "SELECT id, effective_date FROM documents WHERE number = ?",
            (number,)).fetchone()
        if not row:
            logs.append(f"[{number}] override skipped: not in DB")
            continue
        if row[1] == eff:
            logs.append(f"[{number}] effective_date already {eff}")
            continue
        conn.execute("UPDATE documents SET effective_date = ? WHERE id = ?",
                     (eff, row[0]))
        log_fetch(conn, number, "", None, 0, 0, "corrected",
                  f"effective_date {row[1]} -> {eff} "
                  f"(manual override, audit phase 2, 09/11/2026)")
        logs.append(f"[{number}] effective_date {row[1]} -> {eff}")
    for number, manual in MANUAL_TEXTS.items():
        row = conn.execute(
            "SELECT id, source_url, sha256 FROM documents WHERE number = ?",
            (number,)).fetchone()
        if not row:
            logs.append(f"[{number}] manual text skipped: not in DB")
            continue
        text = manual["text"]
        sha = hashlib.sha256(text.encode()).hexdigest()
        if row[2] == sha:
            logs.append(f"[{number}] manual text already applied")
            continue
        conn.execute(
            "UPDATE documents SET extracted_text = ?, sha256 = ? WHERE id = ?",
            (text[:300000], sha, row[0]))
        # Keep the dieu=0 masthead row (other docs keep theirs too);
        # replace every real provision with the transcribed ones.
        # (dieu=0 is NOT NULL in SQL — the condition must be dieu > 0.)
        conn.execute(
            "DELETE FROM provisions WHERE document_number = ? "
            "AND dieu > 0", (number,))
        provs = [dict(p, document_number=number, chuong=None, muc=None,
                      confidence="high") for p in manual["provisions"]]
        n = store_provisions(conn, provs)
        log_fetch(conn, number, row[1], None, 0, len(text), "corrected",
                  manual["provenance"])
        logs.append(f"[{number}] manual text applied: {len(text)} chars, "
                    f"{n} provisions")
    conn.commit()
    return logs


# --------------------------------------------------------------------------
# Ingestion pipeline
# --------------------------------------------------------------------------
def ingest_detail(collector: Collector, conn: sqlite3.Connection,
                  source_url: str, category: str,
                  expected_number: str = "", force: bool = False) -> dict:
    """Ingest one document from its Cong bao detail page.

    Returns a result dict (always returned, never raises for per-doc
    failures — the error is recorded in the result and in fetch_log).
    """
    result = {"source_url": source_url, "status": "failed", "error": None,
              "number": expected_number, "pdfs": 0, "pages": 0,
              "chars": 0, "provisions": 0, "dieus": 0, "action": None}
    try:
        resp = collector.get(source_url)
        meta = parse_detail_page(resp.text, source_url)
        result["number"] = meta["number"] or expected_number
        if not meta["pdf_links"] and not meta["pdf_links_fallback"]:
            result["error"] = "No PDF links found on detail page"
            log_fetch(conn, result["number"], source_url, "", 0, 0,
                      "no_pdf_links", result["error"])
            return result
        pdf_jobs = meta["pdf_links"] or meta["pdf_links_fallback"]
        result["pdf_urls"] = [u for _, u in pdf_jobs]
        if collector.dry_run:
            result.update(status="dry_run", pdfs=len(pdf_jobs),
                          pdf_files=[label for label, _ in pdf_jobs])
            return result
        full_text, total_pages, total_scanned = "", 0, 0
        for idx, (label, pdf_url) in enumerate(pdf_jobs):
            try:
                pdf_bytes = collector.get_bytes(pdf_url, referer=source_url)
            except RuntimeError:
                # Per-file fallback: the signed g7 download link for the
                # same issue (same position in the fallback list).
                fb = meta["pdf_links_fallback"]
                if meta["pdf_links"] and idx < len(fb):
                    pdf_url = fb[idx][1]
                    pdf_bytes = collector.get_bytes(pdf_url, referer=source_url)
                else:
                    raise
            text, pages, scanned = extract_pdf_text(pdf_bytes)
            total_pages += pages
            total_scanned += scanned
            full_text += ("\n" if full_text else "") + text
            log_fetch(conn, result["number"], source_url, pdf_url, pages,
                      len(text), "ok")
        result.update(pdfs=len(pdf_jobs), pages=total_pages,
                      chars=len(full_text))
        if total_scanned and total_scanned == total_pages:
            result["error"] = "All PDF pages are scanned images (no text layer)"
            result["status"] = "scanned"
            log_fetch(conn, result["number"], source_url, "", total_pages,
                      len(full_text), "scanned", result["error"])
            return result
        provisions = parse_provisions(full_text, result["number"])
        dieus = sorted({p["dieu"] for p in provisions if p["dieu"]})
        action, _ = store_document(conn, meta, full_text, category, force)
        # Always replace the provision set wholesale. INSERT OR REPLACE
        # cannot dedupe rows whose khoan/diem are NULL (SQLite treats
        # NULLs as distinct in UNIQUE constraints), so a plain re-store
        # would silently duplicate rows on every re-run.
        conn.execute("DELETE FROM provisions WHERE document_number = ?",
                     (result["number"],))
        conn.commit()
        n_prov = store_provisions(conn, provisions)
        result.update(status="ok", action=action, provisions=n_prov,
                      dieus=len(dieus), sample=full_text[:300])
        return result
    except Exception as exc:  # per-doc failure must not kill the run
        result["error"] = f"{type(exc).__name__}: {exc}"
        try:
            log_fetch(conn, result["number"], source_url, "", 0, 0,
                      "failed", result["error"])
        except Exception:
            pass
        return result


# --------------------------------------------------------------------------
# Local-PDF ingest (PDFs downloaded by another route)
# --------------------------------------------------------------------------
def _tokens(s: str) -> list[str]:
    """Alphanumeric tokens of a string, with Vietnamese Đ folded to D.

    Document numbers use Đ (QĐ, NĐ) but downloaded filenames almost
    always use ASCII D (``149-2001-QD-BTC.pdf``), so the fold is what
    makes those two spellings match.
    """
    s = (s or "").upper().replace("Đ", "D")
    return re.findall(r"[A-Za-z0-9]+", s)


def _subsequence(needle: list[str], haystack: list[str]) -> bool:
    """True if needle appears as a contiguous run inside haystack."""
    n = len(needle)
    if n == 0 or n > len(haystack):
        return False
    return any(haystack[i:i + n] == needle for i in range(len(haystack) - n + 1))


def build_local_lookup(missing_csv: Path, target_csv: Path) -> dict[str, dict]:
    """Index documents by normalised number for local-PDF matching.

    Merges two sources: the 100-document target list (title, category,
    issue/effective dates) and the 56-missing list (the Cong bao detail
    URL found by the listing scan). The detail URL becomes the stored
    source_url, so a row without one cannot be ingested locally and is
    reported as unmatched instead of invented.
    """
    lookup: dict[str, dict] = {}
    if target_csv.exists():
        for row in load_targets(target_csv):
            norm = normalize_number(row.get("so_hieu", ""))
            if not norm:
                continue
            lookup[norm] = {
                "number": row["so_hieu"].strip(),
                "title": (row.get("ten_van_ban") or "").strip(),
                "category": (row.get("nhom") or "").strip(),
                "detail_url": "",
                "issued_date": (row.get("ngay_ban_hanh") or "").strip() or None,
                "effective_date": (row.get("ngay_hieu_luc") or "").strip() or None,
            }
    if missing_csv.exists():
        with open(missing_csv, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                norm = normalize_number(row.get("so_hieu", ""))
                if not norm:
                    continue
                entry = lookup.setdefault(norm, {
                    "number": row["so_hieu"].strip(),
                    "title": "",
                    "category": (row.get("loai_nhom") or "").strip(),
                    "detail_url": "",
                    "issued_date": None,
                    "effective_date": None,
                })
                if (row.get("detail_url") or "").strip():
                    entry["detail_url"] = row["detail_url"].strip()
                if not entry["category"] and (row.get("loai_nhom") or "").strip():
                    entry["category"] = row["loai_nhom"].strip()
    return lookup


def match_pdf_filename(filename: str,
                       lookup: dict[str, dict]) -> tuple[dict | None, str]:
    """Match a PDF filename to a lookup row by document-number tokens.

    Accepted names: ``88-2015-QH13.pdf`` (slashes replaced by - or _),
    the original g7 ``file_name`` (e.g. ``88_2015_QH13(13010).pdf`` or
    ``2015_279 + 280-200_2014_TT-BTC.pdf``), and multi-part names such
    as ``200-2014-TT-BTC-part1.pdf``. The document number's tokens must
    appear as one contiguous run inside the filename's tokens. Returns
    (row, how) or (None, reason) — ambiguous or missing matches are
    never guessed.
    """
    file_tokens = _tokens(Path(filename).stem)
    exact, subseq = [], []
    for row in lookup.values():
        doc_tokens = _tokens(row["number"])
        if doc_tokens == file_tokens:
            exact.append(row)
        elif _subsequence(doc_tokens, file_tokens):
            subseq.append(row)
    if len(exact) == 1:
        return exact[0], "exact filename"
    if len(exact) > 1:
        return None, f"ambiguous filename (matches {len(exact)} documents)"
    if len(subseq) == 1:
        return subseq[0], "number tokens found in filename"
    if len(subseq) > 1:
        names = ", ".join(r["number"] for r in subseq)
        return None, f"ambiguous filename (matches {names})"
    return None, "no document number found in filename"


def ingest_local_pdfs(conn: sqlite3.Connection, pdf_paths: list[Path],
                      lookup: dict[str, dict], force: bool = False,
                      dry_run: bool = False) -> list[dict]:
    """Ingest local PDF files, grouped by matched document.

    Files mapping to the same document (multi-issue docs like TT200,
    whose g7 download is split across several PDFs) are concatenated
    in filename order — which is gazette order for the g7 naming —
    then parsed once with the same parser as the download path
    (same provision split, same 300k extracted_text cap, same
    provisions schema with the ordinal key). Idempotent: the store
    dedupes on source_url + sha256 exactly like the download path.
    Returns one result dict per document plus one per unmatched file.
    """
    grouped: dict[str, tuple[dict, list[Path]]] = {}
    results: list[dict] = []
    for path in sorted(pdf_paths, key=lambda p: p.name):
        if path.suffix.lower() != ".pdf":
            results.append({"file": path.name, "status": "skipped",
                            "error": "not a .pdf file"})
            continue
        row, how = match_pdf_filename(path.name, lookup)
        if row is None:
            results.append({"file": path.name, "status": "unmatched",
                            "error": how})
            continue
        grouped.setdefault(row["number"], (row, []))[1].append(path)

    for number, (row, paths) in sorted(grouped.items()):
        res: dict = {"number": number, "status": "failed", "error": None,
                     "files": [p.name for p in paths], "pages": 0,
                     "chars": 0, "provisions": 0, "dieus": 0, "action": None}
        if not row["title"]:
            res["error"] = ("matched CSV row has no title — "
                            "refusing to invent one")
            results.append(res)
            continue
        # Source URL: Công báo detail URL nếu có; nếu không, dùng trang
        # chi tiết VBPL từ LOCAL_PDF_SOURCE_URLS (văn bản chỉ có trên
        # VBPL gateway). Không có cả hai thì từ chối lưu.
        source_url = row["detail_url"] or LOCAL_PDF_SOURCE_URLS.get(number, "")
        if not source_url:
            res["error"] = ("matched CSV row has no Cong bao detail URL "
                            "and no LOCAL_PDF_SOURCE_URLS entry "
                            "(source_url) — cannot store")
            results.append(res)
            continue
        if not row["category"]:
            res["error"] = "matched CSV row has no category"
            results.append(res)
            continue
        try:
            full_text, total_pages, total_scanned = "", 0, 0
            for p in paths:
                pdf_bytes = p.read_bytes()
                if len(pdf_bytes) > MAX_PDF_BYTES:
                    raise RuntimeError(
                        f"PDF too large ({len(pdf_bytes)} bytes): {p.name}")
                text, pages, scanned = extract_pdf_text(pdf_bytes)
                total_pages += pages
                total_scanned += scanned
                full_text += ("\n" if full_text else "") + text
                if not dry_run:
                    log_fetch(conn, number, source_url,
                              f"local:{p.name}", pages, len(text), "ok")
            res.update(pages=total_pages, chars=len(full_text))
            if total_scanned and total_scanned == total_pages:
                res.update(
                    status="scanned",
                    error="All PDF pages are scanned images (no text layer)")
                results.append(res)
                continue
            provisions = parse_provisions(full_text, number)
            dieus = sorted({p["dieu"] for p in provisions if p["dieu"]})
            if dry_run:
                res.update(status="dry_run", provisions=len(provisions),
                           dieus=len(dieus))
                results.append(res)
                continue
            meta = {"number": number, "title": row["title"],
                    "source_url": source_url,
                    "issued_date": row["issued_date"],
                    "effective_date": row["effective_date"]}
            action, _ = store_document(conn, meta, full_text,
                                       row["category"], force)
            # Always replace the provision set wholesale, mirroring
            # ingest_detail (INSERT OR REPLACE cannot dedupe NULL
            # khoan/diem rows, so a plain re-store would duplicate).
            conn.execute("DELETE FROM provisions WHERE document_number = ?",
                         (number,))
            conn.commit()
            n_prov = store_provisions(conn, provisions)
            res.update(status="ok", action=action, provisions=n_prov,
                       dieus=len(dieus), sample=full_text[:300])
            results.append(res)
        except Exception as exc:  # per-doc failure must not kill the run
            res["error"] = f"{type(exc).__name__}: {exc}"
            try:
                if not dry_run:
                    log_fetch(conn, number, source_url, "", 0, 0,
                              "failed", res["error"])
            except Exception:
                pass
            results.append(res)
    return results


def load_targets(csv_path: Path) -> list[dict]:
    with open(csv_path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def main(argv: list[str] | None = None) -> int:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Cong bao collector for KeToanAI")
    parser.add_argument("--db", default=str(here / "documents.db"))
    parser.add_argument("--target-csv", default=str(
        here.parent / "danh-sach-100-van-ban" / "danh-sach-100-van-ban.csv"))
    parser.add_argument("--only", default="",
                        help="Comma-separated document numbers to ingest")
    parser.add_argument("--limit", type=int, default=0,
                        help="Discover newest N Bo Tai chinh docs and ingest them")
    parser.add_argument("--agency", type=int, default=10,
                        help="Agency id for --limit discovery (10 = Bo Tai chinh)")
    parser.add_argument("--max-pages", type=int, default=10,
                        help="Max listing pages to scan when resolving/discovering")
    parser.add_argument("--max-listing-pages", type=int, default=600,
                        help="Max listing pages per agency in --all mode")
    parser.add_argument("--all", action="store_true",
                        help="Ingest every target-list row not already in the DB "
                             "(resolves non-Cong-bao source URLs via the agency "
                             "listing, cached per agency)")
    parser.add_argument("--local-pdfs", default="",
                        help="Directory of PDFs downloaded by another route "
                             "(e.g. a browser that can reach g7.cdnchinhphu.vn); "
                             "ingest them without re-downloading")
    parser.add_argument("--pdf", default="",
                        help="One local PDF file to ingest (same filename "
                             "matching rules as --local-pdfs)")
    parser.add_argument("--missing-csv", default=str(
        here / "danh-sach-56-van-ban-thieu.csv"),
                        help="CSV with Cong bao detail URLs for the missing "
                             "docs (default: danh-sach-56-van-ban-thieu.csv)")
    parser.add_argument("--results-csv", default="",
                        help="Append per-document results here as they finish "
                             "(default: results-95.csv next to this script, "
                             "only in --all mode)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Discover + parse detail pages only; no PDF downloads, no DB writes")
    parser.add_argument("--force", action="store_true",
                        help="Re-parse and re-store even when the text sha256 is unchanged (parser fixes)")
    parser.add_argument("--apply-manual", action="store_true",
                        help="Apply post-audit manual corrections only "
                             "(effective-date overrides, hand-transcribed "
                             "texts) to --db, then exit")
    args = parser.parse_args(argv)

    collector = Collector(Path(args.db), dry_run=args.dry_run)
    conn = None if args.dry_run else init_db(Path(args.db))
    if conn is None:
        # Dry-run still needs a connection object for the log calls; use
        # an in-memory DB that is discarded.
        conn = sqlite3.connect(":memory:")
        conn.executescript(SCHEMA)

    if args.apply_manual:
        for line in apply_manual_corrections(conn):
            print(line, flush=True)
        collector.close()
        return 0

    targets = load_targets(Path(args.target_csv))
    jobs: list[tuple[str, str, str, str]] = []  # (number, source_url, category, loai)
    results_path: Path | None = None
    results_writer = None
    results_file = None
    listing_items_cache: dict[int, list[dict]] = {}

    def _is_congbao_detail(url: str) -> bool:
        try:
            p = urlparse(url)
        except Exception:
            return False
        return (p.hostname or "").lower() == "congbao.chinhphu.vn" \
            and "/van-ban/" in p.path \
            and not re.search(r"/van-ban-dang-cong-bao/trang-\d+\.htm", url)

    def _scan_agency_items(agency_id: int) -> list[dict]:
        """Scan one agency's listing once per run; cache the items."""
        if agency_id not in listing_items_cache:
            group = [r for r in targets
                     if AGENCY_IDS.get((r["co_quan_ban_hanh"] or "").strip().upper(), 0) == agency_id
                     and normalize_number(r["so_hieu"]) not in ingested_norms]
            dates = [r["ngay_ban_hanh"] for r in group if r.get("ngay_ban_hanh")]
            oldest = min(dates) if dates else "2026-01-01"
            # Cutoff = oldest issue date minus 180 days of gazette lag.
            y, m, d = (int(x) for x in oldest.split("-"))
            from datetime import date, timedelta
            cutoff = (date(y, m, d) - timedelta(days=180)).isoformat()
            print(f"[listing] scanning agency {agency_id} for {len(group)} doc(s), "
                  f"cutoff {cutoff} ...", flush=True)
            items, pages = scan_agency_items(collector, agency_id, cutoff,
                                             max_pages=args.max_listing_pages)
            print(f"[listing] agency {agency_id}: {len(items)} items, "
                  f"{pages} pages scanned", flush=True)
            listing_items_cache[agency_id] = items
        return listing_items_cache[agency_id]

    def _resolve_row(row: dict) -> tuple[str | None, str, str]:
        """Return (detail_url, how, congbao_number) for one target row.

        'direct' = CSV source is already a Cong bao detail page;
        'listing' = exact number match in the agency listing scan;
        'fuzzy' = same issue date + title match, but the Cong bao number
        differs from the CSV number (recorded, never relabelled);
        'unresolved' = not found in the listing.
        """
        src = (row["nguon_chinh_thuc"] or "").strip()
        if _is_congbao_detail(src):
            return src, "direct", ""
        agency_id = AGENCY_IDS.get((row["co_quan_ban_hanh"] or "").strip().upper(), 0)
        if not agency_id:
            return None, "unresolved (unknown agency)", ""
        items = _scan_agency_items(agency_id)
        want = normalize_number(row["so_hieu"])
        for it in items:
            if it["number_norm"] == want:
                return it["url"], "listing", it["ky_hieu"]
        hit = fuzzy_match(row, items)
        if hit:
            return hit["url"], \
                f"fuzzy (CSV {row['so_hieu']} vs Congbao {hit['ky_hieu']})", \
                hit["ky_hieu"]
        return None, "unresolved (not in listing)", ""

    ingested_norms: set[str] = set()
    if conn is not None:
        try:
            ingested_norms = {normalize_number(r[0]) for r in
                              conn.execute("SELECT number FROM documents").fetchall()}
        except sqlite3.Error:
            ingested_norms = set()

    if args.local_pdfs or args.pdf:
        lookup = build_local_lookup(Path(args.missing_csv),
                                    Path(args.target_csv))
        if args.pdf:
            pdf_path = Path(args.pdf)
            if not pdf_path.is_file():
                print(f"ERROR: no such file: {pdf_path}", file=sys.stderr)
                collector.close()
                return 2
            pdf_paths = [pdf_path]
        else:
            pdf_dir = Path(args.local_pdfs)
            if not pdf_dir.is_dir():
                print(f"ERROR: not a directory: {pdf_dir}", file=sys.stderr)
                collector.close()
                return 2
            pdf_paths = sorted(pdf_dir.iterdir(), key=lambda p: p.name)
        print(f"{len(pdf_paths)} file(s) in scope; {len(lookup)} document(s) "
              f"in lookup (dry_run={args.dry_run})", flush=True)
        results = ingest_local_pdfs(conn, pdf_paths, lookup,
                                    force=args.force, dry_run=args.dry_run)
        for r in results:
            if r["status"] in ("ok", "dry_run"):
                print(f"[{r['number']}] -> {r['status']} "
                      f"files={len(r.get('files', []))} pages={r.get('pages')} "
                      f"chars={r.get('chars')} provisions={r.get('provisions')} "
                      f"dieus={r.get('dieus')} action={r.get('action')}",
                      flush=True)
            else:
                print(f"[{r.get('number') or r.get('file')}] -> "
                      f"{r['status']}: {r.get('error')}", flush=True)
        relevant = [r for r in results if r["status"] != "skipped"]
        ok = sum(1 for r in relevant if r["status"] in ("ok", "dry_run"))
        total_docs = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        total_prov = conn.execute("SELECT COUNT(*) FROM provisions").fetchone()[0]
        print(f"\nDone (local PDFs): {ok}/{len(relevant)} ok; DB now has "
              f"{total_docs} documents, {total_prov} provisions")
        collector.close()
        return 0 if ok == len(relevant) else 1

    if args.all:
        if args.results_csv:
            results_path = Path(args.results_csv)
        else:
            results_path = here / "results-95.csv"
        results_file = open(results_path, "w", encoding="utf-8", newline="")
        results_writer = csv.DictWriter(results_file, fieldnames=[
            "stt", "so_hieu", "nhom", "detail_url", "resolved_via",
            "congbao_number", "pdf_urls", "pages", "chars", "provisions",
            "dieus", "status", "error"])
        results_writer.writeheader()
        results_file.flush()
        for row in targets:
            if normalize_number(row["so_hieu"]) in ingested_norms:
                print(f"[{row['so_hieu']}] already in DB, skipped", flush=True)
                continue
            detail, how, cb_number = _resolve_row(row)
            if not detail:
                res = {"stt": row["stt"], "so_hieu": row["so_hieu"],
                       "nhom": row["nhom"], "detail_url": "",
                       "resolved_via": how, "congbao_number": "",
                       "pdf_urls": "", "pages": "",
                       "chars": "", "provisions": "", "dieus": "",
                       "status": "failed", "error": how}
                results_writer.writerow(res)
                results_file.flush()
                print(f"[{row['so_hieu']}] -> failed ({how})", flush=True)
                continue
            print(f"[{row['so_hieu']}] ingesting {detail} ...", flush=True)
            r = ingest_detail(collector, conn, detail, row["nhom"],
                              row["so_hieu"], force=args.force)
            # If the detail page itself parses to a different number than
            # the CSV row, keep the detail-page number in the DB and
            # record the discrepancy instead of relabelling anything.
            stored_number = r.get("number") or ""
            if stored_number and normalize_number(stored_number) != normalize_number(row["so_hieu"]):
                cb_number = stored_number
                how = how + f" | detail-page number {stored_number} differs from CSV {row['so_hieu']}"
            res = {"stt": row["stt"], "so_hieu": row["so_hieu"],
                   "nhom": row["nhom"], "detail_url": detail,
                   "resolved_via": how, "congbao_number": cb_number,
                   "pdf_urls": "; ".join(r.get("pdf_urls", [])),
                   "pages": r.get("pages", ""), "chars": r.get("chars", ""),
                   "provisions": r.get("provisions", ""),
                   "dieus": r.get("dieus", ""), "status": r["status"],
                   "error": r.get("error") or ""}
            results_writer.writerow(res)
            results_file.flush()
            print(f"[{row['so_hieu']}] -> {r['status']}"
                  + (f" pdfs={r.get('pdfs')} pages={r.get('pages')} "
                     f"chars={r.get('chars')} provisions={r.get('provisions')} "
                     f"dieus={r.get('dieus')} action={r.get('action')}"
                     if r["status"] == "ok" else f" error={r.get('error')}"),
                  flush=True)
        results_file.close()
        # Post-audit manual corrections (effective-date overrides,
        # hand-transcribed scanned texts) — applied last so a fresh
        # collection never silently reintroduces a fixed error.
        for line in apply_manual_corrections(conn):
            print(line, flush=True)
        # Recompute totals from the DB for the final summary.
        total_docs = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        total_prov = conn.execute("SELECT COUNT(*) FROM provisions").fetchone()[0]
        print(f"\nDone (all mode): DB now has {total_docs} documents, "
              f"{total_prov} provisions; pages fetched={collector.stats['fetched_pages']}, "
              f"pdfs={collector.stats['fetched_pdfs']}, "
              f"retries={collector.stats['retries']}")
        print(f"Per-document results: {results_path}")
        collector.close()
        return 0

    if args.only:
        wanted = {n.strip().upper() for n in args.only.split(",") if n.strip()}
        for row in targets:
            if row["so_hieu"].strip().upper() in wanted:
                src = (row["nguon_chinh_thuc"] or "").strip()
                if not _is_congbao_detail(src):
                    # CSV source is a mirror/index page, not a Cong bao
                    # detail page: resolve the real detail page via the
                    # agency listing, exactly as --all does.
                    detail, how, _cb = _resolve_row(row)
                    if not detail:
                        print(f"WARNING: {row['so_hieu']} could not be "
                              f"resolved ({how}), skipped", file=sys.stderr)
                        continue
                    print(f"[{row['so_hieu']}] resolved detail page: "
                          f"{detail} ({how})", flush=True)
                    src = detail
                jobs.append((row["so_hieu"], src,
                             row["nhom"], row.get("loai", "")))
        missing = wanted - {j[0].strip().upper() for j in jobs}
        for m in missing:
            print(f"WARNING: {m} not in target list, skipped", file=sys.stderr)
    elif args.limit:
        print(f"Discovering newest {args.limit} documents (agency {args.agency}) ...")
        docs = search_listing(collector, args.agency, 0, args.max_pages)
        for doc in docs[: args.limit]:
            jobs.append((doc["number"], doc["url"], "Khám phá", ""))
    else:
        # Default: the verified documents only.
        for row in targets:
            if row["trang_thai_kiem_chung"] == "đã kiểm chứng":
                jobs.append((row["so_hieu"], row["nguon_chinh_thuc"],
                             row["nhom"], row.get("loai", "")))

    print(f"{len(jobs)} document(s) queued (dry_run={args.dry_run})")
    results = []
    for number, source_url, category, loai in jobs:
        if not source_url:
            results.append({"number": number, "status": "failed",
                            "error": "No source URL in target list",
                            "source_url": source_url})
            continue
        # Listing/index URLs (not a detail page) must be resolved first.
        if re.search(r"/van-ban-dang-cong-bao/trang-\d+\.htm", source_url):
            print(f"[{number}] source is an index page; resolving via listing ...")
            detail = resolve_detail_url(collector, number,
                                        _agency_name(category, loai),
                                        loai, args.max_pages)
            if not detail:
                results.append({"number": number, "status": "failed",
                                "error": "Could not resolve detail page from listing",
                                "source_url": source_url})
                continue
            print(f"[{number}] resolved detail page: {detail}")
            source_url = detail
        print(f"[{number}] ingesting {source_url} ...")
        res = ingest_detail(collector, conn, source_url, category, number,
                            force=args.force)
        res["category"] = category
        results.append(res)
        print(f"[{number}] -> {res['status']}"
              + (f" pdfs={res.get('pdfs')} pages={res.get('pages')} "
                 f"chars={res.get('chars')} provisions={res.get('provisions')} "
                 f"dieus={res.get('dieus')} action={res.get('action')}"
                 if res["status"] == "ok" else f" error={res.get('error')}"))

    ok = sum(1 for r in results if r["status"] in ("ok", "dry_run"))
    print(f"\nDone: {ok}/{len(results)} ok; "
          f"pages fetched={collector.stats['fetched_pages']}, "
          f"pdfs={collector.stats['fetched_pdfs']}, "
          f"retries={collector.stats['retries']}")
    collector.close()
    return 0 if ok == len(results) else 1


def _agency_name(category: str, loai: str) -> str:
    """Best-effort agency guess for listing resolution.

    The target list has no agency column; tax/accounting laws in the list
    are issued by Quoc hoi (Luat) or Bo Tai chinh (Thong tu / Nghi dinh).
    """
    if (loai or "").strip().upper().startswith("LUẬT"):
        return "QUỐC HỘI"
    return "BỘ TÀI CHÍNH"


if __name__ == "__main__":
    raise SystemExit(main())
