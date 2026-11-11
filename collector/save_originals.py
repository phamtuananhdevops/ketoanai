#!/usr/bin/env python3
"""Save the original PDFs/HTML for all 83 collected KeToanAI documents.

Deliverable 4 of the KeToanAI Postgres architecture. Reads ONLY from the
existing SQLite collector database (never writes to it) and re-fetches each
document's original file from the official source, storing it under::

    originals/<source>/<number-sanitized>/<file>

where <source> is ``congbao`` or ``vbpl`` and <number-sanitized> is the
document number with ``/`` -> ``-`` and ``Đ`` -> ``D``
(e.g. ``originals/congbao/88-2015-QH13/12961-1-882015qh1313010.pdf``).

Provenance (read from the DB, never invented):
  * 79 documents come from Cong bao (congbao.chinhphu.vn). Their PDF URLs
    are taken from ``fetch_log`` (status='ok', distinct pdf_url, in
    first-seen order) — the URLs the original collector run actually
    downloaded. Only if fetch_log has no PDF URL for a document is the
    detail page re-fetched and re-parsed for PDF links (same
    ``parse_detail_page`` logic as congbao_collector.py: prefer direct
    congbaocdn.chinhphu.vn links, fall back to signed g7.cdnchinhphu.vn
    links). A document spanning several gazette issues has several PDFs;
    every one is saved, one manifest row per file.
  * 4 documents come from VBPL rendered HTML (13/2008/QH12,
    124/2008/NĐ-CP, 48/2006/QĐ-BTC, 51/2010/NĐ-CP). There is NO PDF for
    these — the detail-page HTML itself is the original and is saved as
    ``<number-sanitized>.html``. (48/2006 is PARTIAL on VBPL: the rendered
    text is only the 6-Điều Quyết định; the 5-part regime itself lives in
    a .rar — that caveat is copied from fetch_log into the manifest note.)

File naming: the local filename is the basename of the PDF URL
(URL-decoded). Older CongBaoCP files have a dotless ``...pdf`` ending
(e.g. ``12961-1-882015qh1313010pdf``); ``.pdf`` is appended in that case
so the saved file keeps a real extension. (The ``88_2015_QH13(13010).pdf``
style name seen in old fetch_log error rows is the g7 ``file_name``
parameter, not the congbaocdn basename, so it is NOT used here — the
saved name always matches the URL it was downloaded from.)

Verification: every downloaded PDF must start with the ``%PDF`` header;
its sha256 is recorded in the manifest. NOTE: the ``sha256`` column in
the documents table is the hash of the *extracted text*, NOT of the
file — the two must never be confused; the manifest's ``file_sha256``
is the hash of the saved file bytes.

Politeness / policy (same as the original collector):
  * max 1 request/second, User-Agent ``KeToanAI-Research-Collector/0.1``;
  * only official domains: congbao.chinhphu.vn, congbaocdn.chinhphu.vn,
    g7.cdnchinhphu.vn, vbpl.vn — and NEVER vbpl.vn /api/ or /Pages/
    (VBPL robots.txt disallows them);
  * g7.cdnchinhphu.vn is unreachable from this VM: a g7-only link is
    tried ONCE and recorded as failed with the reason — never retried
    endlessly. Direct congbaocdn links get at most 2 attempts.
  * the 17 missing documents are NOT fetched — only the 83 rows in the DB.

Idempotency: if a file already exists and the previous manifest.csv has
the same local_path with the same sha256, the download is skipped.
The manifest is written to ``manifest.csv.tmp`` while the run is in
progress and renamed to ``manifest.csv`` only on completion, so a
half-written manifest is never mistaken for a finished one.

Usage:
    python save_originals.py
        # --db defaults to ../../collector-congbao/documents.db (relative
        # to this file), --out defaults to ./originals
    python save_originals.py --db /path/documents.db --out ./originals
    python save_originals.py --only 88/2015/QH13   # one document
    python save_originals.py --force               # re-download everything

Exit code: 0 when every document has all its files saved (status ok);
non-zero when any document failed or is missing a file.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

import httpx
from bs4 import BeautifulSoup

HERE = Path(__file__).resolve().parent
DEFAULT_DB = HERE.parent.parent / "collector-congbao" / "documents.db"
USER_AGENT = "KeToanAI-Research-Collector/0.1"
RATE_LIMIT_SECONDS = 1.0
MAX_PDF_BYTES = 50_000_000
MAX_DIRECT_ATTEMPTS = 2   # congbaocdn / vbpl detail page
MAX_G7_ATTEMPTS = 1       # g7 is unreachable from this VM; try once only

ALLOWED_HOSTS = {
    "congbao.chinhphu.vn",
    "congbaocdn.chinhphu.vn",
    "g7.cdnchinhphu.vn",
    "vbpl.vn",
    # luatvietnam.vn: third-party mirror, only 04/2007/QH12 (no
    # downloadable copy on VBPL/Công báo/quochoi.vn). The mirror HTML
    # page IS the original saved for this document (see the HTML branch
    # in main(): source 'luatvietnam' is treated like 'vbpl').
    # Added 09/11/2026 after run #19 failed on this host.
    "luatvietnam.vn",
}
VBPL_DISALLOWED_PREFIXES = ("/api/", "/pages/")
VBPL_DISALLOWED_EXACT = ("/api", "/pages")

MANIFEST_FIELDS = [
    "number", "source", "source_url", "pdf_url", "local_path",
    "file_sha256", "bytes", "status", "error", "note",
]


# ---------------------------------------------------------------------------
# Small pure helpers (unit-testable without network)
# ---------------------------------------------------------------------------
def sanitize_number(number: str) -> str:
    """'88/2015/QH13' -> '88-2015-QH13'; '124/2008/NĐ-CP' -> '124-2008-ND-CP'."""
    return number.replace("/", "-").replace("Đ", "D").replace("đ", "d").strip()


def pdf_filename_from_url(pdf_url: str) -> str:
    """Local filename for a PDF URL: its basename, '.pdf' added if dotless.

    g7 signed links carry no filename in the path — use the ``file_name``
    query parameter there (falling back to a sanitised host-based name).
    """
    parsed = urlparse(pdf_url)
    if parsed.hostname == "g7.cdnchinhphu.vn":
        from urllib.parse import parse_qs

        name = (parse_qs(parsed.query).get("file_name") or [""])[0]
        name = unquote(name).strip()
        if name:
            return re.sub(r'[<>:"/\\|?*]', "_", name)
        return "g7-download.pdf"
    name = unquote(Path(parsed.path).name)
    if not name:
        raise ValueError(f"PDF URL has no filename: {pdf_url}")
    if name.lower().endswith(".pdf"):
        pass
    elif name.lower().endswith("pdf"):
        # Dotless CongBaoCP name, e.g. 12961-1-882015qh1313010pdf ->
        # 12961-1-882015qh1313010.pdf (insert the dot, don't append).
        name = name[:-3] + ".pdf"
    else:
        name += ".pdf"
    return re.sub(r'[<>:"/\\|?*]', "_", name)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def vbpl_fallback_path(fallback_dir, number: str):
    """Saved VBPL detail page for a document, e.g. 149/2001/QĐ-BTC
    -> <fallback_dir>/149-2001.html. None when no fallback dir is given
    or the number has no <first>-<second> form."""
    if not fallback_dir:
        return None
    parts = number.split("/")
    if len(parts) < 2:
        return None
    return Path(fallback_dir) / f"{parts[0]}-{parts[1]}.html"


def check_policy(url: str) -> str:
    """Refuse any URL outside the allowlist / VBPL robots rules, pre-fetch."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Refused (scheme not http/https): {url}")
    host = (parsed.hostname or "").lower()
    if host not in ALLOWED_HOSTS:
        raise ValueError(f"Refused (host not in allowlist): {url}")
    if host == "vbpl.vn":
        low = (parsed.path or "/").lower()
        if low in VBPL_DISALLOWED_EXACT or low.startswith(VBPL_DISALLOWED_PREFIXES):
            raise ValueError(f"Refused (VBPL robots.txt disallows {parsed.path}): {url}")
    return url


def sanitize_no_proxy() -> None:
    """Drop bracketed IPv6 entries from NO_PROXY (httpx InvalidURL bug)."""
    for key in ("NO_PROXY", "no_proxy"):
        val = os.environ.get(key)
        if not val:
            continue
        os.environ[key] = ",".join(p for p in val.split(",") if "[" not in p)


# ---------------------------------------------------------------------------
# Detail-page parsing — prefer the original collector's function; fall back
# to a local copy of the same logic (congbao_collector.py imports pymupdf
# at module level, which this environment may not have; parsing itself
# only needs BeautifulSoup, so the fallback is behaviour-identical).
# ---------------------------------------------------------------------------
def _local_parse_detail_page(html: str, url: str) -> dict:
    def _is_direct_pdf_link(href: str) -> bool:
        low = href.lower()
        if "congbaocdn.chinhphu.vn" not in low:
            return False
        if low.endswith(".pdf"):
            return True
        return "/congbaocp/vanban/" in low and low.endswith("pdf")

    soup = BeautifulSoup(html, "html.parser")
    direct, signed = [], []
    for a in soup.find_all("a", href=True):
        href, label = a["href"], a.get_text(" ", strip=True)
        if _is_direct_pdf_link(href):
            direct.append((label, href))
        elif href.startswith("https://g7.cdnchinhphu.vn/api/download/stream") \
                and href.lower().endswith(".pdf"):
            signed.append((label, href))
    for tag in soup.find_all(attrs={"data-href": True}):
        href = tag["data-href"]
        label = tag.get("title", "") or tag.get_text(" ", strip=True)
        if _is_direct_pdf_link(href):
            direct.append((label, href))
        elif href.startswith("https://g7.cdnchinhphu.vn/api/download/stream") \
                and href.lower().endswith(".pdf"):
            signed.append((label, href))

    def _dedupe(pairs):
        seen, out = set(), []
        for label, href in pairs:
            if href not in seen:
                seen.add(href)
                out.append((label, href))
        return out

    return {"pdf_links": _dedupe(direct),
            "pdf_links_fallback": _dedupe(signed),
            "source_url": url}


def load_parse_detail_page():
    """Return congbao_collector.parse_detail_page, or the local fallback."""
    collector_dir = HERE.parent.parent / "collector-congbao"
    if str(collector_dir) not in sys.path:
        sys.path.insert(0, str(collector_dir))
    try:
        from congbao_collector import parse_detail_page  # noqa: F401

        return parse_detail_page, True
    except Exception as exc:  # e.g. pymupdf not installed — parse anyway
        print(f"[warn] could not import congbao_collector.parse_detail_page "
              f"({exc}); using identical local fallback parser",
              file=sys.stderr, flush=True)
        return _local_parse_detail_page, False


# ---------------------------------------------------------------------------
# DB reads (read-only connection; this script NEVER writes to the DB)
# ---------------------------------------------------------------------------
def load_documents(db_path: Path) -> list[dict]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        docs = [
            {"number": n, "source_url": u}
            for n, u in conn.execute(
                "SELECT number, source_url FROM documents ORDER BY number")
        ]
        pdfs: dict[str, list[str]] = {}
        for number, pdf_url in conn.execute(
                "SELECT document_number, pdf_url FROM fetch_log "
                "WHERE status='ok' AND pdf_url IS NOT NULL AND pdf_url <> '' "
                "AND pdf_url NOT LIKE 'local:%' ORDER BY id"):
            pdfs.setdefault(number, [])
            if pdf_url not in pdfs[number]:
                pdfs[number].append(pdf_url)
        notes: dict[str, str] = {}
        for number, err in conn.execute(
                "SELECT document_number, error FROM fetch_log "
                "WHERE source_url LIKE '%vbpl%' AND error IS NOT NULL "
                "AND error <> '' ORDER BY id"):
            notes[number] = err  # provenance / PARTIAL caveat for VBPL docs
        for d in docs:
            d["pdf_urls"] = pdfs.get(d["number"], [])
            d["note"] = notes.get(d["number"], "")
        return docs
    finally:
        conn.close()


def load_existing_manifest(manifest_path: Path) -> dict[str, str]:
    """local_path -> file_sha256 from a previous completed manifest."""
    if not manifest_path.exists():
        return {}
    out = {}
    with open(manifest_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("local_path") and row.get("file_sha256"):
                out[row["local_path"]] = row["file_sha256"]
    return out


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------
class Fetcher:
    def __init__(self) -> None:
        sanitize_no_proxy()
        self.client = httpx.Client(
            headers={"User-Agent": USER_AGENT}, timeout=60,
            follow_redirects=True)
        self._last_at = 0.0

    def _wait(self) -> None:
        elapsed = time.monotonic() - self._last_at
        if elapsed < RATE_LIMIT_SECONDS:
            time.sleep(RATE_LIMIT_SECONDS - elapsed)
        self._last_at = time.monotonic()

    def get_bytes(self, url: str, referer: str | None = None,
                  max_attempts: int = MAX_DIRECT_ATTEMPTS) -> bytes:
        """GET with policy check, 1 req/s pacing, bounded retries."""
        check_policy(url)
        last_exc: Exception | None = None
        headers = {"Accept": "application/pdf,*/*"}
        if referer:
            headers["Referer"] = referer
        for attempt in range(1, max_attempts + 1):
            self._wait()
            try:
                with self.client.stream("GET", url, headers=headers) as resp:
                    resp.raise_for_status()
                    size = int(resp.headers.get("content-length", "0") or 0)
                    if size and size > MAX_PDF_BYTES:
                        raise RuntimeError(f"file too large ({size} bytes): {url}")
                    chunks, total = [], 0
                    for chunk in resp.iter_bytes(chunk_size=65536):
                        total += len(chunk)
                        if total > MAX_PDF_BYTES:
                            raise RuntimeError(
                                f"file too large (> {MAX_PDF_BYTES} bytes): {url}")
                        chunks.append(chunk)
                return b"".join(chunks)
            except (httpx.HTTPError, httpx.RemoteProtocolError) as exc:
                last_exc = exc
                if attempt < max_attempts:
                    time.sleep(2 ** attempt)
        raise RuntimeError(
            f"GET failed after {max_attempts} attempt(s): {url}: {last_exc}")

    def close(self) -> None:
        self.client.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Save original PDFs/HTML for all 83 collected documents.")
    ap.add_argument("--db", type=Path, default=DEFAULT_DB,
                    help="read-only source SQLite DB (default: %(default)s)")
    ap.add_argument("--out", type=Path, default=Path("originals"),
                    help="output directory (default: ./originals); "
                         "manifest.csv is written next to it")
    ap.add_argument("--only", default=None,
                    help="only this document number, e.g. 88/2015/QH13")
    ap.add_argument("--force", action="store_true",
                    help="re-download even when the manifest hash matches")
    ap.add_argument("--html-fallback-dir", default=None,
                    help="Directory with saved VBPL detail pages "
                         "(files <first>-<second>.html, e.g. 149-2001.html); "
                         "used ONLY when the live GET of a VBPL original "
                         "fails (VBPL blocks automated fetches since 11/11/2026)")
    args = ap.parse_args(argv)

    if not args.db.exists():
        print(f"[error] DB not found: {args.db}", file=sys.stderr)
        return 2
    out_dir: Path = args.out
    manifest_path = out_dir.parent / "manifest.csv"
    manifest_tmp = out_dir.parent / "manifest.csv.tmp"
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    docs = load_documents(args.db)
    if args.only:
        docs = [d for d in docs if d["number"] == args.only]
        if not docs:
            print(f"[error] document not in DB: {args.only}", file=sys.stderr)
            return 2
    print(f"[info] {len(docs)} document(s) from {args.db}", flush=True)

    parse_detail_page, imported = load_parse_detail_page()
    print(f"[info] detail-page parser: "
          f"{'congbao_collector.parse_detail_page' if imported else 'local fallback'}",
          flush=True)
    prev_manifest = load_existing_manifest(manifest_path)
    if prev_manifest:
        print(f"[info] previous manifest has {len(prev_manifest)} file(s); "
              f"matching hashes will be skipped", flush=True)

    fetcher = Fetcher()
    rows: list[dict] = []
    n_ok_docs = n_failed_docs = 0
    total_bytes = 0
    try:
        # Stream rows into manifest.csv.tmp as we go (flush per doc), so
        # an interrupted run leaves a visible .tmp, never a fake manifest.
        with open(manifest_tmp, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS)
            writer.writeheader()
            f.flush()
            for doc in docs:
                number, source_url = doc["number"], doc["source_url"]
                host = urlparse(source_url).hostname or ""
                # 'luatvietnam' (only 04/2007/QH12): the mirror HTML page
                # is the original, like the VBPL HTML docs — no PDF/DOC
                # exists for it anywhere official.
                source = ("vbpl" if host == "vbpl.vn" else
                          "luatvietnam" if host == "luatvietnam.vn" else
                          "congbao")
                doc_dir = out_dir / source / sanitize_number(number)
                doc_dir.mkdir(parents=True, exist_ok=True)
                doc_ok, doc_bytes = True, 0

                if source in ("vbpl", "luatvietnam"):
                    # The HTML detail page IS the original (no PDF exists).
                    if source == "luatvietnam":
                        note = ("HTML IS the original — mirror page on "
                                "luatvietnam.vn; no downloadable copy on "
                                "VBPL/Công báo/quochoi.vn. Full text also "
                                "saved as PDF in collector/local-pdfs/.")
                    else:
                        note = ("HTML IS the original — no PDF/DOC exists for "
                                "this document on VBPL. " + doc["note"]
                                if doc["note"] else
                                "HTML IS the original — no PDF/DOC exists for "
                                "this document on VBPL.")
                    local = doc_dir / f"{sanitize_number(number)}.html"
                    rel = local.relative_to(manifest_path.parent).as_posix()
                    if (not args.force and local.exists()
                            and prev_manifest.get(rel) == sha256_file(local)):
                        sha, size = prev_manifest[rel], local.stat().st_size
                        print(f"[skip] {number} (hash matches manifest)", flush=True)
                    else:
                        data = None
                        err = ""
                        try:
                            data = fetcher.get_bytes(source_url)
                        except Exception as exc:
                            err = f"{type(exc).__name__}: {exc}"
                            fb = vbpl_fallback_path(
                                args.html_fallback_dir, number)
                            if fb is not None and fb.exists():
                                print(f"[fallback] {number}: live GET failed "
                                      f"({err}); using saved HTML", flush=True)
                                data = fb.read_bytes()
                                note += (" | via saved HTML fallback "
                                         "(live GET failed)")
                        if data is None:
                            sha, size = "", 0
                            doc_ok = False
                            row = {"number": number, "source": source,
                                   "source_url": source_url, "pdf_url": "",
                                   "local_path": rel, "file_sha256": "",
                                   "bytes": 0, "status": "failed",
                                   "error": err,
                                   "note": note}
                            writer.writerow(row); f.flush(); rows.append(row)
                            print(f"[FAIL] {number}: {err}", flush=True)
                            n_failed_docs += 1
                            continue
                        local.write_bytes(data)
                        sha = hashlib.sha256(data).hexdigest()
                        size = len(data)
                    row = {"number": number, "source": source,
                           "source_url": source_url, "pdf_url": "",
                           "local_path": rel, "file_sha256": sha,
                           "bytes": size, "status": "ok", "error": "",
                           "note": note}
                    writer.writerow(row); f.flush(); rows.append(row)
                    doc_bytes += size
                    print(f"[ok] {number} -> {rel} ({size} bytes)", flush=True)
                    n_ok_docs += 1
                    total_bytes += doc_bytes
                    continue

                # --- Cong bao: resolve PDF URLs, prefer fetch_log ---
                pdf_urls = list(doc["pdf_urls"])
                pdf_source = "fetch_log"
                if not pdf_urls:
                    try:
                        html = fetcher.get_bytes(source_url).decode(
                            "utf-8", errors="replace")
                        meta = parse_detail_page(html, source_url)
                        pdf_urls = [u for _, u in meta["pdf_links"]] or \
                                   [u for _, u in meta["pdf_links_fallback"]]
                        pdf_source = "detail-page re-parse"
                    except Exception as exc:
                        pdf_urls = []
                        print(f"[FAIL] {number}: cannot resolve PDF links "
                              f"({type(exc).__name__}: {exc})", flush=True)
                if not pdf_urls:
                    row = {"number": number, "source": source,
                           "source_url": source_url, "pdf_url": "",
                           "local_path": (doc_dir.relative_to(
                               manifest_path.parent)).as_posix(),
                           "file_sha256": "", "bytes": 0,
                           "status": "missing",
                           "error": "No PDF link found (fetch_log empty, "
                                    "detail page has no PDF link)",
                           "note": ""}
                    writer.writerow(row); f.flush(); rows.append(row)
                    n_failed_docs += 1
                    continue
                print(f"[info] {number}: {len(pdf_urls)} PDF(s) via "
                      f"{pdf_source}", flush=True)
                for pdf_url in pdf_urls:
                    try:
                        fname = pdf_filename_from_url(pdf_url)
                    except ValueError as exc:
                        fname = "download.pdf"
                        print(f"[warn] {number}: {exc}; using {fname}",
                              file=sys.stderr, flush=True)
                    local = doc_dir / fname
                    rel = local.relative_to(manifest_path.parent).as_posix()
                    is_g7 = (urlparse(pdf_url).hostname
                             == "g7.cdnchinhphu.vn")
                    if (not args.force and local.exists()
                            and prev_manifest.get(rel)):
                        sha = sha256_file(local)
                        if sha == prev_manifest[rel]:
                            print(f"[skip] {number} {fname} (hash matches)",
                                  flush=True)
                            row = {"number": number, "source": source,
                                   "source_url": source_url,
                                   "pdf_url": pdf_url, "local_path": rel,
                                   "file_sha256": sha,
                                   "bytes": local.stat().st_size,
                                   "status": "ok", "error": "", "note": ""}
                            writer.writerow(row); f.flush(); rows.append(row)
                            doc_bytes += row["bytes"]
                            continue
                    try:
                        data = fetcher.get_bytes(
                            pdf_url, referer=source_url,
                            max_attempts=(MAX_G7_ATTEMPTS if is_g7
                                          else MAX_DIRECT_ATTEMPTS))
                        if not data.startswith(b"%PDF"):
                            raise RuntimeError(
                                "downloaded file is not a PDF "
                                "(missing %PDF header)")
                        local.write_bytes(data)
                        sha = hashlib.sha256(data).hexdigest()
                        row = {"number": number, "source": source,
                               "source_url": source_url, "pdf_url": pdf_url,
                               "local_path": rel, "file_sha256": sha,
                               "bytes": len(data), "status": "ok",
                               "error": "",
                               "note": "g7 fallback link" if is_g7 else ""}
                        doc_bytes += len(data)
                        print(f"[ok] {number} {fname} ({len(data)} bytes)",
                              flush=True)
                    except Exception as exc:
                        doc_ok = False
                        reason = (f"{type(exc).__name__}: {exc}")
                        if is_g7:
                            reason += (" (g7.cdnchinhphu.vn is unreachable "
                                       "from this VM; tried once only)")
                        row = {"number": number, "source": source,
                               "source_url": source_url, "pdf_url": pdf_url,
                               "local_path": rel, "file_sha256": "",
                               "bytes": 0, "status": "failed",
                               "error": reason, "note": ""}
                        print(f"[FAIL] {number} {fname}: {reason}", flush=True)
                    writer.writerow(row); f.flush(); rows.append(row)
                if doc_ok:
                    n_ok_docs += 1
                else:
                    n_failed_docs += 1
                total_bytes += doc_bytes
    finally:
        fetcher.close()

    manifest_tmp.replace(manifest_path)
    n_ok_files = sum(1 for r in rows if r["status"] == "ok")
    n_failed_files = sum(1 for r in rows if r["status"] != "ok")
    print(f"\n[summary] documents ok={n_ok_docs} failed={n_failed_docs} "
          f"(of {len(docs)}); files ok={n_ok_files} "
          f"failed/missing={n_failed_files}; "
          f"total bytes={total_bytes}; manifest={manifest_path}", flush=True)
    if n_failed_docs:
        print("[summary] failed documents:", flush=True)
        for r in rows:
            if r["status"] != "ok":
                print(f"  - {r['number']} {r['local_path']}: "
                      f"{r['status']}: {r['error']}", flush=True)
    return 0 if n_failed_docs == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
