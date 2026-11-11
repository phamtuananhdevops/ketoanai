# VBPL HTML — saved copies (fallback only)

20 saved VBPL detail pages (`<key>.html`):
- 16 files for the documents in `vbpl_html_ingest.py`'s `TARGETS` list
  (fetched 09/11/2026 during the VBPL-16 round).
- 4 more for the other VBPL-sourced documents (234/2003, 12/2005,
  100/2005, 15/2006 — fetched 11/11/2026). These four are ingested from
  local PDFs, but `save_originals.py` still fetches their VBPL detail
  page as the "original", so they need a fallback copy too.

**These files are a fallback, not the source of truth.** Both scripts try
the live VBPL page first and use the saved copy for that document only
when the live GET fails. VBPL has blocked automated fetches (HTTP 403)
from some networks since 11/11/2026 — runs #26 and #27 both failed at the
VBPL HTML ingest step for that reason, and run #28 failed at the
save-originals step; the fallback unblocks them.

**Do not edit these files.** They are verbatim saved pages (Next.js RSC
payloads). If a document's VBPL page genuinely changes, re-fetch the live
page and replace the file, then note the date here. The 16 documents are
old laws (2001–2022) whose texts do not change; staleness risk is minimal.

Provenance of each file is recorded in `fetch_log` when the fallback is
used ("via saved HTML fallback (live GET failed)").
