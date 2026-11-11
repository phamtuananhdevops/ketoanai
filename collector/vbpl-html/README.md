# VBPL HTML — saved copies (fallback only)

16 saved VBPL detail pages (`<key>.html`), fetched 09/11/2026 during the
VBPL-16 round (browser download / curl from the VM, while VBPL still served
automated clients). One file per document in `vbpl_html_ingest.py`'s
`TARGETS` list.

**These files are a fallback, not the source of truth.** The workflow runs
`vbpl_html_ingest.py` with `--html-fallback-dir collector/vbpl-html`: it
tries the live VBPL page first and uses the saved copy for that document
only when the live GET fails. VBPL has blocked automated fetches (HTTP 403)
from some networks since 11/11/2026 — runs #26 and #27 both failed at the
VBPL HTML step for that reason; the fallback unblocks them.

**Do not edit these files.** They are verbatim saved pages (Next.js RSC
payloads). If a document's VBPL page genuinely changes, re-fetch the live
page and replace the file, then note the date here. The 16 documents are
old laws (2001–2022) whose texts do not change; staleness risk is minimal.

Provenance of each file is recorded in `fetch_log` when the fallback is
used ("via saved HTML fallback (live GET failed)").
