#!/usr/bin/env python3
"""Upload the saved originals/ tree to Supabase Storage (bucket 'legal-docs').

Deliverable 4 of the KeToanAI Postgres architecture. Companion to
``../collector/save_originals.py`` — it uploads exactly the tree that
script produces (``originals/<source>/<number-sanitized>/<file>``).

Storage layout decision (documented here, pick one and stick to it):
the ``originals/`` prefix is STRIPPED on upload, so the object key is::

    congbao/88-2015-QH13/12961-1-882015qh1313010.pdf
    vbpl/13-2008-QH12/13-2008-QH12.html

i.e. the key is the path relative to the ``originals/`` directory.
The bucket name is part of the URL, not of the key.

Auth / API (Supabase Storage REST API via httpx — NO supabase-py
dependency; httpx is already in the project's requirements.txt):
  * Base URL: ``{SUPABASE_URL}/storage/v1`` where SUPABASE_URL is the
    project URL, e.g. ``https://<project-ref>.supabase.co`` (no trailing
    slash). Both the ``apikey`` header AND ``Authorization: Bearer
    <service-role key>`` carry the service-role key; the service role
    bypasses Row Level Security, so uploads work even though the bucket
    is private (public-read OFF).
  * Upload one object:
    ``POST /storage/v1/object/{bucket}/{object-key}``
    headers: ``apikey``, ``Authorization: Bearer <key>``,
    ``Content-Type: <mime>``, ``x-upsert: true`` (overwrite an existing
    object with the same key instead of failing with 409).
    Body: the raw file bytes. A 200 response means the object is stored.
  * List objects (existence check):
    ``POST /storage/v1/object/list/{bucket}``
    JSON body: ``{"prefix": "<dir>/", "limit": 100, "offset": 0}``
    (paginated with ``offset``). Response items carry ``name`` and
    ``metadata`` (size, mimetype, lastModified) — but NO sha256/etag.
    Consequence (important): the list API does NOT allow a cheap
    same-sha256 check. This script therefore uploads ALL files by
    default and says so; ``--skip-existing`` only skips when the listed
    object has the SAME BYTE SIZE (a weak check — same size is not same
    content — and it is off by default for exactly that reason).
    ``--verify`` downloads each uploaded object back
    (``GET /storage/v1/object/{bucket}/{object-key}``) and compares its
    sha256 with the local file; use it only when you can afford the
    extra downloads.

Environment (live mode refuses to run without both):
  * SUPABASE_URL — project URL, e.g. https://<ref>.supabase.co
  * SUPABASE_SERVICE_ROLE_KEY — the service-role (secret) key from the
    Supabase dashboard (Settings → API). NEVER commit it, never print
    it; only its first/last few characters are ever shown in logs.
    The anon key must NOT be used here: it cannot write to a private
    bucket without a permissive storage policy.

Prerequisites that CANNOT be checked from here (no Supabase project
exists yet — see "What could not be tested" in the deliverable notes):
  * the 'legal-docs' bucket must already exist (created in the
    dashboard or via the Storage API) with public access OFF;
  * the service-role key must be valid and the project URL correct;
  * actual upload, bucket permissions and key validity can only be
    verified against a live project.

Usage:
    python upload_originals.py --dry-run
        # lists every file that would be uploaded (method, URL path,
        # size, sha256) WITHOUT any network call to Supabase and
        # WITHOUT requiring SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY.
    SUPABASE_URL=https://<ref>.supabase.co \\
    SUPABASE_SERVICE_ROLE_KEY=<key> \\
        python upload_originals.py
    python upload_originals.py --skip-existing   # weak size-only skip
    python upload_originals.py --verify          # GET-back + sha256 check
    python upload_originals.py --bucket legal-docs --originals ../collector/originals

Exit code: 0 when every file is uploaded (or, in --dry-run, listed);
non-zero when any upload fails or the environment is missing.
"""

from __future__ import annotations

import argparse
import hashlib
import mimetypes
import os
import sys
import time
from pathlib import Path

import httpx

HERE = Path(__file__).resolve().parent
DEFAULT_ORIGINALS = HERE.parent / "collector" / "originals"
DEFAULT_BUCKET = "legal-docs"
RATE_LIMIT_SECONDS = 0.2  # polite pacing between Storage API calls
MAX_ATTEMPTS = 3          # retries on 5xx / network errors only, never 4xx


# ---------------------------------------------------------------------------
# Pure helpers — request construction is testable without any network call.
# ---------------------------------------------------------------------------
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def storage_key(local_path: Path, originals_root: Path) -> str:
    """Object key: path relative to originals/, '/'-separated, no prefix."""
    return local_path.relative_to(originals_root).as_posix()


def content_type_for(path: Path) -> str:
    mime, _ = mimetypes.guess_type(str(path))
    if path.suffix.lower() == ".html":
        return "text/html; charset=utf-8"
    return mime or "application/octet-stream"


def redact_key(key: str) -> str:
    if len(key) <= 8:
        return "***"
    return f"{key[:4]}...{key[-4:]}"


def sanitize_no_proxy() -> None:
    """Drop bracketed IPv6 entries from NO_PROXY (httpx InvalidURL bug).

    Same bug the original collector hit: httpx crashes with
    ``InvalidURL: ':1]'`` when NO_PROXY contains an IPv6 literal such as
    [::1]. Stripping those entries keeps proxy use working.
    """
    for key in ("NO_PROXY", "no_proxy"):
        val = os.environ.get(key)
        if not val:
            continue
        os.environ[key] = ",".join(p for p in val.split(",") if "[" not in p)


class StorageClient:
    """Minimal Supabase Storage REST client (httpx, service-role Bearer)."""

    def __init__(self, base_url: str, service_key: str, bucket: str):
        sanitize_no_proxy()
        self.base = base_url.rstrip("/")
        self.bucket = bucket
        self._key = service_key
        self.client = httpx.Client(
            headers={"apikey": service_key,
                     "Authorization": f"Bearer {service_key}"},
            timeout=120, follow_redirects=True)

    # -- request builders (pure: no I/O, used by --dry-run and tests) --
    def build_upload_request(self, key: str, data: bytes,
                             content_type: str) -> httpx.Request:
        return self.client.build_request(
            "POST", f"{self.base}/storage/v1/object/{self.bucket}/{key}",
            headers={"Content-Type": content_type, "x-upsert": "true"},
            content=data)

    def build_list_request(self, prefix: str, limit: int = 100,
                           offset: int = 0) -> httpx.Request:
        return self.client.build_request(
            "POST", f"{self.base}/storage/v1/object/list/{self.bucket}",
            json={"prefix": prefix, "limit": limit, "offset": offset})

    def build_get_request(self, key: str) -> httpx.Request:
        return self.client.build_request(
            "GET", f"{self.base}/storage/v1/object/{self.bucket}/{key}")

    # -- live operations --
    def list_prefix(self, prefix: str) -> dict[str, dict]:
        """{name: metadata} for all objects under prefix (paginated)."""
        out: dict[str, dict] = {}
        offset = 0
        while True:
            req = self.build_list_request(prefix, limit=100, offset=offset)
            resp = self.client.send(req)
            if resp.status_code == 404:
                return {}  # bucket/prefix does not exist (yet)
            resp.raise_for_status()
            items = resp.json()
            if not items:
                break
            for it in items:
                out[it["name"]] = it.get("metadata") or {}
            if len(items) < 100:
                break
            offset += 100
        return out

    def upload(self, key: str, data: bytes, content_type: str) -> None:
        """POST the object; retry 5xx/network only, never 4xx."""
        last_exc: Exception | None = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            req = self.build_upload_request(key, data, content_type)
            try:
                resp = self.client.send(req)
                if resp.status_code < 300:
                    return
                if resp.status_code < 500:
                    raise RuntimeError(
                        f"upload rejected (HTTP {resp.status_code}): "
                        f"{resp.text[:300]}")
                last_exc = RuntimeError(
                    f"HTTP {resp.status_code}: {resp.text[:300]}")
            except httpx.HTTPError as exc:
                last_exc = exc
            if attempt < MAX_ATTEMPTS:
                time.sleep(2 ** attempt)
        raise RuntimeError(f"upload failed after {MAX_ATTEMPTS} attempts: "
                           f"{key}: {last_exc}")

    def download(self, key: str) -> bytes:
        resp = self.client.send(self.build_get_request(key))
        resp.raise_for_status()
        return resp.content

    def close(self) -> None:
        self.client.close()


def collect_files(originals_root: Path) -> list[Path]:
    """All files under originals/, sorted; manifest.csv excluded if present."""
    return sorted(p for p in originals_root.rglob("*")
                  if p.is_file() and p.name != "manifest.csv")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Upload originals/ to Supabase Storage bucket 'legal-docs'.")
    ap.add_argument("--originals", type=Path, default=DEFAULT_ORIGINALS,
                    help="local originals/ tree (default: %(default)s)")
    ap.add_argument("--bucket", default=DEFAULT_BUCKET,
                    help="Supabase Storage bucket (default: %(default)s)")
    ap.add_argument("--dry-run", action="store_true",
                    help="list what would be uploaded; NO network call, "
                         "no env vars required")
    ap.add_argument("--skip-existing", action="store_true",
                    help="skip files whose listed object has the same byte "
                         "size (WEAK check — size is not content; the list "
                         "API returns no sha256)")
    ap.add_argument("--verify", action="store_true",
                    help="after each upload, GET the object back and "
                         "compare sha256 (extra downloads)")
    args = ap.parse_args(argv)

    if not args.originals.is_dir():
        print(f"[error] originals/ directory not found: {args.originals}",
              file=sys.stderr)
        print("[error] run ../collector/save_originals.py first",
              file=sys.stderr)
        return 2
    files = collect_files(args.originals)
    if not files:
        print(f"[error] no files under {args.originals}", file=sys.stderr)
        return 2

    base_url = os.environ.get("SUPABASE_URL", "").strip()
    service_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

    if args.dry_run:
        # No env vars, no network: only request *construction* is shown.
        fake_base = base_url or "https://<project-ref>.supabase.co"
        print(f"[dry-run] bucket={args.bucket} files={len(files)} "
              f"(no network call, no credentials required)", flush=True)
        print(f"[dry-run] upload endpoint: POST "
              f"{fake_base}/storage/v1/object/{args.bucket}/<object-key>",
              flush=True)
        print("[dry-run] headers per upload: apikey=<service-role key>, "
              "Authorization: Bearer <service-role key>, "
              "Content-Type: <mime>, x-upsert: true", flush=True)
        print("[dry-run] NOTE: the Storage list API returns name/size/"
              "mimetype only — NO sha256 — so a cheap same-sha skip is "
              "not possible; a live run uploads all files "
              "(--skip-existing is size-only, weak).", flush=True)
        total = 0
        # Build one real httpx.Request per file to prove the construction
        # is valid (method, URL, headers) without sending anything.
        probe = StorageClient(fake_base, "DUMMY-KEY-FOR-DRY-RUN",
                              args.bucket)
        try:
            for p in files:
                key = storage_key(p, args.originals)
                size = p.stat().st_size
                total += size
                req = probe.build_upload_request(
                    key, b"", content_type_for(p))
                assert req.method == "POST" and key in str(req.url)
                print(f"  would upload {key} ({size} bytes, "
                      f"sha256={sha256_file(p)[:16]}…, "
                      f"{content_type_for(p)})", flush=True)
        finally:
            probe.close()
        print(f"[dry-run] summary: {len(files)} file(s), {total} bytes "
              f"would be uploaded to bucket '{args.bucket}'", flush=True)
        return 0

    # --- live mode: both env vars are mandatory ---
    if not base_url or not service_key:
        missing = [n for n, v in
                   (("SUPABASE_URL", base_url),
                    ("SUPABASE_SERVICE_ROLE_KEY", service_key)) if not v]
        print(f"[error] missing required environment variable(s): "
              f"{', '.join(missing)}", file=sys.stderr)
        print("[error] set SUPABASE_URL (e.g. https://<ref>.supabase.co) "
              "and SUPABASE_SERVICE_ROLE_KEY (service-role key from the "
              "Supabase dashboard, Settings → API) and re-run.",
              file=sys.stderr)
        print("[error] no Supabase project exists yet — create the project "
              "and the private 'legal-docs' bucket first.", file=sys.stderr)
        return 2
    if not base_url.startswith("https://"):
        print(f"[error] SUPABASE_URL must start with https://: {base_url}",
              file=sys.stderr)
        return 2

    print(f"[info] bucket={args.bucket} base={base_url} "
          f"key={redact_key(service_key)} files={len(files)}", flush=True)
    print("[info] NOTE: the Storage list API returns no sha256, so files "
          "are uploaded unconditionally unless --skip-existing (size-only) "
          "is given.", flush=True)
    storage = StorageClient(base_url, service_key, args.bucket)
    existing: dict[str, dict] = {}
    if args.skip_existing:
        prefixes = sorted({storage_key(p, args.originals).split("/")[0]
                           for p in files})
        for prefix in prefixes:
            try:
                listed = storage.list_prefix(prefix + "/")
            except Exception as exc:
                print(f"[error] list {prefix}/ failed: {exc}",
                      file=sys.stderr)
                storage.close()
                return 1
            for name, meta in listed.items():
                existing[f"{prefix}/{name}"] = meta
        print(f"[info] list API: {len(existing)} existing object(s)",
              flush=True)

    n_uploaded = n_skipped = n_failed = n_verified = 0
    total_bytes = 0
    failures: list[str] = []
    try:
        for p in files:
            key = storage_key(p, args.originals)
            size = p.stat().st_size
            if args.skip_existing:
                meta = existing.get(key)
                if meta and meta.get("size") == size:
                    print(f"[skip] {key} (same size {size} already in "
                          f"bucket; sha NOT verified — list API has none)",
                          flush=True)
                    n_skipped += 1
                    continue
            try:
                data = p.read_bytes()
                storage.upload(key, data, content_type_for(p))
                total_bytes += size
                n_uploaded += 1
                print(f"[ok] {key} ({size} bytes, "
                      f"sha256={hashlib.sha256(data).hexdigest()[:16]}…)",
                      flush=True)
                if args.verify:
                    back = storage.download(key)
                    if hashlib.sha256(back).hexdigest() == \
                            hashlib.sha256(data).hexdigest():
                        n_verified += 1
                        print(f"[verified] {key} (sha256 matches)",
                              flush=True)
                    else:
                        n_failed += 1
                        failures.append(f"{key}: sha256 mismatch after upload")
                        print(f"[FAIL] {key}: sha256 mismatch after upload",
                              file=sys.stderr, flush=True)
            except Exception as exc:
                n_failed += 1
                failures.append(f"{key}: {type(exc).__name__}: {exc}")
                print(f"[FAIL] {key}: {exc}", file=sys.stderr, flush=True)
            time.sleep(RATE_LIMIT_SECONDS)
    finally:
        storage.close()

    print(f"\n[summary] uploaded={n_uploaded} skipped={n_skipped} "
          f"failed={n_failed} verified={n_verified} "
          f"total bytes={total_bytes} bucket='{args.bucket}'", flush=True)
    if failures:
        for f_ in failures:
            print(f"  - {f_}", flush=True)
    return 0 if n_failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
