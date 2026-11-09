#!/usr/bin/env python3
"""Migrate the KeToanAI collector SQLite database to Postgres (Supabase).

Usage:
    # Validate only — no Postgres connection, no DATABASE_URL needed:
    python3 migrate_sqlite_to_postgres.py --dry-run

    # Live migration (DATABASE_URL must be set, schema.sql applied first):
    DATABASE_URL=postgresql://... python3 migrate_sqlite_to_postgres.py
    DATABASE_URL=postgresql://... python3 migrate_sqlite_to_postgres.py --sqlite /path/to/documents.db

Source (read-only, never written to):
    ~/workspace/goals/ketoanai-accounting-web-app/files/collector-congbao/documents.db
    Default overridable with --sqlite or the SQLITE_PATH env var.

Mapping rules:
  * documents: upsert by source_url (ON CONFLICT DO UPDATE).
    - source derived from source_url host: congbao.chinhphu.vn -> 'congbao',
      vbpl.vn -> 'vbpl'. Any other host fails validation (never guessed).
    - status: every row -> 'collected', EXCEPT the 5 metadata-verified docs
      (99/2025/TT-BTC, 67/2025/QH15, 108/2025/QH15, 40/2021/TT-BTC,
      109/2025/QH15) -> 'source_verified'.
      'legal_verified' is NEVER assigned here: the CSV hieu-luc data is
      unverified and must not promote anything.
    - file_path / file_sha256 stay NULL (SQLite stores no file info).
    - text_length = len(extracted_text).
    - SQLite dates are ISO 'YYYY-MM-DD' strings; fetched_at is ISO 8601.
  * provisions: SQLite column noi_dung -> Postgres text; linked to the
    Postgres document_id via document_number; upsert on
    UNIQUE (document_id, dieu, khoan, diem, ordinal) — ordinal is part of
    the key (the old SQLite key without ordinal collapsed 111 provisions
    in 4 amending decrees). dieu=0 rows are preamble groups, migrated as-is.
  * fetch_log: copied verbatim; re-runs skip rows already present
    (matched on document_number + source_url + pdf_url + fetched_at +
    status), so the copy is idempotent without deleting history.
  * document_versions: one row per document (first version), inserted only
    if that document has no version row yet — re-runs add nothing.

Idempotency: re-running the live migration must leave counts unchanged
(upserts + NOT EXISTS guards). The script prints before/after counts and a
verification query (per-status counts, total provisions) so a re-run can
be checked at a glance.

Safety:
  * Refuses to run the live migration if DATABASE_URL is unset (exit 2).
  * Opens SQLite read-only (mode=ro URI) — it can never modify the source.
  * psycopg is imported lazily, only for a live run, so --dry-run works
    on a machine without psycopg installed.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections import Counter
from datetime import date, datetime
from urllib.parse import urlparse

DEFAULT_SQLITE_PATH = os.path.expanduser(
    "~/workspace/goals/ketoanai-accounting-web-app/files/"
    "collector-congbao/documents.db"
)

# The 5 documents whose metadata was verified against the official source
# on 2026-11-08. Everything else migrates as 'collected'.
SOURCE_VERIFIED_NUMBERS = {
    "99/2025/TT-BTC",
    "67/2025/QH15",
    "108/2025/QH15",
    "40/2021/TT-BTC",
    "109/2025/QH15",
}

HOST_TO_SOURCE = {
    "congbao.chinhphu.vn": "congbao",
    "vbpl.vn": "vbpl",
}

DOC_COLUMNS = (
    "number, title, category, source, source_url, issued_date, "
    "effective_date, fetched_at, status, sha256, file_path, file_sha256, "
    "text_length, extracted_text"
)
DOC_UPSERT_SQL = f"""
    INSERT INTO documents ({DOC_COLUMNS})
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (source_url) DO UPDATE SET
        number = EXCLUDED.number,
        title = EXCLUDED.title,
        category = EXCLUDED.category,
        source = EXCLUDED.source,
        issued_date = EXCLUDED.issued_date,
        effective_date = EXCLUDED.effective_date,
        fetched_at = EXCLUDED.fetched_at,
        status = EXCLUDED.status,
        sha256 = EXCLUDED.sha256,
        text_length = EXCLUDED.text_length,
        extracted_text = EXCLUDED.extracted_text
    RETURNING id
"""

PROV_UPSERT_SQL = """
    INSERT INTO provisions
        (document_id, chuong, muc, dieu, khoan, diem, ordinal, text, confidence)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (document_id, dieu, khoan, diem, ordinal) DO UPDATE SET
        chuong = EXCLUDED.chuong,
        muc = EXCLUDED.muc,
        text = EXCLUDED.text,
        confidence = EXCLUDED.confidence
"""

VERSION_INSERT_SQL = """
    INSERT INTO document_versions (document_id, sha256, file_sha256, fetched_at, change_summary)
    SELECT d.id, d.sha256, d.file_sha256, d.fetched_at, 'Initial import from SQLite collector DB'
    FROM documents d
    WHERE NOT EXISTS (
        SELECT 1 FROM document_versions v WHERE v.document_id = d.id
    )
"""

FETCH_LOG_SELECT_EXISTING_SQL = """
    SELECT document_number, source_url, pdf_url, fetched_at, status FROM fetch_log
"""
FETCH_LOG_INSERT_SQL = """
    INSERT INTO fetch_log
        (document_number, source_url, pdf_url, pages, chars, status, error, fetched_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

VERIFICATION_SQL = """
    SELECT status, COUNT(*) FROM documents GROUP BY status ORDER BY status;
    SELECT COUNT(*) FROM provisions;
    SELECT COUNT(*) FROM document_versions;
    SELECT COUNT(*) FROM fetch_log;
"""


# ---------------------------------------------------------------------------
# SQLite reading + validation (shared by --dry-run and live runs)
# ---------------------------------------------------------------------------
def parse_date(value, field, number, errors):
    """Parse an ISO 'YYYY-MM-DD' string (or empty/None) to a date."""
    if value is None or value == "":
        return None
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        errors.append(f"document {number!r}: invalid {field} {value!r}")
        return None


def parse_ts(value, field, number, errors):
    """Parse an ISO 8601 timestamp string (or empty/None) to a datetime."""
    if value is None or value == "":
        return None
    try:
        return datetime.fromisoformat(str(value).strip())
    except ValueError:
        errors.append(f"document {number!r}: invalid {field} {value!r}")
        return None


def clean_pg_text(v):
    """Strip NUL bytes from a value for Postgres.

    Postgres `text` columns reject NUL (0x00) outright — one NUL anywhere
    in a row aborts the whole insert. PyMuPDF's PDF text extraction
    leaves literal NULs in form-field separators (e.g. 236/2025/NĐ-CP,
    151/2014/TT-BTC), so every string bound for Postgres passes through
    here. Removing NULs changes no visible content: they are invisible
    separators, never part of the legal text.
    """
    return v.replace("\x00", "") if isinstance(v, str) else v


def load_sqlite(sqlite_path):
    """Read all three SQLite tables read-only; return (docs, provisions, logs, errors).

    docs: list of dicts already mapped to Postgres columns (except id).
    provisions: list of dicts keyed by SQLite document_number (resolved to
        a Postgres id only during the live run).
    """
    errors: list[str] = []
    if not os.path.exists(sqlite_path):
        errors.append(f"SQLite file not found: {sqlite_path}")
        return [], [], [], errors

    con = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        doc_rows = con.execute("SELECT * FROM documents").fetchall()
        prov_rows = con.execute("SELECT * FROM provisions").fetchall()
        log_rows = con.execute("SELECT * FROM fetch_log").fetchall()
    finally:
        con.close()

    docs = []
    seen_urls: set[str] = set()
    for r in doc_rows:
        number = (r["number"] or "").strip()
        title = (r["title"] or "").strip()
        source_url = (r["source_url"] or "").strip()
        if not number:
            errors.append(f"document id={r['id']}: empty number")
        if not title:
            errors.append(f"document {number or r['id']!r}: empty title")
        if not source_url:
            errors.append(f"document {number or r['id']!r}: empty source_url")
            continue
        if source_url in seen_urls:
            errors.append(f"duplicate source_url: {source_url}")
        seen_urls.add(source_url)
        host = urlparse(source_url).netloc.lower()
        source = HOST_TO_SOURCE.get(host)
        if source is None:
            errors.append(
                f"document {number!r}: unknown source host {host!r} "
                f"(URL: {source_url}) — cannot map to a source value"
            )
            continue
        text = clean_pg_text(r["extracted_text"] or "")
        docs.append({
            "number": clean_pg_text(number),
            "title": clean_pg_text(title),
            "category": clean_pg_text((r["category"] or "").strip()),
            "source": source,
            "source_url": clean_pg_text(source_url),
            "issued_date": parse_date(r["issued_date"], "issued_date", number, errors),
            "effective_date": parse_date(r["effective_date"], "effective_date", number, errors),
            "fetched_at": parse_ts(r["fetched_at"], "fetched_at", number, errors),
            "status": (
                "source_verified" if number in SOURCE_VERIFIED_NUMBERS
                else "collected"
            ),
            "sha256": (r["sha256"] or "").strip() or None,
            "file_path": None,
            "file_sha256": None,
            "text_length": len(text),
            "extracted_text": text,
        })

    doc_numbers = {d["number"] for d in docs}
    provisions = []
    seen_prov_keys: set[tuple] = set()
    for r in prov_rows:
        text = r["noi_dung"]
        if text is None or text == "":
            errors.append(
                f"provision id={r['id']} (doc {r['document_number']!r}): empty text"
            )
        if r["document_number"] not in doc_numbers:
            errors.append(
                f"provision id={r['id']}: orphan document_number "
                f"{r['document_number']!r} (no matching document)"
            )
        key = (r["document_number"], r["dieu"], r["khoan"], r["diem"], r["ordinal"])
        if key in seen_prov_keys:
            errors.append(f"duplicate provision key (would violate UNIQUE): {key}")
        seen_prov_keys.add(key)
        provisions.append({
            "document_number": clean_pg_text(r["document_number"]),
            "chuong": clean_pg_text(r["chuong"]),
            "muc": clean_pg_text(r["muc"]),
            "dieu": clean_pg_text(r["dieu"]),
            "khoan": clean_pg_text(r["khoan"]),
            "diem": clean_pg_text(r["diem"]),
            "ordinal": r["ordinal"],
            "text": clean_pg_text(text or ""),
            "confidence": r["confidence"],
        })

    logs = [{k: clean_pg_text(v) for k, v in dict(r).items()} for r in log_rows]
    for r in logs:
        parse_ts(r["fetched_at"], "fetched_at", r["document_number"], errors)
    return docs, provisions, logs, errors


def summarize(docs, provisions, logs):
    status_counts = Counter(d["status"] for d in docs)
    source_counts = Counter(d["source"] for d in docs)
    return {
        "documents": len(docs),
        "by_status": dict(sorted(status_counts.items())),
        "by_source": dict(sorted(source_counts.items())),
        "provisions": len(provisions),
        "fetch_log": len(logs),
        "document_versions": len(docs),  # one first-version row per document
    }


def print_dry_run(docs, provisions, logs, errors):
    s = summarize(docs, provisions, logs)
    print("=== DRY RUN — nothing was written, no Postgres connection made ===")
    print(f"SQLite source: {len(docs)} documents, {len(provisions)} provisions, "
          f"{len(logs)} fetch_log rows")
    print()
    print("Would insert/upsert into Postgres:")
    print(f"  documents:         {s['documents']}")
    print(f"    by status:       {s['by_status']}")
    print(f"    by source:       {s['by_source']}")
    print(f"  provisions:        {s['provisions']}")
    print(f"  fetch_log:         {s['fetch_log']} (skipping rows already present)")
    print(f"  document_versions: {s['document_versions']} (one first-version row per document)")
    print()
    print("Validation:")
    if errors:
        print(f"  FAILED — {len(errors)} problem(s) that would block/fail the migration:")
        for e in errors:
            print(f"    - {e}")
    else:
        print("  OK — no validation failures: no empty number/title/source_url, "
              "no duplicate source_url, no orphan provisions, no duplicate "
              "provision keys, all source hosts mappable, all dates parseable.")
    return 1 if errors else 0


# ---------------------------------------------------------------------------
# Live migration
# ---------------------------------------------------------------------------
def get_psycopg():
    """Import psycopg (v3), falling back to psycopg2 if that is what is installed."""
    try:
        import psycopg  # noqa: F401
        return psycopg
    except ImportError:
        pass
    try:
        import psycopg2  # noqa: F401
        return psycopg2
    except ImportError:
        print(
            "ERROR: neither 'psycopg' nor 'psycopg2' is installed.\n"
            "Install one with: pip install \"psycopg[binary]\"  (or: pip install psycopg2-binary)",
            file=sys.stderr,
        )
        raise SystemExit(2)


def count_all(cur):
    cur.execute("SELECT COUNT(*) FROM documents")
    docs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM provisions")
    provs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM fetch_log")
    logs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM document_versions")
    vers = cur.fetchone()[0]
    return {"documents": docs, "provisions": provs, "fetch_log": logs,
            "document_versions": vers}


def run_live(docs, provisions, logs, database_url):
    psycopg = get_psycopg()
    # TCP keepalives: the Supabase pooler (Supavisor, transaction mode)
    # dropped the connection right after the big migration transaction
    # committed in runs #9/#10 (psycopg.OperationalError "the connection
    # is closed" at the AFTER-counts cursor). Keepalives keep middleboxes
    # from timing the connection out during the long single transaction.
    conn = psycopg.connect(
        database_url,
        connect_timeout=30,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )
    try:
        with conn.cursor() as cur:
            before = count_all(cur)
        print(f"BEFORE: {before}")

        with conn:  # single transaction; rolls back on any error
            with conn.cursor() as cur:
                # 1. Documents (upsert by source_url) -> number -> id map.
                number_to_id: dict[str, int] = {}
                for d in docs:
                    cur.execute(DOC_UPSERT_SQL, (
                        d["number"], d["title"], d["category"], d["source"],
                        d["source_url"], d["issued_date"], d["effective_date"],
                        d["fetched_at"], d["status"], d["sha256"],
                        d["file_path"], d["file_sha256"], d["text_length"],
                        d["extracted_text"],
                    ))
                    number_to_id[d["number"]] = cur.fetchone()[0]

                # 2. Provisions, linked by Postgres document_id.
                # DELETE each migrated document's existing provisions
                # BEFORE upserting its new set. The upsert key includes
                # `ordinal`, a positional index: any parser change that
                # inserts rows early (e.g. the 09/11/2026 VAS preamble
                # split, which turned 156/2013's one-row preamble into
                # ~327 rows) shifts every later row's ordinal, so the
                # upsert can no longer match old rows to new rows and the
                # old ones linger as duplicates (run #15 left ~1,653 stale
                # rows: 156/2013 1,035 old + 1,361 new, 195/2012 618 + 839).
                # Replace-wholesale per document is safe here: provisions
                # are fully derived from the scratch SQLite, the DELETE
                # runs inside the same transaction (rolls back on error),
                # and it only touches documents present in this run —
                # documents absent from the scratch DB keep their rows.
                for _doc_id in number_to_id.values():
                    cur.execute(
                        "DELETE FROM provisions WHERE document_id = %s",
                        (_doc_id,),
                    )
                prov_params = []
                missing = 0
                for p in provisions:
                    doc_id = number_to_id.get(p["document_number"])
                    if doc_id is None:
                        missing += 1
                        continue
                    prov_params.append((
                        doc_id, p["chuong"], p["muc"], p["dieu"], p["khoan"],
                        p["diem"], p["ordinal"], p["text"], p["confidence"],
                    ))
                if missing:
                    print(f"WARNING: skipped {missing} provisions with no "
                          f"matching document (should have failed validation)",
                          file=sys.stderr)
                cur.executemany(PROV_UPSERT_SQL, prov_params)

                # 3. fetch_log — insert only rows not already present.
                cur.execute(FETCH_LOG_SELECT_EXISTING_SQL)
                # Compare fetched_at as datetimes, not isoformat strings:
                # Postgres TIMESTAMPTZ and SQLite text can render the same
                # instant differently ('+00:00' vs 'Z', microseconds), which
                # made re-runs re-insert all 139 fetch_log rows as
                # duplicates. Datetime equality is instant-based and immune.
                existing = {
                    (r[0], r[1], r[2], r[3], r[4])
                    for r in cur.fetchall()
                }
                new_logs = []
                for r in logs:
                    fetched = r["fetched_at"]
                    key = (
                        r["document_number"], r["source_url"], r["pdf_url"],
                        (datetime.fromisoformat(fetched)
                         if fetched else None),
                        r["status"],
                    )
                    if key not in existing:
                        new_logs.append((
                            r["document_number"], r["source_url"], r["pdf_url"],
                            r["pages"], r["chars"], r["status"], r["error"],
                            datetime.fromisoformat(fetched) if fetched else None,
                        ))
                if new_logs:
                    cur.executemany(FETCH_LOG_INSERT_SQL, new_logs)
                print(f"fetch_log: {len(new_logs)} new rows inserted, "
                      f"{len(logs) - len(new_logs)} already present (skipped)")

                # 4. One first-version row per document (only where none exists).
                cur.execute(VERSION_INSERT_SQL)
                print(f"document_versions: {cur.rowcount} new first-version rows")

        # Post-commit verification. The migration transaction above has
        # already committed by this point, so a dead connection here must
        # NOT fail the run: reconnect once on a fresh connection and retry
        # the counts. (Runs #9/#10 died exactly here — the pooler dropped
        # the connection right after COMMIT — even though all data was
        # already in Postgres.)
        def _verify(connection):
            with connection.cursor() as cur:
                after = count_all(cur)
            print(f"AFTER:  {after}")
            print()
            print("=== Verification (live Postgres) ===")
            with connection.cursor() as cur:
                cur.execute("SELECT status, COUNT(*) FROM documents GROUP BY status ORDER BY status")
                print("documents by status:", cur.fetchall())
                cur.execute("SELECT source, COUNT(*) FROM documents GROUP BY source ORDER BY source")
                print("documents by source:", cur.fetchall())
                cur.execute("SELECT COUNT(*) FROM provisions")
                print("total provisions:", cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM document_versions")
                print("total document_versions:", cur.fetchone()[0])
                cur.execute("SELECT COUNT(*) FROM fetch_log")
                print("total fetch_log:", cur.fetchone()[0])
                cur.execute(
                    "SELECT COUNT(*) FROM provisions p "
                    "LEFT JOIN documents d ON d.id = p.document_id WHERE d.id IS NULL"
                )
                print("orphan provisions (must be 0):", cur.fetchone()[0])
            return after

        try:
            _verify(conn)
        except psycopg.OperationalError as e:
            print(f"WARNING: verification connection dropped after COMMIT "
                  f"({e}) — data is committed; retrying counts on a fresh "
                  f"connection...", file=sys.stderr)
            try:
                conn.close()
            except Exception:
                pass
            conn = psycopg.connect(
                database_url,
                connect_timeout=30,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
            _verify(conn)
        print()
        print("Done. Re-run this script to confirm idempotency — "
              "AFTER counts must be identical on the second run.")
    finally:
        conn.close()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sqlite", default=os.environ.get("SQLITE_PATH", DEFAULT_SQLITE_PATH),
                    help="path to the collector SQLite DB (default: %(default)s)")
    ap.add_argument("--database-url", default=os.environ.get("DATABASE_URL"),
                    help="Postgres connection string (default: $DATABASE_URL)")
    ap.add_argument("--dry-run", action="store_true",
                    help="validate the mapping against SQLite only; "
                         "do not connect to Postgres")
    args = ap.parse_args(argv)

    docs, provisions, logs, errors = load_sqlite(args.sqlite)
    if args.dry_run:
        print(f"SQLite source: {args.sqlite}")
        return print_dry_run(docs, provisions, logs, errors)

    if errors:
        print(f"ERROR: {len(errors)} validation failure(s) — fix the SQLite "
              f"data before migrating:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    if not args.database_url:
        print(
            "ERROR: DATABASE_URL is not set.\n"
            "The live migration refuses to run without it.\n"
            "Set it first, e.g.:\n"
            "  export DATABASE_URL='postgresql://postgres:<password>@<host>:5432/postgres'\n"
            "or pass --database-url. Use --dry-run to validate the mapping "
            "without a Postgres connection.",
            file=sys.stderr,
        )
        return 2

    run_live(docs, provisions, logs, args.database_url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
