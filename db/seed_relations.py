"""Nap quan he hieu luc + trang thai hieu luc (buoc 02) vao DB.

Su dung:
  # SQLite local (master copy):
  python db/seed_relations.py --sqlite /tmp/qa-step05.db
  # Postgres live (DATABASE_URL phai dat, schema.sql da ap dung):
  DATABASE_URL='postgresql://...' python db/seed_relations.py --database-url "$DATABASE_URL"

- Tao bang document_relations + document_effectiveness neu chua co
  (DDL theo dialect; Postgres dung dinh nghia trong db/schema.sql).
- Upsert theo (source_number, target_number, relation_type) / number.
- Sau upsert, gan source_doc_id/target_doc_id theo documents.number
  (NULL khi van ban ngoai kho — 49 dich ngoai danh sach 100 + 18 nguon
  ngoai danh sach; do la binh thuong, khong phai loi).
- Idempotent: chay lai chi cap nhat, khong tao trung.
- CHI GHI 2 bang moi; khong sua documents/provisions/fetch_log.
"""
import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REL_SEED = HERE / "relations_seed.json"
EFF_SEED = HERE / "effectiveness_seed.json"

REL_DDL_SQLITE = """
CREATE TABLE IF NOT EXISTS document_relations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_number TEXT NOT NULL,
    target_number TEXT NOT NULL,
    source_doc_id INTEGER REFERENCES documents(id) ON DELETE CASCADE,
    target_doc_id INTEGER REFERENCES documents(id) ON DELETE SET NULL,
    relation_type TEXT NOT NULL CHECK (relation_type IN
        ('SỬA_ĐỔI', 'THAY_THẾ', 'BÃI_BỎ', 'HƯỚNG_DẪN', 'LIÊN_QUAN')),
    scope TEXT,
    evidence TEXT NOT NULL,
    evidence_source TEXT NOT NULL,
    confidence TEXT NOT NULL DEFAULT 'có thể'
        CHECK (confidence IN ('chắc chắn', 'có thể', 'suy luận')),
    verified INTEGER NOT NULL DEFAULT 0,
    needs_verification INTEGER NOT NULL DEFAULT 0,
    note TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (source_number, target_number, relation_type)
);
CREATE INDEX IF NOT EXISTS ix_relations_source ON document_relations (source_number);
CREATE INDEX IF NOT EXISTS ix_relations_target ON document_relations (target_number);
CREATE INDEX IF NOT EXISTS ix_relations_type ON document_relations (relation_type);
"""

EFF_DDL_SQLITE = """
CREATE TABLE IF NOT EXISTS document_effectiveness (
    number TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('còn hiệu lực', 'còn hiệu lực một phần',
        'hết hiệu lực', 'chưa có hiệu lực', 'chưa xác minh')),
    expiry_date TEXT,
    note TEXT,
    source TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

REL_DDL_PG = """
CREATE TABLE IF NOT EXISTS document_relations (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_number TEXT NOT NULL,
    target_number TEXT NOT NULL,
    source_doc_id BIGINT REFERENCES documents(id) ON DELETE CASCADE,
    target_doc_id BIGINT REFERENCES documents(id) ON DELETE SET NULL,
    relation_type TEXT NOT NULL CHECK (relation_type IN
        ('SỬA_ĐỔI', 'THAY_THẾ', 'BÃI_BỎ', 'HƯỚNG_DẪN', 'LIÊN_QUAN')),
    scope TEXT,
    evidence TEXT NOT NULL,
    evidence_source TEXT NOT NULL,
    confidence TEXT NOT NULL DEFAULT 'có thể'
        CHECK (confidence IN ('chắc chắn', 'có thể', 'suy luận')),
    verified BOOLEAN NOT NULL DEFAULT FALSE,
    needs_verification BOOLEAN NOT NULL DEFAULT FALSE,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_number, target_number, relation_type)
);
CREATE INDEX IF NOT EXISTS ix_relations_source ON document_relations (source_number);
CREATE INDEX IF NOT EXISTS ix_relations_target ON document_relations (target_number);
CREATE INDEX IF NOT EXISTS ix_relations_type ON document_relations (relation_type);
CREATE INDEX IF NOT EXISTS ix_relations_source_doc ON document_relations (source_doc_id);
CREATE INDEX IF NOT EXISTS ix_relations_target_doc ON document_relations (target_doc_id);
"""

EFF_DDL_PG = """
CREATE TABLE IF NOT EXISTS document_effectiveness (
    number TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('còn hiệu lực', 'còn hiệu lực một phần',
        'hết hiệu lực', 'chưa có hiệu lực', 'chưa xác minh')),
    expiry_date DATE,
    note TEXT,
    source TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def load_seeds():
    rel = json.loads(REL_SEED.read_text(encoding="utf-8"))["relations"]
    eff = json.loads(EFF_SEED.read_text(encoding="utf-8"))["effectiveness"]
    return rel, eff


def seed_sqlite(db_path):
    rel, eff = load_seeds()
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(REL_DDL_SQLITE)
        conn.executescript(EFF_DDL_SQLITE)
        for r in rel:
            conn.execute(
                """INSERT INTO document_relations
                   (source_number, target_number, relation_type, scope,
                    evidence, evidence_source, confidence, verified,
                    needs_verification, note)
                   VALUES (?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(source_number, target_number, relation_type)
                   DO UPDATE SET scope=excluded.scope,
                       evidence=excluded.evidence,
                       evidence_source=excluded.evidence_source,
                       confidence=excluded.confidence,
                       needs_verification=excluded.needs_verification,
                       note=excluded.note,
                       updated_at=datetime('now')""",
                (r["source_number"], r["target_number"], r["relation_type"],
                 r["scope"], r["evidence"], r["evidence_source"],
                 r["confidence"], 0, 1 if r["needs_verification"] else 0,
                 r["note"]))
        for e in eff:
            conn.execute(
                """INSERT INTO document_effectiveness
                   (number, status, expiry_date, note, source)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(number) DO UPDATE SET
                       status=excluded.status,
                       expiry_date=excluded.expiry_date,
                       note=excluded.note, source=excluded.source,
                       updated_at=datetime('now')""",
                (e["number"], e["status"], e["expiry_date"], e["note"],
                 e["source"]))
        # Gan doc id theo so hieu (NULL khi ngoai kho).
        conn.execute(
            "UPDATE document_relations SET source_doc_id = "
            "(SELECT id FROM documents WHERE documents.number = "
            "document_relations.source_number)")
        conn.execute(
            "UPDATE document_relations SET target_doc_id = "
            "(SELECT id FROM documents WHERE documents.number = "
            "document_relations.target_number)")
        conn.commit()
        n_rel = conn.execute("SELECT COUNT(*) FROM document_relations").fetchone()[0]
        n_nv = conn.execute("SELECT COUNT(*) FROM document_relations WHERE needs_verification=1").fetchone()[0]
        n_eff = conn.execute("SELECT COUNT(*) FROM document_effectiveness").fetchone()[0]
        n_unres_src = conn.execute("SELECT COUNT(*) FROM document_relations WHERE source_doc_id IS NULL").fetchone()[0]
        n_unres_tgt = conn.execute("SELECT COUNT(*) FROM document_relations WHERE target_doc_id IS NULL").fetchone()[0]
        print(f"SQLite {db_path}: relations={n_rel} (needs_verification={n_nv}), "
              f"effectiveness={n_eff}, source ngoai kho={n_unres_src}, "
              f"target ngoai kho={n_unres_tgt}")
        return 0
    finally:
        conn.close()


def seed_pg(database_url):
    from psycopg import connect
    rel, eff = load_seeds()
    conn = connect(database_url)
    try:
        with conn.cursor() as c:
            c.execute(REL_DDL_PG)
            c.execute(EFF_DDL_PG)
            for r in rel:
                c.execute(
                    """INSERT INTO document_relations
                       (source_number, target_number, relation_type, scope,
                        evidence, evidence_source, confidence, verified,
                        needs_verification, note)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,FALSE,%s,%s)
                       ON CONFLICT (source_number, target_number, relation_type)
                       DO UPDATE SET scope=EXCLUDED.scope,
                           evidence=EXCLUDED.evidence,
                           evidence_source=EXCLUDED.evidence_source,
                           confidence=EXCLUDED.confidence,
                           needs_verification=EXCLUDED.needs_verification,
                           note=EXCLUDED.note,
                           updated_at=now()""",
                    (r["source_number"], r["target_number"], r["relation_type"],
                     r["scope"], r["evidence"], r["evidence_source"],
                     r["confidence"], r["needs_verification"], r["note"]))
            for e in eff:
                c.execute(
                    """INSERT INTO document_effectiveness
                       (number, status, expiry_date, note, source)
                       VALUES (%s,%s,%s,%s,%s)
                       ON CONFLICT (number) DO UPDATE SET
                           status=EXCLUDED.status,
                           expiry_date=EXCLUDED.expiry_date,
                           note=EXCLUDED.note, source=EXCLUDED.source,
                           updated_at=now()""",
                    (e["number"], e["status"], e["expiry_date"], e["note"],
                     e["source"]))
            c.execute(
                "UPDATE document_relations SET source_doc_id = "
                "(SELECT id FROM documents WHERE documents.number = "
                "document_relations.source_number)")
            c.execute(
                "UPDATE document_relations SET target_doc_id = "
                "(SELECT id FROM documents WHERE documents.number = "
                "document_relations.target_number)")
            n_rel = c.execute("SELECT COUNT(*) FROM document_relations").fetchone()[0]
            n_nv = c.execute("SELECT COUNT(*) FROM document_relations WHERE needs_verification").fetchone()[0]
            n_eff = c.execute("SELECT COUNT(*) FROM document_effectiveness").fetchone()[0]
            n_unres_src = c.execute("SELECT COUNT(*) FROM document_relations WHERE source_doc_id IS NULL").fetchone()[0]
            n_unres_tgt = c.execute("SELECT COUNT(*) FROM document_relations WHERE target_doc_id IS NULL").fetchone()[0]
        conn.commit()
        print(f"Postgres: relations={n_rel} (needs_verification={n_nv}), "
              f"effectiveness={n_eff}, source ngoai kho={n_unres_src}, "
              f"target ngoai kho={n_unres_tgt}")
        return 0
    finally:
        conn.close()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sqlite", default=None,
                    help="duong dan SQLite DB (mac dinh: collector master)")
    ap.add_argument("--database-url", default=os.getenv("DATABASE_URL"),
                    help="chuoi ket noi Postgres (mac dinh: $DATABASE_URL)")
    args = ap.parse_args(argv)
    if args.database_url:
        return seed_pg(args.database_url)
    db_path = args.sqlite or os.path.expanduser(
        "~/workspace/goals/ketoanai-accounting-web-app/files/"
        "collector-congbao/documents.db")
    return seed_sqlite(db_path)


if __name__ == "__main__":
    raise SystemExit(main())
