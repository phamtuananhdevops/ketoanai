#!/usr/bin/env python3
"""Ingest the 4 VBPL HTML-only documents into the collector DB.

These 4 documents have no PDF/DOC file on VBPL — only the rendered full
text on their vbpl.vn detail pages (Next.js RSC payload). This script
fetches each detail page (robots-checked, 1 req/s, never /api/ or
/Pages/), extracts the text with congbao_collector.extract_vbpl_html_text,
parses provisions with the collector's existing parser and stores them
with the same schema (ordinal key, 300k extracted_text cap) and
source_url = the VBPL detail URL. Provenance is recorded in fetch_log.

Metadata (title, category, issued/effective dates) comes from
../danh-sach-100-van-ban/danh-sach-100-van-ban.csv.

Usage:
  python vbpl_html_ingest.py --db /tmp/documents-vbpl-test.db   # copy test
  python vbpl_html_ingest.py --db documents.db                  # real run
  python vbpl_html_ingest.py --db documents.db --html-dir vbpl-html-scratch
      # use previously fetched page HTML instead of live GET (same content,
      # avoids re-hitting VBPL when it is throttling; fetch provenance is
      # still recorded in fetch_log)
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from congbao_collector import (  # noqa: E402
    Collector,
    extract_vbpl_html_text,
    init_db,
    log_fetch,
    parse_provisions,
    store_document,
    store_provisions,
)

HERE = Path(__file__).resolve().parent
LIST_CSV = HERE.parent / "danh-sach-100-van-ban" / "danh-sach-100-van-ban.csv"

# The 4 html-only documents from results-vbpl-21.csv (status 'found, html-only').
TARGETS = [
    {
        "number": "13/2008/QH12",
        "key": "13-2008",
        "url": "https://vbpl.vn/van-ban/chi-tiet/luat-thue-gia-tri-gia-tang-so-13-2008-qh12--12806",
        "expected_dieus": 16,  # per the assignment brief
    },
    {
        "number": "124/2008/NĐ-CP",
        "key": "124-2008",
        "url": "https://vbpl.vn/van-ban/chi-tiet/nghi-dinh-so-124-2008-nd-cp-quy-dinh-chi-tiet-va-huong-dan-thi-hanh-mot-so-dieu-cua-luat-thue-thu-nhap-doanh-nghiep--11606",
        "expected_dieus": None,
    },
    {
        "number": "48/2006/QĐ-BTC",
        "key": "48-2006",
        "url": "https://vbpl.vn/van-ban/chi-tiet/quyet-dinh-so-48-2006-qd-btc-ve-viec-ban-hanh-che-do-ke-toan-doanh-nghiep-nho-va-vua--15072",
        "expected_dieus": None,
        "note": "PARTIAL: VBPL rendered text is the 6-Dieu Quyet dinh only; "
                "the 5-part Che do ke toan itself is NOT in the HTML "
                "(it is in the .rar 'Bieu mau.rar' / separate content).",
    },
    {
        "number": "51/2010/NĐ-CP",
        "key": "51-2010",
        "url": "https://vbpl.vn/van-ban/chi-tiet/nghi-dinh-so-51-2010-nd-cp-quy-dinh-ve-hoa-don-ban-hang-hoa-cung-ung-dich-vu--25242",
        "expected_dieus": None,
    },
    # Thêm 09/11/2026 (đợt tải VBPL bằng trình duyệt): 4 văn bản còn thiếu
    # mà file gốc tải về không dùng được (91/2022 + 954/2020 là PDF scan
    # không lớp text; 105/2003 không có file tải về; 161/2007 tải PDF
    # không thành công) nhưng VBPL có toàn văn HTML. 91/2022 sửa đổi
    # 126/2020/NĐ-CP; 954/2020 điều chỉnh mức giảm trừ gia cảnh TNCN.
    {
        "number": "105/2003/TT-BTC",
        "key": "105-2003",
        "url": "https://vbpl.vn/van-ban/chi-tiet/thong-tu-so-105-2003-tt-btc-huong-dan-ke-toan-thuc-hien-sau-6-chuan-muc-ke-toan-ban-hanh-theo-quyet-dinh-so-165-2002-qd-btc-ngay-31-12-2002-cua-bo-truong-bo-tai-chinh--24807",
        "expected_dieus": None,
    },
    {
        "number": "161/2007/TT-BTC",
        "key": "161-2007",
        "url": "https://vbpl.vn/van-ban/chi-tiet/thong-tu-so-161-2007-tt-btc-huong-dan-thuc-hien-muoi-sau-16-chuan-muc-ke-toan-ban-hanh-theo-quyet-dinh-so-149-2001-qd-btc-ngay-31-12-2001-quyet-dinh-165-2002-qd-btc-ngay-31-12-2002-va-quyet-dinh-so-234-2003-qd-btc-ngay-30-12-2003-cua-bo-truong-bo-tai-chinh--83069",
        "expected_dieus": None,
    },
    {
        "number": "91/2022/NĐ-CP",
        "key": "91-2022",
        "url": "https://vbpl.vn/van-ban/chi-tiet/nghi-dinh-so-91-2022-nd-cp-sua-doi-bo-sung-mot-so-dieu-cua-nghi-dinh-so-126-2020-nd-cp-ngay-19-thang-10-nam-2020-cua-chinh-phu-quy-dinh-chi-tiet-mot-so-dieu-cua-luat-quan-ly-thue--157326",
        "expected_dieus": None,
    },
    {
        "number": "954/2020/UBTVQH14",
        "key": "954-2020",
        "url": "https://vbpl.vn/van-ban/chi-tiet/nghi-quyet-so-954-2020-ubtvqh14-ve-dieu-chinh-muc-giam-tru-gia-canh-cua-thue-thu-nhap-ca-nhan--ff496840-ab6a-11f1-a59b-956fac755bb4",
        "expected_dieus": None,
    },
    # Thêm 09/11/2026 (đợt tải VBPL bằng trình duyệt, tiếp): 8 văn bản còn
    # thiếu có toàn văn HTML trên VBPL (đã kiểm tra bằng extract_vbpl_html_text
    # từ trang chi tiết thật, không đoán URL). 4 văn bản còn lại của đợt này
    # (234/2003, 12/2005, 100/2005, 15/2006) KHÔNG có HTML đầy đủ nên đi
    # bằng --local-pdfs với file PDF đã convert (xem LOCAL_PDF_SOURCE_URLS
    # trong congbao_collector.py).
    {
        "number": "149/2001/QĐ-BTC",
        "key": "149-2001",
        "url": "https://vbpl.vn/van-ban/chi-tiet/quyet-dinh-so-149-2001-qd-btc-ve-viec-ban-hanh-va-cong-bo-bon-4-chuan-muc-ke-toan-viet-nam-dot-1--22028",
        "expected_dieus": None,
    },
    {
        "number": "165/2002/QĐ-BTC",
        "key": "165-2002",
        "url": "https://vbpl.vn/van-ban/chi-tiet/quyet-dinh-so-165-2002-qd-btc-ve-viec-ban-hanh-va-cong-bo-sau-06-chuan-muc-ke-toan-viet-nam-dot-2--21394",
        "expected_dieus": None,
    },
    {
        "number": "20/2006/TT-BTC",
        "key": "20-2006",
        "url": "https://vbpl.vn/van-ban/chi-tiet/thong-tu-so-20-2006-tt-btc-huong-dan-ke-toan-thuc-hien-sau-06-chuan-muc-ke-toan-ban-hanh-theo-quyet-dinh-so-12-2005-qd-btc-ngay-15-02-2005-cua-bo-truong-bo-tai-chinh--16244",
        "expected_dieus": None,
    },
    {
        "number": "21/2006/TT-BTC",
        "key": "21-2006",
        "url": "https://vbpl.vn/van-ban/chi-tiet/thong-tu-so-21-2006-tt-btc-huong-dan-ke-toan-thuc-hien-bon-04-chuan-muc-ke-toan-ban-hanh-theo-quyet-dinh-so-100-2005-qd-btc-ngay-28-12-2005-cua-bo-truong-bo-tai-chinh--16247",
        "expected_dieus": None,
    },
    {
        "number": "14/2008/QH12",
        "key": "14-2008",
        "url": "https://vbpl.vn/van-ban/chi-tiet/luat-thue-thu-nhap-doanh-nghiep-so-14-2008-qh12--12807",
        "expected_dieus": None,
    },
    {
        "number": "123/2012/TT-BTC",
        "key": "123-2012",
        "url": "https://vbpl.vn/van-ban/chi-tiet/thong-tu-so-123-2012-tt-btc-huong-dan-thi-hanh-mot-so-dieu-cua-luat-thue-thu-nhap-doanh-nghiep-so-14-2008-qh12-va-huong-dan-thi-hanh-nghi-dinh-so-124-2008-nd-cp-ngay-11-12-2008-nghi-dinh-so-122-2011-nd-cp-ngay-27-12-2011-cua-chinh-phu-quy-dinh-chi-tiet-thi-hanh-mot-so-dieu-cua-luat-thue-thu-nhap-doanh-nghiep--46367",
        "expected_dieus": None,
    },
    {
        "number": "26/2012/QH13",
        "key": "26-2012",
        "url": "https://vbpl.vn/van-ban/chi-tiet/luat-sua-doi-bo-sung-mot-so-dieu-cua-luat-thue-thu-nhap-ca-nhan-so-26-2012-qh13--28058",
        "expected_dieus": None,
    },
    {
        "number": "100/2008/NĐ-CP",
        "key": "100-2008",
        "url": "https://vbpl.vn/van-ban/chi-tiet/nghi-dinh-so-100-2008-nd-cp-quy-dinh-chi-tiet-mot-so-dieu-cua-luat-thue-thu-nhap-ca-nhan--24597",
        "expected_dieus": None,
    },
]

PROVENANCE = ("source: VBPL rendered HTML (vbpl.vn detail page, RSC payload); "
              "no PDF/DOC file exists for this document on VBPL")


def load_metadata() -> dict[str, dict]:
    meta = {}
    with open(LIST_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            meta[row["so_hieu"]] = row
    return meta


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip().casefold()


def verify(number: str, title: str, text: str, expected_dieus: int | None) -> dict:
    dieus = [int(m) for m in re.findall(r"^Điều\s+(\d+)\.", text, re.M)]
    checks = {
        "title_in_text": norm(title) in norm(text),
        "dieu1_present": 1 in dieus,
        "dieus_found": len(dieus),
        "dieu_max": max(dieus) if dieus else 0,
        "dieus_sequential": dieus == list(range(1, (max(dieus) + 1) if dieus else 1)),
        "expected_dieus": expected_dieus,
        "expected_match": (expected_dieus is None or
                           (dieus and max(dieus) == expected_dieus
                            and len(set(dieus)) == expected_dieus)),
    }
    return checks


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="SQLite DB path to write")
    ap.add_argument("--html-dir", default=None,
                    help="Directory with previously fetched page HTML "
                         "(files <key>.html); skips live GET")
    args = ap.parse_args(argv)

    meta_all = load_metadata()
    conn = init_db(Path(args.db))
    results = []
    try:
        for t in TARGETS:
            number = t["number"]
            row = meta_all.get(number)
            if row is None:
                print(f"[{number}] FAILED: no metadata row in 100-list CSV")
                results.append((number, "failed", "no metadata", 0, 0, {}))
                continue
            # Fresh HTTP client per page: reusing one keep-alive connection
            # through the egress proxy hangs on the 2nd+ VBPL page (read
            # timeouts), while fresh connections succeed (verified with curl).
            collector = Collector(Path(args.db))
            try:
                if args.html_dir:
                    html = (Path(args.html_dir) / f"{t['key']}.html").read_text(
                        encoding="utf-8", errors="replace")
                    text = extract_vbpl_html_text(html)
                else:
                    resp = collector.get(t["url"])
                    text = extract_vbpl_html_text(resp.text)
            except Exception as exc:  # noqa: BLE001 - per-doc failure, keep going
                print(f"[{number}] FAILED: {exc}")
                log_fetch(conn, number, t["url"], None, 0, 0, "failed", str(exc))
                results.append((number, "failed", str(exc), 0, 0, {}))
                continue
            finally:
                collector.close()
            checks = verify(number, row["ten_van_ban"], text, t["expected_dieus"])
            provisions = parse_provisions(text, number)
            dieus = sorted({p["dieu"] for p in provisions if p["dieu"]})
            meta = {
                "number": number,
                "title": row["ten_van_ban"],
                "source_url": t["url"],
                "issued_date": row["ngay_ban_hanh"] or None,
                "effective_date": row["ngay_hieu_luc"] or None,
            }
            action, _doc_id = store_document(conn, meta, text, row["nhom"])
            if action == "unchanged":
                # Same sha already stored: store_document left the old
                # provisions in place, so re-storing would duplicate rows
                # whose khoan/diem are NULL (NULLs are distinct in SQLite
                # UNIQUE constraints, so INSERT OR REPLACE never conflicts
                # on them). Skip the store; just count what is already there.
                n_prov = conn.execute(
                    "SELECT COUNT(*) FROM provisions WHERE document_number = ?",
                    (number,)).fetchone()[0]
            else:
                # 'inserted' or 'updated': store_document already deleted
                # old provisions on the 'updated' path, so a fresh store is safe.
                n_prov = store_provisions(conn, provisions)
            note = PROVENANCE + (f" | {t['note']}" if t.get("note") else "")
            log_fetch(conn, number, t["url"], None, 0, len(text), "ok", note)
            print(f"[{number}] OK ({action}): {len(text)} chars, "
                  f"{n_prov} provisions, {len(dieus)} Dieu "
                  f"({min(dieus) if dieus else '-'}-{max(dieus) if dieus else '-'}), "
                  f"checks={checks}")
            if t.get("note"):
                print(f"    NOTE: {t['note']}")
            results.append((number, "ok", "", len(text), n_prov, checks))
    finally:
        conn.close()
    ok = sum(1 for r in results if r[1] == "ok")
    print(f"\nIngested {ok}/{len(TARGETS)}")
    return 0 if ok == len(TARGETS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
