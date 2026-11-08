# KeToanAI MVP — kho văn bản kế toán & thuế (bản chỉ đọc)

MVP thực tế gồm: giao diện tiếng Việt responsive, API FastAPI, SQLite, tìm kiếm văn bản, chống trùng theo URL, checksum, nguồn trích dẫn và nhãn `unverified`. Bản công khai **chỉ đọc**: không có form thêm văn bản, các API ghi trả về 403.

## Deploy công khai (Vercel)

- Project Vercel: **ketoanai** — domain production **https://ketoanai-silk.vercel.app** (tài khoản crazy4gemini-3624, Hobby). Lưu ý: `ketoanai.vercel.app` thuộc về một app khác, không phải của dự án này.
- Repo GitHub đề xuất: **phamtuananhdevops/ketoanai** (đã kiểm tra 08/11/2026: URL github.com/phamtuananhdevops/ketoanai trả 404 công khai, tức không trùng repo công khai nào; lúc tạo repo cần kiểm tra lại trong giao diện GitHub vì repo private cùng tên cũng trả 404).
- Cấu trúc deploy: `vercel.json` trỏ mọi request về `api/index.py` (FastAPI app, runtime `@vercel/python`); `requirements.txt` ở root được Vercel cài tự động. **Không cần biến môi trường nào** — Vercel tự đặt `VERCEL=1` nên app chạy chế độ chỉ đọc và mở SQLite dạng `mode=ro`.
- Dữ liệu là file `data/documents.db` đi kèm bản deploy (`includeFiles` trong `vercel.json` đảm bảo file này có trong function). Dữ liệu là tĩnh cho tới khi deploy bản mới — không có ghi lúc chạy, không cần backup runtime.
- Import vào Vercel: Dashboard → Add New → Project → Import repo `phamtuananhdevops/ketoanai` → Framework Preset chọn **Other** (FastAPI/Python, không phải Next.js) → không thêm biến môi trường → Deploy. Kiểm tra sau deploy: mở https://ketoanai-silk.vercel.app, `/api/stats` phải trả `documents: 1`, và POST `/api/documents` phải trả 403.

## Chạy local

Chế độ mặc định là **chỉ đọc** (không cần đặt biến gì):

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Mở http://127.0.0.1:8000 và tài liệu API tại http://127.0.0.1:8000/docs.

Chế độ ghi (chỉ dùng local, trên bản copy của DB — **không bao giờ ghi vào file seed đi kèm**):

```bash
cp data/documents.db /tmp/ketoanai-test.db
READ_WRITE=1 DB_PATH=/tmp/ketoanai-test.db uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Seed lại từ đầu (tạo DB mới rồi chạy `seed.py` — file này tự bật `READ_WRITE=1`):

```bash
rm data/documents.db
python seed.py
```

## API

Đọc (luôn mở): `GET /`, `GET /api/documents?q=&category=`, `GET /api/documents/{id}`, `GET /api/stats`.

Ghi (chỉ khi `READ_WRITE=1`, mặc định trả 403 với thông báo tiếng Việt): `POST /api/documents`, `POST /api/fetch`. Không cần đăng nhập vì bản công khai tắt hẳn hai endpoint này.

## Nguồn dữ liệu

- https://vbpl.vn/
- https://congbao.chinhphu.vn/
- https://vanban.chinhphu.vn/
- https://www.mof.gov.vn/
- https://www.gdt.gov.vn/

Metadata mẫu cho Thông tư 99/2025/TT-BTC được xác minh qua trang Công báo: https://congbao.chinhphu.vn/van-ban/thong-tu-so-99-2025-tt-btc-46529.htm.

## Giới hạn quan trọng (đã biết, chưa sửa)

- Seed chỉ chứa **1 văn bản** (TT99), **chưa có toàn văn**: lần fetch thử Công báo chỉ lấy được 1.735 ký tự menu điều hướng, vì toàn văn nằm trong file PDF/DOCX tải về. VBPL là SPA render bằng JavaScript nên HTML parser hiện tại không lấy được nội dung.
- HTML collector là thử nghiệm theo URL được nhập thủ công, **không phải crawler chạy liên tục**. Không tự theo redirect, không tải PDF, chưa xử lý JavaScript-rendered pages, chưa chuẩn hóa điều/khoản, chưa xác minh quan hệ hiệu lực.
- Đã sửa lỗi `httpx.Client()` crash `InvalidURL: ':1]'` khi `NO_PROXY` chứa IPv6 `[::1]` bằng `trust_env=False` (giữ nguyên User-Agent). Lỗi này chỉ ảnh hưởng đường fetch local, không ảnh hưởng bản chỉ đọc.
- Chưa có RAG, AI phân tích, lịch chạy, email cảnh báo hay kiểm duyệt chuyên gia.
- Robots.txt (kiểm tra 08/11/2026): VBPL cho phép `/` nhưng cấm `/api/` và `/Pages/`, sitemap tại `vbpl.vn/sitemap.xml` — collector VBPL **không được gọi API nội bộ** của VBPL; Công báo cho phép `/`.
- Phải kiểm tra điều kiện khai thác tự động và giới hạn truy cập của từng nguồn trước khi chạy ở quy mô lớn.
- Không dùng dữ liệu/đánh giá hiệu lực trong bản thử nghiệm để đưa ra quyết định pháp lý.

## Các bước tiếp theo

1. Xây collector riêng cho Công báo (tải PDF/DOCX) và VBPL (qua sitemap/trang công khai, không gọi `/api/`), có pagination và incremental discovery.
2. Thêm parser PDF/DOC, chuẩn hóa điều khoản, quan hệ sửa đổi và kiểm tra hiệu lực.
3. Chuyển SQLite sang PostgreSQL, thêm RBAC, audit log, rate limiting, backups.
4. Bổ sung RAG có dẫn chứng và quy trình duyệt nội dung AI.
