import os
# Seed chỉ chạy local: bật ghi trước khi import app (mặc định app chỉ đọc).
os.environ['READ_WRITE'] = '1'
from app.main import Doc,upsert
# Metadata checked against the official Công báo page; not full-text ingestion.
d=Doc(number='99/2025/TT-BTC',title='Thông tư hướng dẫn Chế độ kế toán doanh nghiệp',category='Kế toán',source_url='https://congbao.chinhphu.vn/van-ban/thong-tu-so-99-2025-tt-btc-46529.htm',issued_date='2025-10-27',effective_date='2026-01-01')
print('Seeded document id:',upsert(d))
