"""Vercel serverless entrypoint cho KeToanAI MVP (FastAPI).

Vercel đặt VERCEL=1 nên app.main tự chạy chế độ chỉ đọc và mở SQLite
dạng mode=ro — dữ liệu là file seed đi kèm bản deploy, không ghi lúc chạy.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.main import app  # noqa: F401  (Vercel dùng biến `app` này)
