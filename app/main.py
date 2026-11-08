import hashlib, json, os, re, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, HttpUrl

ROOT=Path(__file__).resolve().parents[1]
DB=Path(os.getenv('DB_PATH',str(ROOT/'data'/'documents.db')))
# Chế độ ghi: chỉ bật khi READ_WRITE=1 (môi trường local). Mặc định là
# chỉ đọc — bản deploy công khai trên Vercel luôn chạy chỉ đọc.
READ_WRITE=os.getenv('READ_WRITE','').strip()=='1'
# Vercel đặt VERCEL=1 khi chạy serverless; hệ file ở đó cũng chỉ đọc
# được ngoài /tmp, nên SQLite phải mở read-only.
ON_VERCEL=os.getenv('VERCEL','').strip()=='1'
READ_ONLY_DB=ON_VERCEL or not READ_WRITE
if READ_WRITE:
 DB.parent.mkdir(parents=True,exist_ok=True)
SOURCES={'congbao.chinhphu.vn','vbpl.vn','vanban.chinhphu.vn','mof.gov.vn','www.mof.gov.vn','gdt.gov.vn','www.gdt.gov.vn'}
app=FastAPI(title='KeToanAI MVP',version='0.2.0')
def conn():
 if READ_ONLY_DB:
  # mode=ro: không bao giờ ghi vào file DB đi kèm bản deploy.
  c=sqlite3.connect(f'file:{DB}?mode=ro',uri=True)
 else:
  c=sqlite3.connect(DB)
 c.row_factory=sqlite3.Row;return c
def init():
 # Production / chỉ đọc: DB seed đã có sẵn trong bản deploy thì không
 # làm gì cả — tuyệt đối không tạo bảng hay ghi file lúc chạy.
 if DB.exists():
  return
 if not READ_WRITE:
  raise RuntimeError(f'Không tìm thấy cơ sở dữ liệu seed: {DB}')
 with conn() as c:
  c.execute('''CREATE TABLE IF NOT EXISTS documents (id INTEGER PRIMARY KEY, number TEXT, title TEXT NOT NULL, category TEXT NOT NULL, source_url TEXT NOT NULL UNIQUE, issued_date TEXT, effective_date TEXT, extracted_text TEXT NOT NULL DEFAULT '', sha256 TEXT NOT NULL DEFAULT '', fetched_at TEXT, review_status TEXT NOT NULL DEFAULT 'unverified')''')
  c.execute('CREATE INDEX IF NOT EXISTS idx_docs_number ON documents(number)')
init()
def require_write():
 if not READ_WRITE:
  raise HTTPException(403,'Chế độ chỉ đọc: bản công khai không nhận thêm văn bản. Dữ liệu do quản trị viên cập nhật qua bản deploy mới.')
class Doc(BaseModel):
 number:str='';title:str;category:str='Kế toán';source_url:HttpUrl;issued_date:str|None=None;effective_date:str|None=None
class Fetch(BaseModel):
 url:HttpUrl;category:str='Kế toán'
def allowed(url):
 p=urlparse(str(url));return p.scheme=='https' and p.hostname in SOURCES and not p.username and not p.password

def upsert(d,text='',fetched=False):
 require_write()
 payload=d.model_dump();url=str(payload.pop('source_url'))
 if not allowed(url):raise HTTPException(400,'Chỉ chấp nhận URL HTTPS từ nguồn chính thức đã cho phép')
 payload['source_url']=url;payload['extracted_text']=text[:300000];payload['sha256']=hashlib.sha256(text.encode()).hexdigest() if text else ''
 payload['fetched_at']=datetime.now(timezone.utc).isoformat() if fetched else None
 payload['review_status']='unverified'
 with conn() as c:
  c.execute('''INSERT INTO documents(number,title,category,source_url,issued_date,effective_date,extracted_text,sha256,fetched_at,review_status) VALUES(:number,:title,:category,:source_url,:issued_date,:effective_date,:extracted_text,:sha256,:fetched_at,:review_status) ON CONFLICT(source_url) DO UPDATE SET number=excluded.number,title=excluded.title,category=excluded.category,issued_date=COALESCE(excluded.issued_date,documents.issued_date),effective_date=COALESCE(excluded.effective_date,documents.effective_date),extracted_text=CASE WHEN excluded.extracted_text!='' THEN excluded.extracted_text ELSE documents.extracted_text END,sha256=CASE WHEN excluded.sha256!='' THEN excluded.sha256 ELSE documents.sha256 END,fetched_at=COALESCE(excluded.fetched_at,documents.fetched_at),review_status='unverified' ''',payload)
  return c.execute('SELECT id FROM documents WHERE source_url=?',(url,)).fetchone()['id']
@app.get('/')
def home():return FileResponse(ROOT/'app'/'static'/'index.html')
@app.get('/api/documents')
def docs(q:str='',category:str='',limit:int=Query(100,ge=1,le=500)):
 with conn() as c:
  rows=c.execute('SELECT id,number,title,category,source_url,issued_date,effective_date,fetched_at,review_status,length(extracted_text) AS text_length FROM documents WHERE (number LIKE ? OR title LIKE ? OR extracted_text LIKE ?) AND (?="" OR category=?) ORDER BY id DESC LIMIT ?',('%'+q+'%',)*3+(category,category,limit)).fetchall()
 return [dict(x) for x in rows]
@app.get('/api/documents/{id}')
def detail(id:int):
 with conn() as c: row=c.execute('SELECT * FROM documents WHERE id=?',(id,)).fetchone()
 if not row:raise HTTPException(404,'Không tìm thấy')
 return dict(row)
@app.post('/api/documents')
def create(d:Doc):
 require_write()
 return {'id':upsert(d),'status':'saved_unverified'}
@app.post('/api/fetch')
def fetch(req:Fetch):
 require_write()
 if not allowed(req.url):raise HTTPException(400,'Nguồn không được phép')
 try:
  # trust_env=False: httpx không đọc proxy/no_proxy của môi trường nên
  # không crash InvalidURL ':1]' khi NO_PROXY chứa IPv6 [::1].
  with httpx.Client(timeout=15,follow_redirects=False,trust_env=False,headers={'User-Agent':'KeToanAI-Research-MVP/0.1 (manual ingestion)'}) as client:
   response=client.get(str(req.url));response.raise_for_status()
  if response.is_redirect:raise ValueError('Không tự động theo redirect')
  if len(response.content)>3_000_000:raise ValueError('Nội dung quá lớn')
  if 'html' not in response.headers.get('content-type','').lower():raise ValueError('MVP chỉ parse HTML; PDF cần bước xử lý riêng')
  soup=BeautifulSoup(response.text,'html.parser')
  for tag in soup(['script','style','nav','footer','header']):tag.decompose()
  title=(soup.title.get_text(' ',strip=True) if soup.title else 'Văn bản chưa xác định')[:500]
  main=soup.select_one('main, article, .content, #content') or soup.body or soup
  content=main.get_text('\n',strip=True)
  number_match=re.search(r'\b\d{1,4}/20\d{2}/(?:TT|NĐ|QĐ|QH|VBHN)[-\w]*',title+' '+content[:2000],re.I)
  doc=Doc(number=number_match.group(0) if number_match else '',title=title,category=req.category,source_url=req.url)
  return {'id':upsert(doc,content,True),'title':title,'characters':len(content),'status':'fetched_unverified','warning':'Văn bản chưa được kiểm tra hiệu lực; HTML có thể chứa nội dung điều hướng'}
 except (httpx.HTTPError,ValueError) as e:raise HTTPException(422,str(e))
@app.get('/api/stats')
def stats():
 with conn() as c:
  return {'documents':c.execute('SELECT COUNT(*) FROM documents').fetchone()[0], 'with_text':c.execute("SELECT COUNT(*) FROM documents WHERE extracted_text!=''").fetchone()[0], 'verified':c.execute("SELECT COUNT(*) FROM documents WHERE review_status='verified'").fetchone()[0]}
