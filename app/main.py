import hashlib, json, os, re, sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timezone
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
# --- Chọn backend theo DATABASE_URL (Postgres / SQLite fallback) ---
# DATABASE_URL được đặt -> MỌI lượt đọc đi Postgres (Supabase) qua psycopg.
# Mỗi request mở MỘT connection mới ngắn hạn rồi đóng ngay: không có pool,
# không có connection ở cấp module — an toàn cho Vercel serverless, nơi mỗi
# invocation có thể chạy trên instance khác nhau. Import module KHÔNG bao giờ
# kết nối DB: DATABASE_URL sai/chết thì request trả 503, import vẫn thành công.
# DATABASE_URL không đặt -> fallback SQLite như cũ (DB_PATH, read-only như cũ).
DATABASE_URL=os.getenv('DATABASE_URL','').strip()
USE_PG=bool(DATABASE_URL)
if READ_WRITE and not USE_PG:
 DB.parent.mkdir(parents=True,exist_ok=True)
SOURCES={'congbao.chinhphu.vn','vbpl.vn','vanban.chinhphu.vn','mof.gov.vn','www.mof.gov.vn','gdt.gov.vn','www.gdt.gov.vn'}
# Trạng thái chuẩn của Postgres (bảng documents.status) và nguồn (documents.source).
STATUSES=('collected','source_verified','legal_verified')
PG_SOURCES=('congbao','vbpl','mof','gdt')
# review_status (SQLite/API cũ) <-> status (Postgres). API vẫn trả review_status
# để tương thích ngược: collected -> 'unverified' (chưa kiểm tra), còn
# source_verified / legal_verified giữ nguyên tên ở cả hai bên.
STATUS_TO_REVIEW={'collected':'unverified','source_verified':'source_verified','legal_verified':'legal_verified'}
REVIEW_TO_STATUS={'unverified':'collected','source_verified':'source_verified','legal_verified':'legal_verified','verified':'legal_verified'}
SOURCE_HOSTS={'congbao':('congbao.chinhphu.vn',),'vbpl':('vbpl.vn',),'mof':('mof.gov.vn','www.mof.gov.vn'),'gdt':('gdt.gov.vn','www.gdt.gov.vn')}
def _source_from_url(url):
 # Suy ra source từ hostname — chỉ dùng cho fallback SQLite (không có cột source).
 host=urlparse(url or '').hostname or ''
 for name,hosts in SOURCE_HOSTS.items():
  if host in hosts:return name
 return 'congbao' if 'congbao' in host else 'vbpl' if 'vbpl' in host else 'mof' if 'mof.gov.vn' in host else 'gdt' if 'gdt.gov.vn' in host else 'unknown'
app=FastAPI(title='KeToanAI MVP',version='0.3.0')
def conn():
 if READ_ONLY_DB:
  # mode=ro: không bao giờ ghi vào file DB đi kèm bản deploy.
  c=sqlite3.connect(f'file:{DB}?mode=ro',uri=True)
 else:
  c=sqlite3.connect(DB)
 c.row_factory=sqlite3.Row;return c
@contextmanager
def pg():
 # Một connection psycopg mới cho mỗi request, đóng ngay khi xong.
 # Import psycopg lười (lazy) để đường SQLite vẫn chạy được dù chưa cài psycopg.
 try:
  from psycopg import connect
  from psycopg.rows import dict_row
 except ImportError:
  raise HTTPException(503,'Chưa cài đặt psycopg')
 try:
  c=connect(DATABASE_URL,row_factory=dict_row)
 except Exception as e:
  raise HTTPException(503,f'Không kết nối được cơ sở dữ liệu Postgres: {e}')
 try:
  yield c
 except HTTPException:
  raise
 except Exception as e:
  # Lỗi SQL / mất kết nối giữa chừng -> 503, không crash app.
  raise HTTPException(503,f'Lỗi truy vấn Postgres: {e}')
 finally:
  c.close()
def init():
 # Production / chỉ đọc: DB seed đã có sẵn trong bản deploy thì không
 # làm gì cả — tuyệt đối không tạo bảng hay ghi file lúc chạy.
 # Đường Postgres không dùng file SQLite nên bỏ qua init này.
 if USE_PG:
  return
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
def _parse_date(value,label):
 # effective_from / effective_to phải là YYYY-MM-DD; so sánh chuỗi ISO cũng
 # là so sánh ngày đúng thứ tự, dùng được cho cả TEXT (SQLite) lẫn DATE (Postgres).
 if not value:
  return None
 if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):
  raise HTTPException(400,f'{label} phải có dạng YYYY-MM-DD')
 try:
  date.fromisoformat(value)
 except ValueError:
  raise HTTPException(400,f'{label} không phải ngày hợp lệ')
 return value
def _pg_like(q):
 # Escape cho ILIKE: \ % _ đều là ký tự đặc biệt -> phải escape rồi mới bọc %.
 # Tham số hóa (không nối chuỗi vào SQL) nên an toàn trước SQL injection.
 return '%'+q.replace('\\','\\\\').replace('%','\\%').replace('_','\\_')+'%'

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
def docs(q:str='',category:str='',status:str='',source:str='',effective_from:str='',effective_to:str='',limit:int=Query(100,ge=1,le=500),offset:int=Query(0,ge=0)):
 ef_from=_parse_date(effective_from,'effective_from');ef_to=_parse_date(effective_to,'effective_to')
 if USE_PG:
  conds=[];params=[]
  if q:
   conds.append('(number ILIKE %s ESCAPE \'\\\' OR title ILIKE %s ESCAPE \'\\\' OR extracted_text ILIKE %s ESCAPE \'\\\')');params+= [_pg_like(q)]*3
  if category:
   conds.append('category=%s');params.append(category)
  if status:
   conds.append('status=%s');params.append(status)
  if source:
   conds.append('source=%s');params.append(source)
  if ef_from:
   conds.append('effective_date>=%s');params.append(ef_from)
  if ef_to:
   conds.append('effective_date<=%s');params.append(ef_to)
  where='WHERE '+(' AND '.join(conds)) if conds else ''
  sql=f'SELECT id,number,title,category,source_url,issued_date,effective_date,fetched_at,status,source,length(COALESCE(extracted_text,\'\')) AS text_length FROM documents {where} ORDER BY id DESC LIMIT %s OFFSET %s'
  with pg() as c:
   rows=c.execute(sql,params+[limit,offset]).fetchall()
  out=[]
  for r in rows:
   d=dict(r);d['review_status']=STATUS_TO_REVIEW.get(d.get('status'),'unverified');out.append(d)
  return out
 conds=[];params=[]
 if q:
  conds.append('(number LIKE ? OR title LIKE ? OR extracted_text LIKE ?)');params+= ['%'+q+'%']*3
 if category:
  conds.append('category=?');params.append(category)
 if status:
  # SQLite không có cột status: map ngược sang review_status.
  conds.append('review_status=?');params.append(STATUS_TO_REVIEW.get(status,status))
 if source:
  hosts=SOURCE_HOSTS.get(source,())
  if hosts:
   conds.append('('+' OR '.join(['source_url LIKE ?']*len(hosts))+')');params+= ['%'+h+'%' for h in hosts]
  else:
   conds.append('1=0')
 if ef_from:
  conds.append('effective_date>=?');params.append(ef_from)
 if ef_to:
  conds.append('effective_date<=?');params.append(ef_to)
 where='WHERE '+(' AND '.join(conds)) if conds else ''
 with conn() as c:
  rows=c.execute(f'SELECT id,number,title,category,source_url,issued_date,effective_date,fetched_at,review_status,length(extracted_text) AS text_length FROM documents {where} ORDER BY id DESC LIMIT ? OFFSET ?',params+[limit,offset]).fetchall()
 out=[]
 for row in rows:
  d=dict(row);d['status']=REVIEW_TO_STATUS.get(d.get('review_status','unverified'),'collected');d['source']=_source_from_url(d.get('source_url',''));out.append(d)
 return out
@app.get('/api/documents/{id}')
def detail(id:int):
 if USE_PG:
  with pg() as c:
   row=c.execute('SELECT id,number,title,category,source_url,issued_date,effective_date,extracted_text,sha256,fetched_at,status,source FROM documents WHERE id=%s',(id,)).fetchone()
  if not row:raise HTTPException(404,'Không tìm thấy')
  d=dict(row);d['review_status']=STATUS_TO_REVIEW.get(d.get('status'),'unverified');return d
 with conn() as c: row=c.execute('SELECT * FROM documents WHERE id=?',(id,)).fetchone()
 if not row:raise HTTPException(404,'Không tìm thấy')
 d=dict(row);d['status']=REVIEW_TO_STATUS.get(d.get('review_status','unverified'),'collected');d['source']=_source_from_url(d.get('source_url',''));return d
@app.get('/api/documents/{id}/provisions')
def provisions(id:int):
 if not USE_PG:
  # 501, không fake: file SQLite của MVP không có bảng provisions —
  # provisions nằm ở DB của collector, chỉ có sau khi migrate sang Postgres.
  raise HTTPException(501,'Provisions chỉ có trên Postgres; bản SQLite fallback không chứa bảng provisions')
 with pg() as c:
  doc=c.execute('SELECT id FROM documents WHERE id=%s',(id,)).fetchone()
  if not doc:raise HTTPException(404,'Không tìm thấy')
  rows=c.execute('SELECT id,chuong,muc,dieu,khoan,diem,ordinal,text,confidence FROM provisions WHERE document_id=%s ORDER BY ordinal ASC',(id,)).fetchall()
 return [dict(r) for r in rows]
@app.post('/api/documents')
def create(d:Doc):
 require_write()
 if USE_PG:
  # Ghi Postgres đi qua collector/migration, không qua API công khai —
  # kể cả khi READ_WRITE=1 (không có đường ghi Postgres trong app này).
  raise HTTPException(501,'Postgres: ghi văn bản đi qua collector/migration, không qua API này')
 return {'id':upsert(d),'status':'saved_unverified'}
@app.post('/api/fetch')
def fetch(req:Fetch):
 require_write()
 if USE_PG:
  # Như POST /api/documents: fetch trực tiếp vào Postgres không được hỗ trợ.
  raise HTTPException(501,'Postgres: fetch văn bản đi qua collector/migration, không qua API này')
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
 if USE_PG:
  with pg() as c:
   row=c.execute("SELECT COUNT(*) AS documents, COUNT(*) FILTER (WHERE extracted_text IS NOT NULL AND extracted_text<>'') AS with_text, COUNT(*) FILTER (WHERE status='legal_verified') AS verified FROM documents").fetchone()
   by_status={s:0 for s in STATUSES}
   for r in c.execute('SELECT status,COUNT(*) AS n FROM documents GROUP BY status'):
    if r['status'] in by_status:by_status[r['status']]=r['n']
   by_source={s:0 for s in PG_SOURCES}
   for r in c.execute('SELECT source,COUNT(*) AS n FROM documents GROUP BY source'):
    if r['source'] in by_source:by_source[r['source']]=r['n']
  return {'documents':row['documents'],'with_text':row['with_text'],'verified':row['verified'],'by_status':by_status,'by_source':by_source}
 with conn() as c:
  documents=c.execute('SELECT COUNT(*) FROM documents').fetchone()[0]
  with_text=c.execute("SELECT COUNT(*) FROM documents WHERE extracted_text!=''").fetchone()[0]
  # by_status/by_source trên SQLite chỉ là suy ra (best effort): status map từ
  # review_status, source map từ hostname của source_url — xem _source_from_url.
  by_status={s:0 for s in STATUSES}
  for rs,n in c.execute('SELECT review_status,COUNT(*) FROM documents GROUP BY review_status'):
   key=REVIEW_TO_STATUS.get(rs,'collected')
   by_status[key]=by_status.get(key,0)+n
  by_source={s:0 for s in PG_SOURCES}
  for (url,) in c.execute('SELECT source_url FROM documents'):
   src=_source_from_url(url)
   if src in by_source:by_source[src]+=1
  # ĐỔI NGỮ NGHĨA so với bản cũ: 'verified' trước đây đếm
  # review_status='verified' (không dòng nào có giá trị đó); giờ verified =
  # số legal_verified (Postgres) / review_status='legal_verified' (SQLite).
  return {'documents':documents,'with_text':with_text,'verified':by_status['legal_verified'],'by_status':by_status,'by_source':by_source}
