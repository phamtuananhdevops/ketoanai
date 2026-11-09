import hashlib, json, os, re, sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
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
app=FastAPI(title='KeToanAI',version='0.5.3')
# --- v0.5.0: AI hỏi đáp có trích dẫn (RAG-lite) + hồ sơ doanh nghiệp ---
# Truy xuất = khớp từ khóa trên bảng provisions (chưa có vector DB — giống
# RAG-lite của các site CPA). AI chỉ được trả lời từ đoạn văn bản truy xuất
# được, kèm trích dẫn đúng số hiệu + Điều/Khoản; không có đoạn nào phù hợp
# thì nói thẳng là không tìm thấy. Không có AI key thì endpoint vẫn trả về
# các đoạn văn bản liên quan (status 'no_key') — kho văn bản tự nó đã hữu ích.
ASK_STOPWORDS={'là','của','và','có','không','khi','cho','với','trong','các','những','một','này','đó','thì','mà','để','theo','về','tại','từ','đến','nếu','như','hay','hoặc','gì','nào','bao','nhiêu','thế','sao','ạ','ơi','nhé','anh','chị','tôi','tao','mày','mình','cho','hỏi','xin','làm','sao','thế','nào','vậy','được','không','có','thể'}
OPENAI_API_KEY=os.getenv('OPENAI_API_KEY','').strip()
GEMINI_API_KEY=os.getenv('AI_KEY','').strip() or os.getenv('GEMINI_API_KEY','').strip()
# --- v0.5.1: Muse làm AI chính (Andy, 09/11) ---
# Meta Model API (dev.meta.ai): OpenAI-compatible, base https://api.meta.ai/v1,
# model muse-spark-1.3 (phiên bản frontier hiện tại; ví dụ của Meta dùng
# muse-spark-1.1). Key lấy tại dev.meta.ai, đặt vào Vercel với tên MUSE_API_KEY
# (chấp nhận cả MODEL_API_KEY — tên gốc của Meta). Thứ tự provider:
# Muse -> OpenAI (gpt-4o) -> Gemini; provider nào lỗi thì thử tiếp provider sau.
def _env_first(*names):
 for n in names:
  v=os.getenv(n,'').strip()
  if v:return v
 return ''
MUSE_API_KEY=_env_first('MUSE_API_KEY','MODEL_API_KEY')
MUSE_MODEL='muse-spark-1.3'
# Các site CPA dùng tên OpenAI_KEY — chấp nhận cả hai tên cho provider OpenAI.
OPENAI_API_KEY=OPENAI_API_KEY or _env_first('OpenAI_KEY')
def _keywords(question):
 # Tách từ khóa tiếng Việt: chữ thường, bỏ stopword, tối đa 8 từ.
 toks=re.findall(r'[a-zà-ỹ0-9]+',question.lower())
 seen=[]
 for t in toks:
  if len(t)>=2 and t not in ASK_STOPWORDS and t not in seen:seen.append(t)
 return seen[:8]
def _like_pat(kw):
 return '%'+kw+'%'
def _retrieve(question,profile_cats=None,limit=12):
 kws=_keywords(question)
 if not kws:return [],[]
 # Cụm 2 từ liên tiếp trong câu hỏi ('tiếp khách', 'thời điểm lập') là tín
 # hiệu mạnh nhất: cộng điểm lớn khi provision chứa nguyên cụm. Từ càng hiếm
 # (tần suất thấp trong toàn bộ provisions) càng có trọng số cao — 'thuế'
 # (9.328 dòng) không được át 'khách' (572 dòng). Điều khoản thật (có số
 # Điều, ngắn) được cộng điểm so với phụ lục dài không số Điều.
 bigrams=[' '.join(kws[i:i+2]) for i in range(len(kws)-1)]
 if USE_PG:
  with pg() as c:
   # psycopg dict_row: SELECT SUM(...), SUM(...) không đặt tên sẽ gộp hết
   # thành một cột 'sum' duy nhất -> phải đặt alias riêng từng cột (s0, s1...)
   # rồi đọc theo alias; lặp trực tiếp trên dict chỉ ra tên cột (str) -> lỗi
   # TypeError ở bước tính trọng số (đây là lỗi 500 trên live, SQLite không
   # bị vì Row trả tuple theo vị trí).
   row=c.execute('SELECT '+','.join([f'SUM(CASE WHEN text ILIKE %s THEN 1 ELSE 0 END) AS s{i}' for i in range(len(kws)+len(bigrams))])+' FROM provisions',[_like_pat(k) for k in kws]+[_like_pat(b) for b in bigrams]).fetchone()
  dfs=[row[f's{i}'] or 0 for i in range(len(kws)+len(bigrams))]
  w=[1/((1+df/100)**0.5) for df in dfs[:len(kws)]]
  wb=[4/((1+df/10)**0.5) for df in dfs[len(kws):]]
  def _case(col,weight):return f'(CASE WHEN {col} ILIKE %s THEN {weight:.4f} ELSE 0 END)'
  score='+'.join([_case('p.text',w[i]) for i in range(len(kws))]+[_case('p.text',wb[i]) for i in range(len(bigrams))]+['(CASE WHEN p.dieu IS NOT NULL THEN 2 ELSE 0 END)','(CASE WHEN length(p.text)<3000 THEN 1 WHEN length(p.text)>20000 THEN -8 ELSE 0 END)'])
  title='+'.join([_case('d.title',w[i]) for i in range(len(kws)) if len(kws[i])>=4]) or '0'
  where=' OR '.join(['p.text ILIKE %s']*(len(kws)+len(bigrams)))
  cat_boost=', (CASE WHEN d.category = ANY(%s) THEN 1 ELSE 0 END) AS catb' if profile_cats else ', 0 AS catb'
  # ORDER BY không được tham chiếu alias score/tscore/catb của chính SELECT
  # đó trên Postgres (lỗi 'column score does not exist' trên live) — bọc
  # truy vấn trong subquery để score/tscore/catb thành cột thật rồi mới sắp
  # xếp ở truy vấn ngoài. Tham số không đổi (chỉ có trong subquery).
  sql=f'SELECT document_id,number,title,category,issued_date,effective_date,dieu,khoan,diem,text FROM (SELECT p.document_id,d.number,d.title,d.category,d.issued_date,d.effective_date,p.dieu,p.khoan,p.diem,p.text,({score}) AS score,({title}) AS tscore{cat_boost},p.ordinal AS ord FROM provisions p JOIN documents d ON d.id=p.document_id WHERE {where}) s ORDER BY score+tscore*2+catb DESC, effective_date DESC NULLS LAST, ord ASC LIMIT 20'
  params=[_like_pat(k) for k in kws]+[_like_pat(b) for b in bigrams]+[_like_pat(k) for k in kws if len(k)>=4]+[_like_pat(k) for k in kws]+[_like_pat(b) for b in bigrams]+([profile_cats] if profile_cats else [])
  with pg() as c:rows=c.execute(sql,params).fetchall()
  out=[]
  for r in rows:
   d=dict(r);d['text']=(d.get('text') or '')[:1200];d.pop('score',None);d.pop('tscore',None);d.pop('catb',None);out.append(d)
  out=out[:limit]
  # Quy tắc chung của một Điều thường nằm ở Khoản 1, 2 — nhưng chúng hay
  # viết gọn ('hóa đơn' thay vì 'hóa đơn điện tử') nên khớp từ khóa kém hơn
  # các trường hợp riêng ở Khoản 4, bị xếp sau (vd Điều 9 NĐ 254/2026 Khoản 1
  # xếp hạng 18 dù là quy tắc chung). Lấy thêm Khoản 1, 2 ngắn của tối đa
  # 3 Điều đứng đầu, đặt lên trước để AI có quy tắc chung.
  seen={(d['number'],d.get('dieu'),d.get('khoan'),d.get('diem')) for d in out}
  pairs=[]
  for d in out:
   k=(d['number'],d.get('dieu'))
   if d.get('dieu') is not None and k not in pairs:pairs.append(k)
  extra=[]
  with pg() as c:
   for number,dieu in pairs[:3]:
    for r in c.execute('SELECT d.id AS document_id,d.number,d.title,d.category,d.issued_date,d.effective_date,p.dieu,p.khoan,p.diem,p.text FROM provisions p JOIN documents d ON d.id=p.document_id WHERE d.number=%s AND p.dieu=%s AND p.khoan IN (1,2) AND p.diem IS NULL AND length(p.text)<1500 ORDER BY p.khoan',(number,dieu)).fetchall():
     dd=dict(r);key=(dd['number'],dd.get('dieu'),dd.get('khoan'),dd.get('diem'))
     if key in seen:continue
     seen.add(key);dd['text']=(dd.get('text') or '')[:1200];extra.append(dd)
  out=extra+out
  return out[:limit],kws
 # SQLite fallback: LIKE chỉ không phân biệt hoa/thường với ASCII nên thử
 # thêm biến thể viết hoa chữ cái đầu (vd 'điều'/'Điều'); bảng provisions
 # của collector dùng document_number + noi_dung (không có document_id/text
 # như Postgres) nên JOIN theo số hiệu.
 with conn() as c:
  dfs=c.execute('SELECT '+','.join(['SUM(CASE WHEN noi_dung LIKE ? OR noi_dung LIKE ? THEN 1 ELSE 0 END)']*(len(kws)+len(bigrams)))+' FROM provisions',sum([[_like_pat(k),_like_pat('%'+k[:1].upper()+k[1:]+'%')] for k in kws+bigrams],[])).fetchone()
 dfs=[d or 0 for d in dfs]
 w=[1/((1+df/100)**0.5) for df in dfs[:len(kws)]]
 wb=[4/((1+df/10)**0.5) for df in dfs[len(kws):]]
 # Pattern chỉ gồm chữ/số (từ _keywords) + % nên nối trực tiếp vào CASE là
 # an toàn; tham số hóa chỉ dùng cho WHERE.
 def _case2(col,pat,weight):
  p2='%'+pat[1:2].upper()+pat[2:]
  return f'(CASE WHEN {col} LIKE \'{pat}\' OR {col} LIKE \'{p2}\' THEN {weight:.4f} ELSE 0 END)'
 score='+'.join([_case2('p.noi_dung',_like_pat(k),w[i]) for i,k in enumerate(kws)]+[_case2('p.noi_dung',_like_pat(b),wb[i]) for i,b in enumerate(bigrams)]+['(CASE WHEN p.dieu IS NOT NULL THEN 2 ELSE 0 END)','(CASE WHEN length(p.noi_dung)<3000 THEN 1 WHEN length(p.noi_dung)>20000 THEN -8 ELSE 0 END)'])
 title='+'.join([_case2('d.title',_like_pat(k),w[i]) for i,k in enumerate(kws) if len(k)>=4]) or '0'
 where=' OR '.join(['(p.noi_dung LIKE ? OR p.noi_dung LIKE ?)']*(len(kws)+len(bigrams)))
 cat_boost=', (CASE WHEN d.category IN (%s) THEN 1 ELSE 0 END) AS catb' % ','.join(['?']*len(profile_cats)) if profile_cats else ', 0 AS catb'
 sql=f'SELECT document_id,number,title,category,issued_date,effective_date,dieu,khoan,diem,text FROM (SELECT d.id AS document_id,d.number,d.title,d.category,d.issued_date,d.effective_date,p.dieu,p.khoan,p.diem,p.noi_dung AS text,({score}) AS score,({title}) AS tscore{cat_boost},p.ordinal AS ord FROM provisions p JOIN documents d ON d.number=p.document_number WHERE {where}) s ORDER BY score+tscore*2+catb DESC, effective_date DESC, ord ASC LIMIT 20'
 pats2=[]
 for k in kws+bigrams:pats2+= [_like_pat(k),_like_pat('%'+k[:1].upper()+k[1:]+'%')]
 params=pats2+((profile_cats or []))
 with conn() as c:rows=c.execute(sql,params).fetchall()
 out=[]
 for r in rows:
  d=dict(r);d['text']=(d.get('text') or '')[:1200];d.pop('score',None);d.pop('tscore',None);d.pop('catb',None);out.append(d)
 out=out[:limit]
 # Như nhánh Postgres: lấy thêm Khoản 1, 2 ngắn của tối đa 3 Điều đứng đầu.
 seen={(d['number'],d.get('dieu'),d.get('khoan'),d.get('diem')) for d in out}
 pairs=[]
 for d in out:
  k=(d['number'],d.get('dieu'))
  if d.get('dieu') is not None and k not in pairs:pairs.append(k)
 extra=[]
 with conn() as c:
  for number,dieu in pairs[:3]:
   for r in c.execute('SELECT d.id AS document_id,d.number,d.title,d.category,d.issued_date,d.effective_date,p.dieu,p.khoan,p.diem,p.noi_dung AS text FROM provisions p JOIN documents d ON d.number=p.document_number WHERE d.number=? AND p.dieu=? AND p.khoan IN (1,2) AND p.diem IS NULL AND length(p.noi_dung)<1500 ORDER BY p.khoan',(number,dieu)).fetchall():
    dd=dict(r);key=(dd['number'],dd.get('dieu'),dd.get('khoan'),dd.get('diem'))
    if key in seen:continue
    seen.add(key);dd['text']=(dd.get('text') or '')[:1200];extra.append(dd)
 out=extra+out
 return out[:limit],kws
def _ask_prompt(question,sources,profile):
 ctx=[]
 for i,s in enumerate(sources,1):
  ref=f"[{i}] {s.get('number')} — Điều {s.get('dieu') or ''}{'.'+str(s['khoan']) if s.get('khoan') else ''}{') '+str(s['diem']) if s.get('diem') else ''} (hiệu lực từ {s.get('effective_date') or 'chưa xác minh'})"
  ctx.append(ref+'\n'+(s.get('text') or '')[:800])
 prof=''
 if profile:
  bits=[]
  if profile.get('business_type'):bits.append('loại hình: '+profile['business_type'])
  if profile.get('vat_method'):bits.append('GTGT: '+profile['vat_method'])
  if profile.get('filing_period'):bits.append('kỳ kê khai: '+profile['filing_period'])
  if profile.get('industry'):bits.append('ngành: '+profile['industry'])
  if bits:prof='\nNgười hỏi là kế toán của doanh nghiệp ('+', '.join(bits)+'). Trả lời theo đúng hoàn cảnh này khi đoạn văn bản cho phép phân biệt.'
 return ('Bạn là trợ lý kế toán – thuế Việt Nam. Chỉ trả lời dựa trên các đoạn văn bản pháp luật dưới đây. '
 'Mỗi ý trả lời phải kèm trích dẫn dạng [số hiệu văn bản, Điều X Khoản Y]. '
 'Nếu các đoạn văn bản chỉ trả lời được một phần câu hỏi, hãy trả lời phần đó kèm trích dẫn và nêu rõ phần nào chưa có căn cứ. '
 'Chỉ nói \'không tìm thấy căn cứ trong kho văn bản\' khi không có đoạn nào liên quan đến câu hỏi. '
 'Không bịa số hiệu, ngày hiệu lực hay nội dung điều khoản. '
 'Phân biệt rõ quy định hiện hành với quy định cũ khi ngày hiệu lực khác nhau. '
 'Trả lời ngắn gọn bằng tiếng Việt, có mục "Điều kiện áp dụng" khi cần.'+prof+
 '\n\nCâu hỏi: '+question+'\n\nĐoạn văn bản:\n'+'\n\n'.join(ctx))
def _call_openai(prompt):
 # OpenAI chat completions, model gpt-4o (mặc định đã duyệt cho các site CPA).
 with httpx.Client(timeout=20,trust_env=False) as client:
  r=client.post('https://api.openai.com/v1/chat/completions',headers={'Authorization':'Bearer '+OPENAI_API_KEY,'Content-Type':'application/json'},json={'model':'gpt-4o','temperature':0,'max_tokens':800,'messages':[{'role':'system','content':'Bạn là trợ lý kế toán – thuế Việt Nam, chỉ trả lời từ văn bản được cung cấp kèm trích dẫn.'},{'role':'user','content':prompt}]})
  r.raise_for_status()
  return r.json()['choices'][0]['message']['content'].strip(),'gpt-4o'
def _call_muse(prompt):
 # Muse qua Meta Model API: OpenAI-compatible chat completions.
 with httpx.Client(timeout=20,trust_env=False) as client:
  r=client.post('https://api.meta.ai/v1/chat/completions',headers={'Authorization':'Bearer '+MUSE_API_KEY,'Content-Type':'application/json'},json={'model':MUSE_MODEL,'temperature':0,'max_tokens':800,'messages':[{'role':'system','content':'Bạn là trợ lý kế toán – thuế Việt Nam, chỉ trả lời từ văn bản được cung cấp kèm trích dẫn.'},{'role':'user','content':prompt}]})
  r.raise_for_status()
  return r.json()['choices'][0]['message']['content'].strip(),MUSE_MODEL
def _call_gemini(prompt):
 with httpx.Client(timeout=20,trust_env=False) as client:
  r=client.post('https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent',headers={'x-goog-api-key':GEMINI_API_KEY,'Content-Type':'application/json'},json={'contents':[{'parts':[{'text':prompt}]}],'generationConfig':{'temperature':0,'maxOutputTokens':800}})
  r.raise_for_status()
  return r.json()['candidates'][0]['content']['parts'][0]['text'].strip(),'gemini-2.0-flash'
class Ask(BaseModel):
 question:str;profile:dict|None=None
@app.post('/api/ask')
def ask(req:Ask):
 q=(req.question or '').strip()
 if len(q)<3:raise HTTPException(400,'Câu hỏi quá ngắn')
 if len(q)>500:raise HTTPException(400,'Câu hỏi tối đa 500 ký tự')
 profile=req.profile if isinstance(req.profile,dict) else None
 cats=profile.get('categories') if profile else None
 if cats and not isinstance(cats,list):cats=None
 sources,kws=_retrieve(q,cats)
 if not sources:
  return {'status':'no_sources','answer':None,'sources':[],'keywords':kws,'message':'Không tìm thấy điều khoản liên quan trong kho 83 văn bản. Thử hỏi ngắn gọn hơn bằng từ khóa trong văn bản (ví dụ: thuế suất GTGT, thời điểm lập hóa đơn).'}
 prompt=_ask_prompt(q,sources,profile)
 # Chuỗi provider: Muse -> OpenAI -> Gemini. Provider nào có key nhưng lỗi
 # (key sai, hết quota, chặn theo quốc gia, timeout) thì thử tiếp provider
 # sau; chỉ khi không có key nào mới trả no_key.
 last_err=None
 for key,fn in ((MUSE_API_KEY,_call_muse),(OPENAI_API_KEY,_call_openai),(GEMINI_API_KEY,_call_gemini)):
  if not key:continue
  try:
   answer,model=fn(prompt)
   return {'status':'ok','answer':answer,'sources':sources,'keywords':kws,'model':model}
  except Exception as e:last_err=e
 if last_err is not None:
  # AI lỗi (key sai, hết quota, timeout) -> vẫn trả đoạn văn bản, không 500.
  return {'status':'ai_error','answer':None,'sources':sources,'keywords':kws,'message':f'Trợ lý AI tạm thời không trả lời được ({last_err}). Dưới đây là các đoạn văn bản liên quan để bạn tự đối chiếu.'}
 return {'status':'no_key','answer':None,'sources':sources,'keywords':kws,'message':'Chưa cấu hình AI key nên trợ lý chỉ hiển thị các đoạn văn bản liên quan kèm trích dẫn. Thêm MUSE_API_KEY (lấy tại dev.meta.ai) vào Vercel để bật trả lời AI bằng Muse.'}
# --- v0.4.0: định hướng "trợ lý tuân thủ" (09/11/2026) ---
# Checklist hành động MẪU theo lĩnh vực cho /api/documents/{id}/impact.
# Đây là checklist tham khảo chung (các bước vận hành thường làm khi có quy định
# mới), KHÔNG phải kết luận pháp lý cho từng doanh nghiệp và không thay thế
# việc đọc văn bản gốc. Phân tích tác động chi tiết bằng AI là bước sau.
CHECKLISTS={
 'Thuế GTGT':['Rà soát thuế suất GTGT đang áp dụng cho hàng hóa, dịch vụ của doanh nghiệp','Kiểm tra điều kiện khấu trừ thuế GTGT đầu vào','Cập nhật tờ khai thuế GTGT cho kỳ kê khai tiếp theo','Thông báo cho bộ phận bán hàng / kế toán nếu có thay đổi về thuế suất hoặc đối tượng chịu thuế','Đối chiếu số thuế GTGT đã kê khai với quy định mới'],
 'Hóa đơn, chứng từ điện tử':['Kiểm tra mẫu hóa đơn điện tử đang sử dụng còn phù hợp không','Cập nhật quy trình xuất hóa đơn: thời điểm lập, nội dung bắt buộc','Rà soát cách xử lý hóa đơn sai sót: điều chỉnh, thay thế, hủy','Thông báo cho khách hàng / nhà cung cấp nếu quy trình thay đổi','Kiểm tra việc lưu trữ hóa đơn điện tử theo thời hạn quy định'],
 'Thuế TNDN':['Rà soát các khoản chi được trừ và không được trừ khi tính thuế TNDN','Cập nhật cách tính thuế TNDN tạm nộp theo quý','Kiểm tra ưu đãi thuế TNDN doanh nghiệp đang áp dụng còn hiệu lực không','Chuẩn bị hồ sơ, số liệu cho quyết toán thuế TNDN','Đối chiếu chi phí thực tế với định mức / điều kiện được trừ'],
 'Chế độ kế toán, VAS, BCTC':['Đối chiếu hệ thống tài khoản kế toán với quy định mới','Cập nhật mẫu biểu báo cáo tài chính nếu có thay đổi','Rà soát chính sách kế toán: khấu hao, hàng tồn kho, doanh thu','Đào tạo / thông báo cho nhân viên kế toán về thay đổi','Kiểm tra số liệu kỳ trước có cần điều chỉnh hồi tố không'],
 'Thuế TNCN':['Cập nhật biểu tính thuế TNCN và mức giảm trừ gia cảnh','Rà soát đăng ký người phụ thuộc của người lao động','Cập nhật bảng lương và số thuế TNCN khấu trừ hằng tháng','Kiểm tra tờ khai quyết toán thuế TNCN sắp tới','Thông báo cho người lao động nếu thu nhập tính thuế thay đổi'],
 'default':['Đọc kỹ văn bản gốc và ghi chú các điều khoản áp dụng cho doanh nghiệp','Xác định ngày hiệu lực và các nghiệp vụ phát sinh từ ngày đó','Cập nhật quy trình, mẫu biểu liên quan','Thông báo cho các bộ phận chịu ảnh hưởng','Lưu văn bản vào hồ sơ pháp lý của doanh nghiệp'],
}
# Ngày hiệu lực placeholder mà collector dùng khi chưa xác minh được
# (không phải ngày hiệu lực thật — phải hiển thị là "chưa xác minh").
UNKNOWN_EFFECTIVE='1900-01-01'
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
  sql=f'SELECT id,number,title,category,source_url,issued_date,effective_date,fetched_at,status,source,length(COALESCE(extracted_text,\'\')) AS text_length FROM documents {where} ORDER BY issued_date DESC NULLS LAST, id DESC LIMIT %s OFFSET %s'
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
  rows=c.execute(f'SELECT id,number,title,category,source_url,issued_date,effective_date,fetched_at,review_status,length(extracted_text) AS text_length FROM documents {where} ORDER BY issued_date DESC, id DESC LIMIT ? OFFSET ?',params+[limit,offset]).fetchall()
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
@app.get('/api/changes')
def changes(category:str='',limit:int=Query(10,ge=1,le=50)):
 # Dashboard "Hôm nay bạn cần biết gì?": văn bản mới thu thập, sắp/mới có
 # hiệu lực, văn bản chưa kiểm chứng pháp lý. Mọi con số đều tính từ dữ
 # liệu thật trong DB — không có ngày hiệu lực tương lai thì upcoming = 0
 # (đúng thực tế), không bịa ngày hết hiệu lực vì DB chưa có trường đó.
 today=date.today();d30=datetime.now(timezone.utc)-timedelta(days=30)
 d90=(today+timedelta(days=90)).isoformat();d180=(today-timedelta(days=180)).isoformat()
 t_iso=today.isoformat();cut30=d30.isoformat()
 def _sum(r):
  d=dict(r);d['review_status']=STATUS_TO_REVIEW.get(d.get('status'),'unverified') if 'status' in d else d.get('review_status','unverified');d['status']=REVIEW_TO_STATUS.get(d.get('review_status','unverified'),'collected') if 'status' not in d else d['status']
  if 'source' not in d:d['source']=_source_from_url(d.get('source_url',''))
  if d.get('effective_date') in (None,'',UNKNOWN_EFFECTIVE):d['effective_date']=None
  return d
 if USE_PG:
  cat=' AND category=%s' if category else ''
  with pg() as c:
   new_n=c.execute('SELECT COUNT(*) FROM documents WHERE fetched_at>=%s'+(' AND category=%s' if category else ''),([d30]+([category] if category else []))).fetchone()['count']
   up_n=c.execute('SELECT COUNT(*) FROM documents WHERE effective_date>%s AND effective_date<=%s'+cat,([t_iso,d90]+([category] if category else []))).fetchone()['count']
   rev_n=c.execute("SELECT COUNT(*) FROM documents WHERE status='collected'"+cat,([category] if category else [])).fetchone()['count']
   new_docs=[_sum(r) for r in c.execute('SELECT id,number,title,category,source_url,issued_date,effective_date,fetched_at,status,source FROM documents WHERE fetched_at>=%s'+(' AND category=%s' if category else '')+' ORDER BY issued_date DESC NULLS LAST, id DESC LIMIT %s',([d30]+([category] if category else [])+[limit])).fetchall()]
   upcoming=[_sum(r) for r in c.execute('SELECT id,number,title,category,source_url,issued_date,effective_date,fetched_at,status,source FROM documents WHERE effective_date>%s AND effective_date<=%s'+cat+' ORDER BY effective_date ASC LIMIT %s',([t_iso,d90]+([category] if category else [])+[limit])).fetchall()]
   recent=[_sum(r) for r in c.execute('SELECT id,number,title,category,source_url,issued_date,effective_date,fetched_at,status,source FROM documents WHERE effective_date>=%s AND effective_date<=%s'+cat+' ORDER BY effective_date DESC LIMIT %s',([d180,t_iso]+([category] if category else [])+[limit])).fetchall()]
   cats=[{'category':r['category'],'count':r['n']} for r in c.execute('SELECT category,COUNT(*) AS n FROM documents GROUP BY category ORDER BY n DESC').fetchall()]
  return {'counts':{'new_30d':new_n,'upcoming':up_n,'needs_review':rev_n},'new_docs':new_docs,'upcoming_effective':upcoming,'recently_effective':recent,'categories':cats}
 with conn() as c:
  qmark=' AND category=?' if category else ''
  new_n=c.execute('SELECT COUNT(*) FROM documents WHERE fetched_at>=?'+(' AND category=?' if category else ''),([cut30]+([category] if category else []))).fetchone()[0]
  up_n=c.execute('SELECT COUNT(*) FROM documents WHERE effective_date>? AND effective_date<=?'+qmark,([t_iso,d90]+([category] if category else []))).fetchone()[0]
  rev_n=c.execute("SELECT COUNT(*) FROM documents WHERE review_status='unverified'"+qmark,([category] if category else [])).fetchone()[0]
  new_docs=[_sum(r) for r in c.execute('SELECT id,number,title,category,source_url,issued_date,effective_date,fetched_at,review_status FROM documents WHERE fetched_at>=?'+(' AND category=?' if category else '')+' ORDER BY issued_date DESC, id DESC LIMIT ?',( [cut30]+([category] if category else [])+[limit])).fetchall()]
  upcoming=[_sum(r) for r in c.execute('SELECT id,number,title,category,source_url,issued_date,effective_date,fetched_at,review_status FROM documents WHERE effective_date>? AND effective_date<=?'+qmark+' ORDER BY effective_date ASC LIMIT ?',([t_iso,d90]+([category] if category else [])+[limit])).fetchall()]
  recent=[_sum(r) for r in c.execute('SELECT id,number,title,category,source_url,issued_date,effective_date,fetched_at,review_status FROM documents WHERE effective_date>=? AND effective_date<=?'+qmark+' ORDER BY effective_date DESC LIMIT ?',([d180,t_iso]+([category] if category else [])+[limit])).fetchall()]
  cats=[{'category':r[0],'count':r[1]} for r in c.execute('SELECT category,COUNT(*) FROM documents GROUP BY category ORDER BY 2 DESC').fetchall()]
 return {'counts':{'new_30d':new_n,'upcoming':up_n,'needs_review':rev_n},'new_docs':new_docs,'upcoming_effective':upcoming,'recently_effective':recent,'categories':cats}
@app.get('/api/compare')
def compare(a:int=Query(...),b:int=Query(...)):
 # So sánh 2 văn bản theo từng Điều/Khoản/Điểm: thêm mới, bãi bỏ, thay đổi,
 # không đổi. Ghép các provision trùng khóa (dieu,khoan,diem) theo ordinal
 # rồi nối text — đúng cả với nghị định sửa đổi có nhiều khoản trùng số.
 # Chỉ có trên Postgres (SQLite fallback không có bảng provisions).
 if not USE_PG:raise HTTPException(501,'So sánh chỉ có trên Postgres; bản SQLite fallback không chứa bảng provisions')
 if a==b:raise HTTPException(400,'Chọn hai văn bản khác nhau để so sánh')
 def _load(doc_id):
  with pg() as c:
   doc=c.execute('SELECT id,number,title,category,issued_date,effective_date,status,source FROM documents WHERE id=%s',(doc_id,)).fetchone()
   if not doc:raise HTTPException(404,f'Không tìm thấy văn bản id={doc_id}')
   rows=c.execute('SELECT dieu,khoan,diem,ordinal,text FROM provisions WHERE document_id=%s ORDER BY ordinal ASC',(doc_id,)).fetchall()
  grouped={}
  for r in rows:
   key=(r['dieu'],r['khoan'],r['diem']);grouped.setdefault(key,[]).append(r['text'] or '')
  return doc,{k:'\n'.join(v) for k,v in grouped.items()}
 da,pa=_load(a);db,pb=_load(b)
 keys=set(pa)|set(pb);changes=[]
 # Sort key phải cùng kiểu mọi phần tử: khoan là INT hoặc NULL (1.060 dòng
 # NULL) nên `x[1] or ''` trộn int với str -> TypeError -> 500 trên live.
 # Dùng chuỗi đệm số 0: dieu/khoan sắp xếp theo số, diem theo chữ.
 for k in sorted(keys,key=lambda x:f"{x[0] or 0:06d}|{x[1] if x[1] is not None else -1:06d}|{x[2] or ''}"):
  ta,tb=pa.get(k),pb.get(k)
  if ta is None:typ='added'
  elif tb is None:typ='removed'
  elif ta==tb:typ='unchanged'
  else:typ='changed'
  changes.append({'dieu':k[0],'khoan':k[1],'diem':k[2],'type':typ,'a_text':(ta or '')[:2000] if typ in ('removed','changed') else None,'b_text':(tb or '')[:2000] if typ in ('added','changed') else None})
 summary={t:sum(1 for ch in changes if ch['type']==t) for t in ('added','removed','changed','unchanged')}
 shown=changes[:200]
 return {'a':dict(da),'b':dict(db),'summary':summary,'truncated':len(changes)>200,'total_keys':len(changes),'changes':shown}
@app.get('/api/documents/{id}/impact')
def impact(id:int):
 # "Doanh nghiệp cần làm gì?": sự kiện thật rút từ văn bản (lĩnh vực, ngày
 # hiệu lực, số điều khoản, chương) + checklist tham khảo theo lĩnh vực.
 # Không suy ra tác động pháp lý cho từng doanh nghiệp — checklist ghi rõ
 # là mẫu chung. Phân tích AI chi tiết là bước sau (cần AI key).
 def _doc_pg():
  with pg() as c:
   row=c.execute('SELECT id,number,title,category,source_url,issued_date,effective_date,fetched_at,status,source,length(COALESCE(extracted_text,\'\')) AS text_length FROM documents WHERE id=%s',(id,)).fetchone()
   if not row:raise HTTPException(404,'Không tìm thấy')
   d=dict(row);d['review_status']=STATUS_TO_REVIEW.get(d.get('status'),'unverified')
   nprov=c.execute('SELECT COUNT(*) FROM provisions WHERE document_id=%s',(id,)).fetchone()['count']
   chaps=[{'chuong':r['chuong'],'count':r['n']} for r in c.execute('SELECT chuong,COUNT(*) AS n FROM provisions WHERE document_id=%s AND chuong IS NOT NULL GROUP BY chuong ORDER BY MIN(ordinal)',(id,)).fetchall()]
   rel=[dict(r) for r in c.execute('SELECT id,number,title,effective_date FROM documents WHERE category=%s AND id<>%s ORDER BY issued_date DESC NULLS LAST LIMIT 5',(d['category'],id)).fetchall()]
  return d,nprov,chaps,rel
 def _doc_lite():
  with conn() as c:
   row=c.execute('SELECT * FROM documents WHERE id=?',(id,)).fetchone()
   if not row:raise HTTPException(404,'Không tìm thấy')
   d=dict(row);d['status']=REVIEW_TO_STATUS.get(d.get('review_status','unverified'),'collected');d['source']=_source_from_url(d.get('source_url',''));d['text_length']=len(d.get('extracted_text') or '')
   rel=[{'id':r[0],'number':r[1],'title':r[2],'effective_date':r[3]} for r in c.execute('SELECT id,number,title,effective_date FROM documents WHERE category=? AND id<>? ORDER BY issued_date DESC LIMIT 5',(d['category'],id)).fetchall()]
  return d,None,[],rel
 d,nprov,chaps,rel=_doc_pg() if USE_PG else _doc_lite()
 eff=d.get('effective_date')
 eff_known=eff not in (None,'',UNKNOWN_EFFECTIVE)
 facts=[f"Thuộc lĩnh vực {d.get('category') or 'chưa phân loại'}",f"Ban hành: {d.get('issued_date') or 'chưa xác định'}",f"Có hiệu lực từ {eff}" if eff_known else 'Chưa xác minh ngày hiệu lực',f"Trạng thái: {'đã xác nhận nguồn' if d.get('status')=='source_verified' else 'đã kiểm chứng pháp lý' if d.get('status')=='legal_verified' else 'đã thu thập, chưa kiểm chứng'}"]
 if nprov is not None:facts.append(f"Gồm {nprov} điều khoản đã tách")
 if chaps:facts.append('Các chương: '+', '.join(f"Chương {ch['chuong']} ({ch['count']} khoản)" for ch in chaps[:8]))
 return {'document':{k:d.get(k) for k in ('id','number','title','category','source_url','issued_date','effective_date','fetched_at','status','review_status','source','text_length')},'effective_known':eff_known,'facts':facts,'provisions_count':nprov,'chapters':chaps,'checklist':CHECKLISTS.get(d.get('category'),CHECKLISTS['default']),'checklist_note':'Checklist tham khảo theo lĩnh vực — mẫu chung, không phải kết luận pháp lý cho từng doanh nghiệp. Đối chiếu với văn bản gốc trước khi áp dụng.','related':rel}
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
