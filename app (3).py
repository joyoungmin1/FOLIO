import os
import io
import json
import time
import requests
import threading
import holidays as _holidays_lib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from bs4 import BeautifulSoup
from google import genai
from pymongo import MongoClient

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'change-this-secret-key')

# =============================================
# API 키 설정 (Render 환경변수에서 불러옴)
# ★ 반드시 함수 선언보다 위에 있어야 함
# =============================================
NAVER_CLIENT_ID     = os.environ.get('NAVER_CLIENT_ID',     'YOUR_NAVER_CLIENT_ID')
NAVER_CLIENT_SECRET = os.environ.get('NAVER_CLIENT_SECRET', 'YOUR_NAVER_CLIENT_SECRET')
DART_API_KEY        = os.environ.get('DART_API_KEY',        'YOUR_DART_API_KEY')
KRX_API_KEY         = os.environ.get('KRX_API_KEY',         '')
APP_PASSWORD        = os.environ.get('APP_PASSWORD',        'stock1234')

# =============================================
# MongoDB 연결
# =============================================
try:
    mongo_client = MongoClient(os.environ.get('MONGODB_URI', ''))
    db = mongo_client['stock_app']
    col_history      = db['history']
    col_stars        = db['stars']
    col_supply_cache = db['supply_cache']
    col_chart_cache  = db['chart_cache']
    print('[MONGODB] 연결 성공')
except Exception as e:
    print(f'[MONGODB] 연결 실패: {e}')
    db = None
    col_history      = None
    col_stars        = None
    col_supply_cache = None
    col_chart_cache  = None

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# =============================================
# 네이버 시가총액 전종목 크롤링 → stocks.json 저장
# =============================================
_stocks = []   # [{'code': '005930', 'name': '삼성전자'}, ...]
STOCKS_FILE = os.path.join(os.path.dirname(__file__), 'stocks.json')

def crawl_naver_stocks():
    """네이버 시가총액 페이지에서 코스피+코스닥 전종목 크롤링"""
    global _stocks
    print('[NAVER CRAWL] 종목 크롤링 시작...')
    all_stocks = []
    seen_codes = set()

    markets = [
        {'sosok': 0, 'pages': 49, 'name': '코스피', 'suffix': '.KS'},
        {'sosok': 1, 'pages': 37, 'name': '코스닥', 'suffix': '.KQ'},
    ]

    for market in markets:
        sosok = market['sosok']
        total_pages = market['pages']
        mname = market['name']
        suffix = market['suffix']
        print(f'[NAVER CRAWL] {mname} 크롤링 중... (총 {total_pages}페이지)')

        for page in range(1, total_pages + 1):
            try:
                url = f'https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}'
                res = requests.get(url, headers=HEADERS, timeout=15)
                soup = BeautifulSoup(res.content, 'html.parser', from_encoding='euc-kr')

                # 종목명 링크: <a href="/item/main.naver?code=005930" class="tltle">삼성전자</a>
                links = soup.select('a.tltle')
                for link in links:
                    href = link.get('href', '')
                    name = link.text.strip()
                    if 'code=' in href and name:
                        code = href.split('code=')[-1].strip()
                        if code and len(code) == 6 and code.isdigit() and code not in seen_codes:
                            all_stocks.append({'code': code, 'name': name, 'market': mname, 'suffix': suffix})
                            seen_codes.add(code)

            except Exception as e:
                print(f'[NAVER CRAWL] {mname} {page}페이지 오류: {e}')
                continue

        print(f'[NAVER CRAWL] {mname} 완료. 누적 {len(all_stocks)}개')

    if all_stocks:
        _stocks = all_stocks
        # stocks.json 파일로 저장 (디스크 캐시)
        try:
            with open(STOCKS_FILE, 'w', encoding='utf-8') as f:
                json.dump(all_stocks, f, ensure_ascii=False)
            print(f'[NAVER CRAWL] stocks.json 저장 완료. 총 {len(all_stocks)}개 종목')
        except Exception as e:
            print(f'[NAVER CRAWL] stocks.json 저장 실패: {e}')
    else:
        print('[NAVER CRAWL] 크롤링 결과 없음. 기존 데이터 유지.')

def load_stocks_from_file():
    """앱 시작 시 stocks.json 파일에서 빠르게 로딩"""
    global _stocks
    if os.path.exists(STOCKS_FILE):
        try:
            with open(STOCKS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if data:
                _stocks = data
                print(f'[STOCKS] stocks.json 로딩 완료. {len(_stocks)}개 종목')
                return True
        except Exception as e:
            print(f'[STOCKS] stocks.json 로딩 실패: {e}')
    return False

def init_stocks():
    """앱 시작 시: 파일 있으면 바로 로딩, 없으면 크롤링"""
    if not load_stocks_from_file():
        print('[STOCKS] stocks.json 없음. 크롤링 시작...')
        crawl_naver_stocks()

# 앱 시작 시 백그라운드에서 종목 로딩
threading.Thread(target=init_stocks, daemon=True).start()

# =============================================
# APScheduler: 매일 오전 8시 자동 갱신
# =============================================
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    scheduler = BackgroundScheduler(timezone='Asia/Seoul')
    scheduler.add_job(crawl_naver_stocks, 'cron', hour=8, minute=0)
    scheduler.start()
    print('[SCHEDULER] 매일 오전 8시 종목 자동 갱신 스케줄 등록 완료')
except Exception as e:
    print(f'[SCHEDULER] APScheduler 오류: {e}')

# =============================================
# Gemini 릴레이 (키 여러 개 → 한도 초과시 자동 전환)
# =============================================
GEMINI_KEYS = [k for k in [
    os.environ.get('GEMINI_KEY_1', 'YOUR_GEMINI_API_KEY'),
    os.environ.get('GEMINI_KEY_2', ''),
    os.environ.get('GEMINI_KEY_3', ''),
    os.environ.get('GEMINI_KEY_4', ''),
    os.environ.get('GEMINI_KEY_5', ''),
] if k and k != '']

gemini_key_index = 0

# consensus 웹검색 전용 키 (KEY_6~8)
GEMINI_CONSENSUS_KEYS = [k for k in [
    os.environ.get('GEMINI_KEY_6', ''),
    os.environ.get('GEMINI_KEY_7', ''),
    os.environ.get('GEMINI_KEY_8', ''),
] if k and k != '']
gemini_consensus_key_index = 0

# 폴백 순서: 3.5 → 3.1 → 2.5
GEMINI_MODEL_FALLBACK = [
    'gemini-3.5-flash',
    'gemini-3-flash-preview',
    'gemini-2.5-flash',
]

# FOLIO 수석 에디터 시스템 프롬프트
FOLIO_SYSTEM_INSTRUCTION = """
당신은 GUIDE X | FOLIO의 수석 시장 칼럼니스트다.
당신은 데이터를 요약하는 AI가 아니다.
당신은 시장 심리를 번역하는 에디터다.

[4축 해석 원칙]
뉴스 = 사건
검색량 = 관심
수급 = 행동
보조지표 = 상태
이 네 가지를 하나의 이야기로 연결하라.

[데이터 연결 규칙]
단일 데이터만으로 결론을 내리지 마라.
반드시 최소 2개 이상의 데이터 소스를 연결해서 해석하라.
좋은 예: 검색량 증가 + 기관 순매수 → 관심이 실제 자금 유입으로 이어지고 있음
나쁜 예: RSI가 높다. 검색량이 증가했다. 기관이 매수했다.

[Stage 정의]
0=무관심 / 1=초기관심 / 2=관심확산 / 3=자금유입 / 4=기대과열 / 5=현실검증
반드시 위 단계 중 하나를 선택하라. 새로운 단계를 만들지 마라.

[문체 원칙]
감성 20%, 해석 80%
비유는 문단당 최대 1회
모든 문단은 반드시 해석으로 끝낸다
숫자나 지표명으로 문단을 끝내지 마라
만연체 절대 금지, 단문 위주

[사용자가 궁금한 것]
"무슨 일이 있었는가"가 아니다.
"그래서 시장은 지금 이 종목을 어떻게 보고 있는가"이다.
모든 해석은 시장의 시선으로 번역하라.

[절대 사용 금지]
매수 유효 / 매수 기회 / 분할 매수 / 적극 매수
비중 확대 / 진입 시점 / 저점 매수 / 추천 / 유망
투자 행동을 암시하지 마라.
당신은 투자 조언가가 아니다. 당신은 시장 해설자다.

[경제소설 방지]
문학적 표현보다 시장 해석을 우선한다.
주가를 사람, 영웅, 거인, 운명, 전설, 드라마로 의인화하지 마라.
비유는 문단당 최대 1회만 허용하며, 비유 없이도 성립하는 문장만 사용하라.
모든 문장은 시장 심리 또는 자금 흐름 해석으로 귀결되어야 한다.

[검색량 데이터 지연 규칙]
네이버 검색량 데이터는 전일 기준이다.
당일 급등, 상한가, 공시, 대형 뉴스가 발생한 경우
검색량이 아직 반영되지 않았을 수 있다.
따라서
주가↑ + 뉴스↑ + 검색량 정체
상황에서는
관심 부족으로 단정하지 말고
검색량 반영 지연 가능성을 먼저 검토하라.

[검색량 교차 해석 강제]
검색량은 단독으로 해석하지 마라.
반드시 가격, 뉴스, 수급 중 최소 1개 이상과 비교하여 해석하라.
특히 다음 괴리를 우선 탐지하라:
주가↑ + 검색량↓ / 주가↓ + 검색량↑
검색량↑ + 수급 부재 / 검색량↓ + 기관 매집
괴리는 시장 심리 변화의 신호로 간주한다.

[관심 괴리 탐지]
주가가 신고가 또는 52주 최고가 부근인데
검색량이 감소하고 있다면
새로운 관심 유입보다
기존 보유자 중심의 시장으로 전환되고 있을 가능성을 검토하라.

[지표 번역 규칙]
RSI, ATR, 이동평균선, 거래량을 직접 설명하지 마라.
지표는 반드시 시장 상태 언어로 번역하라.
ATR 확장 → 상승 속도가 빨라지고 있다
ATR 축소 → 에너지를 응축하고 있다
RSI 과열 → 기대가 빠르게 가격에 반영되고 있다
RSI 침체 → 관심보다 우려가 우세하다
정배열 → 상승 추세가 유지되고 있다
역배열 → 시장 신뢰가 약한 상태다
지표명은 데이터 원문에만 남기고 본문에서는 최소화하라.

[Stage 출력 고정]
최종 의견은 반드시 아래 형식으로 시작한다.
Stage X · [단계명]
한 문장으로 현재 시장 상태를 정의한다.
예시: Stage 4 · 기대과열
AI 기대가 빠르게 가격에 반영되고 있지만, 실제 자금 흐름은 아직 엇갈리고 있습니다.
Stage는 보고서 전체 해석의 결론이며, 반드시 첫 문장에 배치한다.
"""

def call_gemini(prompt, model_name='gemini-3.5-flash'):
    """모델 시도 → 키 5개 순환 → 안되면 하위 모델로 폴백. (실제사용모델, 응답텍스트) 반환"""
    global gemini_key_index
    start = GEMINI_MODEL_FALLBACK.index(model_name) if model_name in GEMINI_MODEL_FALLBACK else 0
    models_to_try = GEMINI_MODEL_FALLBACK[start:]
    for model in models_to_try:
        for attempt in range(len(GEMINI_KEYS)):
            idx = (gemini_key_index + attempt) % len(GEMINI_KEYS)
            key = GEMINI_KEYS[idx]
            try:
                client = genai.Client(api_key=key)
                response = client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config=genai.types.GenerateContentConfig(
                        system_instruction=FOLIO_SYSTEM_INSTRUCTION
                    )
                )
                gemini_key_index = (idx + 1) % len(GEMINI_KEYS)
                try:
                    usage = response.usage_metadata
                    print(f'[TOKEN] {model} | 입력={usage.prompt_token_count} | 출력={usage.candidates_token_count} | 합계={usage.total_token_count}')
                except:
                    pass
                return model, response.text
            except Exception as e:
                err = str(e)
                if '503' in err or 'unavailable' in err.lower():
                    # 서버 과부하 → 같은 키로 2초 대기 후 재시도
                    print(f'[GEMINI] 503 감지, 2초 후 재시도 (키 {idx})')
                    time.sleep(2)
                    continue
                elif '429' in err or 'quota' in err.lower() or 'exhausted' in err.lower():
                    gemini_key_index = (idx + 1) % len(GEMINI_KEYS)
                    continue
                else:
                    raise e
    raise Exception('모든 Gemini 모델과 API 키의 한도가 초과되었습니다.')

def call_gemini_chat(history, message, model_name='gemini-3.5-flash'):
    """채팅용. (실제사용모델, 응답텍스트) 반환"""
    global gemini_key_index
    history_text = ''
    for h in history[-10:]:
        role = '사용자' if h['role'] == 'user' else 'AI'
        history_text += f"{role}: {h.get('parts', [h.get('content', '')])[0]}\n"
    full_prompt = f"당신은 친절한 AI 어시스턴트입니다. 어떤 주제든 자연스럽고 편하게 대화해주세요.\n\n이전 대화:\n{history_text}\n사용자: {message}\nAI:"
    start = GEMINI_MODEL_FALLBACK.index(model_name) if model_name in GEMINI_MODEL_FALLBACK else 0
    models_to_try = GEMINI_MODEL_FALLBACK[start:]
    for model in models_to_try:
        for attempt in range(len(GEMINI_KEYS)):
            idx = (gemini_key_index + attempt) % len(GEMINI_KEYS)
            key = GEMINI_KEYS[idx]
            try:
                client = genai.Client(api_key=key)
                response = client.models.generate_content(
                    model=model,
                    contents=full_prompt
                )
                gemini_key_index = (idx + 1) % len(GEMINI_KEYS)
                return model, response.text
            except Exception as e:
                err = str(e)
                if '429' in err or 'quota' in err.lower() or 'exhausted' in err.lower():
                    gemini_key_index = (idx + 1) % len(GEMINI_KEYS)
                    continue
                else:
                    raise e
    raise Exception('모든 Gemini 모델과 API 키의 한도가 초과되었습니다.')

# =============================================
# 인증
# =============================================
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        pw = request.form.get('password', '')
        if pw == APP_PASSWORD:
            session['logged_in'] = True
            return redirect(url_for('index'))
        return render_template('login.html', error='비밀번호가 틀렸습니다.')
    return render_template('login.html', error=None)

@app.route('/api/verify-password', methods=['POST'])
def verify_password():
    data = request.get_json()
    pw = data.get('password', '')
    if pw == APP_PASSWORD:
        session['logged_in'] = True
        return jsonify({'ok': True})
    return jsonify({'ok': False}), 401

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/download-stocks')
def download_stocks():
    """stocks.json 다운로드 (GitHub 업로드용 — 완성 후 이 라우트 삭제 가능)"""
    from flask import send_file
    if os.path.exists(STOCKS_FILE):
        return send_file(STOCKS_FILE, as_attachment=True, download_name='stocks.json')
    return '아직 크롤링 중이에요. 잠시 후 다시 시도해주세요.', 404

def login_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            if request.path.startswith('/api/'):
                return jsonify({'error': '로그인이 필요합니다.'}), 401
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

# =============================================
# 메인 페이지
# =============================================
@app.route('/')
@login_required
def index():
    return render_template('index.html')

# =============================================
# 종목 자동완성 검색 (네이버 시가총액 DB 기반)
# =============================================
@app.route('/api/search-stock')
@login_required
def api_search_stock():
    q = request.args.get('q', '').strip()
    if not q or len(q) < 1:
        return jsonify([])

    q_lower = q.lower()
    prefix_matches = []
    contain_matches = []

    for s in _stocks:
        name = s['name']
        code = s['code']
        market = s.get('market', '')
        name_lower = name.lower()
        if name_lower.startswith(q_lower):
            prefix_matches.append({'code': code, 'name': name, 'market': market})
        elif q_lower in name_lower or code.lower().startswith(q_lower):
            contain_matches.append({'code': code, 'name': name, 'market': market})

    results = (prefix_matches + contain_matches)[:8]
    return jsonify(results)

# =============================================
# 네이버 증권 크롤링
# =============================================
def get_stock_code(query):
    if query.isdigit() and len(query) == 6:
        try:
            url = f'https://finance.naver.com/item/main.naver?code={query}'
            res = requests.get(url, headers=HEADERS, timeout=10)
            soup = BeautifulSoup(res.content, 'html.parser', from_encoding='euc-kr')
            name_tag = soup.select_one('div.wrap_company h2 a')
            if name_tag:
                return query, name_tag.text.strip(), None
        except:
            pass
        return query, query, None

    # 한글 검색: 메모리 DB에서 먼저 찾기
    for s in _stocks:
        if s['name'] == query:
            return s['code'], s['name'], s.get('suffix')
    # 부분 일치
    for s in _stocks:
        if query in s['name']:
            return s['code'], s['name'], s.get('suffix')

    return None, None, None

def get_krx_dividend(code):
    """KRX data.krx.co.kr OTP 방식으로 개별종목 배당수익률 조회
    - 전종목 PER/PBR/배당수익률 데이터에서 해당 종목 필터링
    - 연간 사업보고서 기준 (연 1회 업데이트)
    """
    try:
        krx_key = KRX_API_KEY
        if not krx_key:
            return '-'
        # 전 영업일 기준 (주말/장마감 후 당일 데이터 없음 방지)
        from datetime import timedelta
        trd_dt = datetime.now()
        if trd_dt.weekday() == 5:   # 토요일
            trd_dt -= timedelta(days=1)
        elif trd_dt.weekday() == 6: # 일요일
            trd_dt -= timedelta(days=2)
        trd_dd = trd_dt.strftime('%Y%m%d')
        # 1단계: OTP 발급
        otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
        otp_params = {
            'searchType':   '1',
            'mktId':        'ALL',
            'trdDd':        trd_dd,
            'csvxls_isNo':  'false',
            'name':         'fileDown',
            'url':          'dbms/MDC/STAT/standard/MDCSTAT03501',
        }
        otp_headers = {
            'Referer':  'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
            'AUTH_KEY': krx_key,
        }
        otp_res = requests.post(otp_url, data=otp_params, headers=otp_headers, timeout=10)
        otp = otp_res.text.strip()
        if not otp:
            print(f'[KRX 배당수익률] OTP 발급 실패')
            return '-'
        # 2단계: 데이터 다운로드 (CSV, EUC-KR)
        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        down_res = requests.post(
            down_url,
            data={'code': otp},
            headers={'Referer': otp_url, 'AUTH_KEY': krx_key},
            timeout=15
        )
        down_res.encoding = 'euc-kr'
        import csv, io
        reader = csv.DictReader(io.StringIO(down_res.text))
        rows = list(reader)
        if not rows:
            print(f'[KRX 배당수익률] 데이터 없음')
            return '-'
        # 3단계: 종목코드 매칭 후 배당수익률 반환 (CSV 컬럼명 한글)
        for row in rows:
            # 컬럼명: '종목코드' 또는 'ISU_SRT_CD' 둘 다 시도
            row_code = row.get('종목코드', row.get('ISU_SRT_CD', '')).strip()
            if row_code == code:
                dvr = row.get('배당수익률', row.get('DVD_YLD', '')).strip()
                if dvr and dvr not in ('-', ''):
                    try:
                        return f'{float(dvr):.2f}%'
                    except:
                        return dvr
        print(f'[KRX 배당수익률] 종목 {code} 미발견')
        return '-'
    except Exception as e:
        print(f'[KRX 배당수익률 오류]: {e}')
        return '-'


def get_stock_info(code):
    """KIS API로 종목 기본정보 조회
    - FHKST01010100: 현재가/시가총액/PER/PBR/52주최고최저/외국인소진율
    - FHKST66430200: ROE
    """
    try:
        app_key = os.environ.get('KIS_APP_KEY', '')
        app_secret = os.environ.get('KIS_APP_SECRET', '')
        token = _kis_get_token()

        if not token or not app_key or not app_secret:
            print('[STOCK INFO] KIS 토큰/키 없음 — 빈 dict 반환')
            return {}

        base_url = 'https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price'

        def _kis_call(tr_id):
            h = {
                'content-type':  'application/json',
                'authorization': f'Bearer {token}',
                'appkey':        app_key,
                'appsecret':     app_secret,
                'tr_id':         tr_id,
                'custtype':      'P',
            }
            p = {
                'fid_cond_mrkt_div_code': 'J',
                'fid_input_iscd':         code,
            }
            r = requests.get(base_url, headers=h, params=p, timeout=10)
            d = r.json()
            if d.get('rt_cd') != '0':
                print(f'[STOCK INFO] {tr_id} 오류: {d.get("msg1", "")}')
                return {}
            return d.get('output', {})

        # FHKST01010100 — 현재가/시가총액/PER/PBR/52주고저/외국인소진율
        o1 = _kis_call('FHKST01010100')

        # ROE — FHKST66430200 재무비율 조회
        roe_val = '-'
        try:
            roe_url = 'https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/finance/financial-ratio'
            roe_h = {
                'content-type':  'application/json',
                'authorization': f'Bearer {token}',
                'appkey':        app_key,
                'appsecret':     app_secret,
                'tr_id':         'FHKST66430300',
                'custtype':      'P',
            }
            roe_p = {
                'fid_cond_mrkt_div_code': 'J',
                'fid_input_iscd':         code,
                'fid_div_cls_code':       '0',
            }
            roe_r = requests.get(roe_url, headers=roe_h, params=roe_p, timeout=10)
            roe_d = roe_r.json()
            if roe_d.get('rt_cd') == '0':
                roe_list = roe_d.get('output', [])
                if roe_list:
                    print(f'[STOCK INFO] ROE 전체 응답: {roe_list[0]}')
                    # 가능한 ROE 필드명 순서대로 시도
                    for field in ['roe_val', 'roe', 'rtn_on_eq', 'rtn_on_eq_val', 'pbr_val']:
                        raw_roe = roe_list[0].get(field, '')
                        if raw_roe and raw_roe not in ('', '0', '.00'):
                            print(f'[STOCK INFO] ROE 필드={field} 값={raw_roe}')
                            try:
                                roe_val = f'{float(raw_roe):.2f}%'
                            except:
                                pass
                            break
                    print(f'[STOCK INFO] ROE 최종값={roe_val}')
            else:
                print(f'[STOCK INFO] ROE API 오류: {roe_d.get("msg1", "")}')
        except Exception as e:
            print(f'[STOCK INFO] ROE 조회 오류: {e}')

        # 등락방향: prdy_vrss_sign 1/2=상승, 3=보합, 4/5=하락
        sign_code = o1.get('prdy_vrss_sign', '3')
        if sign_code in ('1', '2'):
            direction = '상승'
            sign_str  = '+'
        elif sign_code in ('4', '5'):
            direction = '하락'
            sign_str  = '-'
        else:
            direction = '보합'
            sign_str  = ''

        def fmt_num(v):
            try:
                return f'{int(v):,}'
            except:
                return v if v else '-'

        def fmt_mktcap(v):
            try:
                억 = int(v)
                if 억 >= 10000:
                    조 = 억 // 10000
                    나머지 = 억 % 10000
                    return f'{조}조 {나머지:,}억' if 나머지 else f'{조}조'
                return f'{억:,}억'
            except:
                return v if v else '-'

        def fmt_rate(v, prefix=''):
            try:
                return f'{prefix}{float(v):.2f}%'
            except:
                return v if v else '-'

        # 52주 괴리율 계산
        def calc_gap(current_str, target_str):
            try:
                c = int(current_str.replace(',', ''))
                t = int(target_str.replace(',', ''))
                gap = (c - t) / t * 100
                sign = '+' if gap >= 0 else ''
                return f'{sign}{gap:.1f}%'
            except:
                return '-'

        raw_current = o1.get('stck_prpr', '')
        raw_w52_high = o1.get('w52_hgpr', '')
        raw_w52_low  = o1.get('w52_lwpr', '')
        fmt_current  = fmt_num(raw_current)
        fmt_w52_high = fmt_num(raw_w52_high)
        fmt_w52_low  = fmt_num(raw_w52_low)

        info = {
            '현재가':           fmt_current,
            '등락방향':         direction,
            '전일대비금액':     fmt_num(o1.get('prdy_vrss', '')),
            '등락률':           fmt_rate(o1.get('prdy_ctrt', ''), sign_str),
            '시가총액':         fmt_mktcap(o1.get('hts_avls', '')),
            'PER':              fmt_rate(o1.get('per', '')),
            'PBR':              fmt_rate(o1.get('pbr', '')),
            'ROE':              roe_val,
            '52주최고':         fmt_w52_high,
            '52주최저':         fmt_w52_low,
            '52주최고괴리율':   calc_gap(raw_current, raw_w52_high),
            '52주최저괴리율':   calc_gap(raw_current, raw_w52_low),
            '외국인소진율':     fmt_rate(o1.get('hts_frgn_ehrt', '')),
        }

        print(f'[STOCK INFO] KIS 조회 성공: {code} 현재가={info["현재가"]} 외국인={info["외국인소진율"]} ROE={info["ROE"]}')
        return info

    except Exception as e:
        print(f'[get_stock_info 오류]: {e}')
        return {}

def _parse_consensus_with_gemini(stock_name, model_name='gemini-3.5-flash'):
    """Google Search grounding으로 최근 1개월 증권사 목표주가 조회 → JSON 파싱"""
    _consensus_start = time.time()
    cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y년 %m월 %d일')
    prompt = f"""{stock_name} 종목의 {cutoff} 이후 최신 증권사 목표주가를 검색해서 JSON 배열만 출력. 다른 텍스트 절대 금지. 백틱 금지.
⚠️ 반드시 {stock_name} 단일 종목만 수집. 계열사·관련사·유사 종목 절대 포함 금지. (예: LG 검색시 LG이노텍·LG화학·LG전자 등 제외)
각 항목: {{"firm":"증권사명","target":"목표주가(숫자+원, 예:85000원)","date":"날짜(MM.DD 형식)","summary":"해당 증권사가 이 목표주가를 제시한 핵심 근거 1~2줄 (실적 전망, 사업 모멘텀 등 구체적으로)","url":"기사 원문 URL (없으면 빈 문자열)"}}
중복 증권사는 최신 1개만. {cutoff} 이전 데이터는 제외. 목표주가 없으면 빈 배열 반환. summary는 기사 본문 기반으로 작성, 없으면 빈 문자열. url은 실제 기사 URL만, 없으면 반드시 빈 문자열."""
    try:
        import re as _re
        from google.genai import types as _types
        global gemini_consensus_key_index
        keys_to_use = GEMINI_CONSENSUS_KEYS if GEMINI_CONSENSUS_KEYS else GEMINI_KEYS
        if not keys_to_use:
            print('[CONSENSUS PARSE] 오류: 사용 가능한 키 없음')
            return []
        print(f'[CONSENSUS] 키 {len(keys_to_use)}개로 시작 (인덱스 {gemini_consensus_key_index})')
        # 웹검색 모델 고정 (폴백 없음)
        CONSENSUS_MODEL = 'gemini-2.5-flash'
        grounding_tool = _types.Tool(google_search=_types.GoogleSearch())
        config = _types.GenerateContentConfig(tools=[grounding_tool])
        start_index = gemini_consensus_key_index
        for attempt in range(len(keys_to_use)):
            idx = (start_index + attempt) % len(keys_to_use)
            key = keys_to_use[idx]
            try:
                client = genai.Client(api_key=key)
                response = client.models.generate_content(
                    model=CONSENSUS_MODEL,
                    contents=prompt,
                    config=config,
                )
                gemini_consensus_key_index = (idx + 1) % len(keys_to_use)
                raw = response.text.strip()
                # 백틱 제거
                text = raw.replace('```json', '').replace('```', '').strip()
                # JSON 배열만 추출 (앞뒤 텍스트 있어도 파싱 가능하게)
                m = _re.search(r'\[.*\]', text, _re.DOTALL)
                if m:
                    text = m.group(0)
                result = json.loads(text)
                if isinstance(result, list):
                    elapsed = time.time() - _consensus_start
                    print(f'[CONSENSUS] {CONSENSUS_MODEL} {elapsed:.1f}초')
                    print(f'[CONSENSUS PARSE] {len(result)}개 파싱 완료')
                    status = 'empty' if len(result) == 0 else 'ok'
                    return {'list': result, 'status': status}
                print(f'[CONSENSUS PARSE] JSON 배열 아님 — {type(result)}, 다음 키로 재시도')
                continue
            except Exception as e:
                err = str(e)
                if '429' in err or 'quota' in err.lower() or 'exhausted' in err.lower() \
                   or '503' in err or 'unavailable' in err.lower():
                    print(f'[CONSENSUS] 키 {idx} 한도초과/503, 다음 키로')
                    continue
                print(f'[CONSENSUS PARSE] 오류 (키{idx}): {e}')
                continue
        print(f'[CONSENSUS PARSE] 모든 키 실패')
        return {'list': [], 'status': 'error'}
    except Exception as e:
        print(f'[CONSENSUS PARSE] 오류: {e}')
        return {'list': [], 'status': 'error'}

def get_financial_data(code):
    url = f'https://finance.naver.com/item/coinfo.naver?code={code}&target=finsum_more'
    try:
        res = requests.get(url, headers=HEADERS, timeout=8)
        soup = BeautifulSoup(res.content, 'html.parser', from_encoding='euc-kr')
        tables = soup.select('table')
        result = []
        for t in tables[:2]:
            rows = t.select('tr')
            for row in rows:
                cells = [td.text.strip() for td in row.select('th, td')]
                if cells:
                    result.append(' | '.join(cells))
        return '\n'.join(result[:30])
    except:
        return ''

# =============================================
# 네이버 뉴스 API
# =============================================
def _parse_naver_date(pub_date_str):
    """네이버 뉴스 pubDate → datetime 변환"""
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(pub_date_str).replace(tzinfo=None)
    except:
        return None

def get_news(query, display=5):
    url = 'https://openapi.naver.com/v1/search/news.json'
    headers = {
        'X-Naver-Client-Id': NAVER_CLIENT_ID,
        'X-Naver-Client-Secret': NAVER_CLIENT_SECRET,
    }
    params = {'query': query, 'display': display, 'sort': 'date'}
    try:
        res = requests.get(url, headers=headers, params=params, timeout=5)
        items = res.json().get('items', [])
        return [{'title': i['title'].replace('<b>','').replace('</b>',''),
                 'description': i['description'].replace('<b>','').replace('</b>',''),
                 'pubDate': i['pubDate'],
                 'originallink': i.get('originallink', ''),
                 'link': i.get('link', '')} for i in items]
    except:
        return []

def get_target_price_news(stock_name, display=5):
    """목표주가 뉴스 — 제목에 종목명 포함 + 15일 이내 필터링, 링크 포함"""
    url = 'https://openapi.naver.com/v1/search/news.json'
    headers = {
        'X-Naver-Client-Id': NAVER_CLIENT_ID,
        'X-Naver-Client-Secret': NAVER_CLIENT_SECRET,
    }
    params = {'query': stock_name + ' 목표주가', 'display': 100, 'sort': 'date'}
    cutoff = datetime.now() - timedelta(days=15)
    try:
        res = requests.get(url, headers=headers, params=params, timeout=5)
        items = res.json().get('items', [])
        filtered = []
        for i in items:
            title = i['title'].replace('<b>','').replace('</b>','')
            desc = i['description'].replace('<b>','').replace('</b>','')
            if stock_name not in title:
                continue
            pub_dt = _parse_naver_date(i.get('pubDate', ''))
            if pub_dt and pub_dt < cutoff:
                continue
            filtered.append({
                'title': title,
                'description': desc,
                'pubDate': i.get('pubDate', ''),
                'originallink': i.get('originallink', ''),
                'link': i.get('link', '')
            })
            if len(filtered) >= display:
                break
        return filtered
    except:
        return []

def get_feature_news(stock_name, per_day=3, days=30):
    """특징주 뉴스 — 날짜별 균등 수집 (300개 병렬 호출, 하루 최대 per_day개 × days일)"""
    url = 'https://openapi.naver.com/v1/search/news.json'
    headers = {
        'X-Naver-Client-Id': NAVER_CLIENT_ID,
        'X-Naver-Client-Secret': NAVER_CLIENT_SECRET,
    }
    cutoff = datetime.now() - timedelta(days=days)
    day_count = {}
    filtered = []
    seen_titles = set()

    def fetch_page(query, start):
        params = {'query': query, 'display': 100, 'start': start, 'sort': 'date'}
        try:
            res = requests.get(url, headers=headers, params=params, timeout=5)
            return res.json().get('items', [])
        except:
            return []

    # 2개 쿼리 × 3페이지 = 6개 병렬 호출
    queries = [stock_name + ' 특징주', '특징주 ' + stock_name]
    starts = [1, 101, 201]

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(fetch_page, q, s): (q, s) for q in queries for s in starts}
        all_items = []
        for future in as_completed(futures):
            all_items.extend(future.result())

    # 최신순 정렬 후 필터링
    def get_dt(i):
        return _parse_naver_date(i.get('pubDate','')) or datetime.min
    all_items.sort(key=get_dt, reverse=True)

    for i in all_items:
        title = i['title'].replace('<b>','').replace('</b>','')
        desc = i['description'].replace('<b>','').replace('</b>','')
        if stock_name not in title or '특징주' not in title:
            continue
        if title in seen_titles:
            continue
        pub_dt = _parse_naver_date(i.get('pubDate', ''))
        if not pub_dt or pub_dt < cutoff:
            continue
        day_key = pub_dt.strftime('%Y-%m-%d')
        if day_count.get(day_key, 0) >= per_day:
            continue
        day_count[day_key] = day_count.get(day_key, 0) + 1
        seen_titles.add(title)
        filtered.append({
            'title': title,
            'description': desc,
            'pubDate': i.get('pubDate', ''),
            'originallink': i.get('originallink', ''),
            'link': i.get('link', '')
        })

    return filtered

# =============================================
# 네이버 DataLab 검색어 트렌드 API
# =============================================
def get_datalab_trend(stock_name):
    """네이버 DataLab 검색어 트렌드 — 통합/PC/모바일/성별/연령대 병렬 호출"""
    url = 'https://openapi.naver.com/v1/datalab/search'
    headers = {
        'X-Naver-Client-Id': NAVER_CLIENT_ID,
        'X-Naver-Client-Secret': NAVER_CLIENT_SECRET,
        'Content-Type': 'application/json',
    }
    today = datetime.now()
    end_date = today.strftime('%Y-%m-%d')
    start_date = (today - timedelta(days=365)).strftime('%Y-%m-%d')

    def fetch(device=None, gender=None, ages=None):
        body = {
            'startDate': start_date,
            'endDate': end_date,
            'timeUnit': 'date',
            'keywordGroups': [{'groupName': stock_name, 'keywords': [stock_name]}],
        }
        if device:
            body['device'] = device
        if gender:
            body['gender'] = gender
        if ages:
            body['ages'] = ages
        try:
            res = requests.post(url, headers=headers, json=body, timeout=8)
            data = res.json()
            return [{'date': r['period'], 'ratio': r['ratio']} for r in data.get('results', [{}])[0].get('data', [])]
        except Exception as e:
            print(f'[DATALAB] 오류 device={device} gender={gender} ages={ages}: {e}')
            return []

    # 병렬 호출: 통합/PC/모바일/남성/여성/연령대 6개
    with ThreadPoolExecutor(max_workers=8) as ex:
        f_all    = ex.submit(fetch)
        f_pc     = ex.submit(fetch, device='pc')
        f_mobile = ex.submit(fetch, device='mo')
        f_male   = ex.submit(fetch, gender='m')
        f_female = ex.submit(fetch, gender='f')
        f_age20  = ex.submit(fetch, ages=['2'])
        f_age30  = ex.submit(fetch, ages=['3'])
        f_age40  = ex.submit(fetch, ages=['4'])
        f_age50  = ex.submit(fetch, ages=['5'])
        f_age60  = ex.submit(fetch, ages=['6'])

    data_all    = f_all.result()
    data_pc     = f_pc.result()
    data_mobile = f_mobile.result()
    data_male   = f_male.result()
    data_female = f_female.result()
    data_age20  = f_age20.result()
    data_age30  = f_age30.result()
    data_age40  = f_age40.result()
    data_age50  = f_age50.result()
    data_age60  = f_age60.result()

    def calc_stats(data):
        """7일/30일/90일/180일/1년 평균, 대비%, 급등이력, 급등강도 계산"""
        if not data:
            return None
        ratios = [d['ratio'] for d in data]
        avg_1year   = round(sum(ratios) / len(ratios), 1) if ratios else 0
        avg_180days = round(sum(ratios[-180:]) / len(ratios[-180:]), 1) if len(ratios) >= 180 else avg_1year
        avg_90days  = round(sum(ratios[-90:]) / len(ratios[-90:]), 1) if len(ratios) >= 90 else avg_1year
        avg_30days  = round(sum(ratios[-30:]) / len(ratios[-30:]), 1) if len(ratios) >= 30 else avg_1year
        avg_7days   = round(sum(ratios[-7:]) / len(ratios[-7:]), 1) if len(ratios) >= 7 else avg_1year
        avg_1day    = round(ratios[-1], 1) if ratios else 0
        vs_30days   = round((avg_7days - avg_30days) / avg_30days * 100, 1) if avg_30days else 0
        vs_1year    = round((avg_7days - avg_1year) / avg_1year * 100, 1) if avg_1year else 0

        # 급등 감지 (1년 평균 대비 +30% 이상)
        surge_threshold = avg_1year * 1.3
        surge_days = [d for d in data if d['ratio'] >= surge_threshold]

        # 현재 급등 중인지 (최근 3일 중 2일 이상 급등)
        recent_3 = data[-3:] if len(data) >= 3 else data
        is_surging = sum(1 for d in recent_3 if d['ratio'] >= surge_threshold) >= 2

        # 급등 시작 시점 & 지속 기간
        surge_start = None
        surge_duration = 0
        if is_surging:
            for i in range(len(data) - 1, -1, -1):
                if data[i]['ratio'] >= surge_threshold:
                    surge_start = data[i]['date']
                    surge_duration += 1
                else:
                    break

        # 급등 속도 (최근 3일 추세)
        if len(ratios) >= 3:
            r3 = ratios[-3:]
            if r3[-1] > r3[0] * 1.05:
                surge_speed = '가속 중 📈'
            elif r3[-1] < r3[0] * 0.95:
                surge_speed = '둔화 중 📉'
            else:
                surge_speed = '유지 중 →'
        else:
            surge_speed = '정보 없음'

        # 과거 급등 이력 추출 (최대 5개)
        # label 기준으로 합치기 — 같은 주차면 pct 높은 값, days 합산
        history_map = {}  # label → {pct, days, ongoing}
        history_order = []  # label 순서 유지
        in_surge = False
        seg_start = None
        seg_max = 0
        seg_days = 0
        for d in data[:-3] if is_surging and surge_duration > 0 else data:
            if d['ratio'] >= surge_threshold:
                if not in_surge:
                    in_surge = True
                    seg_start = d['date']
                    seg_max = d['ratio']
                    seg_days = 1
                else:
                    seg_max = max(seg_max, d['ratio'])
                    seg_days += 1
            else:
                if in_surge:
                    dt = datetime.strptime(seg_start, '%Y-%m-%d')
                    week_num = (dt.day - 1) // 7 + 1
                    label = f"{str(dt.year)[2:]}년 {dt.month}월 {week_num}째주"
                    pct = round((seg_max - avg_1year) / avg_1year * 100) if avg_1year else 0
                    if label in history_map:
                        if pct > history_map[label]['pct']:
                            history_map[label]['pct'] = pct
                            history_map[label]['seg_start_dt'] = dt
                        history_map[label]['days'] += seg_days
                    else:
                        history_map[label] = {'pct': pct, 'days': seg_days, 'ongoing': False, 'seg_start_dt': dt}
                        history_order.append(label)
                    in_surge = False
        if is_surging and surge_start:
            dt = datetime.strptime(surge_start, '%Y-%m-%d')
            week_num = (dt.day - 1) // 7 + 1
            label = f"{str(dt.year)[2:]}년 {dt.month}월 {week_num}째주"
            pct = round((max(ratios[-surge_duration:]) - avg_1year) / avg_1year * 100) if avg_1year else 0
            if label in history_map:
                if pct > history_map[label]['pct']:
                    history_map[label]['pct'] = pct
                    history_map[label]['seg_start_dt'] = dt
                history_map[label]['days'] += surge_duration
                history_map[label]['ongoing'] = True
            else:
                history_map[label] = {'pct': pct, 'days': surge_duration, 'ongoing': True, 'seg_start_dt': dt}
                history_order.append(label)

        history = []
        for label_week in history_order:
            item = history_map[label_week]
            weeks = max(1, round(item['days'] / 7))
            if item['ongoing']:
                duration_label = '현재 진행 중 🔥'
            else:
                duration_label = f"{weeks}주 지속" if weeks > 1 else "1주 지속"
            seg_dt = item.get('seg_start_dt')
            label_day = seg_dt.strftime('%m/%d') if seg_dt else ''
            history.append({
                'label_week': label_week,
                'label_day': label_day,
                'pct': f"+{item['pct']}%",
                'pct_val': item['pct'],
                'duration': duration_label,
                'ongoing': item['ongoing'],
            })
        # 급등 강도 높은 순 Top5
        history = sorted(history, key=lambda x: x['pct_val'], reverse=True)[:5]

        # 급등 시작 레이블
        if surge_start:
            dt = datetime.strptime(surge_start, '%Y-%m-%d')
            week_num = (dt.day - 1) // 7 + 1
            surge_start_label = f"{dt.month}월 {week_num}째주"
        else:
            surge_start_label = ''

        return {
            'avg_1day': avg_1day,
            'avg_7days': avg_7days,
            'avg_30days': avg_30days,
            'avg_90days': avg_90days,
            'avg_180days': avg_180days,
            'avg_1year': avg_1year,
            'vs_30days': vs_30days,
            'vs_1year': vs_1year,
            'is_surging': is_surging,
            'surge_start': surge_start_label,
            'surge_duration': surge_duration,
            'surge_speed': surge_speed,
            'history': history,
            'raw': data,
        }

    def calc_attention_state(stats):
        """검색량 통계 → Attention State 레이블 변환"""
        if not stats:
            return None

        avg_1year    = stats.get('avg_1year', 0)
        avg_7days    = stats.get('avg_7days', 0)
        avg_30days   = stats.get('avg_30days', 0)
        avg_1day     = stats.get('avg_1day', 0)
        vs_30days    = stats.get('vs_30days', 0)
        vs_1year     = stats.get('vs_1year', 0)
        surge_dur    = stats.get('surge_duration', 0)
        raw          = stats.get('raw', [])

        # ① 절대량 등급 (저검색량 노이즈 필터)
        if avg_1year >= 30:
            abs_level = 'HIGH'
        elif avg_1year >= 10:
            abs_level = 'MID'
        else:
            abs_level = 'LOW'

        # ② 관심도 강도 (vs_30days 기준)
        if vs_30days >= 300:
            attention = 'explosive_attention'
        elif vs_30days >= 150:
            attention = 'strong_attention'
        elif vs_30days >= 50:
            attention = 'rising_attention'
        elif vs_30days <= -30:
            attention = 'fading_attention'
        else:
            attention = 'normal_attention'

        # ③ 멀티타임프레임 모멘텀
        vs_1d_7d = round((avg_1day - avg_7days) / avg_7days * 100, 1) if avg_7days else 0
        vs_30d_1y = round((avg_30days - avg_1year) / avg_1year * 100, 1) if avg_1year else 0

        if vs_1d_7d >= 30:
            short_momentum = 'SPIKE'      # 오늘 단기 뉴스성 급등
        elif vs_1d_7d <= -20:
            short_momentum = 'COOLING'
        else:
            short_momentum = 'STABLE'

        if vs_30d_1y >= 50:
            long_momentum = 'STRUCTURAL_RISE'   # 구조적 관심 증가
        elif vs_30d_1y <= -30:
            long_momentum = 'STRUCTURAL_DECLINE'
        else:
            long_momentum = 'NEUTRAL'

        # ④ Novelty (오랜만의 급부상인가)
        ratios = [d['ratio'] for d in raw] if raw else []
        if len(ratios) >= 30:
            import statistics
            median_1year = statistics.median(ratios)
            novelty_ratio = avg_7days / median_1year if median_1year else 1
            if novelty_ratio >= 3.0:
                novelty = 'FRESH'       # 오랫동안 죽어있다가 갑자기 부상
            elif novelty_ratio >= 1.5:
                novelty = 'RISING'
            else:
                novelty = 'NORMAL'
        else:
            novelty = 'UNKNOWN'

        # ⑤ Attention Concentration (하루 쏠림 vs 지속 관심)
        if len(ratios) >= 7:
            recent_7 = ratios[-7:]
            total_7 = sum(recent_7)
            max_day = max(recent_7)
            concentration = max_day / total_7 if total_7 else 0
            if concentration >= 0.5:
                spike_type = 'ONE_DAY_SPIKE'    # 단발 뉴스성
            elif concentration >= 0.3:
                spike_type = 'SHORT_BURST'
            else:
                spike_type = 'SUSTAINED_INTEREST'  # 지속적 관심
        else:
            spike_type = 'UNKNOWN'

        # ⑥ 지속성
        if surge_dur >= 14:
            persistence = 'sustained'
        elif surge_dur >= 5:
            persistence = 'building'
        else:
            persistence = 'short_term'

        # ⑦ 가속도 (최근 3일)
        if len(ratios) >= 3:
            r3 = ratios[-3:]
            if r3[-1] > r3[0] * 1.3:
                acceleration = 'parabolic'
            elif r3[-1] > r3[0] * 1.1:
                acceleration = 'accelerating'
            elif r3[-1] < r3[0] * 0.9:
                acceleration = 'weakening'
            else:
                acceleration = 'steady'
        else:
            acceleration = 'unknown'

        # ⑧ 피크 감지 (급등 후 꺾이는 중인가)
        if surge_dur >= 3 and acceleration in ('weakening',):
            peak_signal = 'POSSIBLE_PEAK'
        elif surge_dur >= 5 and spike_type == 'ONE_DAY_SPIKE':
            peak_signal = 'LATE_ATTENTION'   # 뒤늦은 관심 가능성
        else:
            peak_signal = 'NO_SIGNAL'

        return {
            'abs_level':      abs_level,
            'attention':      attention,
            'short_momentum': short_momentum,
            'long_momentum':  long_momentum,
            'novelty':        novelty,
            'spike_type':     spike_type,
            'persistence':    persistence,
            'acceleration':   acceleration,
            'peak_signal':    peak_signal,
        }

    def calc_gender_age(male_data, female_data, age_datasets):
        """성별/연령대 비율 계산 (1일/7일/30일/90일/180일 + Gemini용 7일/30일)"""
        def avg_n(data, n):
            vals = [d['ratio'] for d in data[-n:]] if len(data) >= n else [d['ratio'] for d in data]
            return sum(vals) / len(vals) if vals else 0

        def calc_pcts(male_val, female_val, age_vals):
            gender_total = male_val + female_val
            male_pct   = round(male_val / gender_total * 100) if gender_total else 50
            female_pct = 100 - male_pct
            age_total = sum(age_vals.values())
            age_pcts = {}
            if age_total:
                for k, v in age_vals.items():
                    age_pcts[k] = round(v / age_total * 100)
                diff = 100 - sum(age_pcts.values())
                if diff != 0:
                    max_key = max(age_pcts, key=lambda k: age_pcts[k])
                    age_pcts[max_key] += diff
            else:
                for k in age_vals:
                    age_pcts[k] = 20
            top_age = max(age_pcts, key=lambda k: age_pcts[k])
            return male_pct, female_pct, age_pcts, top_age

        # 기간별 계산
        periods = {'1': 1, '7': 7, '30': 30, '90': 90, '180': 180}
        by_period = {}
        for label, n in periods.items():
            m_val = avg_n(male_data, n)
            f_val = avg_n(female_data, n)
            a_vals = {k: avg_n(v, n) for k, v in age_datasets.items()}
            mp, fp, ap, ta = calc_pcts(m_val, f_val, a_vals)
            by_period[label] = {'male_pct': mp, 'female_pct': fp, 'age_pcts': ap, 'top_age': ta}

        # 기본(30일) — 기존 호환
        base = by_period['30']

        return {
            'male_pct':   base['male_pct'],
            'female_pct': base['female_pct'],
            'age_pcts':   base['age_pcts'],
            'top_age':    base['top_age'],
            'by_period':  by_period,
        }

    stats_all    = calc_stats(data_all)
    stats_pc     = calc_stats(data_pc)
    stats_mobile = calc_stats(data_mobile)
    gender_age   = calc_gender_age(
        data_male, data_female,
        {'20': data_age20, '30': data_age30, '40': data_age40, '50': data_age50, '60': data_age60}
    )
    attention_state = calc_attention_state(stats_all)

    return {
        'all': stats_all,
        'pc': stats_pc,
        'mobile': stats_mobile,
        'gender_age': gender_age,
        'attention_state': attention_state,
    }


# =============================================
# DART API
# =============================================
def get_dart_info(corp_code):
    url = 'https://opendart.fss.or.kr/api/majorstock.json'
    params = {'crtfc_key': DART_API_KEY, 'corp_code': corp_code}
    try:
        res = requests.get(url, params=params, timeout=5)
        data = res.json()
        if data.get('status') == '000':
            return data.get('list', [])[:5]
    except:
        pass
    return []

def search_dart_corp(name):
    url = 'https://opendart.fss.or.kr/api/company.json'
    params = {'crtfc_key': DART_API_KEY, 'corp_name': name, 'page_no': 1, 'page_count': 1}
    try:
        res = requests.get(url, params=params, timeout=5)
        data = res.json()
        items = data.get('list', [])
        if items:
            return items[0].get('corp_code', '')
    except:
        pass
    return ''

# =============================================
# Gemini 보고서 생성
# =============================================

# =============================================
# =============================================
# 한국투자증권 API 외국인·기관 수급
# Render 환경변수: KIS_APP_KEY, KIS_APP_SECRET 필요
# =============================================

# 토큰 갱신 시 동시접속 레이스컨디션 방지용 Lock
_kis_token_lock = threading.Lock()

def _kis_get_token():
    """한국투자증권 OAuth 토큰 발급 (MongoDB 캐싱 + 스레드세이프)
    - 발급된 토큰을 MongoDB에 저장
    - 23시간 이내면 DB에서 재사용 (한투 1일 1회 원칙 준수)
    - 만료됐을 때만 새로 발급
    """
    app_key    = os.environ.get('KIS_APP_KEY', '')
    app_secret = os.environ.get('KIS_APP_SECRET', '')
    if not app_key or not app_secret:
        print('[SUPPLY] KIS_APP_KEY 또는 KIS_APP_SECRET 없음')
        return None

    with _kis_token_lock:
        # 1) MongoDB에서 캐시된 토큰 확인
        if db is not None:
            try:
                col_token = db['kis_token']
                cached = col_token.find_one({'_id': 'kis_access_token'})
                if cached:
                    issued_at = cached.get('issued_at')
                    token_val = cached.get('access_token')
                    if issued_at and token_val:
                        elapsed = (datetime.utcnow() - issued_at).total_seconds()
                        if elapsed < 23 * 3600:  # 23시간 이내 → 재사용
                            print(f'[SUPPLY] KIS 토큰 캐시 사용 (발급 후 {int(elapsed//3600)}시간 {int((elapsed%3600)//60)}분 경과)')
                            return token_val
            except Exception as e:
                print(f'[SUPPLY] KIS 토큰 캐시 조회 오류: {e}')

        # 2) 캐시 없거나 만료 → 새로 발급
        url = 'https://openapi.koreainvestment.com:9443/oauth2/tokenP'
        body = {
            'grant_type': 'client_credentials',
            'appkey': app_key,
            'appsecret': app_secret,
        }
        try:
            res = requests.post(url, json=body, timeout=10)
            token = res.json().get('access_token')
            if token:
                print('[SUPPLY] KIS 토큰 신규 발급 성공')
                # 3) MongoDB에 저장
                if db is not None:
                    try:
                        col_token = db['kis_token']
                        col_token.replace_one(
                            {'_id': 'kis_access_token'},
                            {'_id': 'kis_access_token', 'access_token': token, 'issued_at': datetime.utcnow()},
                            upsert=True
                        )
                        print('[SUPPLY] KIS 토큰 MongoDB 저장 완료')
                    except Exception as e:
                        print(f'[SUPPLY] KIS 토큰 MongoDB 저장 오류: {e}')
            else:
                print(f'[SUPPLY] KIS 토큰 발급 실패: {res.text}')
            return token
        except Exception as e:
            print(f'[SUPPLY] KIS 토큰 오류: {e}')
            return None


def _kis_fetch_investor_once(code, token, app_key, app_secret, end_dt):
    """투자자매매동향 단건 호출 (end_dt 기준 과거 약 30건)"""
    url = 'https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily'
    headers = {
        'content-type': 'application/json',
        'authorization': f'Bearer {token}',
        'appkey': app_key,
        'appsecret': app_secret,
        'tr_id': 'FHPTJ04160001',
        'tr_cont': '',
        'custtype': 'P',
    }
    params = {
        'FID_COND_MRKT_DIV_CODE': 'J',
        'FID_INPUT_ISCD': code,
        'FID_INPUT_DATE_1': end_dt.strftime('%Y%m%d'),
        'FID_ORG_ADJ_PRC': '',
        'FID_ETC_CLS_CODE': '1',
    }
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        return res.json().get('output2') or []
    except Exception as e:
        print(f'[SUPPLY] 수급 단건 오류: {e}')
        return []


def _kis_fetch_investor(code, token, app_key, app_secret, start_dt, end_dt):
    """투자자매매동향 — 30건 한계 극복: end_dt를 30영업일씩 앞으로 당기며 최대 6회 호출 후 합치기"""
    all_rows = []
    seen_dates = set()
    cur_end = end_dt
    start_str = start_dt.strftime('%Y%m%d')

    for i in range(6):  # 최대 6회 (약 6개월)
        rows = _kis_fetch_investor_once(code, token, app_key, app_secret, cur_end)
        if not rows:
            break
        new_rows = [r for r in rows if r.get('stck_bsop_date', '') not in seen_dates]
        for r in new_rows:
            seen_dates.add(r.get('stck_bsop_date', ''))
        all_rows.extend(new_rows)
        # 가장 오래된 날짜 파악해서 다음 end_dt 설정 (30영업일 전)
        oldest = min(r.get('stck_bsop_date', '') for r in rows)
        if oldest <= start_str:
            break
        # 다음 호출용 end_dt: 가장 오래된 날짜 하루 전
        oldest_dt = datetime.strptime(oldest, '%Y%m%d') - timedelta(days=1)
        cur_end = oldest_dt

    # start_dt 이후 필터링 + 오름차순 정렬
    all_rows = [r for r in all_rows if r.get('stck_bsop_date', '') >= start_str]
    all_rows.sort(key=lambda r: r.get('stck_bsop_date', ''))
    print(f'[SUPPLY] 투자자매매동향 {code} {len(all_rows)}건 조회')
    return all_rows


def _kis_fetch_short(code, token, app_key, app_secret, start_dt, end_dt):
    """공매도 일별추이 FHPST04830000 — tr_cont 페이징 불가, 단건 호출
    DATE_1(시작) ~ DATE_2(끝) 범위 지정, DATE_1 공백시 전체"""
    url = 'https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/daily-short-sale'
    headers = {
        'content-type': 'application/json',
        'authorization': f'Bearer {token}',
        'appkey': app_key,
        'appsecret': app_secret,
        'tr_id': 'FHPST04830000',
        'custtype': 'P',
    }
    params = {
        'FID_COND_MRKT_DIV_CODE': 'J',
        'FID_INPUT_ISCD': code,
        'FID_INPUT_DATE_1': start_dt.strftime('%Y%m%d'),
        'FID_INPUT_DATE_2': end_dt.strftime('%Y%m%d'),
    }
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        data = res.json()
        rows = data.get('output2') or data.get('output') or []
        # 오름차순 정렬 (과거→최신)
        rows.sort(key=lambda r: r.get('stck_bsop_date', ''))
        print(f'[SUPPLY] 공매도 {code} {len(rows)}건 조회')
        return rows
    except Exception as e:
        print(f'[SUPPLY] 공매도 조회 오류: {e}')
        return []


def _kis_fetch_program_once(code, token, app_key, app_secret, end_dt):
    """프로그램매매 단건 호출 (end_dt 기준 과거 약 30건)"""
    url = 'https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/program-trade-by-stock-daily'
    headers = {
        'content-type': 'application/json',
        'authorization': f'Bearer {token}',
        'appkey': app_key,
        'appsecret': app_secret,
        'tr_id': 'FHPPG04650201',
        'custtype': 'P',
    }
    params = {
        'FID_COND_MRKT_DIV_CODE': 'J',
        'FID_INPUT_ISCD': code,
        'FID_INPUT_DATE_1': end_dt.strftime('%Y%m%d'),
    }
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        return res.json().get('output') or []
    except Exception as e:
        print(f'[SUPPLY] 프로그램 단건 오류: {e}')
        return []


def _kis_fetch_program(code, token, app_key, app_secret, start_dt, end_dt):
    """프로그램매매 — 30건 한계 극복: end_dt를 30영업일씩 앞으로 당기며 최대 6회 호출 후 합치기"""
    all_rows = []
    seen_dates = set()
    cur_end = end_dt
    start_str = start_dt.strftime('%Y%m%d')

    for i in range(6):  # 최대 6회 (약 6개월)
        rows = _kis_fetch_program_once(code, token, app_key, app_secret, cur_end)
        if not rows:
            break
        new_rows = [r for r in rows if r.get('stck_bsop_date', '') not in seen_dates]
        for r in new_rows:
            seen_dates.add(r.get('stck_bsop_date', ''))
        all_rows.extend(new_rows)
        oldest = min(r.get('stck_bsop_date', '') for r in rows)
        if oldest <= start_str:
            break
        oldest_dt = datetime.strptime(oldest, '%Y%m%d') - timedelta(days=1)
        cur_end = oldest_dt

    # start_dt 이후 필터링 + 오름차순 정렬
    all_rows = [r for r in all_rows if r.get('stck_bsop_date', '') >= start_str]
    all_rows.sort(key=lambda r: r.get('stck_bsop_date', ''))
    print(f'[SUPPLY] 프로그램 {code} {len(all_rows)}건 조회')
    return all_rows


def get_supply_demand(code):
    """한국투자증권 API로 외국인·기관·개인 수급 + 공매도 + 프로그램 수집 (최대 6개월)
    MongoDB 24시간 캐싱: 첫 조회만 API 호출, 이후엔 캐시 반환"""
    result = {'1w': [], '1m': [], '3m': [], '6m': []}
    try:
        from datetime import timezone
        _KST = timezone(timedelta(hours=9))
        _now_kst = datetime.now(_KST)
        if _now_kst.hour >= 16:
            end_dt = _now_kst.replace(tzinfo=None)
        else:
            end_dt = _now_kst.replace(tzinfo=None) - timedelta(days=1)

        # ── MongoDB 캐시 확인 ──
        cache_key = f'{code}_{end_dt.strftime("%Y%m%d")}'
        if col_supply_cache is not None:
            try:
                cached = col_supply_cache.find_one({'_id': cache_key})
                if cached:
                    elapsed = (datetime.utcnow() - cached['cached_at']).total_seconds()
                    if elapsed < 24 * 3600:
                        print(f'[SUPPLY] {code} 캐시 사용 (저장 후 {int(elapsed//3600)}시간 {int((elapsed%3600)//60)}분 경과)')
                        return cached['data']
            except Exception as e:
                print(f'[SUPPLY] 캐시 조회 오류: {e}')

        app_key    = os.environ.get('KIS_APP_KEY', '')
        app_secret = os.environ.get('KIS_APP_SECRET', '')

        token = _kis_get_token()
        if not token:
            return result

        start_dt = end_dt - relativedelta(months=6)  # 정확히 6개월 전

        print(f'[SUPPLY] {code} 수급/공매도/프로그램 조회 시작')

        # ── 3개 API 병렬 호출 (수급/프로그램은 내부에서 다중 호출) ──
        with ThreadPoolExecutor(max_workers=3) as ex:
            f_investor = ex.submit(_kis_fetch_investor, code, token, app_key, app_secret, start_dt, end_dt)
            f_short    = ex.submit(_kis_fetch_short,    code, token, app_key, app_secret, start_dt, end_dt)
            f_program  = ex.submit(_kis_fetch_program,  code, token, app_key, app_secret, start_dt, end_dt)

        investor_rows = f_investor.result()
        short_rows    = f_short.result()
        program_rows  = f_program.result()

        if not investor_rows:
            print(f'[SUPPLY] {code} 수급 데이터 없음')
            return result

        # 공매도/프로그램 날짜 인덱스 생성
        short_map   = {r.get('stck_bsop_date', ''): r for r in short_rows}
        program_map = {r.get('stck_bsop_date', ''): r for r in program_rows}

        all_data = []
        cum_f = cum_i = cum_p = 0
        cum_f_amt = cum_i_amt = cum_p_amt = 0
        cum_short = cum_prog = 0
        cum_short_amt = cum_prog_amt = 0

        for row in investor_rows:
            try:
                raw_date = row.get('stck_bsop_date', '')
                date_str = f"{raw_date[:4]}.{raw_date[4:6]}.{raw_date[6:]}" if len(raw_date) == 8 else raw_date

                def _int(v): return int(v or 0)

                # ── 차트용: 외국인/기관/개인 (주 단위 → 천주, 금액 백만원 → 억) ──
                f_qty = _int(row.get('frgn_ntby_qty'))
                i_qty = _int(row.get('orgn_ntby_qty'))
                p_qty = _int(row.get('prsn_ntby_qty'))
                f_amt = _int(row.get('frgn_ntby_tr_pbmn'))   # 백만원
                i_amt = _int(row.get('orgn_ntby_tr_pbmn'))
                p_amt = _int(row.get('prsn_ntby_tr_pbmn'))

                f_th = round(f_qty / 1000);  f_億 = round(f_amt / 100)
                i_th = round(i_qty / 1000);  i_億 = round(i_amt / 100)
                p_th = round(p_qty / 1000);  p_億 = round(p_amt / 100)

                cum_f += f_th; cum_i += i_th; cum_p += p_th
                cum_f_amt += f_億; cum_i_amt += i_億; cum_p_amt += p_億

                # ── Gemini용: 세부 투자자 (천주 단위) ──
                scrt_th  = round(_int(row.get('scrt_ntby_qty'))    / 1000)   # 증권
                ivtr_th  = round(_int(row.get('ivtr_ntby_qty'))    / 1000)   # 투자신탁
                pe_th    = round(_int(row.get('pe_fund_ntby_vol')) / 1000)   # 사모펀드
                bank_th  = round(_int(row.get('bank_ntby_qty'))    / 1000)   # 은행
                insu_th  = round(_int(row.get('insu_ntby_qty'))    / 1000)   # 보험
                fund_th  = round(_int(row.get('fund_ntby_qty'))    / 1000)   # 기금
                etc_th   = round(_int(row.get('etc_ntby_qty'))     / 1000)   # 기타

                # ── 공매도 ──
                sr = short_map.get(raw_date, {})
                s_qty = _int(sr.get('ssts_cntg_qty'))           # 당일 공매도 체결 수량
                s_amt = _int(sr.get('ssts_tr_pbmn'))            # 당일 공매도 거래 대금 (원)
                s_th  = round(s_qty / 1000)                     # 천주 단위
                s_億  = round(s_amt / 100000000)                # 억원 단위
                s_vol_pct  = float(sr.get('ssts_vol_rlim') or 0)    # 거래량 비중 %
                s_amt_pct  = float(sr.get('ssts_tr_pbmn_rlim') or 0) # 거래대금 비중 %
                s_acml_th  = round(_int(sr.get('acml_ssts_cntg_qty')) / 1000)  # 누적 공매도 수량 (천주)
                cum_short += s_th
                cum_short_amt += s_億

                # ── 프로그램 ──
                pr = program_map.get(raw_date, {})
                pg_qty = _int(pr.get('whol_smtn_ntby_qty'))
                pg_amt = _int(pr.get('whol_smtn_ntby_tr_pbmn'))  # 원 단위
                pg_th  = round(pg_qty / 1000)
                pg_億  = round(pg_amt / 100000000)
                cum_prog += pg_th
                cum_prog_amt += pg_億

                all_data.append({
                    'date': date_str,
                    # 차트용 — 누적
                    'foreign': cum_f, 'institution': cum_i, 'individual': cum_p,
                    # 차트용 — 당일 수량(천주)/금액(억)
                    'foreign_day': f_th, 'institution_day': i_th, 'individual_day': p_th,
                    'foreign_amt': f_億, 'institution_amt': i_億, 'individual_amt': p_億,
                    # 수급 누적 금액(억)
                    'foreign_cum_amt': cum_f_amt, 'institution_cum_amt': cum_i_amt, 'individual_cum_amt': cum_p_amt,
                    # 공매도 (차트: 누적 공매도 수량 / 툴팁: 당일 거래량+금액+비중%)
                    'short': s_acml_th,           # 차트 라인 → 누적 공매도 수량 (천주)
                    'short_day': s_th,             # 당일 거래량 (천주)
                    'short_amt': s_億,             # 당일 거래대금 (억원)
                    'short_cum_amt': cum_short_amt,# 누적 거래대금 (억원)
                    'short_vol_pct': round(s_vol_pct, 2),   # 거래량 비중 %
                    'short_amt_pct': round(s_amt_pct, 2),   # 거래대금 비중 %
                    # 프로그램
                    'program': cum_prog, 'program_day': pg_th, 'program_amt': pg_億, 'program_cum_amt': cum_prog_amt,
                    # Gemini용 세부 투자자 (천주)
                    'detail': {
                        'scrt': scrt_th, 'ivtr': ivtr_th, 'pe_fund': pe_th,
                        'bank': bank_th, 'insu': insu_th, 'fund': fund_th, 'etc': etc_th,
                    },
                })
            except Exception as row_e:
                print(f'[SUPPLY] 행 파싱 오류: {row_e}')
                continue

        if not all_data:
            print(f'[SUPPLY] {code} 수급 파싱 실패')
            return result

        # 기간별 슬라이싱 (영업일: 1주=5일, 1개월=20일, 3개월=60일, 6개월=120일)
        result['1w'] = all_data[-5:]   if len(all_data) >= 5   else all_data
        result['1m'] = all_data[-20:]  if len(all_data) >= 20  else all_data
        result['3m'] = all_data[-60:]  if len(all_data) >= 60  else all_data
        result['6m'] = all_data[-120:] if len(all_data) >= 120 else all_data

        print(f'[SUPPLY] {code} 조회 성공: 수급 {len(investor_rows)}건 / 공매도 {len(short_rows)}건 / 프로그램 {len(program_rows)}건')

        # ── MongoDB 캐시 저장 (24시간) ──
        if col_supply_cache is not None:
            try:
                col_supply_cache.replace_one(
                    {'_id': cache_key},
                    {'_id': cache_key, 'data': result, 'cached_at': datetime.utcnow()},
                    upsert=True
                )
                print(f'[SUPPLY] {code} 캐시 저장 완료')
            except Exception as ce:
                print(f'[SUPPLY] 캐시 저장 오류: {ce}')

    except Exception as e:
        print(f'[SUPPLY] {code} 오류: {e}')

    return result


def calc_technical_state(code):
    """yfinance로 보조지표 State 계산 (RSI, ATR, 이평선, 거래량, 세력단가, 240일최고종가)"""
    try:
        import yfinance as yf
        import pandas as pd
        import numpy as np

        # suffix 확인
        df = pd.DataFrame()
        for suffix in ['.KS', '.KQ']:
            try:
                ticker = yf.Ticker(code + suffix)
                tmp = ticker.history(period='2y', auto_adjust=False)
                if not tmp.empty:
                    df = tmp[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
                    break
            except:
                continue

        if df.empty or len(df) < 20:
            print(f'[TECH STATE] {code} 데이터 부족')
            return {}

        close = df['Close']
        high  = df['High']
        low   = df['Low']
        volume = df['Volume']

        # ── RSI (14일) ──
        delta = close.diff()
        gain = delta.clip(lower=0).rolling(14).mean()
        loss = (-delta.clip(upper=0)).rolling(14).mean()
        rs = gain / loss.replace(0, float('nan'))
        rsi = 100 - (100 / (1 + rs))
        rsi_val = round(float(rsi.iloc[-1]), 1)

        if rsi_val >= 80:
            rsi_state = 'extremely_overheated'
        elif rsi_val >= 70:
            rsi_state = 'overheated'
        elif rsi_val >= 55:
            rsi_state = 'bullish_momentum'
        elif rsi_val <= 30:
            rsi_state = 'oversold'
        else:
            rsi_state = 'neutral'

        # ── ATR (14일) ──
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs()
        ], axis=1).max(axis=1)
        atr_now  = tr.rolling(14).mean().iloc[-1]
        atr_prev = tr.rolling(14).mean().iloc[-15] if len(df) >= 29 else atr_now
        atr_change = round((atr_now - atr_prev) / atr_prev * 100, 1) if atr_prev else 0

        if atr_change >= 50:
            atr_state = 'volatility_expansion'
        elif atr_change <= -30:
            atr_state = 'volatility_compression'
        else:
            atr_state = 'normal_volatility'

        # ── 이평선 ──
        ma20  = close.rolling(20).mean().iloc[-1]
        ma60  = close.rolling(60).mean().iloc[-1]
        ma120 = close.rolling(120).mean().iloc[-1]

        if ma20 > ma60 > ma120:
            ma_state = 'strong_uptrend'
        elif ma20 > ma60:
            ma_state = 'uptrend'
        elif ma20 < ma60 < ma120:
            ma_state = 'strong_downtrend'
        elif ma20 < ma60:
            ma_state = 'downtrend'
        else:
            ma_state = 'sideways'

        # ── 거래량 State ──
        vol_ma20 = volume.rolling(20).mean().iloc[-1]
        vol_now  = volume.iloc[-1]
        vol_ratio = round(vol_now / vol_ma20, 2) if vol_ma20 else 1

        if vol_ratio >= 3.0:
            vol_state = 'volume_breakout'
        elif vol_ratio >= 1.5:
            vol_state = 'volume_expansion'
        elif vol_ratio <= 0.5:
            vol_state = 'drying_volume'
        else:
            vol_state = 'normal_volume'

        # ── 세력단가 (VWAP 240일) ──
        n = min(240, len(df))
        df_240 = df.iloc[-n:]
        typical = (df_240['High'] + df_240['Low'] + df_240['Close']) / 3
        vwap_240 = (typical * df_240['Volume']).sum() / df_240['Volume'].sum()
        vwap_240 = round(float(vwap_240))
        current_close = round(float(close.iloc[-1]))
        vwap_diff_pct = round((current_close - vwap_240) / vwap_240 * 100, 1)

        if vwap_diff_pct >= 10:
            vwap_state = 'above_force_price_strong'
        elif vwap_diff_pct >= 0:
            vwap_state = 'above_force_price'
        elif vwap_diff_pct >= -10:
            vwap_state = 'below_force_price'
        else:
            vwap_state = 'below_force_price_strong'

        # ── 240일 최고 종가 ──
        high240_close = round(float(close.iloc[-n:].max()))
        high240_diff_pct = round((current_close - high240_close) / high240_close * 100, 1)

        if high240_diff_pct >= -5:
            high240_state = 'near_high'
        elif high240_diff_pct >= -20:
            high240_state = 'pullback_from_high'
        elif high240_diff_pct >= -40:
            high240_state = 'deep_pullback'
        else:
            high240_state = 'far_from_high'

        result = {
            'rsi': {'value': rsi_val, 'state': rsi_state},
            'atr': {'change_pct': atr_change, 'state': atr_state},
            'ma': {'ma20': round(float(ma20)), 'ma60': round(float(ma60)), 'ma120': round(float(ma120)), 'state': ma_state},
            'volume': {'ratio_vs_ma20': vol_ratio, 'state': vol_state},
            'vwap240': {'value': vwap_240, 'diff_pct': vwap_diff_pct, 'state': vwap_state},
            'high240_close': {'value': high240_close, 'diff_pct': high240_diff_pct, 'state': high240_state},
        }
        print(f'[TECH STATE] {code} 계산 성공: RSI={rsi_val} / MA={ma_state} / VWAP={vwap_diff_pct}%')
        return result

    except Exception as e:
        print(f'[TECH STATE] {code} 오류: {e}')
        return {}


def _sanitize(text):
    """줄바꿈·탭·제어문자를 공백으로 치환해 JSON 파싱 오류 방지"""
    if not text:
        return ''
    import re as _re
    text = text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    text = _re.sub(r'[\x00-\x1f\x7f]', ' ', text)
    text = _re.sub(r' {2,}', ' ', text)
    return text.strip()

def _calc_surge_rank(t):
    """급등강도 레이블 계산 (index.html의 calcRankLabel과 동일 로직)"""
    if not t.get('is_surging'):
        return '급등 없음'
    raw = t.get('raw', [])
    current_val = t.get('avg_7days', 0)
    if not raw or not current_val:
        return '급등 중'
    ratios = sorted([d['ratio'] for d in raw], reverse=True)
    rank = next((i+1 for i, v in enumerate(ratios) if current_val >= v), len(ratios))
    return f'1년 기준 {rank}위 수준 📈'

def generate_report(stock_name, code, stock_info, news_list, financial_data, dart_info, target_news_list=None, feature_news_list=None, trend_data=None, supply_demand=None, parsed_consensus=None, model_name='gemini-3.5-flash', tech_state=None):
    dart_text = ' / '.join([f"{d.get('nm','')}: {d.get('trmend_posesn_stock_rate','')}%" for d in dart_info]) if dart_info else '없음'
    financial_text = _sanitize(financial_data[:400]) if financial_data else '없음'

    feature_news_list = feature_news_list or []
    target_news_list = target_news_list or []

    # 7일 이내 특징주 (특징주뉴스 분석용 - 최대 20개)
    cutoff_7 = datetime.now() - timedelta(days=7)
    recent_7 = [n for n in feature_news_list if (_parse_naver_date(n.get('pubDate','')) or datetime.min) >= cutoff_7]
    feature_lines = []
    for idx, n in enumerate(recent_7[:20]):
        feature_lines.append(f"[{idx}] {_sanitize(n['title'])}: {_sanitize(n['description'])}")
    feature_text = '\n'.join(feature_lines) or '정보없음'

    # 목표주가 뉴스 텍스트
    target_text = ' / '.join([f"{_sanitize(n['title'])}: {_sanitize(n['description'])}" for n in target_news_list[:3]]) or '정보없음'

    # parsed_consensus → Gemini 프롬프트용 텍스트 변환
    parsed_consensus = parsed_consensus or []
    if parsed_consensus:
        consensus_lines = [f"{c.get('firm','-')}: 목표가 {c.get('target','-')} ({c.get('date','-')})" for c in parsed_consensus[:10]]
        consensus_text = ' / '.join(consensus_lines)
    else:
        consensus_text = '데이터 없음'

    # AI 핵심 뉴스용 — 특징주 전체 + 목표주가 전체 번호 매핑
    all_news_pool = feature_news_list + target_news_list
    pool_lines = []
    for idx, n in enumerate(all_news_pool):
        pool_lines.append(f"[{idx}] {_sanitize(n['title'])}: {_sanitize(n['description'])}")
    pool_text = '\n'.join(pool_lines) or '없음'

    # AI 핵심 뉴스 픽 기준
    ai_pick_criteria = """다음 우선순위 기준으로 선별:
1. 발언자 관련 (빅테크CEO·젠슨황·샘올트먼·사티아나델라·미국대통령·이재명대통령·Fed의장·한국은행총재·장관·무역대표부·고객사IR)
2. 정책·규제 관련 (정부정책·규제·보조금·지원법)
3. 기술·제품 관련 (신기술·신제품·특허·양산)
4. 계약·수주·공시·실적 관련 (수주·MOU·협약·공시·실적발표·유상증자·합병·분할)
5. 기업 이벤트 (노조·파업·경영진변동·소송·제재·기관외국인수급)
위 기준 해당 없으면 빈 배열 반환."""

    # 검색량 트렌드 텍스트 (Gemini 프롬프트용)
    trend_text = ''
    if trend_data and trend_data.get('all'):
        t = trend_data['all']
        ga = trend_data.get('gender_age', {})
        att = trend_data.get('attention_state') or {}
        att_text = ''
        if att:
            att_text = f"""
[검색량 Attention State]
절대관심도: {att.get('abs_level','-')} | 관심강도: {att.get('attention','-')}
단기모멘텀: {att.get('short_momentum','-')} | 장기모멘텀: {att.get('long_momentum','-')}
신선도(Novelty): {att.get('novelty','-')} | 집중도: {att.get('spike_type','-')}
지속성: {att.get('persistence','-')} | 가속도: {att.get('acceleration','-')}
피크신호: {att.get('peak_signal','-')}
→ Attention State 값은 절대 직접 언급하거나 독립 문장으로 쓰지 말 것 (예: "오랜만의 재조명" 같은 State 직역 금지)
→ State는 수급·기술지표·뉴스 흐름 해석의 판단 근거로만 자연스럽게 녹여쓸 것
→ abs_level=LOW이면 검색량 변화율 과대해석 없이 보수적으로 서술
→ spike_type=ONE_DAY_SPIKE이면 "단기 뉴스성 반응 가능성" 을 수급 흐름 문단에 자연스럽게 연결
→ peak_signal=POSSIBLE_PEAK 또는 LATE_ATTENTION이면 기술지표·수급 흐름과 연결해서 단기 고점 가능성 언급
→ novelty=FRESH이면 장기 침체 후 관심 회복이라는 맥락을 수급·거래량 해석에 녹여쓸 것"""
        # 오늘 날짜 컨텍스트 생성
        _kr_holidays = _holidays_lib.Korea()
        _today = datetime.now()
        _wd = _today.weekday()
        if _wd >= 5:
            _day_ctx = f"오늘은 {'토요일' if _wd==5 else '일요일'}(주말)입니다. 주말은 검색량이 자연스럽게 낮으므로 오늘 검색량을 평일 기준 평균과 비교하거나 관심 감소로 해석하지 말 것!!"
        elif _today.date() in _kr_holidays:
            _day_ctx = f"오늘은 공휴일입니다. 공휴일은 검색량이 자연스럽게 낮으므로 오늘 검색량을 평일 기준 평균과 비교하거나 관심 감소로 해석하지 말 것!!"
        else:
            _day_ctx = "오늘은 평일입니다. 정상적으로 분석할 것."

        trend_text = f"""
[오늘 날짜 컨텍스트]
{_day_ctx}

[검색량 트렌드 데이터]
7일 평균={t['avg_7days']} | 30일 대비={'+' if t['vs_30days']>=0 else ''}{t['vs_30days']}% {'급증' if t['vs_30days']>30 else ''}
30일 평균={t['avg_30days']} | 1년 대비={'+' if t['vs_1year']>=0 else ''}{t['vs_1year']}% {'급증' if t['vs_1year']>30 else ''}
1년 평균={t['avg_1year']}
급등 시작: {t['surge_start'] or '없음'} / 지속 기간: {t['surge_duration']}일째
현재 급등 강도: {_calc_surge_rank(t)} (7일 평균 기준)
과거 급등 이력: {len(t['history'])}회
성별(7일): 남성 {ga.get('by_period',{}).get('7',{}).get('male_pct', ga.get('male_pct',50))}% / 여성 {ga.get('by_period',{}).get('7',{}).get('female_pct', ga.get('female_pct',50))}%
성별(30일): 남성 {ga.get('by_period',{}).get('30',{}).get('male_pct', ga.get('male_pct',50))}% / 여성 {ga.get('by_period',{}).get('30',{}).get('female_pct', ga.get('female_pct',50))}%
연령대(7일): {' / '.join([f"{k}대 {v}%" for k,v in ga.get('by_period',{}).get('7',{}).get('age_pcts', ga.get('age_pcts',{})).items()])}
연령대(30일): {' / '.join([f"{k}대 {v}%" for k,v in ga.get('by_period',{}).get('30',{}).get('age_pcts', ga.get('age_pcts',{})).items()])}
→ 특징주 분석 시 검색량 급등 시점과 뉴스 날짜 교차 분석할 것
→ 7일 평균이 30일 평균 상회할수록 가산점 높게
→ 지속 기간 길수록 가산점 높게 (단순 하루 반짝 vs 지속 급등 구분)
→ 1년 기준 순위가 높을수록 가산점 높게
→ 성별·연령대 데이터는 최종의견에서 수급 성향 분석에 반드시 활용할 것
→ 검색량 급등은 대중 관심 급증 신호이며 단기 고점 가능성도 함께 언급할 것{att_text}"""

    # 수급 데이터 텍스트 (Gemini 프롬프트용)
    supply_text = ''
    if supply_demand:
        rows_20 = supply_demand.get('1m', [])
        if rows_20:
            def _s(v): return ('+' if v >= 0 else '') + f'{v:,}'
            supply_lines = []
            for r in rows_20[-10:]:
                f_day = r.get('foreign_day', 0)
                i_day = r.get('institution_day', 0)
                p_day = r.get('individual_day', 0)
                supply_lines.append(f"{r['date']}: 외국인 {_s(f_day)} / 기관 {_s(i_day)} / 개인 {_s(p_day)} (천주)")
            last = rows_20[-1]
            detail_cum = {'scrt':0,'ivtr':0,'pe_fund':0,'bank':0,'insu':0,'fund':0,'etc':0}
            for r in rows_20:
                for k in detail_cum:
                    detail_cum[k] += r.get('detail', {}).get(k, 0)
            supply_text = f"""
[투자자별 수급 데이터 (최근 1개월)]
최근 10일 일별 순매수 (천주):
{chr(10).join(supply_lines)}

1개월 누적 (천주):
· 외국인 {_s(last['foreign'])} / 기관 {_s(last['institution'])} / 개인 {_s(last.get('individual',0))}

기관 세부 (1개월 누적, 천주):
· 증권 {_s(detail_cum['scrt'])} / 투자신탁 {_s(detail_cum['ivtr'])} / 사모펀드 {_s(detail_cum['pe_fund'])}
· 은행 {_s(detail_cum['bank'])} / 보험 {_s(detail_cum['insu'])} / 기금 {_s(detail_cum['fund'])} / 기타 {_s(detail_cum['etc'])}

공매도 1개월 누적: {_s(last.get('short',0))} 천주
프로그램 1개월 누적: {_s(last.get('program',0))} 천주
→ 외국인·기관·개인 흐름 + 기관 내 세부 주체(기금/투신/보험 등) 분화 양상을 최종의견에 녹여쓸 것
→ 누적 플러스=매집, 마이너스=매도 흐름으로 해석"""


    # 보조지표 State 텍스트 (Gemini 프롬프트용)
    tech_state = tech_state or {}
    tech_text = ''
    if tech_state:
        rsi   = tech_state.get('rsi', {})
        atr   = tech_state.get('atr', {})
        ma    = tech_state.get('ma', {})
        vol   = tech_state.get('volume', {})
        vwap  = tech_state.get('vwap240', {})
        h240  = tech_state.get('high240_close', {})
        tech_text = f"""
[보조지표 State]
RSI: {rsi.get('value','-')} → {rsi.get('state','-')}
ATR 변동성: {atr.get('change_pct','-')}% → {atr.get('state','-')}
이평선: MA20={ma.get('ma20','-')} / MA60={ma.get('ma60','-')} / MA120={ma.get('ma120','-')} → {ma.get('state','-')}
거래량: 20일 평균 대비 {vol.get('ratio_vs_ma20','-')}배 → {vol.get('state','-')}
세력단가(VWAP240): {vwap.get('value','-')}원 / 현재가 대비 {'+' if (vwap.get('diff_pct') or 0)>=0 else ''}{vwap.get('diff_pct','-')}% → {vwap.get('state','-')}
240일 최고종가: {h240.get('value','-')}원 / 현재가 대비 {h240.get('diff_pct','-')}% → {h240.get('state','-')}
→ 위 수치는 최종의견 본문 안에서 흐름 해석에 자연스럽게 녹여쓸 것. 수치 나열 금지.
→ [데이터원문] 항목에만 수치 그대로 출력할 것."""

    prompt = f"""{stock_name}({code}) 투자 분석. JSON만 출력. 다른 텍스트 절대 금지. 백틱 금지.

지표: {json.dumps(stock_info, ensure_ascii=False)}
재무: {financial_text}
대주주: {dart_text}
증권사 목표주가 (최근 1개월, Gemini 웹검색 수집 — 최종의견에서 평균목표가·상승여력 언급에 활용): {consensus_text}
{trend_text}
{supply_text}
{tech_text}

[최근7일 특징주뉴스]
{feature_text}

[AI핵심뉴스 선별용 전체풀 (특징주+목표주가)]
{pool_text}

규칙:
1. 최근7일 특징주뉴스 기반으로 특징주뉴스 항목 작성 (2줄).
2. AI핵심뉴스: {ai_pick_criteria} 최대 10개 번호만 배열로 반환.
3. 모든 문자열값에 큰따옴표 안에 큰따옴표 사용 금지.

{{"한줄요약":"한문장 — 팩트 70% + 감성 30%. 지금 이 종목에 무슨 일이 일어나고 있는지 첫 문장에 바로 파악되게. 30자 이내. 전문용어 최소화. 감성 표현은 마지막 한 단어 수준으로만.","수급요약":"한문장 — 외인/기관/개인 자금 흐름을 감성 문장으로. 예: 거대한 자금이 빠져나간 자리를 개인들의 확신이 채워가고 있습니다.","검색요약":"한문장 — 대중 관심도 변화를 감성 문장으로. 예: 새로운 세대의 호기심이 종목의 온도를 급격히 되살리는 중입니다.","특징주뉴스":"중요뉴스기반2줄 — 감성 문체로. 뉴스 제목 나열 금지.","변동원인":"3줄","시나리오":{{"bull":"강세근거","base":"기본흐름","bear":"리스크"}},"최종의견":"시장 칼럼니스트 스타일로 작성. 만연체 금지. 단문 위주. 주제가 전환될 때마다 빈 줄로 문단을 나눌 것. 한 문단은 3문장 이내. 각 문단이 앞 문단을 자연스럽게 이어받아 하나의 스토리 흐름으로 연결될 것. 심리 주체(개인/기관/외국인/단타자금/추격자금) 2개 이상 등장. 엇갈리는 신호 반드시 포함. 수치는 흐름에 녹여쓸것. 수치 나열 금지. 확신형 단정 금지. 가능성형으로. 구조: 첫 문단=현재 분위기 한 문장(현재 OOO 단계로 보입니다 형식). 본문=검색량+수급+보조지표+뉴스 흐름을 주제 전환마다 문단 나눠서 스토리로 연결. 시나리오=긍정/부정 각 한 문장씩 빈 줄로 구분. 마지막=[데이터원문] RSI=값 / ATR변동=값% / MA20=값 MA60=값 MA120=값 / 거래량=20일평균대비값배 / VWAP240=값원(현재가대비값%) / 240일최고=값원(현재가대비값%). 절대금지: 투자권유 / 유튜브식 과장 / 애널리스트 말투","핵심키워드":[{{"태그":"키워드1","설명":"설명"}},{{"태그":"키워드2","설명":"설명"}},{{"태그":"키워드3","설명":"설명"}},{{"태그":"키워드4","설명":"설명"}},{{"태그":"키워드5","설명":"설명"}},{{"태그":"키워드6(선택)","설명":"설명"}},{{"태그":"키워드7(선택)","설명":"설명"}},{{"태그":"키워드8(선택)","설명":"설명"}}],"ai핵심뉴스인덱스":[0,1,2]}}\n핵심키워드는 최소 5개 최대 8개로 작성할 것. 6~8번째는 중요도가 충분할 때만 포함."""
    try:
        import time as _time
        _t0 = _time.time()
        used_model, text = call_gemini(prompt, model_name)
        _elapsed = round(_time.time() - _t0, 1)
        print(f'[GEMINI] {used_model} {_elapsed}초')
        text = text.strip().replace('```json', '').replace('```', '').strip()
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1:
            text = text[start:end+1]
        import re as _re
        text = _re.sub(r'[\x00-\x09\x0b\x0c\x0e-\x1f\x7f]', ' ', text)
        result = json.loads(text)
        if '한줄요약' not in result:
            return {'error': '응답 형식 오류', '한줄요약': '보고서 생성 실패', '핵심키워드': []}, used_model, []
        # AI 핵심 뉴스 인덱스로 실제 뉴스 매핑
        ai_idx_list = result.pop('ai핵심뉴스인덱스', [])
        ai_news_list = []
        for idx in ai_idx_list:
            if isinstance(idx, int) and 0 <= idx < len(all_news_pool):
                ai_news_list.append(all_news_pool[idx])
        return result, used_model, ai_news_list
    except Exception as e:
        print(f'[GEMINI ERROR]: {e}')
        return {'error': str(e), '한줄요약': '보고서 생성 실패', '핵심키워드': []}, model_name, []


# =============================================
# API 엔드포인트
# =============================================
@app.route('/api/report', methods=['POST'])
@login_required
def api_report():
    data = request.get_json()
    query = data.get('query', '').strip()
    model_name = data.get('model', 'gemini-3.5-flash')
    if not query:
        return jsonify({'error': '종목명을 입력해주세요.'}), 400
    code, name = get_stock_code(query)[:2]
    if not code:
        return jsonify({'error': f'종목을 찾을 수 없습니다: {query}'}), 404
    stock_info = get_stock_info(code)
    # 뉴스 API + DataLab + 수급 병렬 호출
    ex = ThreadPoolExecutor(max_workers=8)
    f_feature_news = ex.submit(get_feature_news, name)
    f_target_news  = ex.submit(get_target_price_news, name, 5)
    f_financial    = ex.submit(get_financial_data, code)
    f_trend        = ex.submit(get_datalab_trend, name)
    f_supply       = ex.submit(get_supply_demand, code)
    f_tech_state   = ex.submit(calc_technical_state, code)

    # 각 작업 개별 timeout 적용 (느린 작업이 전체를 블로킹하지 않도록)
    try: feature_news_list = f_feature_news.result(timeout=12)
    except Exception: feature_news_list = []
    try: target_news_list  = f_target_news.result(timeout=8)
    except Exception: target_news_list = []
    try: financial_data    = f_financial.result(timeout=10)
    except Exception: financial_data = ''
    try: trend_data        = f_trend.result(timeout=12)
    except Exception: trend_data = {}
    try: supply_demand     = f_supply.result(timeout=12)
    except Exception: supply_demand = {}
    try: tech_state        = f_tech_state.result(timeout=15)
    except Exception:
        tech_state = {}
        print('[TECH STATE] 타임아웃 - 빈 딕셔너리로 진행')
    ex.shutdown(wait=False)

    # Gemini 목표주가 파싱 + 보고서 생성 병렬
    ex2 = ThreadPoolExecutor(max_workers=2)
    f_parsed_consensus = ex2.submit(_parse_consensus_with_gemini, name, model_name)
    f_report = ex2.submit(generate_report, name, code, stock_info, [], financial_data, [], target_news_list, feature_news_list, trend_data, supply_demand, None, model_name, tech_state)

    try: parsed_consensus = f_parsed_consensus.result(timeout=60)
    except Exception:
        parsed_consensus = {'list': [], 'status': 'error'}
        print('[CONSENSUS PARSE] 타임아웃')
    try: report, used_model, ai_news_list = f_report.result(timeout=60)
    except Exception:
        report, used_model, ai_news_list = {}, 'error', []
    ex2.shutdown(wait=False)

    # 7일 이내 / 8~30일 분리
    cutoff_7 = datetime.now() - timedelta(days=7)
    feature_7days  = [n for n in feature_news_list if (_parse_naver_date(n.get('pubDate','')) or datetime.min) >= cutoff_7]
    feature_30days = [n for n in feature_news_list if (_parse_naver_date(n.get('pubDate','')) or datetime.min) < cutoff_7]

    return jsonify({
        'name': name,
        'code': code,
        'stock_info': stock_info,
        'report': report,
        'target_news_list': target_news_list,
        'feature_7days': feature_7days,
        'feature_30days': feature_30days,
        'ai_news_list': ai_news_list,
        'used_model': used_model,
        'trend_data': trend_data,
        'supply_demand': supply_demand,
        'parsed_consensus': parsed_consensus.get('list', []) if isinstance(parsed_consensus, dict) else parsed_consensus,
        'consensus_status': parsed_consensus.get('status', 'error') if isinstance(parsed_consensus, dict) else 'error',
    })

@app.route('/api/sector', methods=['POST'])
@login_required
def api_sector():
    data = request.get_json()
    sector = data.get('query', '').strip()
    model_name = data.get('model', 'gemini-3.5-flash')
    if not sector:
        return jsonify({'error': '섹터명을 입력해주세요.'}), 400
    news_list = get_news(sector + ' 업종 주식', display=8)
    news_text = '\n'.join([f"- {n['title']}: {n['description']}" for n in news_list])
    prompt = f"""
{sector} 섹터/업종 분석 보고서를 작성해주세요.

[관련 뉴스]
{news_text}

JSON 형식으로만 응답하세요.
{{
  "한줄요약": "섹터 현황 한 문장",
  "섹터동향": "최근 섹터 동향 분석 (3~4줄)",
  "주요종목": "해당 섹터 주요 종목 및 특징",
  "투자포인트": "섹터 투자 시 핵심 포인트 (3~4줄)",
  "리스크": "주요 리스크 요인",
  "핵심키워드": [
    {{"태그": "키워드1", "설명": "설명"}},
    {{"태그": "키워드2", "설명": "설명"}},
    {{"태그": "키워드3", "설명": "설명"}},
    {{"태그": "키워드4", "설명": "설명"}},
    {{"태그": "키워드5", "설명": "설명"}}
  ]
}}
핵심키워드는 최소 5개로 작성할 것.
"""
    try:
        used_model, text = call_gemini(prompt, model_name)
        text = text.strip().replace('```json','').replace('```','').strip()
        report = json.loads(text)
    except Exception as e:
        used_model = model_name
        report = {'error': str(e), '한줄요약': '섹터 분석 실패', '핵심키워드': []}
    return jsonify({'name': sector, 'report': report, 'used_model': used_model})

@app.route('/api/chat', methods=['POST'])
@login_required
def api_chat():
    data = request.get_json()
    message = data.get('message', '').strip()
    history = data.get('history', [])
    model_name = data.get('model', 'gemini-3.5-flash')
    if not message:
        return jsonify({'error': '메시지를 입력해주세요.'}), 400
    chat_history = [{'role': h['role'], 'parts': [h['content']]} for h in history[-10:]]
    try:
        used_model, reply = call_gemini_chat(chat_history, message, model_name)
        return jsonify({'reply': reply, 'used_model': used_model})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# =============================================
# 검색기록 / 즐겨찾기 API (MongoDB)
# =============================================
@app.route('/api/history', methods=['GET'])
@login_required
def api_get_history():
    if col_history is None:
        return jsonify([])
    try:
        cutoff = datetime.utcnow() - timedelta(days=90)
        starred_codes = list(set(s['code'] for s in col_stars.find({}, {'_id': 0, 'code': 1})))
        raw = list(col_history.find(
            {'ts': {'$gte': cutoff}, 'code': {'$nin': starred_codes}}
        ).sort('ts', -1).limit(200))
        items = []
        for r in raw:
            r['_id'] = str(r['_id'])
            items.append(r)
        return jsonify(items)
    except Exception as e:
        return jsonify([])

@app.route('/api/history', methods=['POST'])
@login_required
def api_save_history():
    if col_history is None:
        return jsonify({'ok': False})
    try:
        data = request.get_json()
        code = data.get('code', '')
        created_at = data.get('createdAt', '')
        # 같은 code + createdAt 기존 항목 먼저 삭제 (중복 방지)
        if code and created_at:
            col_history.delete_many({'code': code, 'createdAt': created_at})
        col_history.insert_one({
            'name': data.get('name', ''),
            'code': code,
            'report': data.get('report', {}),
            'stockInfo': data.get('stockInfo', {}),
            'date': data.get('date', ''),
            'createdAt': data.get('createdAt', ''),
            'newsList': data.get('newsList', []),
            'targetNewsList': data.get('targetNewsList', []),
            'feature7days': data.get('feature7days', []),
            'feature30days': data.get('feature30days', []),
            'aiNewsList': data.get('aiNewsList', []),
            'trendData': data.get('trendData', {}),
            'supplyDemand': data.get('supplyDemand', {}),
            'consensusList': data.get('consensusList', []),
            'ts': datetime.utcnow()
        })
        # 90일 지난 기록 자동 삭제 (즐겨찾기 제외)
        cutoff = datetime.utcnow() - timedelta(days=90)
        stars = set(s['code'] for s in col_stars.find({}, {'_id': 0, 'code': 1}))
        col_history.delete_many({'ts': {'$lt': cutoff}, 'code': {'$nin': list(stars)}})
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/history/delete-all', methods=['POST'])
@login_required
def api_delete_all_history():
    if col_history is None:
        return jsonify({'ok': False})
    try:
        col_history.delete_many({})
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/stars/delete-all', methods=['POST'])
@login_required
def api_delete_all_stars():
    if col_stars is None:
        return jsonify({'ok': False})
    try:
        col_stars.delete_many({})
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/history/delete', methods=['POST'])
@login_required
def api_delete_history():
    if col_history is None:
        return jsonify({'ok': False})
    try:
        from bson import ObjectId
        data = request.get_json()
        oid = data.get('_id', '')
        if oid:
            col_history.delete_one({'_id': ObjectId(oid)})
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/stars', methods=['GET'])
@login_required
def api_get_stars():
    if col_stars is None:
        return jsonify([])
    try:
        items = list(col_stars.find({}, {'_id': 0}))
        return jsonify(items)
    except:
        return jsonify([])

@app.route('/api/stars', methods=['POST'])
@login_required
def api_save_star():
    if col_stars is None:
        return jsonify({'ok': False})
    try:
        data = request.get_json()
        code = data.get('code', '')
        col_stars.delete_many({'code': code})
        col_stars.insert_one({
            'name': data.get('name', ''),
            'code': code,
            'report': data.get('report', {}),
            'stockInfo': data.get('stockInfo', {}),
            'newsList': data.get('newsList', []),
            'targetNewsList': data.get('targetNewsList', []),
            'feature7days': data.get('feature7days', []),
            'feature30days': data.get('feature30days', []),
            'aiNewsList': data.get('aiNewsList', []),
            'trendData': data.get('trendData', {}),
            'supplyDemand': data.get('supplyDemand', {}),
            'consensusList': data.get('consensusList', []),
            'createdAt': data.get('createdAt', ''),
            'date': data.get('date', ''),
        })
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/api/stars/<code>', methods=['DELETE'])
@login_required
def api_delete_star(code):
    if col_stars is None:
        return jsonify({'ok': False})
    try:
        col_stars.delete_many({'code': code})
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

# =============================================
# 차트 데이터 API
# =============================================
EMA_PERIODS = [5, 20, 60, 120, 240]

# =============================================
# 차트 캐시 — MongoDB + zlib 압축
# last_dt 분 단위 저장
# 장중(08:00~20:00, NXT 포함): 5분 TTL → 오늘 봉 재조회
# 장후(20:00~): 확정 → 다음날까지 그대로
# =============================================
import zlib

def _is_market_open(now=None):
    """장중 여부 — NXT 포함 기준 08:00~20:00"""
    from datetime import time as _time
    now = now or datetime.now()
    return _time(8, 0) <= now.time() <= _time(20, 0)

def _get_chart_cache(code, period_div='D', market='J'):
    """MongoDB 차트 캐시 조회
    반환:
    - (cached_result, None)         : 캐시 유효 → cached_result 바로 반환 (EMA 포함)
    - (None, (last_kst, candles, last_ema)) : 캐시 만료 → 증분 재조회 필요
    - (None, None)                  : 캐시 없음 → 전체 수집 필요
    """
    if col_chart_cache is None:
        return None, None
    try:
        mkt_suffix = '' if market == 'J' else f'_{market}'
        doc_id = f'{code}_{period_div}{mkt_suffix}'
        doc = col_chart_cache.find_one({'_id': doc_id})
        if not doc:
            return None, None

        last_dt = doc.get('last_dt')
        if not last_dt:
            return None, None

        now_kst  = datetime.utcnow() + timedelta(hours=9)
        last_kst = last_dt + timedelta(hours=9)
        elapsed  = (now_kst - last_kst).total_seconds()

        def _load_incremental():
            raw = doc.get('candles_z')
            candles = json.loads(zlib.decompress(raw).decode('utf-8')) if raw else []
            last_ema = doc.get('last_ema', {})
            return None, (last_kst, candles, last_ema)

        if _is_market_open(now_kst):
            if elapsed > 300:
                print(f'[CHART CACHE] {doc_id} 장중 TTL 만료 ({int(elapsed//60)}분 경과) → 증분 재조회')
                return _load_incremental()
        else:
            if last_kst.date() < now_kst.date():
                print(f'[CHART CACHE] {doc_id} 날짜 넘어감 → 증분 재조회')
                return _load_incremental()

        # 캐시 유효 → EMA 결과까지 바로 반환
        result_raw = doc.get('result_z')
        if result_raw:
            result = json.loads(zlib.decompress(result_raw).decode('utf-8'))
            print(f'[CHART CACHE] HIT {doc_id} (EMA 포함, {last_kst.strftime("%m/%d %H:%M")})')
            return result, None

        # result_z 없으면 (구버전 문서) candles만 반환해서 재계산 유도
        raw = doc.get('candles_z')
        if not raw:
            return None, None
        candles = json.loads(zlib.decompress(raw).decode('utf-8'))
        last_ema = doc.get('last_ema', {})
        return None, (last_kst, candles, last_ema)

    except Exception as e:
        print(f'[CHART CACHE] 조회 오류: {e}')
        return None, None

def _set_chart_cache(code, period_div, candles, result=None, market='J'):
    """MongoDB 차트 캐시 저장 (zlib 압축)
    candles: raw rows (증분 업데이트용)
    result:  {'candles': [...], 'ema5': [...], ...} (프론트 반환용, EMA 포함)
    last_ema: EMA 마지막값 저장 (장중 증분 EMA 계산용)
    """
    if col_chart_cache is None:
        return
    try:
        import pandas as pd
        mkt_suffix = '' if market == 'J' else f'_{market}'
        doc_id  = f'{code}_{period_div}{mkt_suffix}'
        now_utc = datetime.utcnow()

        candles_z = zlib.compress(json.dumps(candles, ensure_ascii=False).encode('utf-8'))

        # last_ema 계산 (EMA 마지막값 저장 — 증분 계산용)
        last_ema = {}
        if candles:
            df_tmp = _kis_rows_to_df(candles)
            if not df_tmp.empty:
                for p in EMA_PERIODS:
                    ema_series = df_tmp['Close'].ewm(span=p, adjust=False).mean()
                    last_ema[str(p)] = round(float(ema_series.iloc[-1]), 2)

        doc = {
            '_id':       doc_id,
            'candles_z': candles_z,
            'last_ema':  last_ema,
            'last_dt':   now_utc,
            'updated_at': now_utc,
        }

        # result(EMA 포함 프론트 반환 데이터)도 압축 저장
        if result is not None:
            doc['result_z'] = zlib.compress(json.dumps(result, ensure_ascii=False).encode('utf-8'))

        col_chart_cache.replace_one({'_id': doc_id}, doc, upsert=True)
        result_kb = len(doc.get('result_z', b'')) / 1024
        print(f'[CHART CACHE] SET {doc_id} ({len(candles)}건, candles={len(candles_z)/1024:.1f}KB, result={result_kb:.1f}KB)')
    except Exception as e:
        print(f'[CHART CACHE] 저장 오류: {e}')

# 분봉 캐시: 메모리 유지 (분봉은 미구현 상태 — 나중에 MongoDB로 교체 예정)
_minute_cache = {}
_minute_cache_lock = threading.Lock()
MINUTE_CACHE_MAX  = 20
MINUTE_CACHE_SECS = 300  # 5분

def _get_minute_cache(code, interval):
    key = f'{code}_{interval}m'
    with _minute_cache_lock:
        entry = _minute_cache.get(key)
        if entry and (datetime.utcnow() - entry['ts']).total_seconds() < MINUTE_CACHE_SECS:
            print(f'[MINUTE CACHE] HIT {key}')
            return entry['data']
    return None

def _set_minute_cache(code, interval, data):
    key = f'{code}_{interval}m'
    with _minute_cache_lock:
        if key not in _minute_cache and len(_minute_cache) >= MINUTE_CACHE_MAX * 2:
            oldest = min(_minute_cache, key=lambda k: _minute_cache[k]['ts'])
            del _minute_cache[oldest]
            print(f'[MINUTE CACHE] EVICT {oldest}')
        _minute_cache[key] = {'data': data, 'ts': datetime.utcnow()}
        print(f'[MINUTE CACHE] SET {key} (총 {len(_minute_cache)}개)')

def _get_ticker(code):
    """코스피 → 코스닥 순서로 ticker와 시장명 반환"""
    import yfinance as yf
    import pandas as pd
    for suffix, mname in [('.KS', '코스피'), ('.KQ', '코스닥')]:
        try:
            ticker = yf.Ticker(code + suffix)
            tmp = ticker.history(period='5d', auto_adjust=False)  # 가볍게 확인
            if not tmp.empty:
                print(f'[CHART] {code}{suffix} ({mname}) 확인 성공')
                return ticker, code + suffix, mname
        except Exception as e:
            print(f'[CHART] {code}{suffix} 확인 실패: {e}')
    return None, None, None

def _make_candles_date(d):
    """날짜형 봉 (일/주/월) - time을 YYYY-MM-DD 문자열로"""
    import pandas as pd
    candles = []
    for ts, row in d.iterrows():
        try:
            t = ts.strftime('%Y-%m-%d')
        except:
            t = str(ts)[:10]
        candles.append({
            'time': t,
            'open': round(float(row['Open'])),
            'high': round(float(row['High'])),
            'low': round(float(row['Low'])),
            'close': round(float(row['Close'])),
            'volume': int(row['Volume']) if 'Volume' in row.index and not pd.isna(row['Volume']) else 0,
        })
    return candles

def _make_candles_ts(d):
    """분봉 - time을 Unix timestamp(초)로"""
    import pandas as pd
    KST_OFFSET = 9 * 3600  # UTC+9
    candles = []
    for ts, row in d.iterrows():
        try:
            # KST로 변환 (LightweightCharts에 KST timestamp 전달)
            if hasattr(ts, 'tzinfo') and ts.tzinfo is not None:
                t = int(ts.tz_convert('UTC').timestamp()) + KST_OFFSET
            else:
                t = int(ts.timestamp()) + KST_OFFSET
        except:
            t = int(pd.Timestamp(ts).timestamp()) + KST_OFFSET
        candles.append({
            'time': t,
            'open': round(float(row['Open'])),
            'high': round(float(row['High'])),
            'low': round(float(row['Low'])),
            'close': round(float(row['Close'])),
            'volume': int(row['Volume']) if 'Volume' in row.index and not pd.isna(row['Volume']) else 0,
        })
    return candles

def _make_ema_date(d, period):
    import pandas as pd
    ema = d['Close'].ewm(span=period, adjust=False).mean()
    result = []
    for ts, val in ema.items():
        if pd.notna(val):
            try:
                t = ts.strftime('%Y-%m-%d')
            except:
                t = str(ts)[:10]
            result.append({'time': t, 'value': round(float(val), 2)})
    return result

def _make_ema_ts(d, period):
    import pandas as pd
    KST_OFFSET = 9 * 3600
    ema = d['Close'].ewm(span=period, adjust=False).mean()
    result = []
    for ts, val in ema.items():
        if pd.notna(val):
            try:
                if hasattr(ts, 'tzinfo') and ts.tzinfo is not None:
                    t = int(ts.tz_convert('UTC').timestamp()) + KST_OFFSET
                else:
                    t = int(ts.timestamp()) + KST_OFFSET
            except:
                t = int(pd.Timestamp(ts).timestamp()) + KST_OFFSET
            result.append({'time': t, 'value': round(float(val), 2)})
    return result

def _kis_fetch_ohlcv_once(code, token, app_key, app_secret, end_dt_str, period_div='D', market_div='J'):
    """한투 API FHKST03010100 - 일/주/월봉 단건 조회 (최대 100건)
    period_div: D=일봉, W=주봉, M=월봉, Y=년봉
    end_dt_str: 'YYYYMMDD'
    반환: list of dict {date, open, high, low, close, volume}
    """
    url = 'https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice'
    headers = {
        'content-type':  'application/json',
        'authorization': f'Bearer {token}',
        'appkey':        app_key,
        'appsecret':     app_secret,
        'tr_id':         'FHKST03010100',
        'custtype':      'P',
    }
    # 시작일: end_dt 기준 3년 전
    try:
        end_dt_obj = datetime.strptime(end_dt_str, '%Y%m%d')
        start_dt_str = (end_dt_obj - timedelta(days=365*3)).strftime('%Y%m%d')
    except Exception:
        start_dt_str = '20220101'

    params = {
        'FID_COND_MRKT_DIV_CODE': market_div,
        'FID_INPUT_ISCD':         code,
        'FID_INPUT_DATE_1':       start_dt_str,
        'FID_INPUT_DATE_2':       end_dt_str,
        'FID_PERIOD_DIV_CODE':    period_div,
        'FID_ORG_ADJ_PRC':        '0',  # 0=수정주가
    }
    try:
        res = requests.get(url, headers=headers, params=params, timeout=15)
        data = res.json()
        if data.get('rt_cd') != '0':
            print(f'[CHART-KIS] FHKST03010100 오류: {data.get("msg1","")} 전체응답: {str(data)[:300]}')
            return []
        output2 = data.get('output2') or []
        if isinstance(output2, dict):
            output2 = [output2]
        print(f'[CHART-KIS] {code} {period_div} {end_dt_str} -> {len(output2)}건')
        rows = []
        for r in output2:
            date_str = r.get('stck_bsop_date', '')
            if not date_str or len(date_str) != 8:
                continue
            try:
                rows.append({
                    'date':   f'{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}',
                    'open':   int(r.get('stck_oprc', 0) or 0),
                    'high':   int(r.get('stck_hgpr', 0) or 0),
                    'low':    int(r.get('stck_lwpr', 0) or 0),
                    'close':  int(r.get('stck_clpr', 0) or 0),
                    'volume': int(r.get('acml_vol', 0) or 0),
                })
            except Exception:
                continue
        return rows
    except Exception as e:
        print(f'[CHART-KIS] 단건 조회 오류: {e}')
        return []


def _kis_fetch_ohlcv_full(code, token, app_key, app_secret, period_div='D', years=5, market_div='J'):
    """한투 API 5년치 수집 (100건씩 반복 호출 후 이어붙이기)"""
    all_rows = []
    end_dt = datetime.now()
    seen_dates = set()
    max_iter = 60

    for _ in range(max_iter):
        end_str = end_dt.strftime('%Y%m%d')
        rows = _kis_fetch_ohlcv_once(code, token, app_key, app_secret, end_str, period_div, market_div=market_div)
        if not rows:
            break

        new_rows = [r for r in rows if r['date'] not in seen_dates]
        for r in new_rows:
            seen_dates.add(r['date'])
        all_rows.extend(new_rows)

        cutoff = (datetime.now() - timedelta(days=365 * years)).strftime('%Y-%m-%d')
        oldest = min(r['date'] for r in new_rows) if new_rows else ''
        if oldest and oldest <= cutoff:
            break

        if new_rows:
            end_dt = datetime.strptime(oldest, '%Y-%m-%d') - timedelta(days=1)
        else:
            break

    all_rows.sort(key=lambda r: r['date'])
    cutoff = (datetime.now() - timedelta(days=365 * years)).strftime('%Y-%m-%d')
    all_rows = [r for r in all_rows if r['date'] >= cutoff]
    print(f'[CHART-KIS] {code} {period_div} 전체 {len(all_rows)}건 수집')
    return all_rows


def _kis_fetch_ohlcv_since(code, token, app_key, app_secret, since_date_str, period_div='D', market_div='J'):
    """last_dt 이후 ~ 오늘까지 증분 수집
    since_date_str: 'YYYY-MM-DD' (이 날짜 포함 이후 데이터 반환)
    """
    all_rows = []
    end_dt = datetime.now()
    seen_dates = set()
    max_iter = 10

    for _ in range(max_iter):
        end_str = end_dt.strftime('%Y%m%d')
        rows = _kis_fetch_ohlcv_once(code, token, app_key, app_secret, end_str, period_div, market_div=market_div)
        if not rows:
            break

        new_rows = [r for r in rows if r['date'] not in seen_dates and r['date'] >= since_date_str]
        for r in new_rows:
            seen_dates.add(r['date'])
        all_rows.extend(new_rows)

        oldest = min(r['date'] for r in rows) if rows else ''
        if oldest and oldest <= since_date_str:
            break

        if rows:
            end_dt = datetime.strptime(oldest, '%Y-%m-%d') - timedelta(days=1)
        else:
            break

    all_rows.sort(key=lambda r: r['date'])
    print(f'[CHART-KIS] {code} {period_div} 증분 {len(all_rows)}건 수집 (since {since_date_str})')
    return all_rows


def _kis_rows_to_df(rows):
    """한투 rows → pandas DataFrame (일봉용)"""
    import pandas as pd
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    df.columns = [c.capitalize() for c in df.columns]  # open→Open 등
    df = df[['Open','High','Low','Close','Volume']].dropna()
    return df


@app.route('/api/chart/<code>', methods=['GET'])
@login_required
def api_chart(code):
    """일봉/주봉/월봉 데이터 (한투 API) — MongoDB 증분 캐시 + EMA 캐싱 + zlib 압축
    흐름:
    1) 캐시 유효 → result_z(EMA 포함) 바로 반환 (계산 없음!)
    2) 캐시 만료 → 증분 수집 + 오늘 봉 EMA만 재계산 후 저장
    3) 캐시 없음 → 5년치 전체 수집 + EMA 전체 계산 후 저장
    """
    try:
        import pandas as pd

        # ── market 이름 ──
        market_name = ''
        for s in _stocks:
            if s['code'] == code:
                suffix = s.get('suffix', '')
                market_name = '코스피' if 'KS' in suffix else '코스닥' if 'KQ' in suffix else ''
                break

        # ── KRX/NXT/통합 파라미터 ──
        market_div = request.args.get('market', 'J')  # J: KRX, NX: NXT, UN: 통합
        if market_div not in ('J', 'NX', 'UN'):
            market_div = 'J'

        # ── 캐시 확인 (일봉) ──
        cached_result, incremental_info = _get_chart_cache(code, 'D', market_div)
        if cached_result is not None:
            # EMA 포함 결과 그대로 반환 — 계산 없음!!
            cached_week, _  = _get_chart_cache(code, 'W', market_div)
            cached_month, _ = _get_chart_cache(code, 'M', market_div)
            if cached_week is not None and cached_month is not None:
                return jsonify({'market': market_name, 'day': cached_result, 'week': cached_week, 'month': cached_month})

        app_key    = os.environ.get('KIS_APP_KEY', '')
        app_secret = os.environ.get('KIS_APP_SECRET', '')
        token = _kis_get_token()
        if not token or not app_key or not app_secret:
            return jsonify({'error': 'KIS 토큰/키 없음'}), 500

        # ── 일봉 수집 ──
        if incremental_info:
            last_kst, existing_candles, last_ema = incremental_info
            since_date = last_kst.strftime('%Y-%m-%d')
            print(f'[CHART] {code} 증분 수집 since {since_date}')
            new_rows = _kis_fetch_ohlcv_since(code, token, app_key, app_secret, since_date, 'D', market_div=market_div)

            # ── 증분 0건: 새 데이터 없음 → last_dt만 갱신하고 기존 result_z 반환 ──
            if not new_rows:
                print(f'[CHART] {code} 증분 0건 → last_dt 갱신 후 기존 캐시 반환')
                now_utc = datetime.utcnow()
                if col_chart_cache is not None:
                    for div in ['D', 'W', 'M']:
                        col_chart_cache.update_one(
                            {'_id': f'{code}_{div}'},
                            {'$set': {'last_dt': now_utc, 'updated_at': now_utc}}
                        )
                # result_z 직접 꺼내서 반환 (캐시 재조회 없이)
                try:
                    def _load_result(div):
                        doc = col_chart_cache.find_one({'_id': f'{code}_{div}'})
                        if doc and doc.get('result_z'):
                            return json.loads(zlib.decompress(doc['result_z']).decode('utf-8'))
                        return None
                    r_d = _load_result('D')
                    r_w = _load_result('W')
                    r_m = _load_result('M')
                    if r_d and r_w and r_m:
                        return jsonify({'market': market_name, 'day': r_d, 'week': r_w, 'month': r_m})
                except Exception as e:
                    print(f'[CHART] 증분 0건 캐시 반환 오류: {e}')
                # 실패 시 existing_candles로 재계산 fallback
                day_rows = existing_candles

            old_rows = [r for r in existing_candles if r['date'] < since_date]
            day_rows = old_rows + new_rows
            day_rows.sort(key=lambda r: r['date'])
        else:
            print(f'[CHART] {code} 최초 수집 (5년치)')
            day_rows = _kis_fetch_ohlcv_full(code, token, app_key, app_secret, 'D', years=5, market_div=market_div)
            last_ema = {}

        if not day_rows:
            return jsonify({'error': f'{code} 차트 데이터를 가져올 수 없습니다.'}), 404

        df = _kis_rows_to_df(day_rows)
        if df.empty:
            return jsonify({'error': f'{code} 차트 데이터 변환 실패'}), 404

        # 주봉/월봉 resample
        df_week  = df.resample('W').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'}).dropna()
        df_month = df.resample('ME').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'}).dropna()

        def build(d):
            r = {'candles': _make_candles_date(d)}
            for p in EMA_PERIODS:
                r[f'ema{p}'] = _make_ema_date(d, p)
            return r

        day_result   = build(df)
        week_result  = build(df_week)
        month_result = build(df_month)

        # 주봉/월봉 rows
        week_rows  = [{'date': ts.strftime('%Y-%m-%d'), 'open': int(r['Open']), 'high': int(r['High']), 'low': int(r['Low']), 'close': int(r['Close']), 'volume': int(r['Volume'])} for ts, r in df_week.iterrows()]
        month_rows = [{'date': ts.strftime('%Y-%m-%d'), 'open': int(r['Open']), 'high': int(r['High']), 'low': int(r['Low']), 'close': int(r['Close']), 'volume': int(r['Volume'])} for ts, r in df_month.iterrows()]

        # MongoDB 저장 (EMA 결과까지 통째로)
        _set_chart_cache(code, 'D', day_rows,   result=day_result,  market=market_div)
        _set_chart_cache(code, 'W', week_rows,  result=week_result, market=market_div)
        _set_chart_cache(code, 'M', month_rows, result=month_result,market=market_div)

        return jsonify({'market': market_name, 'day': day_result, 'week': week_result, 'month': month_result})

    except Exception as e:
        print(f'[CHART] 오류: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/api/chart/<code>/minute/<int:interval>', methods=['GET'])
@login_required
def api_chart_minute(code, interval):
    """분봉 데이터 (1/3/5/10/15/30/45/60분) - 한투 API FHKST03010230"""
    try:
        import pandas as pd

        if interval not in [1, 3, 5, 10, 15, 30, 45, 60]:
            return jsonify({'error': '지원하지 않는 분봉 단위입니다.'}), 400

        # 캐시 확인
        cached = _get_minute_cache(code, interval)
        if cached:
            return jsonify(cached)

        app_key    = os.environ.get('KIS_APP_KEY', '')
        app_secret = os.environ.get('KIS_APP_SECRET', '')
        token = _kis_get_token()

        if not token or not app_key or not app_secret:
            return jsonify({'error': 'KIS 토큰/키 없음'}), 500

        # 한투 분봉 API: 120건씩, 여러 번 호출해서 이어붙이기
        url = 'https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice'
        headers = {
            'content-type':  'application/json',
            'authorization': f'Bearer {token}',
            'appkey':        app_key,
            'appsecret':     app_secret,
            'tr_id':         'FHKST03010200',
            'custtype':      'P',
        }

        all_rows = []
        seen_times = set()
        # 당일 분봉만 제공 (한투 API 스펙)
        # 153000부터 역방향으로 30건씩 여러 번 호출
        end_time = '153000'

        for i in range(10):
            params = {
                'FID_ETC_CLS_CODE':       '',
                'FID_COND_MRKT_DIV_CODE': 'J',
                'FID_INPUT_ISCD':         code,
                'FID_INPUT_HOUR_1':       end_time,
                'FID_PW_DATA_INCU_YN':    'Y',
            }
            try:
                res = requests.get(url, headers=headers, params=params, timeout=15)
                data = res.json()
                if data.get('rt_cd') != '0':
                    print(f'[CHART-MIN] FHKST03010200 오류: {data.get("msg1","")} 전체: {str(data)[:300]}')
                    break
                output2 = data.get('output2') or []
                if isinstance(output2, dict):
                    output2 = [output2]
                print(f'[CHART-MIN] {code} {interval}분 {end_time} -> {len(output2)}건')
                if not output2:
                    break

                new_rows = []
                for r in output2:
                    date_str = r.get('stck_bsop_date', '')
                    time_str = r.get('stck_cntg_hour', '')
                    if not date_str or not time_str:
                        continue
                    key = f'{date_str}{time_str}'
                    if key in seen_times:
                        continue
                    seen_times.add(key)
                    try:
                        # KST timestamp
                        dt_str = f'{date_str[:4]}-{date_str[4:6]}-{date_str[6:]} {time_str[:2]}:{time_str[2:4]}:{time_str[4:]}'
                        from datetime import timezone
                        kst = timezone(timedelta(hours=9))
                        dt_kst = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=kst)
                        ts = int(dt_kst.timestamp())
                        new_rows.append({
                            'time':   ts,
                            'open':   int(r.get('stck_oprc', 0) or 0),
                            'high':   int(r.get('stck_hgpr', 0) or 0),
                            'low':    int(r.get('stck_lwpr', 0) or 0),
                            'close':  int(r.get('stck_prpr', 0) or 0),
                            'volume': int(r.get('cntg_vol', 0) or 0),
                        })
                    except Exception:
                        continue

                all_rows.extend(new_rows)

                # 다음 호출: 가장 오래된 시간 기준
                if new_rows:
                    oldest = min(new_rows, key=lambda x: x['time'])
                    oldest_dt = datetime.fromtimestamp(oldest['time'])
                    end_time = (oldest_dt - timedelta(minutes=interval)).strftime('%H%M%S')
                else:
                    break
            except Exception as e:
                print(f'[CHART-MIN] 분봉 조회 오류: {e}')
                break

        if not all_rows:
            return jsonify({'error': f'{interval}분봉 데이터를 가져올 수 없습니다.'}), 404

        # 시간순 정렬
        all_rows.sort(key=lambda x: x['time'])

        # pandas DataFrame으로 변환해서 EMA 계산
        df = pd.DataFrame(all_rows)
        df_indexed = df.set_index(pd.to_datetime(df['time'], unit='s'))
        df_indexed['Open']   = df['open']
        df_indexed['High']   = df['high']
        df_indexed['Low']    = df['low']
        df_indexed['Close']  = df['close']
        df_indexed['Volume'] = df['volume']

        # market 이름
        market = ''
        for s in _stocks:
            if s['code'] == code:
                suffix = s.get('suffix', '')
                market = '코스피' if 'KS' in suffix else '코스닥' if 'KQ' in suffix else ''
                break

        result = {'candles': all_rows, 'market': market}
        for p in EMA_PERIODS:
            result[f'ema{p}'] = _make_ema_ts(df_indexed, p)

        _set_minute_cache(code, interval, result)
        print(f'[CHART-MIN] {code} {interval}분봉 총 {len(all_rows)}건')
        return jsonify(result)
    except Exception as e:
        print(f'[CHART-MIN] 오류: {e}')
        return jsonify({'error': str(e)}), 500

# =============================================
# 임시 디버그: 수급 캐시 삭제
# =============================================
@app.route('/debug-clear/<code>')
def debug_clear(code):
    from datetime import timezone
    _KST = timezone(timedelta(hours=9))
    _now_kst = datetime.now(_KST)
    if _now_kst.hour >= 16:
        end_dt = _now_kst.replace(tzinfo=None)
    else:
        end_dt = _now_kst.replace(tzinfo=None) - timedelta(days=1)
    cache_key = f'{code}_{end_dt.strftime("%Y%m%d")}'
    if col_supply_cache is not None:
        col_supply_cache.delete_one({'_id': cache_key})
    return jsonify({'deleted': cache_key})

# =============================================
# 임시 디버그: 수급 데이터 건수 확인
# =============================================
@app.route('/debug-supply/<code>')
def debug_supply(code):
    result = get_supply_demand(code)
    return jsonify({
        '1w': len(result.get('1w', [])),
        '1m': len(result.get('1m', [])),
        '3m': len(result.get('3m', [])),
        '6m': len(result.get('6m', [])),
    })

# =============================================
# 실행
# =============================================
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
