# S&P 500 약세추세 스크리너 (텔레그램 알림)

조건:

1. 종가가 볼린저밴드 하단 이탈
2. 이평선 역배열 (`MA_SHORT < MA_MID < MA_LONG`)
3. 시가 갭이 전일 종가 대비 -3% 이하

해당 조건을 만족하는 S&P 500 종목을 찾아 텔레그램으로 전송합니다.

전송 내용:

- 조건 충족 종목 요약 개수
- 종목별 기술조건 수치(종가/갭/BB/이평)
- 시가총액, 업종, 회사 요약
- TradingView / Yahoo 차트 바로보기 링크
- 관련 뉴스(기본 3건)

## 1) 설치

```bash
cd /Users/giho/basementbot
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) 환경변수 설정

```bash
cp .env.example .env
```

`.env`에서 아래 값 설정:

- `TELEGRAM_BOT_TOKEN`: BotFather에서 발급
- `TELEGRAM_CHAT_ID`: 알림 받을 채팅 ID

기본 파라미터:

- `BB_WINDOW=20`
- `BB_STD=2`
- `MA_SHORT=20`
- `MA_MID=60`
- `MA_LONG=120`
- `MIN_CLOSE_CHANGE_PCT=-2` (전일대비 하락률 최소 강도)
- `MIN_BB_BREACH_PCT=0.5` (BB 하단 이탈 강도 최소치, %)
- `MIN_GAP_PCT=-3` (시가 갭 하락 최소 강도, %)
- `MAX_RESULTS=10` (전송 최대 종목 수)
- `LOOKBACK_DAYS=260`
- `NEWS_COUNT=3`

로그(로테이션) 파라미터:

- `LOG_FILE=logs/screener.log`
- `LOG_LEVEL=INFO`
- `LOG_MAX_BYTES=5242880` (기본 5MB)
- `LOG_BACKUP_COUNT=7` (백업 로그 7개 유지)

## 3) 실행

```bash
source .venv/bin/activate
python screener.py
```

특정 날짜 기준 실행:

```bash
python screener.py 2026-03-13
```

- 날짜 형식: `YYYY-MM-DD`
- 해당 날짜가 휴장일이면, 해당 날짜 이전의 가장 최근 거래일 데이터를 사용

- 텔레그램 토큰/채팅ID가 없으면 콘솔 출력만 수행
- 값이 있으면 텔레그램 전송 수행

### 다음날 수익률 백테스트

신호일 종가 매수 → 다음 영업일 종가 매도 기준:

```bash
./.venv/bin/python backtest_next_day.py --days 10
```

옵션:

- `--days 10` : 최근 N 영업일 테스트
- `--start YYYY-MM-DD --end YYYY-MM-DD` : 기간 지정 테스트

## 4) 매일 자동 실행 (macOS cron 예시)

```bash
crontab -e
```

예: 미국장 마감 후 한국시간 오전 6:10 실행

```cron
10 6 * * 1-5 cd /Users/giho/basementbot && /Users/giho/basementbot/.venv/bin/python /Users/giho/basementbot/screener.py >> /Users/giho/basementbot/screener.log 2>&1
```

시간은 사용자 환경(서머타임/브로커 데이터 업데이트 시점)에 맞게 조정하세요.

참고:

- 스크립트 내부에서 `logs/screener.log` 파일 로테이션을 수행합니다.
- cron 리다이렉션 로그까지 별도 관리하려면 위 줄을 유지하고, 내부 로그만 쓰려면 리다이렉션은 제거해도 됩니다.

## 5) 함수 설명 (`screener.py`)

- `get_sp500_tickers()`
  - 위키피디아 S&P 500 테이블을 읽어 티커 리스트를 반환합니다.
  - 티커 내 `.` 문자는 야후 파이낸스 형식에 맞게 `-`로 치환합니다.

- `download_ohlcv(tickers, lookback_days)`
  - 지정한 티커들의 OHLCV 데이터를 `lookback_days` 범위로 다운로드합니다.
  - 내부적으로 `yfinance.download(..., group_by="ticker")` 형식의 멀티인덱스 DataFrame을 반환합니다.

- `calculate_signals(df, ticker, bb_window, bb_std, ma_short, ma_mid, ma_long, min_close_change_pct, min_bb_breach_pct, min_gap_pct, target_date)`
  - 단일 티커에 대해 아래 3가지 조건을 동시에 검사합니다.
    1. 종가 < 볼린저밴드 하단
    2. `MA_SHORT < MA_MID < MA_LONG`
    3. `gap_pct <= MIN_GAP_PCT` (기본 -3)
  - 추가 강도 필터:
    - `close_change_pct <= MIN_CLOSE_CHANGE_PCT`
    - `bb_breach_pct >= MIN_BB_BREACH_PCT`
  - `target_date`가 있으면 해당 일자(또는 직전 거래일) 기준으로 계산합니다.
  - 조건 충족 시 결과 딕셔너리를 반환하고, 미충족/데이터 부족 시 `None`을 반환합니다.

- `get_company_context(ticker, news_count)`
  - `yfinance`에서 회사명/시가총액/업종/회사요약/뉴스를 가져옵니다.
  - 회사요약이 비어있으면 위키 검색 결과 요약으로 보완합니다.
  - Yahoo, TradingView 차트 링크를 함께 생성해 반환합니다.

- `build_messages(results, ma_short, ma_mid, ma_long)`
  - 스크리닝 결과를 텔레그램 다중 메시지 포맷으로 변환합니다.
  - 첫 메시지는 요약, 이후는 종목별 상세(기술지표 + 회사정보 + 차트링크 + 뉴스)입니다.

- `send_telegram(bot_token, chat_id, message)`
  - 텔레그램 Bot API `sendMessage`를 호출해 메시지를 발송합니다.
  - 요청 실패 시 예외(`raise_for_status`)를 발생시킵니다.

- `load_env_from_dotenv(path=".env")`
  - `.env` 파일을 읽어 환경변수로 로드합니다.
  - 이미 OS 환경변수에 있는 키는 덮어쓰지 않습니다.

- `main()`
  - 전체 실행 엔트리포인트입니다.
  - 환경변수 로드 → 티커 수집 → 데이터 다운로드 → 조건 필터링 → 메시지 생성 → 콘솔 출력/텔레그램 전송 순서로 실행합니다.
