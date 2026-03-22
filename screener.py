import os
import math
import time
import re
import argparse
import logging
import requests
import pandas as pd
import yfinance as yf
from html import escape
from urllib.parse import quote
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler


PRESET_UNIVERSES: dict[str, dict[str, object]] = {
    "sp500": {
        "label": "S&P500",
    },
    "commodities": {
        "label": "원자재",
        "tickers": [
            "CL=F",
            "BZ=F",
            "NG=F",
            "GC=F",
            "SI=F",
            "HG=F",
            "ZC=F",
            "ZW=F",
            "ZS=F",
            "KC=F",
            "CT=F",
            "SB=F",
        ],
    },
    "energy": {
        "label": "에너지",
        "tickers": ["CL=F", "BZ=F", "NG=F", "RB=F", "HO=F"],
    },
    "metals": {
        "label": "금속",
        "tickers": ["GC=F", "SI=F", "PL=F", "PA=F", "HG=F"],
    },
}


def get_sp500_tickers() -> list[str]:
    csv_url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

    try:
        table = pd.read_csv(csv_url)
        if "Symbol" in table.columns:
            tickers = table["Symbol"].astype(str).str.replace(".", "-", regex=False).tolist()
            if tickers:
                return tickers
    except Exception:
        pass

    table = pd.read_html(wiki_url)[0]
    tickers = table["Symbol"].astype(str).str.replace(".", "-", regex=False).tolist()
    return tickers


def parse_ticker_list(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []

    parts = re.split(r"[\s,]+", raw_value.strip())
    tickers = []
    seen = set()
    for part in parts:
        ticker = part.strip().upper().replace(".", "-")
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        tickers.append(ticker)
    return tickers


def get_supported_universes() -> list[str]:
    return sorted(PRESET_UNIVERSES.keys())


def resolve_tickers(universe: str | None, custom_tickers: str | None = None) -> tuple[list[str], str]:
    manual_tickers = parse_ticker_list(custom_tickers)
    if manual_tickers:
        return manual_tickers, "커스텀"

    universe_key = (universe or "sp500").strip().lower()
    preset = PRESET_UNIVERSES.get(universe_key)
    if preset is None:
        supported = ", ".join(get_supported_universes())
        raise ValueError(f"지원하지 않는 UNIVERSE입니다: {universe_key} (지원값: {supported})")

    preset_tickers = preset.get("tickers")
    if preset_tickers:
        return [str(ticker) for ticker in preset_tickers], str(preset["label"])

    return get_sp500_tickers(), str(preset["label"])


def resolve_index_position(indexer) -> int:
    if isinstance(indexer, int):
        return indexer
    if isinstance(indexer, slice):
        if indexer.start is None:
            raise ValueError("유효한 인덱스 위치를 찾을 수 없습니다.")
        return int(indexer.start)
    if hasattr(indexer, "__len__") and len(indexer) > 0:
        return int(indexer[0])
    raise ValueError("유효한 인덱스 위치를 찾을 수 없습니다.")


def setup_logging(log_file: str, log_level: str, log_max_bytes: int, log_backup_count: int) -> logging.Logger:
    logger = logging.getLogger("screener")
    if logger.handlers:
        return logger

    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)

    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=log_max_bytes,
        backupCount=log_backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger


def parse_target_date(date_text: str | None) -> pd.Timestamp | None:
    if not date_text:
        return None
    parsed = datetime.strptime(date_text, "%Y-%m-%d")
    return pd.Timestamp(parsed.date())


def download_ohlcv(tickers: list[str], lookback_days: int, target_date: pd.Timestamp | None = None) -> pd.DataFrame:
    now_utc = datetime.now(timezone.utc)
    if target_date is None:
        start = (now_utc - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        end = None
    else:
        target_dt = target_date.to_pydatetime().replace(tzinfo=timezone.utc)
        start = (target_dt - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        end = (target_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    data = yf.download(
        tickers=tickers,
        start=start,
        end=end,
        auto_adjust=False,
        group_by="ticker",
        threads=True,
        progress=False,
    )
    return data


def calculate_signals(
    df: pd.DataFrame,
    ticker: str,
    bb_window: int,
    bb_std: float,
    ma_short: int,
    ma_mid: int,
    ma_long: int,
    min_close_change_pct: float,
    min_bb_breach_pct: float,
    min_gap_pct: float,
    target_date: pd.Timestamp | None,
):
    if ticker not in df.columns.get_level_values(0):
        return None

    sub = df[ticker].copy().dropna()
    required_cols = {"Open", "Close"}
    if not required_cols.issubset(set(sub.columns)):
        return None

    if len(sub) < max(bb_window, ma_long) + 2:
        return None

    close = sub["Close"]
    ma = close.rolling(bb_window).mean()
    std = close.rolling(bb_window).std(ddof=0)
    lower_band = ma - (bb_std * std)

    ma_s = close.rolling(ma_short).mean()
    ma_m = close.rolling(ma_mid).mean()
    ma_l = close.rolling(ma_long).mean()

    if target_date is None:
        target_idx = len(sub) - 1
    else:
        eligible = sub[sub.index <= target_date]
        if eligible.empty:
            return None
        target_pos = eligible.index[-1]
        loc = sub.index.get_loc(target_pos)
        target_idx = resolve_index_position(loc)

    if target_idx < 1:
        return None

    last = sub.iloc[target_idx]
    prev = sub.iloc[target_idx - 1]

    last_close = float(last["Close"])
    last_open = float(last["Open"])
    prev_close = float(prev["Close"])

    if any(math.isnan(x) for x in [lower_band.iloc[target_idx], ma_s.iloc[target_idx], ma_m.iloc[target_idx], ma_l.iloc[target_idx]]):
        return None

    cond_bb_breakdown = last_close < float(lower_band.iloc[target_idx])
    cond_bearish_ma = float(ma_s.iloc[target_idx]) < float(ma_m.iloc[target_idx]) < float(ma_l.iloc[target_idx])
    gap_pct = ((last_open - prev_close) / prev_close) * 100
    close_change_pct = ((last_close - prev_close) / prev_close) * 100
    bb_breach_pct = ((float(lower_band.iloc[target_idx]) - last_close) / float(lower_band.iloc[target_idx])) * 100

    cond_min_drop = close_change_pct <= min_close_change_pct
    cond_min_bb_breach = bb_breach_pct >= min_bb_breach_pct
    cond_min_gap = gap_pct <= min_gap_pct

    if cond_bb_breakdown and cond_bearish_ma and cond_min_drop and cond_min_bb_breach and cond_min_gap:
        return {
            "ticker": ticker,
            "date": str(sub.index[target_idx].date()),
            "close": round(last_close, 2),
            "prev_close": round(prev_close, 2),
            "open": round(last_open, 2),
            "gap_pct": round(gap_pct, 2),
            "close_change_pct": round(close_change_pct, 2),
            "bb_breach_pct": round(bb_breach_pct, 2),
            "lower_band": round(float(lower_band.iloc[target_idx]), 2),
            "ma_short": round(float(ma_s.iloc[target_idx]), 2),
            "ma_mid": round(float(ma_m.iloc[target_idx]), 2),
            "ma_long": round(float(ma_l.iloc[target_idx]), 2),
        }

    return None


def split_long_message(message: str, max_len: int = 3500) -> list[str]:
    if len(message) <= max_len:
        return [message]

    lines = message.splitlines()
    chunks = []
    current = []
    current_len = 0

    for line in lines:
        next_len = current_len + len(line) + 1
        if next_len > max_len and current:
            chunks.append("\n".join(current))
            current = [line]
            current_len = len(line) + 1
        else:
            current.append(line)
            current_len = next_len

    if current:
        chunks.append("\n".join(current))

    return chunks


def send_telegram(bot_token: str, chat_id: str, message: str):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    for chunk in split_long_message(message):
        payload = {"chat_id": chat_id, "text": chunk, "parse_mode": "HTML"}

        max_retry = 4
        for attempt in range(max_retry):
            response = requests.post(url, data=payload, timeout=20)
            if response.ok:
                break

            json_body = {}
            try:
                json_body = response.json() or {}
            except Exception:
                json_body = {}

            description = json_body.get("description") or response.text
            lowered = str(description).lower()

            if response.status_code == 429:
                params = json_body.get("parameters") or {}
                if not isinstance(params, dict):
                    params = {}
                retry_after = int(params.get("retry_after", 3))
                time.sleep(max(retry_after, 1))
                continue

            if "can't parse entities" in lowered:
                plain_text = re.sub(r"<[^>]+>", "", chunk)
                fallback_payload = {"chat_id": chat_id, "text": plain_text}

                fallback_resp = requests.post(url, data=fallback_payload, timeout=20)
                if fallback_resp.ok:
                    break

                fallback_json = {}
                try:
                    fallback_json = fallback_resp.json() or {}
                except Exception:
                    fallback_json = {}

                if fallback_resp.status_code == 429:
                    retry_after = int((fallback_json.get("parameters") or {}).get("retry_after", 3))
                    time.sleep(max(retry_after, 1))
                    continue

                raise RuntimeError(
                    f"Telegram send failed after HTML fallback: {fallback_json.get('description') or fallback_resp.text}"
                )

            raise RuntimeError(f"Telegram send failed: {description}")
        else:
            raise RuntimeError("Telegram send failed: retry limit exceeded")


def send_telegram_messages(bot_token: str, chat_id: str, messages: list[str]):
    for message in messages:
        send_telegram(bot_token, chat_id, message)
        time.sleep(0.35)


def format_market_cap(market_cap: float | int | None) -> str:
    if market_cap is None:
        return "N/A"
    value = float(market_cap)
    if value >= 1_000_000_000_000:
        return f"${value / 1_000_000_000_000:.2f}T"
    if value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    return f"${value:,.0f}"


def truncate_text(text: str, max_length: int) -> str:
    if len(text) <= max_length:
        return text
    return text[: max_length - 3].rstrip() + "..."


def extract_news_items(raw_news: list, news_count: int) -> list[dict]:
    news_items = []

    for item in raw_news or []:
        if not isinstance(item, dict):
            continue

        title = item.get("title")
        link = item.get("link")
        publisher = item.get("publisher")

        content = item.get("content") if isinstance(item.get("content"), dict) else None
        if content:
            title = title or content.get("title")
            canonical_url = content.get("canonicalUrl") if isinstance(content.get("canonicalUrl"), dict) else {}
            click_url = content.get("clickThroughUrl") if isinstance(content.get("clickThroughUrl"), dict) else {}
            link = link or canonical_url.get("url") or click_url.get("url")
            provider = content.get("provider") if isinstance(content.get("provider"), dict) else {}
            publisher = publisher or provider.get("displayName")

        if not title or not link:
            continue

        news_items.append(
            {
                "title": str(title),
                "link": str(link),
                "publisher": str(publisher) if publisher else "Unknown",
            }
        )

        if len(news_items) >= news_count:
            break

    return news_items


def build_chart_links(ticker: str, exchange_code: str | None) -> dict:
    exchange_map = {
        "NMS": "NASDAQ",
        "NAS": "NASDAQ",
        "NGM": "NASDAQ",
        "NCM": "NASDAQ",
        "NYQ": "NYSE",
        "ASE": "AMEX",
        "PCX": "AMEX",
        "BTS": "NYSE",
    }

    yahoo_ticker = ticker
    tv_ticker = ticker.replace("-", ".")

    yahoo_url = f"https://finance.yahoo.com/quote/{quote(yahoo_ticker, safe='')}/chart"

    tv_exchange = exchange_map.get((exchange_code or "").upper())
    if tv_exchange:
        symbol = quote(f"{tv_exchange}:{tv_ticker}", safe="")
        tv_url = f"https://www.tradingview.com/chart/?symbol={symbol}"
    else:
        tv_url = f"https://www.tradingview.com/symbols/{quote(tv_ticker, safe='')}/"

    return {
        "chart_yahoo": yahoo_url,
        "chart_tradingview": tv_url,
    }


def fetch_company_summary_fallback(company_name: str | None, ticker: str) -> str | None:
    query = (company_name or "").strip() or ticker
    search_url = "https://en.wikipedia.org/w/api.php"

    try:
        search_response = requests.get(
            search_url,
            params={
                "action": "query",
                "list": "search",
                "srsearch": query,
                "format": "json",
                "utf8": 1,
            },
            timeout=10,
        )
        search_response.raise_for_status()
        search_data = search_response.json()
        hits = (search_data.get("query") or {}).get("search") or []
        if not hits:
            return None

        snippet = str(hits[0].get("snippet") or "")
        if not snippet:
            return None

        cleaned = re.sub(r"<[^>]+>", "", snippet).strip()
        return truncate_text(cleaned, 220) if cleaned else None
    except Exception:
        return None


def get_company_context(ticker: str, news_count: int) -> dict:
    stock = yf.Ticker(ticker)

    info = {}
    try:
        info = stock.info or {}
    except Exception:
        info = {}

    market_cap = info.get("marketCap")
    if market_cap is None:
        try:
            market_cap = stock.fast_info.get("market_cap")
        except Exception:
            market_cap = None

    sector = info.get("sector") or info.get("industry") or "N/A"
    company_name = info.get("longName") or info.get("shortName") or ticker
    exchange_code = info.get("exchange") or info.get("fullExchangeName")
    summary = info.get("longBusinessSummary") or info.get("shortBusinessSummary") or "정보 없음"
    if summary == "정보 없음" or len(summary.strip()) < 40:
        fallback = fetch_company_summary_fallback(company_name=company_name, ticker=ticker)
        if fallback:
            summary = fallback
    summary = truncate_text(summary.replace("\n", " "), 420)

    raw_news = []
    try:
        raw_news = stock.get_news(count=max(news_count, 1))
    except Exception:
        try:
            raw_news = stock.news
        except Exception:
            raw_news = []

    news_items = extract_news_items(raw_news, news_count)
    chart_links = build_chart_links(ticker, exchange_code)

    return {
        "market_cap": format_market_cap(market_cap),
        "company_name": company_name,
        "sector": sector,
        "summary": summary,
        "news": news_items,
        "chart_yahoo": chart_links["chart_yahoo"],
        "chart_tradingview": chart_links["chart_tradingview"],
    }


def build_messages(results: list[dict], ma_short: int, ma_mid: int, ma_long: int, universe_label: str) -> list[str]:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not results:
        return [f"📉 {universe_label} 스크리닝 결과 ({now_str})\n조건 만족 자산 없음"]

    messages = [
        "\n".join(
            [
                f"📉 {universe_label} 스크리닝 결과 ({now_str})",
                f"조건 만족 자산: {len(results)}개",
                "(조건: BB 하단 이탈 + 이평 역배열 + 갭 -3% 이하)",
            ]
        )
    ]

    for r in results:
        news_lines = []
        if r.get("news"):
            for idx, news in enumerate(r["news"], start=1):
                title = escape(news["title"])
                publisher = escape(news["publisher"])
                link = escape(news["link"])
                news_lines.append(f"{idx}) {title} ({publisher})")
                news_lines.append(link)
        else:
            news_lines.append("관련 뉴스 없음")

        detail_lines = [
            f"<b>{escape(r['ticker'])}</b> ({escape(r['date'])})",
            f"- 이름: {escape(r.get('company_name', r['ticker']))}",
            f"- 종가: {r['close']} (전일대비 {r['close_change_pct']}%)",
            f"- 갭: {r['gap_pct']}% (시가 {r['open']} / 전일종가 {r['prev_close']})",
            f"- BB 하단 이탈 강도: {r.get('bb_breach_pct', 0)}%",
            f"- BB 하단: {r['lower_band']}",
            f"- 이평: MA{ma_short}={r['ma_short']} < MA{ma_mid}={r['ma_mid']} < MA{ma_long}={r['ma_long']}",
            f"- 시가총액: {escape(r.get('market_cap', 'N/A'))}",
            f"- 분류: {escape(r.get('sector', 'N/A'))}",
            f"- 요약: {escape(r.get('summary', '정보 없음'))}",
            "- 차트 바로보기:",
            f"  • TradingView: <a href=\"{escape(r.get('chart_tradingview', ''), quote=True)}\">열기</a>",
            f"  • Yahoo: <a href=\"{escape(r.get('chart_yahoo', ''), quote=True)}\">열기</a>",
            "- 관련 뉴스:",
            *news_lines,
        ]
        messages.append("\n".join(detail_lines))

    return messages


def load_env_from_dotenv(path: str = ".env"):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def main():
    parser = argparse.ArgumentParser(description="자산군별 기술적 조건 스크리너")
    parser.add_argument(
        "date",
        nargs="?",
        default=None,
        help="기준일 (YYYY-MM-DD). 미입력 시 최신 거래일 기준",
    )
    parser.add_argument(
        "--universe",
        default=None,
        help="대상 자산군 (sp500, commodities, energy, metals)",
    )
    parser.add_argument(
        "--tickers",
        default=None,
        help="직접 스크리닝할 티커 목록. 쉼표 또는 공백 구분 예: CL=F,GC=F,USO,GLD",
    )
    args = parser.parse_args()

    try:
        target_date = parse_target_date(args.date)
    except ValueError:
        raise SystemExit("날짜 형식 오류: YYYY-MM-DD 형식으로 입력하세요. 예) 2026-03-13")

    load_env_from_dotenv()

    log_file = os.getenv("LOG_FILE", "logs/screener.log").strip()
    log_level = os.getenv("LOG_LEVEL", "INFO").strip()
    log_max_bytes = int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024)))
    log_backup_count = int(os.getenv("LOG_BACKUP_COUNT", "7"))
    logger = setup_logging(
        log_file=log_file,
        log_level=log_level,
        log_max_bytes=log_max_bytes,
        log_backup_count=log_backup_count,
    )

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    bb_window = int(os.getenv("BB_WINDOW", "20"))
    bb_std = float(os.getenv("BB_STD", "2"))
    ma_short = int(os.getenv("MA_SHORT", "20"))
    ma_mid = int(os.getenv("MA_MID", "60"))
    ma_long = int(os.getenv("MA_LONG", "120"))
    min_close_change_pct = float(os.getenv("MIN_CLOSE_CHANGE_PCT", "-2"))
    min_bb_breach_pct = float(os.getenv("MIN_BB_BREACH_PCT", "0.5"))
    min_gap_pct = float(os.getenv("MIN_GAP_PCT", "-3"))
    max_results = int(os.getenv("MAX_RESULTS", "10"))
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "260"))
    news_count = int(os.getenv("NEWS_COUNT", "3"))
    universe = args.universe or os.getenv("UNIVERSE", "sp500")
    custom_tickers = args.tickers or os.getenv("CUSTOM_TICKERS", "")

    try:
        tickers, universe_label = resolve_tickers(universe=universe, custom_tickers=custom_tickers)
    except ValueError as exc:
        raise SystemExit(str(exc))

    if target_date is None:
        logger.info("스크리닝 시작 (%s, 기준일: 최신 거래일)", universe_label)
    else:
        logger.info("스크리닝 시작 (%s, 기준일: %s)", universe_label, target_date.strftime("%Y-%m-%d"))

    logger.info("스크리닝 대상 수집 완료 (%s): %d개", universe_label, len(tickers))

    data = download_ohlcv(tickers, lookback_days, target_date=target_date)
    logger.info("가격 데이터 다운로드 완료")

    results = []
    for ticker in tickers:
        try:
            signal = calculate_signals(
                data,
                ticker,
                bb_window=bb_window,
                bb_std=bb_std,
                ma_short=ma_short,
                ma_mid=ma_mid,
                ma_long=ma_long,
                min_close_change_pct=min_close_change_pct,
                min_bb_breach_pct=min_bb_breach_pct,
                min_gap_pct=min_gap_pct,
                target_date=target_date,
            )
            if signal:
                results.append(signal)
        except Exception as exc:
            logger.debug("신호 계산 실패 (%s): %s", ticker, exc)
            continue

    results.sort(key=lambda x: x["close_change_pct"])
    if max_results > 0:
        results = results[:max_results]

    if results:
        matched_tickers = [result["ticker"] for result in results]
        logger.info("조건 충족 자산 수: %d개", len(results))
        logger.info("조건 충족 티커: %s", ", ".join(matched_tickers))
        for result in results:
            try:
                context = get_company_context(result["ticker"], news_count=news_count)
            except Exception:
                context = {
                    "market_cap": "N/A",
                    "sector": "N/A",
                    "summary": "정보 조회 실패",
                    "news": [],
                }
                logger.debug("회사 정보 조회 실패 (%s)", result["ticker"])
            result.update(context)
    else:
        logger.info("조건 충족 자산 없음")

    messages = build_messages(results, ma_short=ma_short, ma_mid=ma_mid, ma_long=ma_long, universe_label=universe_label)

    if not bot_token or not chat_id:
        for index, message in enumerate(messages, start=1):
            if index > 1:
                print("\n" + "-" * 80)
            print(message)
        print("\n[INFO] TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID가 없어 콘솔에만 출력했습니다.")
        logger.warning("텔레그램 설정 누락으로 콘솔 출력만 수행")
        return

    send_telegram_messages(bot_token, chat_id, messages)
    logger.info("텔레그램 전송 완료. 자산 수: %d", len(results))
    print(f"[OK] 텔레그램 전송 완료. 자산 수: {len(results)}")


if __name__ == "__main__":
    main()
