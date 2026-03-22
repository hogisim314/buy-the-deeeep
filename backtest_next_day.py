import argparse
import os
from datetime import datetime, timedelta, timezone

import pandas as pd

import screener


def get_signal_and_next_row(sub: pd.DataFrame, target_date: pd.Timestamp):
    eligible = sub[sub.index <= target_date]
    if eligible.empty:
        return None, None

    signal_pos = sub.index.get_loc(eligible.index[-1])
    signal_idx = screener.resolve_index_position(signal_pos)

    next_idx = signal_idx + 1
    if next_idx >= len(sub):
        return None, None

    return sub.iloc[signal_idx], sub.iloc[next_idx]


def main():
    parser = argparse.ArgumentParser(description="신호 발생 다음날 수익률 백테스트")
    parser.add_argument("--start", default=None, help="시작일 YYYY-MM-DD (미입력 시 최근 N영업일)")
    parser.add_argument("--end", default=None, help="종료일 YYYY-MM-DD (기본: 오늘)")
    parser.add_argument("--days", type=int, default=60, help="최근 영업일 수 (start 미입력 시 사용)")
    parser.add_argument("--universe", default=None, help="대상 자산군 (sp500, commodities, energy, metals)")
    parser.add_argument("--tickers", default=None, help="직접 백테스트할 티커 목록. 쉼표 또는 공백 구분")
    args = parser.parse_args()

    bb_window = int(os.getenv("BB_WINDOW", "20"))
    bb_std = float(os.getenv("BB_STD", "2"))
    ma_short = int(os.getenv("MA_SHORT", "20"))
    ma_mid = int(os.getenv("MA_MID", "60"))
    ma_long = int(os.getenv("MA_LONG", "120"))
    min_close_change_pct = float(os.getenv("MIN_CLOSE_CHANGE_PCT", "-2"))
    min_bb_breach_pct = float(os.getenv("MIN_BB_BREACH_PCT", "0.5"))
    min_gap_pct = float(os.getenv("MIN_GAP_PCT", "-3"))
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "260"))
    universe = args.universe or os.getenv("UNIVERSE", "sp500")
    custom_tickers = args.tickers or os.getenv("CUSTOM_TICKERS", "")

    end_date = pd.Timestamp(args.end) if args.end else pd.Timestamp(datetime.now(timezone.utc).date())
    if args.start:
        start_date = pd.Timestamp(args.start)
    else:
        start_date = pd.bdate_range(end=end_date, periods=max(args.days, 1))[0]

    signal_dates = pd.bdate_range(start=start_date, end=end_date)

    fetch_start = (start_date - pd.Timedelta(days=lookback_days + 20)).strftime("%Y-%m-%d")
    fetch_end = (end_date + pd.Timedelta(days=3)).strftime("%Y-%m-%d")

    tickers, universe_label = screener.resolve_tickers(universe=universe, custom_tickers=custom_tickers)
    data = pd.DataFrame()
    data = screener.yf.download(
        tickers=tickers,
        start=fetch_start,
        end=fetch_end,
        auto_adjust=False,
        group_by="ticker",
        threads=True,
        progress=False,
    )

    trades = []

    for d in signal_dates:
        for ticker in tickers:
            try:
                signal = screener.calculate_signals(
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
                    target_date=d,
                )
                if not signal:
                    continue

                sub = data[ticker].copy().dropna()
                signal_row, next_row = get_signal_and_next_row(sub, pd.Timestamp(signal["date"]))
                if signal_row is None or next_row is None:
                    continue

                entry_close = float(signal_row["Close"])
                next_close = float(next_row["Close"])
                ret_pct = ((next_close - entry_close) / entry_close) * 100

                trades.append(
                    {
                        "date": str(pd.Timestamp(signal["date"]).date()),
                        "ticker": ticker,
                        "entry_close": round(entry_close, 2),
                        "next_close": round(next_close, 2),
                        "next_day_ret_pct": ret_pct,
                    }
                )
            except Exception:
                continue

    if not trades:
        print("[RESULT] 조건 충족 거래가 없어 백테스트 결과가 없습니다.")
        return

    df = pd.DataFrame(trades)
    avg_ret = df["next_day_ret_pct"].mean()
    med_ret = df["next_day_ret_pct"].median()
    win_rate = (df["next_day_ret_pct"] > 0).mean() * 100

    daily = df.groupby("date")["next_day_ret_pct"].agg(["count", "mean"]).reset_index()

    print("=== 백테스트 요약 ===")
    print(f"자산군: {universe_label}")
    print(f"기간: {start_date.date()} ~ {end_date.date()} (영업일 {len(signal_dates)}일)")
    print("가정: 신호일 종가 매수 -> 다음 영업일 종가 매도")
    print(f"총 거래 수: {len(df)}")
    print(f"평균 다음날 수익률: {avg_ret:.3f}%")
    print(f"중앙값 다음날 수익률: {med_ret:.3f}%")
    print(f"승률(다음날 수익률 > 0): {win_rate:.2f}%")

    print("\n=== 일자별 결과 (count, mean%) ===")
    for _, row in daily.iterrows():
        print(f"{row['date']}: {int(row['count'])}건, {row['mean']:.3f}%")

    print("\n=== 상위/하위 10개 거래 ===")
    worst = df.sort_values("next_day_ret_pct").head(10)
    best = df.sort_values("next_day_ret_pct", ascending=False).head(10)

    print("[WORST]")
    for _, row in worst.iterrows():
        print(f"{row['date']} {row['ticker']} {row['next_day_ret_pct']:.3f}%")

    print("\n[BEST]")
    for _, row in best.iterrows():
        print(f"{row['date']} {row['ticker']} {row['next_day_ret_pct']:.3f}%")


if __name__ == "__main__":
    main()
