"""
══════════════════════════════════════════════════════════════
  data_loader.py — 실제 데이터 연동 어댑터

  데이터 수집 파트 담당자에게 전달할 파일

  ▸ 이 파일이 합성 데이터 → 실제 데이터로 교체하는 유일한 접점입니다
  ▸ 아래 4개 함수만 실제 데이터를 읽도록 바꾸면 백테스트 엔진은
    한 줄도 수정할 필요 없습니다
══════════════════════════════════════════════════════════════

[폴더 구조]

  project/
  ├── quant_alpha_v3_4_unified.py   ← 알고리즘 (수정 금지)
  ├── backtest_engine.py            ← 백테스트 엔진 (수정 금지)
  ├── data_loader.py                ← 이 파일 (데이터 연동)
  │
  └── data/                         ← ★ 실제 데이터 여기
      │
      ├── 1_price/                  ← 종목별 일별 OHLCV
      │   ├── AAPL.csv             ← Date,Open,High,Low,Close,Adj Close,Volume
      │   └── ...
      │
      ├── 2_fundamental/            ← 연간 재무 데이터
      │   ├── AAPL.csv             ← year,revenue,operating_income,net_income,...
      │   └── ...
      │
      ├── 3_signal/                 ← 시그널 (flat 구조)
      │   ├── AAPL_consensus.csv
      │   ├── AAPL_earnings_surprise.csv
      │   ├── AAPL_short_interest.csv
      │   └── ...
      │
      ├── 4_macro/                  ← 매크로 지표
      │   ├── vix.csv              ← Date,Value
      │   ├── yield_spread.csv
      │   ├── fedfunds.csv
      │   ├── claims.csv
      │   ├── cpi.csv
      │   ├── wti.csv
      │   ├── gold_fred.csv
      │   ├── copper.csv
      │   ├── dxy.csv
      │   ├── rate_kr.csv
      │   ├── rate_jp.csv
      │   └── rate_ecb.csv
      │
      ├── 5_benchmark/              ← S&P500 지수
      │   └── SP500.csv            ← Date,Open,High,Low,Close,Adj Close,Volume
      │
      └── 6_universe/               ← 종목 메타데이터
          └── sp500_current.csv    ← symbol,name,sector,sub_sector,...
"""

import os
import csv
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from quant_alpha_v3_4_unified import StockMetrics

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# 섹터명 정규화 (sp500_current.csv → 알고리즘 내부 표준명)
_SECTOR_MAP = {
    "Technology": "Technology",
    "Information Technology": "Technology",
    "Healthcare": "Healthcare",
    "Health Care": "Healthcare",
    "Financials": "Financials",
    "Financial Services": "Financials",
    "Consumer Discretionary": "ConsumerDisc",
    "Consumer Cyclical": "ConsumerDisc",
    "Industrials": "Industrials",
    "Communication Services": "Communication",
    "Communication": "Communication",
    "Consumer Staples": "ConsumerStaples",
    "Consumer Defensive": "ConsumerStaples",
    "Energy": "Energy",
    "Utilities": "Utilities",
    "Materials": "Materials",
    "Basic Materials": "Materials",
    "Real Estate": "RealEstate",
}


def _normalize_sector(raw: str) -> str:
    return _SECTOR_MAP.get(raw, raw)


# ══════════════════════════════════════════════════════════════
# 교체 지점 ① — 벤치마크 (S&P500 일별 종가)
# ══════════════════════════════════════════════════════════════

# ── 파일 형식: data/5_benchmark/SP500.csv ──
# Date,Open,High,Low,Close,Adj Close,Volume
# 2019-01-02,2510.03,...

def load_benchmark(dates: list) -> dict:
    """
    S&P500 일별 종가 로드.

    Returns:
        {datetime: float} — 날짜별 종가 딕셔너리
    """
    path = os.path.join(DATA_DIR, "5_benchmark", "SP500.csv")
    prices = {}
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                date_str = row.get("Date") or row.get("date", "")
                close_str = row.get("Close") or row.get("close") or row.get("Adj Close", "")
                if not date_str or not close_str:
                    continue
                dt = datetime.strptime(date_str.strip(), "%Y-%m-%d")
                prices[dt] = float(close_str)
            except (ValueError, KeyError):
                pass
    return prices


# ══════════════════════════════════════════════════════════════
# 교체 지점 ② — 종목 유니버스 (메타데이터)
# ══════════════════════════════════════════════════════════════

# ── 파일 형식: data/6_universe/sp500_current.csv ──
# symbol,name,sector,sub_sector,headquarters,date_added,cik,founded
# AAPL,Apple Inc.,Technology,Consumer Electronics,...

def load_universe() -> List[dict]:
    """
    투자 유니버스 로드. 가격 파일이 존재하는 종목만 반환.

    Returns:
        [{"symbol", "sector", "country", "industry_type", "beta", "market_cap"}, ...]
    """
    path = os.path.join(DATA_DIR, "6_universe", "sp500_current.csv")
    price_dir = os.path.join(DATA_DIR, "1_price")
    stocks = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = row.get("symbol", "").strip()
            if not sym:
                continue
            # 가격 파일 있는 종목만 포함
            if not os.path.exists(os.path.join(price_dir, f"{sym}.csv")):
                continue
            raw_sector = row.get("sector", "Technology").strip()
            sector = _normalize_sector(raw_sector)
            stocks.append({
                "symbol": sym,
                "sector": sector,
                "country": "US",
                "industry_type": "C",   # S&P500 대형주 기본값
                "beta": 1.0,            # 가격 데이터 없을 경우 기본값
                "market_cap": 50e9,     # 기본값 (대형주)
            })
    return stocks


# ══════════════════════════════════════════════════════════════
# 교체 지점 ③ — 종목별 메트릭 (가장 중요)
# ══════════════════════════════════════════════════════════════

# ── 가격 파일 형식: data/1_price/{SYMBOL}.csv ──
# Date,Open,High,Low,Close,Adj Close,Volume
# 2019-01-02,157.36,158.85,154.55,157.92,157.12,37039737

# ── 재무 파일 형식: data/2_fundamental/{SYMBOL}.csv ──
# year,revenue,operating_income,net_income,total_assets,total_equity,total_debt,operating_cash_flow,capex,eps
# 2023,383285000000,114301000000,96995000000,...

# ── 시그널 파일 형식: data/3_signal/{SYMBOL}_consensus.csv ──
# date,action,grading_company,from_grade,to_grade

# ── 시그널 파일 형식: data/3_signal/{SYMBOL}_earnings_surprise.csv ──
# date,actual_eps,estimated_eps,surprise_pct

# ── 시그널 파일 형식: data/3_signal/{SYMBOL}_short_interest.csv ──
# date,short_interest_shares,float_shares,sir


def load_stock_metrics(
    symbol: str,
    date: datetime,
    sector: str,
    country: str,
    industry_type: str,
    beta: float,
    market_cap: float,
    sector_roic_mean: float = 0.15,
    sector_roic_std: float = 0.08,
) -> Optional[StockMetrics]:
    """
    실제 데이터에서 StockMetrics 객체 생성.

    Returns:
        StockMetrics 또는 None (데이터 부족 시)
    """

    # ── 1. 가격 데이터 로드 ──
    price_path = os.path.join(DATA_DIR, "1_price", f"{symbol}.csv")
    if not os.path.exists(price_path):
        return None

    prices = []
    with open(price_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                date_str = row.get("Date") or row.get("date", "")
                close_str = row.get("Adj Close") or row.get("Close") or row.get("close", "")
                vol_str = row.get("Volume") or row.get("volume", "0")
                if not date_str or not close_str:
                    continue
                dt = datetime.strptime(date_str.strip(), "%Y-%m-%d")
                if dt <= date:
                    prices.append({
                        "date": dt,
                        "close": float(close_str),
                        "volume": float(vol_str) if vol_str else 0.0,
                    })
            except (ValueError, KeyError):
                pass

    if len(prices) < 200:
        return None

    prices.sort(key=lambda x: x["date"], reverse=True)

    current_price = prices[0]["close"]
    closes = [p["close"] for p in prices]
    volumes = [p["volume"] for p in prices]

    # 이동평균
    ma20 = sum(closes[:20]) / 20
    ma50 = sum(closes[:50]) / 50
    ma120 = sum(closes[:120]) / 120
    ma200 = sum(closes[:200]) / 200

    # RSI(14)
    rsi = _calculate_rsi(closes[:15])

    # 62일 모멘텀
    momentum_return = (closes[0] - closes[min(61, len(closes)-1)]) / closes[min(61, len(closes)-1)]

    # 평균 일거래대금
    avg_daily_volume = sum(v * c for v, c in zip(volumes[:20], closes[:20])) / 20

    # ── 2. 재무 데이터 로드 (연간 CSV) ──
    fund_path = os.path.join(DATA_DIR, "2_fundamental", f"{symbol}.csv")
    roic = wacc = roa = ocf = None
    growth_cagr = 0.0
    profit_trend_yoy = 0.5
    pe_relative = 1.0
    efficiency = 0.5
    days_since_report = 90

    if os.path.exists(fund_path):
        annual_data = []
        with open(fund_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    yr = int(row["year"])
                    # 연간 보고서는 다음해 3월말 이후 사용 (look-ahead bias 방지)
                    report_avail = datetime(yr + 1, 3, 31)
                    if report_avail <= date:
                        rev = float(row.get("revenue") or 0)
                        op_inc = float(row.get("operating_income") or 0)
                        net_inc = float(row.get("net_income") or 0)
                        assets = float(row.get("total_assets") or 0)
                        equity = float(row.get("total_equity") or 0)
                        debt = float(row.get("total_debt") or 0)
                        ocf_val = float(row.get("operating_cash_flow") or 0)
                        annual_data.append({
                            "year": yr,
                            "report_date": report_avail,
                            "revenue": rev,
                            "operating_income": op_inc,
                            "net_income": net_inc,
                            "total_assets": assets,
                            "total_equity": equity,
                            "total_debt": debt,
                            "operating_cash_flow": ocf_val,
                        })
                except (ValueError, KeyError, TypeError):
                    pass

        annual_data.sort(key=lambda x: x["year"], reverse=True)

        if annual_data:
            latest = annual_data[0]
            days_since_report = (date - latest["report_date"]).days

            # ROIC = net_income / invested_capital
            invested_cap = latest["total_equity"] + latest["total_debt"]
            roic = latest["net_income"] / invested_cap if invested_cap > 0 else None

            # ROA = net_income / total_assets
            roa = latest["net_income"] / latest["total_assets"] if latest["total_assets"] > 0 else None

            # OCF
            ocf = latest["operating_cash_flow"]

            # WACC 기본값
            wacc = 0.09

            # 이익률 YoY
            cur_margin = latest["operating_income"] / latest["revenue"] if latest["revenue"] > 0 else 0
            if len(annual_data) >= 2:
                prev = annual_data[1]
                prev_margin = prev["operating_income"] / prev["revenue"] if prev["revenue"] > 0 else 0
                profit_trend_yoy = min(1.0, max(0.0, 0.5 + (cur_margin - prev_margin) * 5))

            # 3년 매출 CAGR
            if len(annual_data) >= 4:
                rev_now = latest["revenue"]
                rev_3y = annual_data[3]["revenue"]
                if rev_3y > 0 and rev_now > 0:
                    growth_cagr = (rev_now / rev_3y) ** (1/3) - 1
            elif len(annual_data) >= 2:
                rev_now = latest["revenue"]
                rev_old = annual_data[-1]["revenue"]
                n_years = latest["year"] - annual_data[-1]["year"]
                if rev_old > 0 and rev_now > 0 and n_years > 0:
                    growth_cagr = (rev_now / rev_old) ** (1/n_years) - 1

    # ROIC Z-Score
    roic_zscore = 0.5
    if roic is not None and sector_roic_std > 0:
        z = (roic - sector_roic_mean) / sector_roic_std
        z = max(-3, min(3, z))
        roic_zscore = (z + 3) / 6

    # ── 3. 컨센서스 ──
    consensus_up_ratio = None
    has_consensus = False
    con_path = os.path.join(DATA_DIR, "3_signal", f"{symbol}_consensus.csv")
    if os.path.exists(con_path):
        with open(con_path) as f:
            reader = csv.DictReader(f)
            actions = []
            for row in reader:
                try:
                    dt = datetime.strptime(row["date"].strip(), "%Y-%m-%d")
                    if (date - dt).days <= 30:
                        action = row.get("action", "").strip().lower()
                        if action in ("upgrade", "downgrade"):
                            actions.append(action)
                except (ValueError, KeyError):
                    pass
        if actions:
            has_consensus = True
            consensus_up_ratio = sum(1 for a in actions if a == "upgrade") / len(actions)

    # ── 4. 어닝 서프라이즈 ──
    earnings_metric = None
    has_earnings = False
    es_path = os.path.join(DATA_DIR, "3_signal", f"{symbol}_earnings_surprise.csv")
    if os.path.exists(es_path):
        with open(es_path) as f:
            reader = csv.DictReader(f)
            earnings = []
            for row in reader:
                try:
                    dt = datetime.strptime(row["date"].strip(), "%Y-%m-%d")
                    if dt <= date:
                        actual = float(row.get("actual_eps") or row.get("actualEPS", 0))
                        estimated = float(row.get("estimated_eps") or row.get("estimatedEPS", 0))
                        earnings.append({"date": dt, "actualEPS": actual, "estimatedEPS": estimated})
                except (ValueError, KeyError, TypeError):
                    pass

        earnings.sort(key=lambda x: x["date"], reverse=True)

        if len(earnings) >= 4:
            has_earnings = True
            weights = [0.4, 0.3, 0.2, 0.1]
            surprises = []
            for e in earnings[:4]:
                est = abs(e["estimatedEPS"]) if e["estimatedEPS"] != 0 else 1
                surprises.append((e["actualEPS"] - e["estimatedEPS"]) / est)

            weighted = sum(s * w for s, w in zip(surprises, weights))

            consecutive_beats = 0
            for s in surprises:
                if s > 0:
                    consecutive_beats += 1
                else:
                    break
            bonus = {4: 1.20, 3: 1.10}.get(consecutive_beats, 1.0)
            earnings_metric = weighted * bonus

    # ── 5. Short Interest ──
    si_composite = None
    has_si = False
    si_path = os.path.join(DATA_DIR, "3_signal", f"{symbol}_short_interest.csv")
    if os.path.exists(si_path):
        with open(si_path) as f:
            reader = csv.DictReader(f)
            si_rows = []
            for row in reader:
                try:
                    dt = datetime.strptime(row["date"].strip(), "%Y-%m-%d")
                    if dt <= date:
                        sir_val = float(row.get("sir") or 0)
                        si_rows.append({"date": dt, "sir": sir_val})
                except (ValueError, KeyError, TypeError):
                    pass

        si_rows.sort(key=lambda x: x["date"], reverse=True)
        if len(si_rows) >= 2:
            has_si = True
            sir = si_rows[0]["sir"]
            sir_prev = si_rows[1]["sir"]
            sir_change = sir - sir_prev

            level_signal = 1.0 - min(1.0, max(0.0, sir / 0.15))
            change_signal = min(1.0, max(0.0, (-sir_change + 0.20) / 0.40))

            if sir >= 0.10:
                si_composite = level_signal * 0.2 + change_signal * 0.8
            else:
                si_composite = level_signal * 0.4 + change_signal * 0.6

    # ── StockMetrics 조립 ──
    return StockMetrics(
        symbol=symbol,
        country=country,
        sector=sector,
        industry_type=industry_type,
        price=current_price,
        ma120=ma120,
        ma200=ma200,
        roic=roic,
        wacc=wacc,
        roa=roa,
        ocf=ocf,
        days_since_report=days_since_report,
        avg_daily_volume=avg_daily_volume,
        market_cap=market_cap,
        roic_zscore=roic_zscore,
        profit_trend_yoy=profit_trend_yoy,
        growth_cagr=growth_cagr,
        consensus_up_ratio=consensus_up_ratio,
        momentum_return=momentum_return,
        pe_relative=pe_relative,
        efficiency=efficiency,
        rsi=rsi,
        earnings_surprise_metric=earnings_metric,
        si_composite=si_composite,
        ma20=ma20,
        ma50=ma50,
        roic_score_normalized=roic_zscore,
        profit_trend_normalized=profit_trend_yoy,
        is_held=False,
        beta=beta,
        has_consensus=has_consensus,
        has_earnings_surprise=has_earnings,
        has_short_interest=has_si,
    )


# ══════════════════════════════════════════════════════════════
# 교체 지점 ④ — 매크로 데이터
# ══════════════════════════════════════════════════════════════

def load_macro_data(date: datetime) -> dict:
    """
    매크로 데이터에서 해당 날짜 직전 최신값 로드.

    Returns:
        {"vix", "yield_curve", "claims", "cpi_yoy", "oil", "gold",
         "copper", "dollar", "us_rate", "kr_rate", "jp_rate", "eu_rate"}
    """
    series_map = {
        "vix":        "vix",
        "yield_curve":"yield_spread",
        "claims":     "claims",
        "cpi_yoy":    "cpi",
        "oil":        "wti",
        "gold":       "gold_fred",
        "copper":     "copper",
        "dollar":     "dxy",
        "us_rate":    "fedfunds",
        "kr_rate":    "rate_kr",
        "jp_rate":    "rate_jp",
        "eu_rate":    "rate_ecb",
    }

    result = {}
    for key, fname in series_map.items():
        path = os.path.join(DATA_DIR, "4_macro", f"{fname}.csv")
        if os.path.exists(path):
            result[key] = _get_latest_value(path, date)
        else:
            result[key] = None

    return result


# ── 유틸리티 ──

def _get_latest_value(csv_path: str, as_of: datetime) -> Optional[float]:
    """CSV에서 as_of 이전 최신값 반환. Date/date, Value/value 컬럼 모두 지원."""
    latest = None
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                date_str = row.get("Date") or row.get("date", "")
                val_str = row.get("Value") or row.get("value", "")
                if not date_str or not val_str:
                    continue
                dt = datetime.strptime(date_str.strip(), "%Y-%m-%d")
                if dt <= as_of:
                    latest = float(val_str)
            except (ValueError, KeyError):
                pass
    return latest


def _calculate_rsi(closes: list, period: int = 14) -> float:
    """Wilder RSI 계산."""
    if len(closes) < period + 1:
        return 50.0

    changes = [closes[i] - closes[i+1] for i in range(period)]
    gains = [c for c in changes if c > 0]
    losses = [-c for c in changes if c < 0]

    avg_gain = sum(gains) / period if gains else 0
    avg_loss = sum(losses) / period if losses else 0.0001

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def load_all_prices(symbols: list) -> dict:
    """
    모든 종목의 일별 가격을 미리 로드.
    백테스트 엔진에서 일별 수익률 계산에 사용.

    Returns:
        {symbol: {datetime: close_price}}
    """
    all_prices = {}
    for sym in symbols:
        path = os.path.join(DATA_DIR, "1_price", f"{sym}.csv")
        if not os.path.exists(path):
            continue
        prices = {}
        with open(path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    date_str = row.get("Date") or row.get("date", "")
                    close_str = row.get("Adj Close") or row.get("Close") or row.get("close", "")
                    if not date_str or not close_str:
                        continue
                    dt = datetime.strptime(date_str.strip(), "%Y-%m-%d")
                    prices[dt] = float(close_str)
                except (ValueError, KeyError):
                    pass
        all_prices[sym] = prices
    return all_prices
