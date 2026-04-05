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
      ├── 1_price/                  ← FMP: /historical-price-full
      │   ├── AAPL.csv
      │   ├── MSFT.csv
      │   ├── ...
      │   └── (S&P500 전 종목, 일별 OHLCV)
      │
      ├── 2_fundamental/            ← FMP: /key-metrics, /ratios, /income-statement, /cash-flow
      │   ├── AAPL.json
      │   ├── MSFT.json
      │   └── (분기별 재무 데이터)
      │
      ├── 3_signal/                 ← FMP: /upgrades-downgrades, /earnings-surprises, /short-interest
      │   ├── consensus/
      │   │   ├── AAPL.csv
      │   │   └── ...
      │   ├── earnings_surprise/
      │   │   ├── AAPL.json
      │   │   └── ...
      │   └── short_interest/
      │       ├── AAPL.json
      │       └── ...
      │
      ├── 4_macro/                  ← FRED API (12개 시리즈)
      │   ├── VIXCLS.csv
      │   ├── T10Y2Y.csv
      │   ├── FEDFUNDS.csv
      │   ├── ICSA.csv
      │   ├── CPIAUCSL.csv
      │   ├── DCOILWTICO.csv
      │   ├── GOLDAMGBD228NLBM.csv
      │   ├── PCOPPUSDM.csv
      │   ├── DTWEXBGS.csv
      │   ├── INTDSRKRM.csv
      │   ├── INTDSRJPM.csv
      │   └── ECBMRRFR.csv
      │
      ├── 5_benchmark/              ← S&P500 지수
      │   └── SPY.csv               ← 일별 종가 (또는 ^GSPC)
      │
      └── 6_universe/               ← 종목 메타데이터
          └── sp500_constituents.csv ← 종목코드, 섹터, 국가, industry_type, beta
"""

import os
import csv
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from quant_alpha_v3_4_unified import StockMetrics

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


# ══════════════════════════════════════════════════════════════
# 교체 지점 ① — 벤치마크 (S&P500 일별 종가)
# 
# 현재: gen_bench() → np.random으로 합성 생성
# 교체: load_benchmark() → data/5_benchmark/SPY.csv 읽기
# ══════════════════════════════════════════════════════════════

# ── 파일 형식: data/5_benchmark/SPY.csv ──
# date,close
# 2019-01-02,2510.03
# 2019-01-03,2447.89
# ...

def load_benchmark(dates: list) -> dict:
    """
    S&P500 일별 종가 로드.
    
    Returns:
        {datetime: float} — 날짜별 종가 딕셔너리
    """
    path = os.path.join(DATA_DIR, "5_benchmark", "SPY.csv")
    prices = {}
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            dt = datetime.strptime(row["date"], "%Y-%m-%d")
            prices[dt] = float(row["close"])
    return prices


# ══════════════════════════════════════════════════════════════
# 교체 지점 ② — 종목 유니버스 (메타데이터)
#
# 현재: gen_stocks(80) → 랜덤 종목 생성
# 교체: load_universe() → data/6_universe/sp500_constituents.csv
# ══════════════════════════════════════════════════════════════

# ── 파일 형식: data/6_universe/sp500_constituents.csv ──
# symbol,sector,country,industry_type,beta,market_cap
# AAPL,Technology,US,C,1.21,2900000000000
# MSFT,Technology,US,C,0.95,2800000000000
# NVDA,Technology,US,A,1.65,1200000000000
# JPM,Financials,US,C,1.12,450000000000
# ...
#
# industry_type 기준:
#   A = 소형 성장 (시가총액 < $10B)
#   B = 중형       ($10B ~ $50B)
#   C = 대형 안정  ($50B ~ $500B)
#   D = 특수/고확신 (재량 판단)

def load_universe() -> List[dict]:
    """
    투자 유니버스 로드.
    
    Returns:
        [{"symbol", "sector", "country", "industry_type", "beta", "market_cap"}, ...]
    """
    path = os.path.join(DATA_DIR, "6_universe", "sp500_constituents.csv")
    stocks = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            stocks.append({
                "symbol": row["symbol"],
                "sector": row["sector"],
                "country": row.get("country", "US"),
                "industry_type": row.get("industry_type", "C"),
                "beta": float(row.get("beta", 1.0)),
                "market_cap": float(row.get("market_cap", 50e9)),
            })
    return stocks


# ══════════════════════════════════════════════════════════════
# 교체 지점 ③ — 종목별 메트릭 (가장 중요)
#
# 현재: sim_metrics() → np.random으로 팩터 값 합성
# 교체: load_stock_metrics() → 가격 + 재무 + 시그널 조합
#
# 이 함수가 StockMetrics 객체를 반환하면 알고리즘 파이프라인에
# 그대로 들어갑니다. 변환 로직은 여기서 처리합니다.
# ══════════════════════════════════════════════════════════════

# ── 파일 형식 ──

# data/1_price/{SYMBOL}.csv (FMP /historical-price-full)
# date,open,high,low,close,volume
# 2019-01-02,157.36,158.85,154.55,157.92,37039737
# ...

# data/2_fundamental/{SYMBOL}.json (FMP /key-metrics + /ratios + /income + /cashflow 병합)
# [
#   {
#     "date": "2024-09-30",           ← 분기 종료일
#     "period": "Q3",
#     "roic": 0.287,                  ← ROIC
#     "wacc": 0.095,                  ← WACC (추정치)
#     "roa": 0.214,                   ← ROA
#     "operatingCashFlow": 26000000000,  ← OCF
#     "operatingProfitMargin": 0.312, ← 영업이익률 (profit_trend 산출용)
#     "revenue": 94930000000,         ← 매출 (3년 CAGR 산출용)
#     "pe": 28.5,                     ← P/E
#     "pe_5y_avg": 25.2,              ← 5년 평균 P/E (pe_relative 산출)
#     "inventoryTurnover": 8.2        ← 재고회전율
#   },
#   ...  ← 최소 12분기 (3년치)
# ]

# data/3_signal/consensus/{SYMBOL}.csv (FMP /upgrades-downgrades)
# date,action
# 2024-10-15,upgrade
# 2024-10-12,downgrade
# 2024-10-01,upgrade
# ...

# data/3_signal/earnings_surprise/{SYMBOL}.json (FMP /earnings-surprises)
# [
#   {"date":"2024-10-28","actualEPS":1.64,"estimatedEPS":1.55},  ← Q0 (최신)
#   {"date":"2024-07-25","actualEPS":1.40,"estimatedEPS":1.35},  ← Q1
#   {"date":"2024-04-25","actualEPS":1.53,"estimatedEPS":1.50},  ← Q2
#   {"date":"2024-01-25","actualEPS":2.18,"estimatedEPS":2.10},  ← Q3
# ]

# data/3_signal/short_interest/{SYMBOL}.json (FMP /short-interest + /shares-float)
# {
#   "current_sir": 0.032,       ← Short Interest Ratio (잔고/유통주식)
#   "previous_sir": 0.035,      ← 직전 보고 SIR
#   "sir_change": -0.003,       ← 변화량
#   "shares_short": 15000000,
#   "shares_float": 468000000
# }


def load_stock_metrics(
    symbol: str,
    date: datetime,
    sector: str,
    country: str,
    industry_type: str,
    beta: float,
    market_cap: float,
    sector_roic_mean: float = 0.15,    # 섹터 평균 ROIC (Z-Score용)
    sector_roic_std: float = 0.08,     # 섹터 표준편차
) -> Optional[StockMetrics]:
    """
    실제 데이터에서 StockMetrics 객체 생성.
    
    이 함수 하나가 data/1_price, data/2_fundamental, data/3_signal을
    전부 읽어서 알고리즘이 요구하는 StockMetrics로 변환합니다.
    
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
            dt = datetime.strptime(row["date"], "%Y-%m-%d")
            if dt <= date:
                prices.append({
                    "date": dt,
                    "close": float(row["close"]),
                    "volume": float(row["volume"]),
                })
    
    if len(prices) < 200:
        return None  # 이동평균 산출 불가
    
    prices.sort(key=lambda x: x["date"], reverse=True)
    
    current_price = prices[0]["close"]
    closes = [p["close"] for p in prices]
    volumes = [p["volume"] for p in prices]
    
    # 이동평균
    ma20 = sum(closes[:20]) / 20
    ma50 = sum(closes[:50]) / 50
    ma120 = sum(closes[:120]) / 120
    ma200 = sum(closes[:200]) / 200
    
    # RSI(14) — Wilder 방식
    rsi = _calculate_rsi(closes[:15])
    
    # 62일 모멘텀
    momentum_return = (closes[0] - closes[min(61, len(closes)-1)]) / closes[min(61, len(closes)-1)]
    
    # 평균 일거래대금
    avg_daily_volume = sum(v * c for v, c in zip(volumes[:20], closes[:20])) / 20
    
    # ── 2. 재무 데이터 로드 ──
    fund_path = os.path.join(DATA_DIR, "2_fundamental", f"{symbol}.json")
    roic = wacc = roa = ocf = None
    growth_cagr = 0.0
    profit_trend_yoy = 0.5
    pe_relative = 1.0
    efficiency = 0.5
    days_since_report = 90
    
    if os.path.exists(fund_path):
        with open(fund_path) as f:
            quarters = json.load(f)
        
        # 날짜 기준 필터링 (look-ahead bias 방지!)
        quarters = [q for q in quarters if datetime.strptime(q["date"], "%Y-%m-%d") <= date]
        quarters.sort(key=lambda x: x["date"], reverse=True)
        
        if quarters:
            latest = quarters[0]
            roic = latest.get("roic")
            wacc = latest.get("wacc")
            roa = latest.get("roa")
            ocf = latest.get("operatingCashFlow")
            
            # 보고서 경과일
            report_date = datetime.strptime(latest["date"], "%Y-%m-%d")
            days_since_report = (date - report_date).days
            
            # P/E relative
            pe = latest.get("pe", 0)
            pe_5y = latest.get("pe_5y_avg", pe)
            pe_relative = pe / pe_5y if pe_5y > 0 else 1.0
            
            # 재고회전율 → 효율성
            inv_turn = latest.get("inventoryTurnover", 5)
            efficiency = min(1.0, max(0.0, inv_turn / 10))
            
            # 이익률 YoY
            if len(quarters) >= 5:
                current_margin = latest.get("operatingProfitMargin", 0)
                yoy_margin = quarters[4].get("operatingProfitMargin", 0)
                profit_trend_yoy = min(1.0, max(0.0, 
                    0.5 + (current_margin - yoy_margin) * 5))
            
            # 3년 매출 CAGR
            if len(quarters) >= 12:
                rev_now = sum(q.get("revenue", 0) for q in quarters[:4])
                rev_3y = sum(q.get("revenue", 0) for q in quarters[8:12])
                if rev_3y > 0:
                    growth_cagr = (rev_now / rev_3y) ** (1/3) - 1
    
    # ROIC Z-Score
    roic_zscore = 0.5
    if roic is not None and sector_roic_std > 0:
        z = (roic - sector_roic_mean) / sector_roic_std
        z = max(-3, min(3, z))
        roic_zscore = (z + 3) / 6  # [-3,3] → [0,1]
    
    # ── 3. 컨센서스 ──
    consensus_up_ratio = None
    has_consensus = False
    con_path = os.path.join(DATA_DIR, "3_signal", "consensus", f"{symbol}.csv")
    if os.path.exists(con_path):
        with open(con_path) as f:
            reader = csv.DictReader(f)
            actions = []
            for row in reader:
                dt = datetime.strptime(row["date"], "%Y-%m-%d")
                if (date - dt).days <= 30:
                    actions.append(row["action"])
        if actions:
            has_consensus = True
            consensus_up_ratio = sum(1 for a in actions if a == "upgrade") / len(actions)
    
    # ── 4. 어닝 서프라이즈 ──
    earnings_metric = None
    has_earnings = False
    es_path = os.path.join(DATA_DIR, "3_signal", "earnings_surprise", f"{symbol}.json")
    if os.path.exists(es_path):
        with open(es_path) as f:
            earnings = json.load(f)
        earnings = [e for e in earnings if datetime.strptime(e["date"], "%Y-%m-%d") <= date]
        earnings.sort(key=lambda x: x["date"], reverse=True)
        
        if len(earnings) >= 4:
            has_earnings = True
            weights = [0.4, 0.3, 0.2, 0.1]
            surprises = []
            for e in earnings[:4]:
                est = abs(e["estimatedEPS"]) if e["estimatedEPS"] != 0 else 1
                surprises.append((e["actualEPS"] - e["estimatedEPS"]) / est)
            
            weighted = sum(s * w for s, w in zip(surprises, weights))
            
            # 일관성 보너스
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
    si_path = os.path.join(DATA_DIR, "3_signal", "short_interest", f"{symbol}.json")
    if os.path.exists(si_path):
        with open(si_path) as f:
            si_data = json.load(f)
        has_si = True
        sir = si_data.get("current_sir", 0)
        sir_change = si_data.get("sir_change", 0)
        
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
# 교체 지점 ④ — 매크로 데이터 (FRED)
#
# 현재: regime_params() → 날짜별 하드코딩
# 교체: load_macro_data() → data/4_macro/*.csv
# ══════════════════════════════════════════════════════════════

# ── 파일 형식: data/4_macro/VIXCLS.csv ──
# date,value
# 2019-01-02,21.38
# 2019-01-03,23.22
# ...

def load_macro_data(date: datetime) -> dict:
    """
    FRED 매크로 데이터에서 해당 날짜 직전 최신값 로드.
    
    Returns:
        {
            "vix": 18.5,
            "yield_curve": 0.42,
            "claims": 220000,
            "cpi_yoy": 0.032,
            "oil": 72.5,
            "gold": 1950.0,
            "copper": 8500.0,
            "dollar": 103.2,
            "us_rate": 5.25,
            "kr_rate": 3.50,
            "jp_rate": 0.10,
            "eu_rate": 4.50,
        }
    """
    series_map = {
        "vix": "VIXCLS",
        "yield_curve": "T10Y2Y",
        "claims": "ICSA",
        "cpi_yoy": "CPIAUCSL",
        "oil": "DCOILWTICO",
        "gold": "GOLDAMGBD228NLBM",
        "copper": "PCOPPUSDM",
        "dollar": "DTWEXBGS",
        "us_rate": "FEDFUNDS",
        "kr_rate": "INTDSRKRM",
        "jp_rate": "INTDSRJPM",
        "eu_rate": "ECBMRRFR",
    }
    
    result = {}
    for key, series_id in series_map.items():
        path = os.path.join(DATA_DIR, "4_macro", f"{series_id}.csv")
        if os.path.exists(path):
            result[key] = _get_latest_value(path, date)
        else:
            result[key] = None
    
    return result


# ── 유틸리티 ──

def _get_latest_value(csv_path: str, as_of: datetime) -> Optional[float]:
    """CSV에서 as_of 이전 최신값 반환."""
    latest = None
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            dt = datetime.strptime(row["date"], "%Y-%m-%d")
            if dt <= as_of:
                try:
                    latest = float(row["value"])
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


# ══════════════════════════════════════════════════════════════
# 백테스트 엔진 연동 방법
#
# backtest_engine.py에서 바꿀 줄:
#
#   Before (합성):
#     self.stocks = gen_stocks(80)
#     self.bench = gen_bench(self.dates)
#     metrics = sim_metrics(stk, date, mkt_ret, reg)
#
#   After (실제):
#     from data_loader import load_universe, load_benchmark, load_stock_metrics, load_macro_data
#     self.stocks = load_universe()
#     self.bench = [load_benchmark(self.dates)[d] for d in self.dates]
#     metrics = load_stock_metrics(stk["symbol"], date, stk["sector"], ...)
#
# 총 수정량: backtest_engine.py에서 3줄 교체
# ══════════════════════════════════════════════════════════════
