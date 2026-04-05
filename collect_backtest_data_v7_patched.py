#!/usr/bin/env python3
"""
=============================================================================
  Quant-Alpha v3.3 백테스트용 데이터 수집 스크립트 v3.0
  문서번호: QA-2026-BT-001
=============================================================================

  설계 원칙:
    FMP Premium을 모든 수집의 메인 소스로 사용하고,
    각 단계마다 yfinance/FRED를 폴백으로 배치.

    [주가 50종목]    FMP historical-price-eod  →  폴백: yfinance
    [벤치마크 SP500]  FMP index-historical      →  폴백: yfinance ^GSPC
    [매크로 7개]      FRED API (원본 소스)       →  폴백: 없음 (FRED가 원본)
    [금 가격]         FMP commodity GCUSD        →  폴백: yfinance GC=F
    [재무제표 50종목]  FMP income/balance/cf      →  폴백: yfinance

  변경 이력:
    v1.0 - 초기 (yfinance + FMP 레거시)
    v2.1 - FMP /stable/ 마이그레이션, FRED 금 중단 대응
    v3.0 - FMP Premium 전면 메인 전환
         - 주가/벤치마크도 FMP 우선 수집으로 변경
         - 모든 단계에 폴백 로직 완비
         - 종목별 실패/성공 추적 및 폴백 리포트

  실행:
    pip install requests yfinance pandas
    python collect_backtest_data_v3.py
=============================================================================
"""

import os
import sys
import csv
import json
import time
import logging
from datetime import datetime
from pathlib import Path

import requests
import yfinance as yf
import pandas as pd

# ============================================================================
# 1. 설정
# ============================================================================

FMP_API_KEY = os.environ.get("FMP_API_KEY", "Wz4nOOqLm8sTMyindByujmGybLmFRyY6")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "ae2279093d5c518c3d15904012a2146b")
FMP_BASE = "https://financialmodelingprep.com/stable"

START_DATE = "2013-06-01"
END_DATE   = "2024-12-31"
YEAR_START = 2013
YEAR_END   = 2024

TICKERS = [
    "NVDA","ASML","AMZN","CRWD","V",  "LLY","GE",  "XOM","KO",  "VZ",
    "AMD", "AMAT","AAPL","PANW","MA", "ABBV","TSLA","CVX","PEP", "AMT",
    "AVGO","MSFT","GOOGL","JPM","BRK-B","PFE","HD","NEE","COST","PLD",
    "QCOM","CRM", "META","BAC", "UNH","CAT","NKE","LIN","WMT", "TXN",
    "TSM", "ORCL","NFLX","GS",  "JNJ","HON","MCD","PG", "T",   "INTC",
]

FRED_SERIES = {
    "VIXCLS":     "vix.csv",
    "T10Y2Y":     "yield_spread.csv",
    "ICSA":       "claims.csv",
    "CPIAUCSL":   "cpi.csv",
    "DCOILWTICO": "wti.csv",
    "PCOPPUSDM":  "copper.csv",
    "DTWEXBGS":   "dxy.csv",
}

BASE_DIR   = Path("data")
PRICES_DIR = BASE_DIR / "1_price"
MACRO_DIR  = BASE_DIR / "4_macro"
BENCH_DIR  = BASE_DIR / "5_benchmark"
FUND_DIR   = BASE_DIR / "2_fundamental"
SIGNAL_DIR = BASE_DIR / "3_signal"
UNIV_DIR   = BASE_DIR / "6_universe"

FMP_DELAY    = 0.4
FRED_DELAY   = 0.3
FINRA_DELAY  = 0.5
MAX_RETRIES  = 3
RETRY_WAIT   = 5

# ============================================================================
# 2. 로깅
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("collect_log.txt", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ============================================================================
# 3. 유틸리티
# ============================================================================

def makedirs():
    for d in [PRICES_DIR, MACRO_DIR, BENCH_DIR, FUND_DIR, SIGNAL_DIR, UNIV_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    log.info("✅ 폴더 구조 생성 완료")



def generate_universe():
    """엔진이 읽는 유니버스 파일 생성 (6_universe/)"""
    filepath = UNIV_DIR / "sp500_current.csv"
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["symbol", "sector", "country", "industry_type"])
        for ticker in TICKERS:
            w.writerow([ticker, "", "US", "C"])
    log.info(f"✅ 유니버스 파일 생성: {filepath}")


def fmp_get(endpoint, params=None):
    """FMP /stable/ 안전 요청 (재시도 + 에러 핸들링)"""
    if params is None:
        params = {}
    params["apikey"] = FMP_API_KEY
    url = f"{FMP_BASE}/{endpoint}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=30)

            if r.status_code == 429:
                wait = RETRY_WAIT * attempt * 2
                log.warning(f"    ⏳ Rate limit 429. {wait}초 대기 ({attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                log.warning(f"    ⚠️  HTTP {r.status_code}: {endpoint}")
                return None

            data = r.json()
            if isinstance(data, dict) and "Error Message" in data:
                log.error(f"    ❌ FMP 에러: {data['Error Message'][:100]}")
                return None
            return data

        except requests.exceptions.Timeout:
            log.warning(f"    ⏳ 타임아웃 ({attempt}/{MAX_RETRIES})")
            time.sleep(RETRY_WAIT)
        except requests.exceptions.ConnectionError:
            log.warning(f"    ⏳ 연결 실패 ({attempt}/{MAX_RETRIES})")
            time.sleep(RETRY_WAIT)
        except Exception as e:
            log.error(f"    ❌ 예외: {e}")
            return None
    return None


def fred_get(series_id):
    """FRED API 안전 요청"""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": START_DATE,
        "observation_end": END_DATE,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if "observations" in data:
                    return data["observations"]
            return None
        except Exception:
            time.sleep(RETRY_WAIT)
    return None


def save_price_csv(df, filepath):
    """
    yfinance DataFrame을 명세서 포맷 CSV로 저장
    멀티인덱스 헤더 → Date,Open,High,Low,Close,Adj Close,Volume
    """
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    cols = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
    for c in cols:
        if c not in df.columns:
            return False
    df = df[cols].copy()
    df.index.name = "Date"
    df.to_csv(filepath, encoding="utf-8", lineterminator="\n")
    return True


# ============================================================================
# 4. [1/8] 주가 50종목: FMP 메인 → yfinance 폴백
# ============================================================================

def fetch_prices():
    log.info("\n" + "=" * 60)
    log.info("[1/8] 종목별 일별 주가 수집")
    log.info("      메인: FMP historical-price-eod")
    log.info("      폴백: yfinance")
    log.info("=" * 60)

    fmp_ok, yf_ok, fail = 0, 0, 0

    for ticker in TICKERS:
        filepath = PRICES_DIR / f"{ticker}.csv"

        # --- FMP 메인 ---
        log.info(f"  [{TICKERS.index(ticker)+1}/50] {ticker} ─ FMP 시도...")
        data = fmp_get("historical-price-eod/full", {
            "symbol": ticker, "from": START_DATE, "to": END_DATE,
        })

        historical = None
        if isinstance(data, dict) and "historical" in data:
            historical = data["historical"]
        elif isinstance(data, list) and len(data) > 0:
            historical = data

        if historical and len(historical) > 100:
            rows = sorted(historical, key=lambda x: x.get("date", ""))
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, lineterminator="\n")
                w.writerow(["Date","Open","High","Low","Close","Adj Close","Volume"])
                for r in rows:
                    w.writerow([
                        r.get("date",""),
                        r.get("open",""),
                        r.get("high",""),
                        r.get("low",""),
                        r.get("close",""),
                        r.get("adjClose", r.get("close","")),
                        r.get("volume",""),
                    ])
            log.info(f"    ✅ FMP: {len(rows)}행")
            fmp_ok += 1
            time.sleep(FMP_DELAY)
            continue

        # --- yfinance 폴백 ---
        log.info(f"    ⚠️  FMP 실패 → yfinance 폴백...")
        try:
            df = yf.download(ticker, start=START_DATE, end=END_DATE,
                             auto_adjust=False, progress=False)
            if not df.empty and save_price_csv(df, filepath):
                log.info(f"    ✅ yfinance 폴백: {len(df)}행")
                yf_ok += 1
            else:
                log.error(f"    ❌ 폴백도 실패: 데이터 없음")
                fail += 1
        except Exception as e:
            log.error(f"    ❌ 폴백 예외: {e}")
            fail += 1

        time.sleep(FMP_DELAY)

    log.info(f"\n  주가 수집 완료: FMP {fmp_ok} | 폴백(yf) {yf_ok} | 실패 {fail}")


# ============================================================================
# 5. [2/8] 벤치마크 SP500: FMP 메인 → yfinance 폴백
# ============================================================================

def fetch_benchmark():
    log.info("\n" + "=" * 60)
    log.info("[2/8] S&P 500 벤치마크 수집")
    log.info("      메인: FMP index-historical-price-eod")
    log.info("      폴백: yfinance ^GSPC")
    log.info("=" * 60)

    filepath = BENCH_DIR / "SP500.csv"

    # --- FMP 메인 ---
    log.info("  FMP 시도 (^GSPC)...")
    data = fmp_get("historical-price-eod/full", {
        "symbol": "^GSPC", "from": START_DATE, "to": END_DATE,
    })

    historical = None
    if isinstance(data, dict) and "historical" in data:
        historical = data["historical"]
    elif isinstance(data, list) and len(data) > 0:
        historical = data

    if historical and len(historical) > 100:
        rows = sorted(historical, key=lambda x: x.get("date", ""))
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(["Date","Open","High","Low","Close","Adj Close","Volume"])
            for r in rows:
                w.writerow([
                    r.get("date",""),
                    r.get("open",""),
                    r.get("high",""),
                    r.get("low",""),
                    r.get("close",""),
                    r.get("adjClose", r.get("close","")),
                    r.get("volume",""),
                ])
        log.info(f"  ✅ FMP: {len(rows)}행")
        return

    # --- yfinance 폴백 ---
    log.info("  ⚠️  FMP 실패 → yfinance 폴백...")
    try:
        df = yf.download("^GSPC", start=START_DATE, end=END_DATE,
                         auto_adjust=False, progress=False)
        if not df.empty and save_price_csv(df, filepath):
            log.info(f"  ✅ yfinance 폴백: {len(df)}행")
        else:
            log.error("  ❌ 폴백도 실패")
    except Exception as e:
        log.error(f"  ❌ 폴백 예외: {e}")


# ============================================================================
# 6. [3/8] 매크로: FRED 7개 + FMP 금(GCUSD)
# ============================================================================

def fetch_macro():
    log.info("\n" + "=" * 60)
    log.info("[3/8] 매크로 경제지표 수집 (8개)")
    log.info("      FRED 7개 시리즈 (원본 소스)")
    log.info("      금 가격: FMP GCUSD 메인 → yfinance GC=F 폴백")
    log.info("=" * 60)

    # --- 6-1. FRED 7개 ---
    log.info("\n  ── FRED 매크로 7개 ──")
    fred_ok, fred_fail = 0, 0

    for series_id, filename in FRED_SERIES.items():
        obs = fred_get(series_id)
        if obs:
            filepath = MACRO_DIR / filename
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f, lineterminator="\n")
                w.writerow(["Date", "Value"])
                for o in obs:
                    w.writerow([o.get("date"), o.get("value")])
            log.info(f"  ✅ {filename} ({series_id}): {len(obs)}행")
            fred_ok += 1
        else:
            log.error(f"  ❌ {filename} ({series_id}): 수집 실패")
            fred_fail += 1
        time.sleep(FRED_DELAY)

    log.info(f"\n  FRED 완료: 성공 {fred_ok}/7, 실패 {fred_fail}")

    # --- 6-2. 금 가격: FMP 메인 → yfinance 폴백 ---
    log.info("\n  ── 금 가격 (FRED 중단 → FMP 대체) ──")
    filepath = MACRO_DIR / "gold.csv"

    data = fmp_get("historical-price-eod/full", {
        "symbol": "GCUSD", "from": START_DATE, "to": END_DATE,
    })

    historical = None
    if isinstance(data, dict) and "historical" in data:
        historical = data["historical"]
    elif isinstance(data, list) and len(data) > 0:
        historical = data

    if historical and len(historical) > 100:
        rows = sorted(historical, key=lambda x: x.get("date", ""))
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(["Date", "Value"])
            for r in rows:
                price = r.get("adjClose") or r.get("close")
                if r.get("date") and price is not None:
                    w.writerow([r["date"], price])
        log.info(f"  ✅ gold.csv (FMP GCUSD): {len(rows)}행, 첫 값=${rows[0].get('close','?')}/oz")
        return

    # --- yfinance 폴백: GC=F ---
    log.info("  ⚠️  FMP 실패 → yfinance GC=F 폴백...")
    try:
        df = yf.download("GC=F", start=START_DATE, end=END_DATE,
                         auto_adjust=False, progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        if not df.empty and "Close" in df.columns:
            gold = df[["Close"]].copy()
            gold.columns = ["Value"]
            gold.index.name = "Date"
            gold.to_csv(filepath, encoding="utf-8", lineterminator="\n")
            log.info(f"  ✅ gold.csv (yf GC=F 폴백): {len(gold)}행")
        else:
            log.error("  ❌ 폴백도 실패")
    except Exception as e:
        log.error(f"  ❌ 폴백 예외: {e}")


# ============================================================================
# 7. [4/8] 재무제표: FMP 메인 → yfinance 폴백
# ============================================================================

# FMP 응답 필드 → 명세서 컬럼 매핑
INCOME_MAP = {
    "revenue": "revenue",
    "operatingIncome": "operating_income",
    "operating_income": "operating_income",
    "netIncome": "net_income",
    "net_income": "net_income",
    "epsDiluted": "eps",
    "eps": "eps",
    "epsdiluted": "eps",
    "eps_diluted": "eps",
}
BALANCE_MAP = {
    "totalAssets": "total_assets",
    "total_assets": "total_assets",
    "totalStockholdersEquity": "total_equity",
    "total_stockholders_equity": "total_equity",
    "totalEquity": "total_equity",
    "total_equity": "total_equity",
    "longTermDebt": "total_debt",
    "long_term_debt": "total_debt",
}
CASHFLOW_MAP = {
    "netCashProvidedByOperatingActivities": "operating_cash_flow",
    "net_cash_provided_by_operating_activities": "operating_cash_flow",
    "operatingCashFlow": "operating_cash_flow",
    "operating_cash_flow": "operating_cash_flow",
    "capitalExpenditure": "capex",
    "capital_expenditure": "capex",
    "capitalExpenditures": "capex",
    "capital_expenditures": "capex",
}

FUND_COLS = [
    "year","revenue","operating_income","net_income",
    "total_assets","total_equity","total_debt",
    "operating_cash_flow","capex","eps",
]


def _extract_year(rec):
    """FMP 레코드에서 연도 추출"""
    for key in ("calendar_year", "fiscal_year"):
        if key in rec and rec[key]:
            try:
                return int(rec[key])
            except (ValueError, TypeError):
                pass
    if "date" in rec and rec["date"]:
        try:
            return int(str(rec["date"])[:4])
        except (ValueError, TypeError):
            pass
    return None


def _fmp_fundamentals(ticker):
    """FMP에서 재무제표 3종 수집 → 연도별 병합 dict 반환"""
    endpoints = [
        ("income-statement",       INCOME_MAP),
        ("balance-sheet-statement", BALANCE_MAP),
        ("cash-flow-statement",    CASHFLOW_MAP),
    ]
    merged = {}  # {year: {col: val}}

    for ep, field_map in endpoints:
        data = fmp_get(ep, {"symbol": ticker, "period": "annual", "limit": 50})
        if not data or not isinstance(data, list):
            continue
        for rec in data:
            year = _extract_year(rec)
            if year is None or year < YEAR_START or year > YEAR_END:
                continue
            if year not in merged:
                merged[year] = {"year": year}
            for fmp_key, our_col in field_map.items():
                if fmp_key in rec and rec[fmp_key] is not None:
                    merged[year][our_col] = rec[fmp_key]
        time.sleep(FMP_DELAY)

    return merged


def _yf_fundamentals(ticker):
    """yfinance에서 재무제표 3종 수집 → 연도별 병합 dict 반환"""
    t = yf.Ticker(ticker)
    merged = {}

    # Income Statement
    src = t.financials
    if src is not None and not src.empty:
        mapping = {
            "Total Revenue": "revenue",
            "Operating Income": "operating_income",
            "Net Income": "net_income",
            "Diluted EPS": "eps",
            "Basic EPS": "eps",
        }
        for col_date in src.columns:
            yr = col_date.year
            if yr < YEAR_START or yr > YEAR_END:
                continue
            if yr not in merged:
                merged[yr] = {"year": yr}
            for idx, col in mapping.items():
                if idx in src.index:
                    v = src.loc[idx, col_date]
                    if pd.notna(v) and col not in merged[yr]:
                        merged[yr][col] = v

    # Balance Sheet
    src = t.balance_sheet
    if src is not None and not src.empty:
        mapping = {
            "Total Assets": "total_assets",
            "Stockholders Equity": "total_equity",
            "Total Stockholders Equity": "total_equity",
            "Stockholders' Equity": "total_equity",
            "Long Term Debt": "total_debt",
            "Long Term Debt And Capital Lease Obligation": "total_debt",
        }
        for col_date in src.columns:
            yr = col_date.year
            if yr < YEAR_START or yr > YEAR_END:
                continue
            if yr not in merged:
                merged[yr] = {"year": yr}
            for idx, col in mapping.items():
                if idx in src.index:
                    v = src.loc[idx, col_date]
                    if pd.notna(v) and col not in merged[yr]:
                        merged[yr][col] = v

    # Cash Flow
    src = t.cashflow
    if src is not None and not src.empty:
        mapping = {
            "Operating Cash Flow": "operating_cash_flow",
            "Cash Flow From Continuing Operating Activities": "operating_cash_flow",
            "Capital Expenditure": "capex",
            "Capital Expenditures": "capex",
        }
        for col_date in src.columns:
            yr = col_date.year
            if yr < YEAR_START or yr > YEAR_END:
                continue
            if yr not in merged:
                merged[yr] = {"year": yr}
            for idx, col in mapping.items():
                if idx in src.index:
                    v = src.loc[idx, col_date]
                    if pd.notna(v) and col not in merged[yr]:
                        merged[yr][col] = v

    return merged


def _save_fundamentals(merged, filepath):
    """병합된 재무 dict → CSV 저장. 반환: 저장 행수"""
    if not merged:
        return 0
    rows = sorted(merged.values(), key=lambda x: x["year"])
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FUND_COLS,
                           lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({c: row.get(c, "") for c in FUND_COLS})
    return len(rows)


def fetch_fundamentals():
    log.info("\n" + "=" * 60)
    log.info("[4/8] 재무제표 수집 (50종목 × 9컬럼)")
    log.info("      메인: FMP income / balance-sheet / cash-flow")
    log.info("      폴백: yfinance .financials / .balance_sheet / .cashflow")
    log.info("=" * 60)

    fmp_ok, yf_ok, fail = 0, 0, 0

    for i, ticker in enumerate(TICKERS, 1):
        filepath = FUND_DIR / f"{ticker}.csv"
        log.info(f"  [{i}/50] {ticker} ─ FMP 시도...")

        # --- FMP 메인 ---
        merged = _fmp_fundamentals(ticker)
        count = _save_fundamentals(merged, filepath)

        if count >= 3:  # 최소 3년치 이상이면 유효
            years = sorted(merged.keys())
            log.info(f"    ✅ FMP: {count}년치 ({min(years)}~{max(years)})")
            fmp_ok += 1
            continue

        # --- yfinance 폴백 ---
        log.info(f"    ⚠️  FMP 부족({count}년) → yfinance 폴백...")
        try:
            yf_merged = _yf_fundamentals(ticker)

            # FMP 데이터가 일부라도 있으면 yfinance로 보충 (연도 단위 병합)
            for yr, vals in yf_merged.items():
                if yr not in merged:
                    merged[yr] = vals
                else:
                    for col, val in vals.items():
                        if col not in merged[yr] or merged[yr][col] == "":
                            merged[yr][col] = val

            count = _save_fundamentals(merged, filepath)
            if count >= 2:
                years = sorted(merged.keys())
                log.info(f"    ✅ FMP+yf 병합: {count}년치 ({min(years)}~{max(years)})")
                yf_ok += 1
            else:
                log.warning(f"    ⚠️  병합 후에도 {count}년치 (부족)")
                yf_ok += 1

        except Exception as e:
            log.error(f"    ❌ 폴백 예외: {e}")
            fail += 1

    log.info(f"\n  재무제표 완료: FMP단독 {fmp_ok} | FMP+yf보충 {yf_ok} | 실패 {fail}")


# ============================================================================
# [5/8] 컨센서스 (애널리스트 추정치): FMP analyst-estimates
# ============================================================================

def fetch_consensus():
    log.info("\n" + "=" * 60)
    log.info("[5/8] 컨센서스 추정치 수집 (50종목)")
    log.info("      FMP: analyst-estimates (annual)")
    log.info("=" * 60)

    ok, fail = 0, 0

    for i, ticker in enumerate(TICKERS, 1):
        filepath = SIGNAL_DIR / f"{ticker}_consensus.csv"
        log.info(f"  [{i}/50] {ticker}")

        data = fmp_get("analyst-estimates", {
            "symbol": ticker, "period": "annual", "limit": 50,
        })

        if not data:
            log.warning(f"    ⚠️  응답 없음")
            fail += 1
            time.sleep(FMP_DELAY)
            continue

        # 응답 형식 처리 (list 또는 dict 대응)
        records = []
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            # dict 안에 리스트가 있을 수 있음
            for v in data.values():
                if isinstance(v, list):
                    records = v
                    break
            if not records:
                records = [data]

        if not records:
            log.warning(f"    ⚠️  파싱 가능한 데이터 없음 (타입: {type(data).__name__})")
            fail += 1
            time.sleep(FMP_DELAY)
            continue

        # 날짜 필터 없이 전체 저장 (미래 추정치 포함, 알고리즘에서 기간 필터링)
        cols = ["date", "symbol",
                "revenueAvg", "revenueHigh", "revenueLow",
                "epsAvg", "epsHigh", "epsLow",
                "ebitdaAvg", "netIncomeAvg",
                "sgaExpenseAvg", "numAnalystsRevenue", "numAnalystsEps"]
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n", extrasaction="ignore")
            w.writeheader()
            for r in sorted(records, key=lambda x: x.get("date", "")):
                w.writerow({c: r.get(c, "") for c in cols})

        log.info(f"    ✅ {len(records)}건 (전체 저장, 알고리즘에서 기간 필터링)")
        ok += 1
        time.sleep(FMP_DELAY)

    log.info(f"\n  컨센서스 완료: 성공 {ok} | 실패 {fail}")


# ============================================================================
# [6/8] 어닝서프라이즈: FMP earnings-calendar (분기별 수집 → 종목 필터)
# ============================================================================

def fetch_earnings_surprise():
    """earnings-calendar는 symbol 필터가 안 되므로,
    연도별로 전체 데이터를 받아서 우리 종목만 필터링"""
    log.info("\n" + "=" * 60)
    log.info("[6/8] 어닝서프라이즈 수집 (50종목)")
    log.info("      FMP: earnings-calendar (연도별 수집 → 종목 필터)")
    log.info("=" * 60)

    ticker_set = set(TICKERS)
    ticker_rows = {t: [] for t in TICKERS}

    start_y = int(START_DATE[:4])
    end_y = int(END_DATE[:4])

    total_fetched = 0
    for year in range(start_y, end_y + 1):
        q_from = f"{year}-01-01"
        q_to = f"{year}-12-31"
        log.info(f"  {year}년 수집 중...")

        data = fmp_get("earnings-calendar", {
            "from": q_from, "to": q_to,
        })

        if not data:
            log.warning(f"    ⚠️  {year}년 응답 없음")
            time.sleep(FMP_DELAY)
            continue

        # 응답 형식 처리
        records = []
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    records = v
                    break

        if not records:
            log.warning(f"    ⚠️  {year}년 데이터 파싱 실패 (타입: {type(data).__name__})")
            time.sleep(FMP_DELAY)
            continue

        # 우리 종목만 필터
        count = 0
        for rec in records:
            sym = rec.get("symbol", "")
            if sym in ticker_set:
                if rec.get("epsActual") is not None or rec.get("epsEstimated") is not None:
                    ticker_rows[sym].append(rec)
                    count += 1
        total_fetched += count
        log.info(f"    ✅ 전체 {len(records)}건 중 대상종목 {count}건")
        time.sleep(FMP_DELAY)

    # 종목별 CSV 저장
    ok, empty = 0, 0
    cols = ["date", "symbol", "epsActual", "epsEstimated",
            "revenueActual", "revenueEstimated"]

    for ticker in TICKERS:
        filepath = SIGNAL_DIR / f"{ticker}_earnings.csv"
        rows = ticker_rows[ticker]
        if not rows:
            empty += 1
            continue
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n", extrasaction="ignore")
            w.writeheader()
            for r in sorted(rows, key=lambda x: x.get("date", "")):
                w.writerow({c: r.get(c, "") for c in cols})
        ok += 1

    log.info(f"\n  어닝서프라이즈 완료: {ok}종목 저장 | {empty}종목 데이터 없음 | 총 {total_fetched}건")


# ============================================================================
# [7/8] FINRA 공매도 잔고: FINRA Equity Short Interest API
# ============================================================================

def _finra_short_interest(ticker):
    """FINRA API에서 종목별 공매도 잔고 조회"""
    url = "https://api.finra.org/data/group/otcMarket/name/EquityShortInterest"
    payload = {
        "compareFilters": [
            {
                "compareType": "EQUAL",
                "fieldName": "issueSymbolIdentifier",
                "fieldValue": ticker,
            }
        ],
        "limit": 200,
        "sortFields": ["-settlementDate"],
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=30)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                wait = RETRY_WAIT * attempt * 2
                log.warning(f"    ⏳ FINRA 429. {wait}초 대기")
                time.sleep(wait)
            else:
                log.warning(f"    ⚠️  FINRA HTTP {r.status_code}")
                return None
        except Exception as e:
            log.warning(f"    ⏳ FINRA 예외 ({attempt}/{MAX_RETRIES}): {e}")
            time.sleep(RETRY_WAIT)
    return None


def fetch_short_interest():
    log.info("\n" + "=" * 60)
    log.info("[7/8] FINRA 공매도 잔고 수집 (50종목)")
    log.info("      FINRA Equity Short Interest API (월 2회)")
    log.info("      ⚠️  해외 IP에서 차단될 수 있음 → 실패 시 스킵")
    log.info("=" * 60)

    # 먼저 FINRA API 접근 가능 여부 테스트
    log.info("  🔍 FINRA API 연결 테스트...")
    test = _finra_short_interest("AAPL")
    if test is None:
        log.warning("  ⚠️  FINRA API 접근 불가. 공매도 수집 전체 스킵.")
        log.warning("     (해외 IP 차단 또는 네트워크 문제)")
        return

    log.info("  ✅ FINRA 연결 정상")
    ok, fail = 0, 0

    for i, ticker in enumerate(TICKERS, 1):
        filepath = SIGNAL_DIR / f"{ticker}_short.csv"
        log.info(f"  [{i}/50] {ticker}")

        data = _finra_short_interest(ticker)

        if not data or not isinstance(data, list) or len(data) == 0:
            log.warning(f"    ⚠️  데이터 없음")
            fail += 1
            time.sleep(FINRA_DELAY)
            continue

        # 수집 기간 내 데이터만 필터
        rows = []
        for rec in data:
            sd = rec.get("settlementDate", "")
            if sd and START_DATE <= sd <= END_DATE:
                rows.append(rec)

        if not rows:
            log.warning(f"    ⚠️  기간 내 데이터 없음")
            fail += 1
            time.sleep(FINRA_DELAY)
            continue

        # CSV 저장
        cols = ["settlementDate", "issueSymbolIdentifier", "issueName",
                "currentShortShareNumber", "previousShortShareNumber",
                "changePercent", "averageShortShareNumber", "daysToCover"]
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols, lineterminator="\n", extrasaction="ignore")
            w.writeheader()
            for r in sorted(rows, key=lambda x: x.get("settlementDate", "")):
                w.writerow({c: r.get(c, "") for c in cols})

        log.info(f"    ✅ {len(rows)}건")
        ok += 1
        time.sleep(FINRA_DELAY)

    log.info(f"\n  공매도 잔고 완료: 성공 {ok} | 실패 {fail}")


# ============================================================================
# [8/8] 검증
# ============================================================================

def validate():
    log.info("\n" + "=" * 60)
    log.info("[8/8] 수집 완료 검증")
    log.info("=" * 60)

    errors, warns = [], []

    # 1. 주가 파일 수
    pf = list(PRICES_DIR.glob("*.csv"))
    if len(pf) == 50:
        log.info(f"  ✅ 1_price/: {len(pf)}개 CSV")
    else:
        msg = f"1_price/: {len(pf)}개 (50개 필요)"
        errors.append(msg)
        log.error(f"  ❌ {msg}")
        missing = set(TICKERS) - {f.stem for f in pf}
        if missing:
            log.error(f"     누락: {', '.join(sorted(missing))}")

    # 2. 주가 헤더 + 날짜
    hdr_ok, date_ok = 0, 0
    for f in pf:
        try:
            df = pd.read_csv(f, nrows=2)
            if df.columns[0] == "Date" and "Adj Close" in df.columns:
                hdr_ok += 1
            first = str(df["Date"].iloc[0])
            if "2013-06" in first or "2013-07" in first or f.stem == "CRWD":
                date_ok += 1
        except Exception:
            pass
    log.info(f"  ✅ 헤더 정상: {hdr_ok}/{len(pf)}")
    log.info(f"  ✅ 날짜 범위 정상: {date_ok}/{len(pf)}")

    # 3. 매크로 8개
    mf = list(MACRO_DIR.glob("*.csv"))
    if len(mf) >= 8:
        log.info(f"  ✅ 4_macro/: {len(mf)}개 CSV (FRED 7 + FMP gold 1)")
    else:
        msg = f"4_macro/: {len(mf)}개 (8개 필요)"
        errors.append(msg)
        log.error(f"  ❌ {msg}")

    # 4. 벤치마크
    sp = BENCH_DIR / "SP500.csv"
    if sp.exists():
        lines = sum(1 for _ in open(sp)) - 1
        log.info(f"  ✅ SP500.csv: {lines}행")
        if lines < 2700:
            warns.append(f"SP500.csv {lines}행 (2700+ 기대)")
    else:
        errors.append("SP500.csv 없음")
        log.error("  ❌ SP500.csv 없음")

    # 5. 금 가격 단위 확인
    gp = MACRO_DIR / "gold.csv"
    if gp.exists():
        try:
            gdf = pd.read_csv(gp, nrows=3)
            v = float(gdf["Value"].iloc[0])
            if v > 500:
                log.info(f"  ✅ gold.csv: ${v:.0f}/oz (정상 범위)")
            else:
                warns.append(f"gold.csv 첫값 {v} (GLD ETF?)")
                log.warning(f"  ⚠️  gold.csv: 값 낮음 ${v} (GCUSD 기대)")
        except Exception:
            pass

    # 6. 재무제표 커버리지
    ff = list(FUND_DIR.glob("*.csv"))
    valid, short = 0, []
    for f in ff:
        try:
            df = pd.read_csv(f)
            cols = set(df.columns)
            cnt = len(df)
            if cnt >= 2:
                valid += 1
            if cnt < 5:
                short.append(f.stem)
            # 9개 컬럼 체크
            missing_cols = set(FUND_COLS) - cols
            if missing_cols:
                warns.append(f"{f.stem}: 누락 컬럼 {missing_cols}")
        except Exception:
            pass

    log.info(f"  ✅ 2_fundamental/: {len(ff)}개 CSV, 유효 {valid}개")
    if short:
        log.info(f"     5년 미만: {', '.join(short[:15])}{'...' if len(short)>15 else ''}")

    # 7. 컨센서스
    cf = list(SIGNAL_DIR.glob("*_consensus.csv"))
    log.info(f"  ✅ 3_signal/*_consensus: {len(cf)}개 CSV")
    if len(cf) < 40:
        warns.append(f"3_signal/*_consensus: {len(cf)}개 (40+ 기대)")

    # 8. 어닝서프라이즈
    sf = list(SIGNAL_DIR.glob("*_earnings.csv"))
    log.info(f"  ✅ 3_signal/*_earnings: {len(sf)}개 CSV")
    if len(sf) < 40:
        warns.append(f"3_signal/*_earnings: {len(sf)}개 (40+ 기대)")

    # 9. 공매도 잔고
    si = list(SIGNAL_DIR.glob("*_short.csv"))
    log.info(f"  ✅ 3_signal/*_short: {len(si)}개 CSV")
    if len(si) < 30:
        warns.append(f"3_signal/*_short: {len(si)}개 (FINRA 커버리지 제한 가능)")

    # 요약
    log.info("\n" + "-" * 60)
    if not errors:
        log.info("  🎉 필수 항목 전체 통과!")
    else:
        log.error(f"  ❌ 필수 오류 {len(errors)}건")
        for e in errors:
            log.error(f"     - {e}")
    if warns:
        log.warning(f"  ⚠️  경고 {len(warns)}건")
        for w in warns:
            log.warning(f"     - {w}")

    return len(errors) == 0


# ============================================================================
# 9. 메인
# ============================================================================

def main():
    t0 = time.time()

    log.info("🚀 Quant-Alpha v3.3 데이터 수집 스크립트 v3.0")
    log.info(f"   전략: FMP Premium 메인 → yfinance/FRED 폴백")
    log.info(f"   수집 기간: {START_DATE} ~ {END_DATE}")
    log.info(f"   종목 수: {len(TICKERS)}개")
    log.info(f"   FMP Key: {'설정됨' if FMP_API_KEY else '미설정'}")
    log.info(f"   FRED Key: {'설정됨' if FRED_API_KEY else '미설정'}")

    # API 연결 테스트
    log.info("\n  🔍 FMP API 연결 테스트...")
    test = fmp_get("profile", {"symbol": "AAPL"})
    if test:
        log.info("  ✅ FMP 연결 정상")
    else:
        log.warning("  ⚠️  FMP 연결 실패. 전체 yfinance 폴백으로 동작합니다.")

    makedirs()
    generate_universe()

    fetch_prices()           # [1/8] 주가: FMP → yf
    fetch_benchmark()        # [2/8] 벤치마크: FMP → yf
    fetch_macro()            # [3/8] 매크로: FRED + FMP금 → yf
    fetch_fundamentals()     # [4/8] 재무: FMP → yf
    fetch_consensus()        # [5/8] 컨센서스: FMP analyst-estimates
    fetch_earnings_surprise() # [6/8] 어닝서프라이즈: FMP earnings-calendar
    fetch_short_interest()   # [7/8] 공매도: FINRA Short Interest API
    ok = validate()          # [8/8] 검증

    elapsed = time.time() - t0
    log.info("\n" + "=" * 60)
    status = "✅ 완료!" if ok else "⚠️  일부 오류와 함께 완료"
    log.info(f"{status} (소요: {elapsed/60:.1f}분)")
    log.info("=" * 60)
    log.info(f"\n📁 결과: {BASE_DIR.resolve()}")
    log.info("   → ZIP 압축 후 파트장님에게 전달해 주세요.")


if __name__ == "__main__":
    main()
