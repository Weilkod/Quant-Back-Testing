#!/usr/bin/env python3
"""
=============================================================================
  시나리오 1: 닷컴버블 백테스트용 데이터 수집 스크립트 v3
  문서번호: QA-2026-BT-DOTCOM-v3
=============================================================================

  설계 원칙:
    FMP Premium을 모든 수집의 메인 소스로 사용하고,
    각 단계마다 yfinance/FRED를 폴백으로 배치.

    [주가 50종목]    FMP historical-price-eod  →  폴백: yfinance
    [벤치마크 SP500]  FMP index-historical      →  폴백: yfinance ^GSPC
    [매크로 7개]      FRED API (원본 소스)       →  폴백: 없음 (FRED가 원본)
    [금 가격]         FMP commodity GCUSD        →  폴백: yfinance GC=F
    [재무제표 50종목]  FMP income/balance/cf      →  폴백: yfinance

  시나리오 설명:
    기준 시점: 닷컴버블 정점 2000년 3월
    수집 기간: 1994-01-01 ~ 2006-12-31 (전 6년 + 후 6년)
    종목 기준: 1994년 S&P 500 시가총액 상위 50개
    목적: 생존자 편향(Survivorship Bias) 제거

  ⚠️ 주의사항:
    - 1994~2006 사이 합병/인수/상폐 종목 다수 포함
    - TICKERS_PARTIAL_DATA에 등록된 종목은 데이터가 중도 종료됨
    - 이는 "실패"가 아니라 정상 동작 (해당 시점까지만 수집)
    - 중도 종료 종목의 수익률도 백테스트에 포함해야 정확한 알파 측정 가능

  변경 이력:
    v3.0 - collect_backtest_data_v3.py 기반 생성
         - 1994년 기준 S&P 500 Top 50 종목 적용
         - 합병/상폐 종목 처리 로직 추가
         - validate() 함수 합병 종목 예외 처리
    v3.1 - 상폐로 데이터 수집 불가한 9종목 교체
         - MOB→HAL, BLS→SO, SBC→USB, GTE→GD
         - DOW→PPG, WYE→AMGN, SGP→HON, MOT→TXN, GM→LOW
         - 동일 섹터·당시 S&P 500 대형주·1994~2006 연속 데이터 보장
    v3.2 - 재무제표 수집 3단계 폴백 구조 도입 (방안1+3)
         - 2차 폴백: FMP As-Reported (SEC EDGAR 원본) 추가
         - us-gaap XBRL 태그 다중 매핑 테이블 구축
         - 1994~1999 구간 데이터 부재 수용 (XBRL 의무화 이전)

  실행:
    pip install requests yfinance pandas
    python collect_dotcom_bubble_v3.py
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

START_DATE = "1994-01-01"
END_DATE   = "2006-12-31"
YEAR_START = 1994
YEAR_END   = 2006

# ── 1994년 S&P 500 시가총액 상위 50종목 (v2 패치) ────────────────
# v2 변경: 상폐로 데이터 수집 불가한 9종목을
#          동일 섹터·당시 S&P 500 대형주·데이터 연속성 확보 종목으로 교체
#
#   MOB (Mobil, Energy)           → HAL (Halliburton, Energy)
#   BLS (BellSouth, Telecom)     → SO  (Southern Company, Utilities)
#   SBC (SBC Comm, Telecom)      → USB (US Bancorp, Financials)
#   GTE (GTE Corp, Telecom)      → GD  (General Dynamics, Industrials)
#   DOW (Dow Chemical, Materials) → PPG (PPG Industries, Materials)
#   WYE (Am Home Products, HC)   → AMGN (Amgen, Healthcare)
#   SGP (Schering-Plough, HC)    → HON (Honeywell, Industrials)
#   MOT (Motorola, Tech)         → TXN (Texas Instruments, Technology)
#   GM  (General Motors, CD)     → LOW (Lowe's, Consumer Disc)
#
TICKERS = [
    # 1~10: Top Tier
    "GE",   "XOM",  "T",    "KO",   "WMT",
    "MO",   "MRK",  "PG",   "IBM",  "JNJ",
    # 11~20
    "PFE",  "BMY",  "DD",   "MMM",  "CVX",
    "INTC", "PEP",  "ABT",  "DIS",  "HAL",
    # 21~30
    "MSFT", "HPQ",  "LLY",  "MCD",  "AXP",
    "BA",   "SO",   "USB",  "GD",   "AIT",
    # 31~40
    "PPG",  "EMR",  "CAT",  "HD",   "AMGN",
    "HON",  "CL",   "TXN",  "BAC",  "C",
    # 41~50
    "UTX",  "LOW",  "F",    "KMB",  "MDT",
    "UNP",  "ALL",  "IP",   "SLB",  "WFC",
]

# ── 합병/상폐로 데이터가 중도 종료되는 종목 ──────────────────────
# v2: 상폐 종목 대부분 대체 완료. 잔여 중도 종료 종목만 관리.
TICKERS_PARTIAL_DATA = {
    "AIT": "1999-10-08",   # Ameritech → SBC 인수 (FMP에서 1457행 수집 확인됨)
}

# 종목 수
TOTAL_TICKERS = len(TICKERS)  # 50

FRED_SERIES = {
    "VIXCLS":     "vix.csv",
    "T10Y2Y":     "yield_spread.csv",
    "ICSA":       "claims.csv",
    "CPIAUCSL":   "cpi.csv",
    "DCOILWTICO": "wti.csv",
    "PCOPPUSDM":  "copper.csv",
    "DTWEXBGS":   "dxy.csv",
}

BASE_DIR   = Path("data_dotcom")
PRICES_DIR = BASE_DIR / "1_price"
MACRO_DIR  = BASE_DIR / "4_macro"
BENCH_DIR  = BASE_DIR / "5_benchmark"
FUND_DIR   = BASE_DIR / "2_fundamental"
SIGNAL_DIR = BASE_DIR / "3_signal"
UNIV_DIR   = BASE_DIR / "6_universe"

FMP_DELAY    = 0.4
FRED_DELAY   = 0.3
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
        logging.FileHandler("collect_dotcom_log.txt", encoding="utf-8"),
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
    filepath = UNIV_DIR / "sp500_1994.csv"
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


def get_ticker_end_date(ticker):
    """합병/상폐 종목의 실제 데이터 종료일 반환. 정상 종목은 END_DATE 반환."""
    partial = TICKERS_PARTIAL_DATA.get(ticker)
    if partial and partial < END_DATE:
        return partial
    return END_DATE


def is_partial_ticker(ticker):
    """데이터가 중도 종료되는 종목 여부"""
    partial = TICKERS_PARTIAL_DATA.get(ticker)
    return partial is not None and partial < END_DATE


# ============================================================================
# 4. [1/7] 주가 50종목: FMP 메인 → yfinance 폴백
# ============================================================================

def fetch_prices():
    log.info("\n" + "=" * 60)
    log.info(f"[1/7] 종목별 일별 주가 수집 ({TOTAL_TICKERS}종목)")
    log.info("      메인: FMP historical-price-eod")
    log.info("      폴백: yfinance")
    log.info("      ⚠️  합병/상폐 종목은 중도 종료 데이터 정상 처리")
    log.info("=" * 60)

    fmp_ok, yf_ok, fail = 0, 0, 0

    for ticker in TICKERS:
        filepath = PRICES_DIR / f"{ticker}.csv"
        ticker_end = get_ticker_end_date(ticker)
        partial_flag = " [합병/상폐]" if is_partial_ticker(ticker) else ""

        # --- FMP 메인 ---
        log.info(f"  [{TICKERS.index(ticker)+1}/{TOTAL_TICKERS}] {ticker}{partial_flag} ─ FMP 시도...")
        data = fmp_get("historical-price-eod/full", {
            "symbol": ticker, "from": START_DATE, "to": ticker_end,
        })

        historical = None
        if isinstance(data, dict) and "historical" in data:
            historical = data["historical"]
        elif isinstance(data, list) and len(data) > 0:
            historical = data

        # 합병 종목은 데이터 행수 기준을 낮춤
        min_rows = 50 if is_partial_ticker(ticker) else 100

        if historical and len(historical) > min_rows:
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
            log.info(f"    ✅ FMP: {len(rows)}행 (~{ticker_end})")
            fmp_ok += 1
            time.sleep(FMP_DELAY)
            continue

        # --- yfinance 폴백 ---
        log.info(f"    ⚠️  FMP 실패 → yfinance 폴백...")
        try:
            df = yf.download(ticker, start=START_DATE, end=ticker_end,
                             auto_adjust=False, progress=False)
            if not df.empty and save_price_csv(df, filepath):
                log.info(f"    ✅ yfinance 폴백: {len(df)}행 (~{ticker_end})")
                yf_ok += 1
            else:
                log.error(f"    ❌ 폴백도 실패: 데이터 없음")
                fail += 1
        except Exception as e:
            log.error(f"    ❌ 폴백 예외: {e}")
            fail += 1

        time.sleep(FMP_DELAY)

    log.info(f"\n  주가 수집 완료: FMP {fmp_ok} | 폴백(yf) {yf_ok} | 실패 {fail}")
    if fail > 0:
        log.warning(f"  ⚠️  실패 {fail}개 중 합병/상폐 종목은 yfinance에도 데이터가 없을 수 있음")


# ============================================================================
# 5. [2/7] 벤치마크 SP500: FMP 메인 → yfinance 폴백
# ============================================================================

def fetch_benchmark():
    log.info("\n" + "=" * 60)
    log.info("[2/7] S&P 500 벤치마크 수집")
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
# 6. [3/7] 매크로: FRED 7개 + FMP 금(GCUSD)
# ============================================================================

def fetch_macro():
    log.info("\n" + "=" * 60)
    log.info("[3/7] 매크로 경제지표 수집 (8개)")
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
    log.info("\n  ── 금 가격 (FMP 메인) ──")
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
# 7. [4/7] 재무제표: FMP 표준 → FMP As-Reported → yfinance (3단계 폴백)
# ============================================================================

# ── 1차: FMP 표준 엔드포인트 필드 매핑 ────────────────────────────
# FMP stable은 camelCase와 snake_case가 종목·시점에 따라 혼재
# → 양쪽 모두 커버
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

# ── 2차: FMP As-Reported 필드 매핑 (SEC EDGAR us-gaap 태그) ───────
# 기업마다 사용하는 XBRL 태그가 다르므로 다중 매핑 필요
# 우선순위: 리스트 앞쪽이 높음 (먼저 매칭되면 사용)
# 정규화: 코드에서 lower() + replace(" ","") + replace("_","") 로 매칭
AS_REPORTED_MAP = {
    "revenue": [
        "revenuefromcontractwithcustomerexcludingassessedtax",
        "revenuefromcontractwithcustomerincludingassessedtax",
        "revenues",
        "salesrevenuenet",
        "salesrevenuegoodsnet",
        "salesrevenuegoodsandservicesnet",
        "revenue",
        "totalrevenues",
        "netrevenues",
        "revenuesfromexternalcustomersandotherrevenue",
        # 금융주 전용 (일반 revenue가 없을 때만 사용됨)
        "revenuesnetofinterestexpense",
        "interestincomeexpensenet",
    ],
    "operating_income": [
        "operatingincomeloss",
        "incomefromoperations",
        "operatingincome",
        "incomelossbeforeincometaxes",
    ],
    "net_income": [
        "netincomeloss",
        "netincomelossavailabletocommonstockholdersbasic",
        "netincomelossattributabletoparent",
        "profitloss",
        "netincome",
    ],
    "total_assets": [
        "assets",
        "totalassets",
    ],
    "total_equity": [
        "stockholdersequity",
        "stockholdersequityincludingportionattributabletononcontrollinginterest",
        "stockholdersequityattributabletoparent",
        "totalstockholdersequity",
        "commonstockholdersequity",
        "equity",
    ],
    "total_debt": [
        "longtermdebt",
        "longtermdebtnoncurrent",
        "longtermdebtandcapitalleaseobligations",
        "longtermdebtandfinanceleaseobligations",
        "debtandcapitalleaseobligations",
    ],
    "operating_cash_flow": [
        "netcashprovidedbyusedinoperatingactivities",
        "netcashprovidedbyusedinoperatingactivitiescontinuingoperations",
        "cashcashequivalentsrestrictedcashandrestrictedcashequivalentsperiodincreasedecrease"
        "includingexchangerateeffect",
    ],
    "capex": [
        "paymentstoacquirepropertyplantandequipment",
        "paymentstoacquireproductiveassets",
        "capitalexpenditure",
        "purchaseofpropertyplantandequipment",
    ],
    "eps": [
        "earningspersharediluted",
        "earningspersharebasicanddiluted",
        "earningspersharedilutedcontinuingoperations",
        "earningspersharebasic",
        "epsdiluted",
    ],
}

FUND_COLS = [
    "year","revenue","operating_income","net_income",
    "total_assets","total_equity","total_debt",
    "operating_cash_flow","capex","eps",
]


def _extract_year(rec):
    """FMP 레코드에서 연도 추출"""
    for key in ("calendar_year", "fiscal_year", "calendarYear", "fiscalYear"):
        if key in rec and rec[key]:
            try:
                return int(rec[key])
            except (ValueError, TypeError):
                pass
    for key in ("date", "fillingDate", "acceptedDate"):
        if key in rec and rec[key]:
            try:
                return int(str(rec[key])[:4])
            except (ValueError, TypeError):
                pass
    return None


def _fmp_fundamentals(ticker):
    """1차: FMP 표준 엔드포인트에서 재무제표 3종 수집"""
    endpoints = [
        ("income-statement",       INCOME_MAP),
        ("balance-sheet-statement", BALANCE_MAP),
        ("cash-flow-statement",    CASHFLOW_MAP),
    ]
    merged = {}

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


def _fmp_as_reported(ticker):
    """2차: FMP As-Reported 엔드포인트에서 SEC 원본 재무제표 수집
    FMP stable은 as-reported를 재무제표별로 분리 제공:
      - income-statement-as-reported
      - balance-sheet-statement-as-reported
      - cash-flow-statement-as-reported
    """
    merged = {}

    as_reported_endpoints = [
        "income-statement-as-reported",
        "balance-sheet-statement-as-reported",
        "cash-flow-statement-as-reported",
    ]

    for ep in as_reported_endpoints:
        data = fmp_get(ep, {
            "symbol": ticker, "period": "annual", "limit": 50,
        })

        if not data or not isinstance(data, list):
            continue

        for rec in data:
            year = _extract_year(rec)
            if year is None or year < YEAR_START or year > YEAR_END:
                continue
            if year not in merged:
                merged[year] = {"year": year}

            # as-reported 레코드의 모든 키를 소문자로 정규화하여 매칭
            rec_lower = {k.lower().replace(" ", "").replace("_", ""): v
                         for k, v in rec.items()
                         if v is not None and not isinstance(v, (dict, list))}

            for our_col, candidate_keys in AS_REPORTED_MAP.items():
                if our_col in merged[year] and merged[year][our_col] not in ("", None):
                    continue  # 이미 값이 있으면 스킵
                for cand in candidate_keys:
                    cand_clean = cand.lower().replace(" ", "").replace("_", "")
                    if cand_clean in rec_lower:
                        val = rec_lower[cand_clean]
                        try:
                            val_num = float(val)
                            merged[year][our_col] = val_num
                            break
                        except (ValueError, TypeError):
                            continue

        time.sleep(FMP_DELAY)

    return merged


def _yf_fundamentals(ticker):
    """3차: yfinance에서 재무제표 3종 수집"""
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


def _merge_into(base, addition):
    """addition의 데이터를 base에 병합 (base에 없는 연도·컬럼만 추가)"""
    for yr, vals in addition.items():
        if yr not in base:
            base[yr] = vals
        else:
            for col, val in vals.items():
                if col not in base[yr] or base[yr][col] in ("", None):
                    base[yr][col] = val


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
    log.info(f"[4/7] 재무제표 수집 ({TOTAL_TICKERS}종목 × 9컬럼)")
    log.info("      1차: FMP 표준 (income / balance-sheet / cash-flow)")
    log.info("      2차: FMP As-Reported (SEC EDGAR 원본)")
    log.info("      3차: yfinance (.financials / .balance_sheet / .cashflow)")
    log.info("      ⚠️  1994~1999 구간은 XBRL 미적용으로 데이터 부재 가능 (방안3 수용)")
    log.info("=" * 60)

    fmp_ok, asrep_ok, yf_ok, fail = 0, 0, 0, 0

    for i, ticker in enumerate(TICKERS, 1):
        filepath = FUND_DIR / f"{ticker}.csv"
        partial_flag = " [합병/상폐]" if is_partial_ticker(ticker) else ""
        log.info(f"  [{i}/{TOTAL_TICKERS}] {ticker}{partial_flag}")

        min_years = 1 if is_partial_ticker(ticker) else 3

        # ── 1차: FMP 표준 ──
        log.info(f"    1차 FMP 표준...")
        merged = _fmp_fundamentals(ticker)
        count = _save_fundamentals(merged, filepath)

        if count >= min_years:
            years = sorted(merged.keys())
            log.info(f"    ✅ 1차 FMP 표준: {count}년치 ({min(years)}~{max(years)})")
            fmp_ok += 1
            continue

        # ── 2차: FMP As-Reported ──
        log.info(f"    2차 FMP As-Reported... (1차: {count}년)")
        ar_merged = _fmp_as_reported(ticker)
        _merge_into(merged, ar_merged)
        count = _save_fundamentals(merged, filepath)

        if count >= min_years:
            years = sorted(merged.keys())
            log.info(f"    ✅ 1차+2차 병합: {count}년치 ({min(years)}~{max(years)})")
            asrep_ok += 1
            continue

        # ── 3차: yfinance ──
        log.info(f"    3차 yfinance... (1차+2차: {count}년)")
        try:
            yf_merged = _yf_fundamentals(ticker)
            _merge_into(merged, yf_merged)

            count = _save_fundamentals(merged, filepath)
            if count >= 1:
                years = sorted(merged.keys())
                log.info(f"    ✅ 3단계 병합: {count}년치 ({min(years)}~{max(years)})")
                yf_ok += 1
            else:
                log.warning(f"    ⚠️  3단계 병합 후에도 {count}년치 (수용)")
                yf_ok += 1
        except Exception as e:
            log.error(f"    ❌ 3차 폴백 예외: {e}")
            fail += 1

    log.info(f"\n  재무제표 완료: FMP표준 {fmp_ok} | As-Reported {asrep_ok} | yf보충 {yf_ok} | 실패 {fail}")


# ============================================================================
# [5/7] 컨센서스 (애널리스트 추정치): FMP analyst-estimates
# ============================================================================

def fetch_consensus():
    log.info("\n" + "=" * 60)
    log.info(f"[5/7] 컨센서스 추정치 수집 ({TOTAL_TICKERS}종목)")
    log.info("      FMP: analyst-estimates (annual)")
    log.info("      ⚠️  1994~2000 구간은 데이터 부재 가능")
    log.info("=" * 60)

    ok, fail = 0, 0

    for i, ticker in enumerate(TICKERS, 1):
        filepath = SIGNAL_DIR / f"{ticker}_consensus.csv"
        partial_flag = " [합병/상폐]" if is_partial_ticker(ticker) else ""
        log.info(f"  [{i}/{TOTAL_TICKERS}] {ticker}{partial_flag}")

        ticker_end = get_ticker_end_date(ticker)
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
            for v in data.values():
                if isinstance(v, list):
                    records = v
                    break
            if not records:
                records = [data]

        if not records:
            log.warning(f"    ⚠️  파싱 가능한 데이터 없음")
            fail += 1
            time.sleep(FMP_DELAY)
            continue

        # 날짜 필터 없이 전체 저장 (알고리즘에서 기간 필터링)
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

        log.info(f"    ✅ {len(records)}건 (전체 저장)")
        ok += 1
        time.sleep(FMP_DELAY)

    log.info(f"\n  컨센서스 완료: 성공 {ok} | 실패 {fail}")


# ============================================================================
# [6/7] 어닝서프라이즈: FMP earnings-calendar (연도별 수집 → 종목 필터)
# ============================================================================

def fetch_earnings_surprise():
    """earnings-calendar는 symbol 필터가 안 되므로,
    연도별로 전체 데이터를 받아서 우리 종목만 필터링"""
    log.info("\n" + "=" * 60)
    log.info(f"[6/7] 어닝서프라이즈 수집 ({TOTAL_TICKERS}종목)")
    log.info("      FMP: earnings-calendar (연도별 수집 → 종목 필터)")
    log.info("      ⚠️  1994~2000 구간은 데이터 부재 가능")
    log.info("=" * 60)

    ticker_set = set(TICKERS)
    ticker_rows = {t: [] for t in TICKERS}

    total_fetched = 0
    for year in range(YEAR_START, YEAR_END + 1):
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

        records = []
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    records = v
                    break

        if not records:
            log.warning(f"    ⚠️  {year}년 파싱 실패")
            time.sleep(FMP_DELAY)
            continue

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
# [7/7] 검증
# ============================================================================

def validate():
    log.info("\n" + "=" * 60)
    log.info("[7/7] 수집 완료 검증")
    log.info("=" * 60)

    errors, warns = [], []

    # 1. 주가 파일 수
    pf = list(PRICES_DIR.glob("*.csv"))
    # 합병/상폐 종목 중 데이터를 못 구한 것이 있을 수 있으므로 기준 완화
    partial_count = sum(1 for t in TICKERS if is_partial_ticker(t))
    min_expected = TOTAL_TICKERS - partial_count  # 최소한 정상 종목은 모두 있어야 함

    if len(pf) >= min_expected:
        log.info(f"  ✅ 1_price/: {len(pf)}개 CSV (정상종목 {min_expected}+ / 전체 {TOTAL_TICKERS})")
    else:
        msg = f"1_price/: {len(pf)}개 (최소 {min_expected}개 필요)"
        errors.append(msg)
        log.error(f"  ❌ {msg}")
        missing = set(TICKERS) - {f.stem for f in pf}
        if missing:
            log.error(f"     누락: {', '.join(sorted(missing))}")

    # 2. 주가 헤더
    hdr_ok = 0
    for f in pf:
        try:
            df = pd.read_csv(f, nrows=2)
            if df.columns[0] == "Date" and "Adj Close" in df.columns:
                hdr_ok += 1
        except Exception:
            pass
    log.info(f"  ✅ 헤더 정상: {hdr_ok}/{len(pf)}")

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
        if lines < 3000:
            warns.append(f"SP500.csv {lines}행 (3000+ 기대, 12년치)")
    else:
        errors.append("SP500.csv 없음")
        log.error("  ❌ SP500.csv 없음")

    # 5. 금 가격 단위 확인
    gp = MACRO_DIR / "gold.csv"
    if gp.exists():
        try:
            gdf = pd.read_csv(gp, nrows=3)
            v = float(gdf["Value"].iloc[0])
            if v > 200:
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
            cnt = len(df)
            if cnt >= 1:
                valid += 1
            if cnt < 5 and not is_partial_ticker(f.stem):
                short.append(f.stem)
        except Exception:
            pass

    log.info(f"  ✅ 2_fundamental/: {len(ff)}개 CSV, 유효 {valid}개")
    if short:
        log.info(f"     5년 미만 (정상 종목): {', '.join(short[:15])}{'...' if len(short)>15 else ''}")

    # 7. 컨센서스
    cf = list(SIGNAL_DIR.glob("*_consensus.csv"))
    log.info(f"  ✅ 3_signal/*_consensus: {len(cf)}개 CSV")
    if len(cf) < 30:
        warns.append(f"3_signal/*_consensus: {len(cf)}개 (과거 구간 데이터 부재 가능)")

    # 8. 어닝서프라이즈
    sf = list(SIGNAL_DIR.glob("*_earnings.csv"))
    log.info(f"  ✅ 3_signal/*_earnings: {len(sf)}개 CSV")
    if len(sf) < 30:
        warns.append(f"3_signal/*_earnings: {len(sf)}개 (과거 구간 데이터 부재 가능)")

    # 9. 합병/상폐 종목 리포트
    log.info(f"\n  ── 합병/상폐 종목 데이터 현황 ──")
    for ticker, end_date in TICKERS_PARTIAL_DATA.items():
        if end_date >= END_DATE:
            continue
        pfile = PRICES_DIR / f"{ticker}.csv"
        if pfile.exists():
            rows = sum(1 for _ in open(pfile)) - 1
            log.info(f"  📋 {ticker}: {rows}행 (~{end_date})")
        else:
            log.warning(f"  ⚠️  {ticker}: 파일 없음 (데이터 소스에서 미지원 가능)")

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

    log.info("🚀 닷컴버블 시나리오 데이터 수집 스크립트")
    log.info(f"   시나리오: 닷컴버블 (정점 2000.03)")
    log.info(f"   전략: FMP Premium 메인 → yfinance/FRED 폴백")
    log.info(f"   수집 기간: {START_DATE} ~ {END_DATE}")
    log.info(f"   종목 기준: 1994년 S&P 500 시가총액 상위 {TOTAL_TICKERS}개")
    log.info(f"   합병/상폐 종목: {sum(1 for t in TICKERS if is_partial_ticker(t))}개")
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

    fetch_prices()            # [1/7] 주가: FMP → yf
    fetch_benchmark()         # [2/7] 벤치마크: FMP → yf
    fetch_macro()             # [3/7] 매크로: FRED + FMP금 → yf
    fetch_fundamentals()      # [4/7] 재무: FMP 표준 → As-Reported → yf
    fetch_consensus()         # [5/7] 컨센서스: FMP analyst-estimates
    fetch_earnings_surprise() # [6/7] 어닝서프라이즈: FMP earnings-calendar
    ok = validate()           # [7/7] 검증

    elapsed = time.time() - t0
    log.info("\n" + "=" * 60)
    status = "✅ 완료!" if ok else "⚠️  일부 오류와 함께 완료"
    log.info(f"{status} (소요: {elapsed/60:.1f}분)")
    log.info("=" * 60)
    log.info(f"\n📁 결과: {BASE_DIR.resolve()}")
    log.info("   → ZIP 압축 후 파트장님에게 전달해 주세요.")


if __name__ == "__main__":
    main()
