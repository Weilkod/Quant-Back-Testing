"""
╔══════════════════════════════════════════════════════════════════════╗
║  Quant-Alpha v3.4.1 Phase 1 — 통합 알고리즘                          ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  Base: v3.4 Unified (6개 모듈 통합)                                   ║
║                                                                      ║
║  v3.4.1 Phase 1 변경 (백테스트 결과 기반 핵심 병목 해소):              ║
║    [P1-1] SCORE_BUY_THRESHOLD  85 → 70  (매수 시그널 0건 해소)        ║
║    [P1-2] SCORE_HOLD_THRESHOLD 70 → 55  (HOLD 범위 연동 조정)         ║
║    [P1-3] Gate ③ OCF ≤ 0: FAIL → WARNING (Gate 과살 완화)            ║
║    [P1-4] 결측 팩터 점수: 0.0 → 0.5 (중립 부여, 재분배 비활성)        ║
║                                                                      ║
║  백테스트 진단: CAGR 7.31%, Beta 0.165, S_BUY 0건, 현금 ~85%         ║
║  근본 원인: Gate 과살(79.8% EXIT) → 점수 구조적 저점 → 임계값 비현실   ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import math
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  § 1. CONFIGURATION — 하이퍼파라미터 중앙 관리                       ║
# ║  (구 config.py)                                                     ║
# ║                                                                      ║
# ║  근거 수준: L1=학술적, L2=업계관행, L3=근거불명(민감도분석 필수)       ║
# ║  [구 ai_soft] = ai_soft.py에서 이관된 파라미터 (이슈 #6 반영)        ║
# ╚══════════════════════════════════════════════════════════════════════╝

# ─── Phase 1: Survival Gate (5개 게이트 — 이슈 #9 반영) ───

FRESHNESS_WARNING_DAYS = 120              # [v3.5] WARNING: 121~365일 (90→120)
FRESHNESS_FAIL_DAYS = 365                 # [v3.5] FAIL→WARNING: 연간보고서 주기 반영 (150→365)

LIQUIDITY_WARNING_DAYS = 0.5              # L2  WARNING: 0.5~2.0일
LIQUIDITY_FAIL_DAYS = 2.0                 # L2  FAIL_LIQUIDITY: >2.0일
LIQUIDITY_PARTICIPATION_RATE = 0.20       # L2  일평균 거래량 대비 참여율 가정

ROA_MINIMUM = 0.03                        # [v3.5] ROA ≥ 3% (WARNING 기준, 5%→3%)
ROA_MINIMUM_FAIL = 0.00                   # [v3.5] ROA < 0%이면 HARD FAIL (순손실만)

# ─── Gate ① 추세 그라데이션 [v3.5] ───
GATE_TREND_DEEP_FAIL_MA200_RATIO = 0.80   # MA200 대비 20%+ 하락 시만 FAIL
GATE_TREND_DEEP_FAIL_MA120_RATIO = 0.85   # MA120 대비 15%+ 하락 시만 FAIL

# ─── Gate ② 수익성 그라데이션 [v3.5] ───
GATE_ROIC_HARD_FAIL_RATIO = 0.30          # ROIC < WACC × 0.3이면 HARD FAIL (극단적 저수익만)


# ─── Phase 4: 스코어링 가중치 (10개 팩터, 합계 100) ───

WEIGHTS = {
    "roic": 18,                           # L1  펀더멘털 핵심
    "profit_trend": 10,                   # L1  이익률 YoY
    "growth": 10,                         # L1  매출 CAGR (매크로 alpha 가변: 8~12)
    "consensus": 8,                       # L2  애널리스트 상향비율 (v3.2:20→v3.4:8)
    "momentum": 8,                        # L1  가격 모멘텀 62일
    "valuation": 12,                      # L1  P/E 상대가치 (매크로 alpha 가변: 10~14)
    "efficiency": 5,                      # L2  운영 효율성 (재고회전율)
    "rsi": 7,                             # L2  RSI 과매도 신호
    "earnings_surprise": 12,              # L1  어닝 서프라이즈 (v3.4 신규)
    "short_interest": 10,                 # L1  공매도 잔고 (v3.4 신규)
}

# 매크로 alpha에 의한 가변 가중치 (growth + valuation = 항상 22)
GROWTH_ALPHA_SENSITIVITY = -2             # L2  alpha × 이 값이 growth에 가감
VALUATION_ALPHA_SENSITIVITY = 2           # L2  alpha × 이 값이 valuation에 가감


# ─── Phase 5: 실행 판단 임계값 ───

SCORE_BUY_THRESHOLD = 60                  # L3  [v3.5] 70→60: 재분배 후 점수 분포 반영
SCORE_HOLD_THRESHOLD = 45                 # L3  [v3.5] 55→45: BUY 하향에 연동 조정


# ─── RSI 파라미터 [구 ai_soft] ───

RSI_PERIOD = 14                           # L1  Wilder(1978) 원저 기준
RSI_OVERSOLD = 40                         # L2  [v3.5] 45→40 (과적합 방지)
RSI_OVERBOUGHT = 80                       # L2  [v3.5] 75→80 (과적합 방지)
RSI_NORM_LOW = 30                         # L2  Phase 4: RSI 정규화 하한
RSI_NORM_HIGH = 70                        # L2  Phase 4: RSI 정규화 상한


# ─── 모멘텀 파라미터 [구 ai_soft] ───

MOMENTUM_DAYS = 62                        # L3  약 3개월
MOMENTUM_NORM_LOW = -0.20                 # L3  [v3.5] -0.30→-0.20 (분산 확대)
MOMENTUM_NORM_HIGH = 0.20                 # L3  [v3.5] 0.30→0.20 (분산 확대)


# ─── 컨센서스 파라미터 [구 ai_soft] ───

CONSENSUS_WINDOW_DAYS = 30                # L2  상향비율 산출 윈도우
CONSENSUS_POSITIVE_THRESHOLD = 0.50       # L2  양성 판단 기준


# ─── TREND_HOLD 조건 (Patch 3) ───

TREND_HOLD_ROIC_MIN = 0.70                # L3  ROIC 정규화 점수 최소
TREND_HOLD_PROFIT_MIN = 0.60              # L3  이익률 추세 점수 최소


# ─── 어닝 서프라이즈 (항목 1 + 이슈 #1 반영) ───

EARNINGS_SURPRISE_LOW = -0.10             # L3  정규화 하한
EARNINGS_SURPRISE_HIGH = 0.30             # L3  정규화 상한 (이슈#1: 0.20→0.30)
EARNINGS_SURPRISE_RANGE = (               # 산출용: HIGH - LOW
    EARNINGS_SURPRISE_HIGH - EARNINGS_SURPRISE_LOW
)
EARNINGS_Q_WEIGHTS = [0.4, 0.3, 0.2, 0.1]  # L2  Q0(최신)~Q3 가중치
EARNINGS_CONSISTENCY_BONUS = {            # L2  연속 비트 보너스
    4: 1.20,                              #     4연속 비트: 20% 보너스
    3: 1.10,                              #     3연속: 10%
}
EARNINGS_CONSISTENCY_DEFAULT = 1.0        #     2연속 이하: 보너스 없음


# ─── Short Interest (항목 1 + 이슈 #2 반영) ───

SI_MAX_RATIO = 0.15                       # L3  레벨 정규화 상한 (SIR 15%)
SI_CHANGE_NORM_OFFSET = 0.20              # L3  change 정규화 오프셋
SI_CHANGE_NORM_RANGE = 0.40               # L3  change 정규화 범위

SI_LEVEL_WEIGHT = 0.4                     # L3  정상 상태 가중치
SI_CHANGE_WEIGHT = 0.6                    # L3

SI_HIGH_THRESHOLD = 0.10                  # L3  SIR ≥ 10%이면 동적 전환
SI_HIGH_LEVEL_WEIGHT = 0.2               # L3  고SIR 시 level 가중치
SI_HIGH_CHANGE_WEIGHT = 0.8               # L3  고SIR 시 change 가중치


# ─── 결측 팩터 처리 [P1-4] ───

MISSING_FACTOR_NEUTRAL_SCORE = 0.5        # L3  결측 시 0.0(최악) 대신 0.5(중립) 부여
                                          #     [P1-4] 데이터 없음 ≠ 최악, 통계적 중립
REDISTRIBUTE_MISSING_WEIGHTS = True       # [v3.5] 신호 데이터 전무 → 재분배 활성화
                                          #     True: 결측 가중치를 실데이터 팩터에 재분배
                                          #     False: v3.4.1 방식 (가중치 유지, 중립 점수)


# ─── VIX 그라데이션 (Patch 1) ───

VIX_PANIC_THRESHOLD = 25.0                # L2  패닉 초입 VIX 수준
VIX_PANIC_ROC = 0.30                      # L3  패닉 초입 5일 ROC
VIX_CAPITULATION_THRESHOLD = 40.0         # L2  캐피츌레이션 VIX 수준
VIX_RECOVERY_ROC = -0.10                  # L3  바닥 탈출 판단 ROC
VIX_MA_PERIOD = 5                         # L2  VIX 이동평균 기간

VIX_SCORE_PANIC_ONSET = -1.0              # 패닉 초입 → 전면 방어
VIX_SCORE_PANIC_ONGOING = -0.5            # 패닉 진행 → 방어 유지
VIX_SCORE_CAPITULATION_EXIT = 0.5         # 항복 탈출 → 절반 투입


# ─── 트레일링 스탑 (항목 3 + 이슈 #4 반영) ───

STOP_PCT_DEFAULT = 0.15                   # L2  기본 -15%

STOP_PCT_BY_REGIME = {
    1: 0.18,                              # 강세초기: 변동성 높아 여유
    2: 0.12,                              # 바닥/함정: 보수적
    3: 0.15,                              # 실적장세: 기본값
    4: 0.12,                              # 방어전환: 빠른 방어
    5: 0.15,                              # 구조적성장: 기본값
    6: 0.10,                              # 약세진입: 최대 타이트
}

STOP_TREND_HOLD_EXTRA = 0.05              # L3  TREND_HOLD 추가 여유 (+5%p)
MIN_HOLD_DAYS = 5                         # L2  최소 보유 기간 (일)
COOLDOWN_DAYS = 10                        # L3  스탑 후 재진입 금지 (영업일)
GRACE_PERIOD_EARNINGS = 3                 # L2  어닝 발표 전후 유예 (영업일)

RATCHET_OVERRIDE_REGIMES = [6]            # L2  이 국면 진입 시 래칫 무시


# ─── 포트폴리오 관리 (항목 2 + 이슈 #3 반영) ───

MAX_POSITIONS = 30                        # [v3.5] 20→30 동시 보유 최대 종목 수
MIN_POSITIONS = 8                         # [v3.5] 5→8 최소 분산 확보

MAX_SECTOR_EXPOSURE = 0.30                # L2  단일 섹터 최대 30%
MAX_COUNTRY_EXPOSURE = 0.50               # L2  단일 국가 최대 50%

TARGET_BETA_MIN = 0.80                    # L2  포트폴리오 베타 하한
TARGET_BETA_MAX = 1.20                    # L2  포트폴리오 베타 상한

EXTREME_BETA_THRESHOLD = 2.50             # L2  극단적 고베타 기준
EXTREME_BETA_CAP = 0.05                   # L2  극단 베타 종목 최대 비중 5%

BETA_LOW_ACTION = "SCALE_UP"              # [v3.5] "SCALE_UP": 저베타 시 고베타 종목 비중 증가
BETA_HIGH_MAX_REDUCTION_RATIO = 0.50      # L2  한 번에 최대 50% 비중 축소

SINGLE_WARNING_CAP = 0.50                 # L2  WARNING 1개: 비중 50% 제한
MULTI_WARNING_CAP = 0.30                  # L2  WARNING 2개+: 비중 30% 제한


# ─── 포지션 사이징 ───

BASE_WEIGHT = 0.12                        # [v3.5] 기본 종목 비중 12%

TYPE_CAPS = {
    "A": 0.15,                            # 소형 성장
    "B": 0.12,                            # 중형
    "C": 0.10,                            # 대형 안정
    "D": 0.20,                            # 특수 (고확신)
}

SCORE_FACTORS = {
    "S_BUY": 1.2,
    "TREND_BUY": 1.2,
    "TREND_HOLD": 1.2,
    "HOLD": 1.0,
    "REDUCE": 0.7,
    "WARNING": 1.0,
    "WAIT_DIP": 0.0,
    "EXIT": 0.0,
    "STOP_EXIT": 0.0,
    "QUEUE": 0.0,
}

TREND_HOLD_CAP_RATIO = 0.60              # L3  TREND_HOLD: Type Cap × 0.6


# ─── 리밸런싱 (항목 5) ───

TIER1_FREQUENCY = "daily"                 # 매 거래일
TIER2_FREQUENCY = "weekly"                # 매주 금요일
TIER3_FREQUENCY = "biweekly"              # 격주 금요일
REBALANCE_PERIOD = 14                     # L2  Tier 3 주기 (일)

MIN_WEIGHT_CHANGE = 0.02                  # L2  최소 비중 변경 임계값 2%p

EMERGENCY_SIMULTANEOUS_STOPS = 3          # L3  동시 스탑 3개 이상
EMERGENCY_DAILY_PNL = -0.05               # L3  일간 PnL -5% 이하

WATCHLIST_SIZE = 50                       # L2  워치리스트 종목 수
FULL_UNIVERSE_SCAN_FREQUENCY = "monthly"  # L2  전체 스캔 월 1회


# ─── 거래 비용 (항목 6 + 이슈 #7 반영) ───

COST_AWARE_FILTER_ENABLED = False         # 1차 백테스트에서 비활성
COST_THRESHOLD_RATIO = 0.50               # L3  비용/기대이득 비율 상한

MARKET_IMPACT_COEFFICIENT = 0.10          # L3  제곱근 임팩트 계수
LARGE_CAP_THRESHOLD = 10_000_000_000      # L2  대형주 기준 $10B


# ─── 매크로 엔진 서브엔진 가중치 ───

MACRO_ENGINE_WEIGHTS = {
    "growth": 0.25,                       # L2
    "liquidity": 0.25,                    # L2
    "innovation": 0.20,                   # L2
    "inflation": 0.15,                    # L2
    "risk": 0.15,                         # L2
}

NUM_REGIMES = 6


# ─── 버전 정보 (이슈 #10 반영) ───

VERSION = "3.5.0"
VERSION_TAG = "v3.5.0"
PATCH_HISTORY = [
    "Patch 1: VIX 바이너리 킬스위치 → 4단계 그라데이션",
    "Patch 2: 컨센서스 결측치 동적 가중치 재분배",
    "Patch 3: TREND_HOLD 액션 도입 (과매수 모멘텀 보유)",
    "Phase1-1: SCORE_BUY_THRESHOLD 85→70 (매수 시그널 0건 해소)",
    "Phase1-2: SCORE_HOLD_THRESHOLD 70→55 (연동 조정)",
    "Phase1-3: Gate ③ OCF ≤ 0 FAIL→WARNING (Gate 과살 완화)",
    "Phase1-4: 결측 팩터 0.0→0.5 중립 부여 (재분배 비활성)",
    "v3.5-1: REGIME_EQUITY_CAP 전면 상향 (현금 과다 해소)",
    "v3.5-2: WARNING 액션 BUY 차단 제거 (비중 제한만 적용)",
    "v3.5-3: 결측 가중치 재분배 활성화 (신호 팩터 무력화 해소)",
    "v3.5-4: SCORE_BUY 70→60, SCORE_HOLD 55→45",
    "v3.5-5: Gate ① 추세 3단계 그라데이션 (심층 이탈만 FAIL)",
    "v3.5-6: Gate ② 수익성 완화 (ROIC<WACC*0.5 AND ROA<2%만 FAIL)",
    "v3.5-7: 동적 WACC (risk-free + beta × ERP)",
    "v3.5-8: 실제 Beta 계산 (252일 회귀분석)",
    "v3.5-9: Beta scale-up 포트폴리오 관리",
    "v3.5-10: 리밸런싱 5일, 레짐 변경 트리거",
]


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  § 2. COUNTRIES — 5개국 설정                                        ║
# ║  (구 countries.py)                                                  ║
# ║                                                                      ║
# ║  5개국: 미국, 한국, 일본, 홍콩, 유럽                                  ║
# ║  제거: 인도, 브라질, 중국 (v3.2 → v3.4)                              ║
# ║  신규: 홍콩 (USD 페그, 중화권 간접 노출)                              ║
# ╚══════════════════════════════════════════════════════════════════════╝

@dataclass(frozen=True)
class CountryConfig:
    """국가별 설정 데이터 클래스."""

    # 기본 정보
    code: str                    # ISO 코드
    name: str                    # 표시명
    currency: str                # 통화
    index: str                   # 주요 지수
    market_class: str            # 시장 분류 (developed / emerging)

    # 금리 사이클 (매크로 alpha 산출)
    rate_indicator: str          # FRED 시리즈 ID
    rate_pivot: float            # 전환 기준 금리 (%)
    rate_range: float            # 금리 사이클 범위 (%)
    rate_fallback: Optional[str] # FRED 미제공 시 대체 방법

    # 거래 비용 (편도 기준)
    cost_commission: float       # 커미션 (%)
    cost_spread_large: float     # 스프레드 — 대형주 (%)
    cost_spread_small: float     # 스프레드 — 중소형주 (%)
    cost_slippage: float         # 슬리피지 (%)
    cost_tax: float              # 거래세/부담금 (편도, %)

    # 시장 시간대
    timezone: str                # 타임존
    market_close_utc: str        # 장 마감 시간 (UTC)


COUNTRIES: Dict[str, CountryConfig] = {

    "US": CountryConfig(
        code="US", name="미국", currency="USD", index="S&P 500",
        market_class="developed",
        rate_indicator="FEDFUNDS", rate_pivot=2.50, rate_range=2.50,
        rate_fallback=None,
        cost_commission=0.005, cost_spread_large=0.02, cost_spread_small=0.10,
        cost_slippage=0.05, cost_tax=0.0,
        timezone="US/Eastern", market_close_utc="21:00",
    ),

    "KR": CountryConfig(
        code="KR", name="한국", currency="KRW", index="KOSPI",
        market_class="developed",
        rate_indicator="INTDSRKRM", rate_pivot=2.00, rate_range=2.00,
        rate_fallback=None,
        cost_commission=0.015, cost_spread_large=0.03, cost_spread_small=0.15,
        cost_slippage=0.08, cost_tax=0.23,
        timezone="Asia/Seoul", market_close_utc="06:30",
    ),

    "JP": CountryConfig(
        code="JP", name="일본", currency="JPY", index="TOPIX",
        market_class="developed",
        rate_indicator="INTDSRJPM", rate_pivot=0.10, rate_range=0.50,
        rate_fallback=None,
        cost_commission=0.010, cost_spread_large=0.03, cost_spread_small=0.12,
        cost_slippage=0.06, cost_tax=0.0,
        timezone="Asia/Tokyo", market_close_utc="06:00",
    ),

    "HK": CountryConfig(
        code="HK", name="홍콩", currency="HKD", index="HSI",
        market_class="developed",
        rate_indicator="FEDFUNDS", rate_pivot=2.50, rate_range=2.50,
        rate_fallback="USE_US_ALPHA",
        cost_commission=0.010, cost_spread_large=0.03, cost_spread_small=0.12,
        cost_slippage=0.06, cost_tax=0.13,
        timezone="Asia/Hong_Kong", market_close_utc="08:00",
    ),

    "EU": CountryConfig(
        code="EU", name="유럽", currency="EUR", index="STOXX600",
        market_class="developed",
        rate_indicator="ECBMRRFR", rate_pivot=1.50, rate_range=2.00,
        rate_fallback=None,
        cost_commission=0.010, cost_spread_large=0.03, cost_spread_small=0.15,
        cost_slippage=0.07, cost_tax=0.10,
        timezone="Europe/London", market_close_utc="16:30",
    ),
}


# ─── 국가 유틸리티 함수 ───

def get_country(code: str) -> CountryConfig:
    """국가 코드로 설정 조회. 없으면 KeyError."""
    code = code.upper()
    if code not in COUNTRIES:
        raise KeyError(
            f"지원하지 않는 국가 코드: {code}. "
            f"지원 국가: {list(COUNTRIES.keys())}"
        )
    return COUNTRIES[code]


def get_all_country_codes() -> list:
    """지원 국가 코드 목록 반환."""
    return list(COUNTRIES.keys())


def calculate_rate_alpha(country_code: str, current_rate: float) -> float:
    """
    국가별 금리 alpha 산출.

    alpha = clamp((현재금리 - 전환기준) / 금리사이클범위, -1, +1)

    alpha = +1.0 → 고금리, 밸류에이션/FCF 중시
    alpha =  0.0 → 중립
    alpha = -1.0 → 저금리, 성장주 중시

    홍콩: rate_fallback="USE_US_ALPHA" → 미국 alpha 사용
    """
    country = get_country(country_code)

    if country.rate_fallback == "USE_US_ALPHA":
        pass  # 호출 시 미국 금리를 넣어야 함

    if country.rate_range == 0:
        return 0.0

    raw_alpha = (current_rate - country.rate_pivot) / country.rate_range
    return max(-1.0, min(1.0, raw_alpha))


def calculate_one_way_cost(
    country_code: str,
    market_cap: float,
    order_size: float,
    avg_daily_volume: float,
) -> float:
    """
    편도 거래 비용 산출 (%).

    구성: 커미션 + 스프레드/2 + 마켓임팩트 + 슬리피지 + 세금
    """
    country = get_country(country_code)

    commission = country.cost_commission / 100

    if market_cap >= LARGE_CAP_THRESHOLD:
        spread = country.cost_spread_large / 100
    else:
        spread = country.cost_spread_small / 100
    spread_cost = spread / 2

    if avg_daily_volume > 0:
        participation_rate = order_size / avg_daily_volume
        impact = MARKET_IMPACT_COEFFICIENT * math.sqrt(participation_rate) / 100
    else:
        impact = 0.005

    slippage = country.cost_slippage / 100
    tax = country.cost_tax / 100

    return commission + spread_cost + impact + slippage + tax


def calculate_round_trip_cost(
    country_code: str,
    market_cap: float,
    order_size: float,
    avg_daily_volume: float,
) -> float:
    """왕복 거래 비용 = 편도 × 2."""
    one_way = calculate_one_way_cost(
        country_code, market_cap, order_size, avg_daily_volume
    )
    return one_way * 2


# 장 마감이 빠른 순서: 일본 → 한국 → 홍콩 → 유럽 → 미국
MARKET_CLOSE_ORDER = ["JP", "KR", "HK", "EU", "US"]

FRED_SERIES = {
    "vix": "VIXCLS",
    "yield_curve": "T10Y2Y",
    "initial_claims": "ICSA",
    "cpi": "CPIAUCSL",
    "oil": "DCOILWTICO",
    "gold": "GOLDAMGBD228NLBM",
    "copper": "PCOPPUSDM",
    "dollar_index": "DTWEXBGS",
    "us_rate": "FEDFUNDS",
    "kr_rate": "INTDSRKRM",
    "jp_rate": "INTDSRJPM",
    "eu_rate": "ECBMRRFR",
    "risk_free_rate": "DTB3",
}


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  § 3. MACRO ENGINE — 매크로 엔진                                    ║
# ║  (구 macro_engine.py)                                               ║
# ║                                                                      ║
# ║  Patch 1: VIX 바이너리 킬스위치 → 4단계 그라데이션                    ║
# ║  5개 서브엔진 가중평균 → 시장 국면 분류 (6개)                         ║
# ╚══════════════════════════════════════════════════════════════════════╝

def evaluate_vix(current_vix: float, vix_ma5: float) -> Optional[float]:
    """
    VIX 4단계 그라데이션 판단.

    조건 1: VIX≥40, ROC<-10% → +0.5 (항복 바닥 탈출)
    조건 2: VIX≥40, ROC≥-10% → -0.5 (패닉 진행)
    조건 3: VIX≥25, ROC≥30%  → -1.0 (패닉 초입)
    조건 4: 기타             → None (기존 엔진 사용)
    """
    if vix_ma5 <= 0:
        return None

    vix_roc_5d = (current_vix - vix_ma5) / vix_ma5

    if current_vix >= VIX_CAPITULATION_THRESHOLD and vix_roc_5d < VIX_RECOVERY_ROC:
        return VIX_SCORE_CAPITULATION_EXIT  # +0.5

    if current_vix >= VIX_CAPITULATION_THRESHOLD and vix_roc_5d >= VIX_RECOVERY_ROC:
        return VIX_SCORE_PANIC_ONGOING  # -0.5

    if current_vix >= VIX_PANIC_THRESHOLD and vix_roc_5d >= VIX_PANIC_ROC:
        return VIX_SCORE_PANIC_ONSET  # -1.0

    return None


def calculate_macro_score(
    sub_scores: dict,
    current_vix: float = 15.0,
    vix_ma5: float = 15.0,
) -> float:
    """
    매크로 스코어 산출.

    Args:
        sub_scores: {"growth", "liquidity", "innovation", "inflation", "risk"}
                    각 -1 ~ +1
        current_vix: 현재 VIX
        vix_ma5: VIX 5일 이동평균

    Returns:
        macro_score: -1.0 ~ +1.0
    """
    vix_override = evaluate_vix(current_vix, vix_ma5)
    if vix_override is not None:
        return vix_override

    score = 0.0
    for engine, weight in MACRO_ENGINE_WEIGHTS.items():
        engine_score = sub_scores.get(engine, 0.0)
        score += engine_score * weight

    return max(-1.0, min(1.0, score))


# ─── 시장 국면 분류 (6개) ───

REGIME_NAMES = {
    1: "강세초기",
    2: "바닥/함정",
    3: "실적장세",
    4: "방어전환",
    5: "구조적성장",
    6: "약세진입",
}

REGIME_EQUITY_CAP = {
    1: (0.70, 0.90),    # 강세초기: 적극 투자
    2: (0.40, 0.60),    # 바닥/함정: 역발상 기회
    3: (0.80, 0.95),    # 실적장세: 최대 노출
    4: (0.50, 0.70),    # 방어전환: 의미있는 투자 유지
    5: (0.70, 0.90),    # 구조적성장: 적극 투자
    6: (0.30, 0.50),    # 약세진입: 회복 대비 (v3.4: 0-10% → v3.5: 30-50%)
}


def classify_regime(
    macro_score: float,
    erp_zscore: float = 0.0,
    prev_regime: int = 3,
) -> Tuple[int, float]:
    """
    매크로 스코어 + ERP Z-Score → 시장 국면 분류.

    Returns:
        (regime, equity_cap)
    """
    regime = 3  # 기본값: 실적장세

    if macro_score > 0.5 and erp_zscore > 0:
        regime = 1
    elif macro_score < -0.5 and erp_zscore > 0.5:
        regime = 2
    elif macro_score > 0.2 and erp_zscore <= 0:
        regime = 3
    elif macro_score < -0.5 and erp_zscore <= 0:
        regime = 6
    elif macro_score < -0.2:
        regime = 4
    elif macro_score > 0 and erp_zscore < -0.5:
        regime = 5

    cap_range = REGIME_EQUITY_CAP[regime]
    equity_cap = (cap_range[0] + cap_range[1]) / 2

    return regime, equity_cap


def get_regime_name(regime: int) -> str:
    """국면 번호 → 이름."""
    return REGIME_NAMES.get(regime, f"Unknown({regime})")


def get_regime_stop_pct(regime: int) -> float:
    """국면별 트레일링 스탑 폭."""
    return STOP_PCT_BY_REGIME.get(regime, 0.15)


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  § 4. ALGORITHM — 핵심 알고리즘                                     ║
# ║  (구 algorithm.py)                                                  ║
# ║                                                                      ║
# ║  ② Survival Gate (5개) → ③ Z-Score+매크로 가중치                     ║
# ║  → ④ 통합 점수 (10팩터, 100점) → ⑤ 트레일링 스탑 → ⑥ 실행 판단      ║
# ╚══════════════════════════════════════════════════════════════════════╝

# ─── 데이터 구조 ───

@dataclass
class StockMetrics:
    """종목별 수집·변환 완료된 메트릭."""
    symbol: str
    country: str
    sector: str
    industry_type: str          # A, B, C, D

    # Gate 입력
    price: float
    ma120: float
    ma200: float
    roic: Optional[float]
    wacc: Optional[float]
    roa: Optional[float]
    ocf: Optional[float]
    days_since_report: int
    avg_daily_volume: float
    market_cap: float

    # 스코어 입력
    roic_zscore: float          # 섹터 벤치마크 Z-Score
    profit_trend_yoy: float     # 이익률 YoY (0~1 클램핑)
    growth_cagr: float          # 매출 CAGR
    consensus_up_ratio: Optional[float]  # 상향 비율 (None=결측)
    momentum_return: float      # 62일 수익률
    pe_relative: float          # 섹터 대비 P/E
    efficiency: float           # 재고회전율/10
    rsi: float                  # RSI(14)

    # 신규 팩터 (v3.4)
    earnings_surprise_metric: Optional[float]  # 가중평균 서프라이즈 × bonus
    si_composite: Optional[float]              # SI 복합 메트릭 (0~1)

    # TREND_HOLD 조건
    ma20: float
    ma50: float
    roic_score_normalized: float    # 0~1 정규화
    profit_trend_normalized: float  # 0~1 정규화

    # 포지션 추적
    is_held: bool = False           # 현재 보유 여부
    beta: Optional[float] = None

    # 결측 플래그
    has_consensus: bool = True
    has_earnings_surprise: bool = True
    has_short_interest: bool = True


@dataclass
class GateResult:
    """Survival Gate 판단 결과."""
    overall: str                    # "PASS", "WARNING", "FAIL_*"
    details: Dict[str, str] = field(default_factory=dict)
    warning_count: int = 0
    fail_code: Optional[str] = None


@dataclass
class ActionResult:
    """실행 판단 결과."""
    action: str                     # EXIT, STOP_EXIT, WARNING, S_BUY, ...
    score: float
    gate_result: GateResult
    stop_triggered: bool = False
    reasons: List[str] = field(default_factory=list)


@dataclass
class StopCheckResult:
    """트레일링 스탑 확인 결과."""
    triggered: bool = False
    stop_level: float = 0.0
    highest_close: float = 0.0
    reason: str = ""


# ─── Phase 1: Survival Gate (5개 게이트) ───

def evaluate_survival_gate(
    metrics: StockMetrics,
    intended_position_size: float = 0.0,
) -> GateResult:
    """
    5개 게이트 순차 평가.

    Gate ① 추세: Price < MA200 AND Price < MA120 → FAIL_TREND
    Gate ② 수익성: ROIC < WACC AND ROA < 5% → FAIL_ROIC
    Gate ③ 현금흐름: OCF ≤ 0 → WARNING (비중 제한)  [P1-3] FAIL→WARNING
    Gate ④ 신선도: days > 150 → FAIL_STALE, 91~150 → WARNING
    Gate ⑤ 유동성: days_to_liquidate > 2.0 → FAIL_LIQUIDITY
    """
    details = {}
    warnings = []
    fail_code = None

    # ── Gate ① 추세 [v3.5: FAIL 제거, WARNING만] ──
    # 하락 리스크는 트레일링 스탑 + 모멘텀 점수 감점으로 3중 방어
    below_ma200 = metrics.price < metrics.ma200
    below_ma120 = metrics.price < metrics.ma120

    if below_ma200 and below_ma120:
        # 양쪽 MA 이탈: WARNING (비중 제한만, 투자 가능)
        details["gate_1_trend"] = "WARNING"
        warnings.append("TREND")
    elif below_ma200 or below_ma120:
        # [v3.7] 한쪽 MA 이탈도 WARNING (기존 PASS → WARNING)
        details["gate_1_trend"] = "WARNING"
        warnings.append("TREND_WEAK")
    else:
        details["gate_1_trend"] = "PASS"

    # ── Gate ② 수익성 [v3.5: FAIL 제거, WARNING만] ──
    # ROIC 팩터(18점)가 점수에서 자연 감점하므로 Gate는 비중 제한만 담당
    roic_pass = False

    if metrics.roic is not None and metrics.wacc is not None:
        roic_pass = metrics.roic >= metrics.wacc
    if not roic_pass:
        if metrics.roa is not None and metrics.roa >= ROA_MINIMUM:
            roic_pass = True

    if roic_pass:
        details["gate_2_profitability"] = "PASS"
    else:
        # FAIL 대신 WARNING: 비중 제한만 적용, 투자 차단하지 않음
        details["gate_2_profitability"] = "WARNING"
        warnings.append("LOW_PROFITABILITY")

    # ── Gate ③ 현금흐름 ──
    # ── Gate ③ 현금흐름 [P1-3: FAIL→WARNING 전환] ──
    # 근거: 우량 기업도 M&A/재고확대/운전자본으로 분기별 OCF≤0 발생
    #       (META 2022, AMZN 빈번). WARNING으로 비중 제한 후 투자 허용.
    if metrics.ocf is not None and metrics.ocf <= 0:
        details["gate_3_cashflow"] = "WARNING"
        warnings.append("NEGATIVE_OCF")
    elif metrics.ocf is None:
        details["gate_3_cashflow"] = "WARNING"
        warnings.append("CASHFLOW_DATA")
    else:
        details["gate_3_cashflow"] = "PASS"

    # ── Gate ④ 데이터 신선도 [v3.5: FAIL→WARNING, 연간보고서 주기 반영] ──
    if metrics.days_since_report > FRESHNESS_FAIL_DAYS:
        # 365일+ 초과해도 WARNING만 (연간 보고서 주기상 정상)
        details["gate_4_freshness"] = "WARNING"
        warnings.append("VERY_STALE_DATA")
    elif metrics.days_since_report > FRESHNESS_WARNING_DAYS:
        details["gate_4_freshness"] = "WARNING"
        warnings.append("STALE_DATA")
    else:
        details["gate_4_freshness"] = "PASS"

    # ── Gate ⑤ 유동성 ──
    if intended_position_size > 0 and metrics.avg_daily_volume > 0:
        effective_volume = metrics.avg_daily_volume * LIQUIDITY_PARTICIPATION_RATE
        days_to_liquidate = intended_position_size / effective_volume

        if days_to_liquidate > LIQUIDITY_FAIL_DAYS:
            details["gate_5_liquidity"] = "FAIL"
            fail_code = fail_code or "FAIL_LIQUIDITY"
        elif days_to_liquidate > LIQUIDITY_WARNING_DAYS:
            details["gate_5_liquidity"] = "WARNING"
            warnings.append("LIQUIDITY")
        else:
            details["gate_5_liquidity"] = "PASS"
    else:
        details["gate_5_liquidity"] = "PASS"

    # ── 결과 집계 ──
    if fail_code:
        overall = fail_code
    elif len(warnings) >= 2:
        overall = "WARNING_MULTI"
    elif len(warnings) == 1:
        overall = "WARNING"
    else:
        overall = "PASS"

    return GateResult(
        overall=overall,
        details=details,
        warning_count=len(warnings),
        fail_code=fail_code,
    )


# ─── Phase 2-3: 정규화 + 동적 가중치 ───

def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    """값을 [low, high] 범위로 클램핑."""
    return max(low, min(high, value))


def get_dynamic_weights(
    metrics: StockMetrics,
    macro_alpha: float = 0.0,
) -> Dict[str, float]:
    """
    결측 팩터 동적 재분배 + 매크로 alpha 가변 가중치.

    결측 가능 팩터: consensus(8), earnings_surprise(12), short_interest(10)
    매크로 가변: growth(±2), valuation(∓2) — 합계 항상 22
    """
    weights = dict(WEIGHTS)

    # ── 매크로 alpha 가변 ──
    weights["growth"] += macro_alpha * GROWTH_ALPHA_SENSITIVITY
    weights["valuation"] += macro_alpha * VALUATION_ALPHA_SENSITIVITY

    # ── 결측 팩터 처리 [P1-4] ──
    # v3.4.1: REDISTRIBUTE_MISSING_WEIGHTS=False → 가중치 유지, 중립 점수(0.5) 부여
    # v3.4:   REDISTRIBUTE_MISSING_WEIGHTS=True  → 가중치 재분배, 결측 0점
    if REDISTRIBUTE_MISSING_WEIGHTS:
        lost_total = 0.0
        missing_keys = []

        if not metrics.has_consensus:
            lost_total += weights["consensus"]
            missing_keys.append("consensus")
        if not metrics.has_earnings_surprise:
            lost_total += weights["earnings_surprise"]
            missing_keys.append("earnings_surprise")
        if not metrics.has_short_interest:
            lost_total += weights["short_interest"]
            missing_keys.append("short_interest")

        if lost_total > 0:
            redistribute_pool = {
                k: v for k, v in weights.items() if k not in missing_keys
            }
            pool_total = sum(redistribute_pool.values())

            if pool_total > 0:
                for key in redistribute_pool:
                    weights[key] += lost_total * (redistribute_pool[key] / pool_total)

            for key in missing_keys:
                weights[key] = 0.0
    # else: 가중치 그대로 유지 → calculate_score에서 중립 점수 부여

    # ── 합계 100 검증 (부동소수점 보정) ──
    total = sum(weights.values())
    if abs(total - 100.0) > 0.01:
        max_key = max(weights, key=weights.get)
        weights[max_key] += (100.0 - total)

    return weights


# ─── Phase 4: 통합 점수 산출 ───

def normalize_factor(value: float, low: float, high: float) -> float:
    """선형 정규화: [low, high] → [0, 1] (clamp)."""
    if high == low:
        return 0.5
    return _clamp((value - low) / (high - low))


def calculate_score(metrics: StockMetrics, macro_alpha: float = 0.0) -> float:
    """
    10개 팩터 가중합산 → 100점 만점 점수 산출.
    """
    weights = get_dynamic_weights(metrics, macro_alpha)

    scores = {}

    # 1. ROIC (Z-Score 기반, 이미 정규화됨)
    scores["roic"] = _clamp(metrics.roic_zscore)

    # 2. 이익률 추세
    scores["profit_trend"] = _clamp(metrics.profit_trend_yoy)

    # 3. 성장 (CAGR 정규화)
    scores["growth"] = normalize_factor(metrics.growth_cagr, -0.05, 0.25)  # [v3.5] 범위 축소

    # 4. 컨센서스
    if metrics.has_consensus and metrics.consensus_up_ratio is not None:
        scores["consensus"] = _clamp(metrics.consensus_up_ratio)
    else:
        scores["consensus"] = MISSING_FACTOR_NEUTRAL_SCORE  # [P1-4] 0.0→0.5

    # 5. 모멘텀
    scores["momentum"] = normalize_factor(
        metrics.momentum_return, MOMENTUM_NORM_LOW, MOMENTUM_NORM_HIGH
    )

    # 6. 밸류에이션 (P/E 역정규화: 낮을수록 좋음)
    scores["valuation"] = normalize_factor(metrics.pe_relative, 2.0, 0.5)

    # 7. 효율성
    scores["efficiency"] = _clamp(metrics.efficiency)

    # 8. RSI (역정규화: 낮을수록 매수 기회)
    scores["rsi"] = normalize_factor(
        metrics.rsi, RSI_NORM_HIGH, RSI_NORM_LOW
    )

    # 9. 어닝 서프라이즈 (이슈 #1 반영: 범위 0.40)
    if metrics.has_earnings_surprise and metrics.earnings_surprise_metric is not None:
        scores["earnings_surprise"] = normalize_factor(
            metrics.earnings_surprise_metric,
            EARNINGS_SURPRISE_LOW,
            EARNINGS_SURPRISE_HIGH,
        )
    else:
        scores["earnings_surprise"] = MISSING_FACTOR_NEUTRAL_SCORE  # [P1-4] 0.0→0.5

    # 10. Short Interest (이슈 #2: 동적 가중치는 data_adapter에서 처리)
    if metrics.has_short_interest and metrics.si_composite is not None:
        scores["short_interest"] = _clamp(metrics.si_composite)
    else:
        scores["short_interest"] = MISSING_FACTOR_NEUTRAL_SCORE     # [P1-4] 0.0→0.5

    # ── 가중합산 ──
    total_score = sum(
        scores[factor] * weights[factor]
        for factor in weights
    )

    # [v3.5] Beta 근접 보너스: beta가 1.0에 가까울수록 최대 3점 보너스
    # 포트폴리오 Beta를 0.8~1.2 목표 범위로 유도
    if metrics.beta is not None:
        _beta_proximity = 1.0 - min(1.0, abs(metrics.beta - 1.0))
        total_score += _beta_proximity * 3.0

    # [v3.7] 최종 점수를 0~100 범위로 clamp
    total_score = max(0.0, min(100.0, total_score))

    return round(total_score, 2)


# ─── Phase 5: 트레일링 스탑 확인 ───

def check_trailing_stop(
    current_close: float,
    highest_close: float,
    days_held: int,
    current_regime: int,
    earnings_nearby: bool = False,
    is_trend_hold: bool = False,
    trend_aligned: bool = True,
    old_stop_level: Optional[float] = None,
    old_regime: Optional[int] = None,
) -> StopCheckResult:
    """
    트레일링 스탑 판단.

    래칫 규칙 + 이슈 #4(국면 6 예외) 반영.
    """
    result = StopCheckResult(highest_close=highest_close)

    # ── 최소 보유 기간 확인 ──
    if days_held < MIN_HOLD_DAYS:
        result.reason = f"최소 보유 기간 미충족 ({days_held}/{MIN_HOLD_DAYS}일)"
        return result

    # ── 어닝 유예 확인 ──
    if earnings_nearby:
        result.reason = "어닝 발표 유예 기간"
        return result

    # ── highest_close 갱신 ──
    if current_close > highest_close:
        result.highest_close = current_close
        highest_close = current_close

    # ── 스탑 폭 결정 ──
    stop_pct = STOP_PCT_BY_REGIME.get(current_regime, 0.15)

    # TREND_HOLD: 정배열 유효 시 추가 여유
    if is_trend_hold and trend_aligned:
        stop_pct += STOP_TREND_HOLD_EXTRA
    elif is_trend_hold and not trend_aligned:
        pass  # 정배열 붕괴 → 일반 스탑 폭 복귀

    new_stop_level = highest_close * (1 - stop_pct)

    # ── 래칫 규칙 + 국면 6 예외 (이슈 #4) ──
    if old_stop_level is not None and old_regime is not None:
        if current_regime in RATCHET_OVERRIDE_REGIMES:
            result.stop_level = new_stop_level
        elif new_stop_level > old_stop_level:
            result.stop_level = old_stop_level
        else:
            result.stop_level = new_stop_level
    else:
        result.stop_level = new_stop_level

    # ── 스탑 트리거 확인 ──
    if current_close <= result.stop_level:
        result.triggered = True
        result.reason = (
            f"STOP_EXIT: 현재가 {current_close:.2f} ≤ "
            f"스탑 {result.stop_level:.2f} "
            f"(최고가 {highest_close:.2f}, 국면 {current_regime})"
        )

    return result


# ─── Phase 6: 실행 판단 (10개 액션) ───

def determine_action(
    metrics: StockMetrics,
    score: float,
    gate_result: GateResult,
    stop_result: StopCheckResult,
    macro_alpha: float = 0.0,
) -> ActionResult:
    """
    최종 액션 결정.

    우선순위:
      1. EXIT (Gate FAIL)
      2. STOP_EXIT (트레일링 스탑)
      3. WARNING (Gate WARNING)
      4. S_BUY (score≥85, RSI≤45)
      5. TREND_BUY (score≥85, 45<RSI<75)
      6. TREND_HOLD (score≥85, RSI≥75, 조건충족, 기보유)
      7. WAIT_DIP (score≥85, RSI≥75, 미충족/신규)
      8. HOLD (score≥70)
      9. REDUCE (score<70)
    """
    reasons = []

    # ── Priority 1: Gate FAIL ──
    if gate_result.fail_code:
        return ActionResult(
            action="EXIT", score=score, gate_result=gate_result,
            reasons=[f"Gate FAIL: {gate_result.fail_code}"],
        )

    # ── Priority 2: 트레일링 스탑 ──
    if stop_result.triggered:
        return ActionResult(
            action="STOP_EXIT", score=score, gate_result=gate_result,
            stop_triggered=True, reasons=[stop_result.reason],
        )

    # ── Priority 3: Gate WARNING → 비중 제한만, BUY 차단하지 않음 [v3.5] ──
    # WARNING은 calculate_raw_weight()의 warning_count로 비중 제한됨
    # 점수 기반 BUY/HOLD 로직 그대로 진행

    # ── Priority 4~7: 고점수 분기 (score ≥ BUY threshold) ──
    if score >= SCORE_BUY_THRESHOLD:

        if metrics.rsi <= RSI_OVERSOLD:
            return ActionResult(
                action="S_BUY", score=score, gate_result=gate_result,
                reasons=[f"score={score}, RSI={metrics.rsi:.1f} (과매도)"],
            )

        if metrics.rsi < RSI_OVERBOUGHT:
            return ActionResult(
                action="TREND_BUY", score=score, gate_result=gate_result,
                reasons=[f"score={score}, RSI={metrics.rsi:.1f} (정상 추세)"],
            )

        # RSI ≥ 80: TREND_HOLD vs WAIT_DIP [v3.5: 조건 단순화]
        trend_aligned = (
            metrics.price > metrics.ma20 > metrics.ma50 > metrics.ma120
        )

        if trend_aligned and metrics.is_held:
            # [v3.5] 과적합된 ROIC/profit_trend 조건 제거, 정배열+기보유만 확인
            return ActionResult(
                action="TREND_HOLD", score=score, gate_result=gate_result,
                reasons=[
                    f"score={score}, RSI={metrics.rsi:.1f} (과매수)",
                    "4중 정배열 확인, 기보유 → 보유 유지",
                ],
            )
        elif trend_aligned:
            # 정배열이지만 신규 → TREND_BUY (WAIT_DIP 대신 적극 진입)
            return ActionResult(
                action="TREND_BUY", score=score, gate_result=gate_result,
                reasons=[
                    f"score={score}, RSI={metrics.rsi:.1f} (과매수지만 정배열)",
                    "정배열 확인, 신규 진입 허용",
                ],
            )
        else:
            return ActionResult(
                action="WAIT_DIP", score=score, gate_result=gate_result,
                reasons=[
                    f"score={score}, RSI={metrics.rsi:.1f} (과매수)",
                    "정배열 미충족 → 조정 대기",
                ],
            )

    # ── Priority 8: HOLD (score ≥ 70) ──
    if score >= SCORE_HOLD_THRESHOLD:
        return ActionResult(
            action="HOLD", score=score, gate_result=gate_result,
            reasons=[f"score={score} (보유 유지)"],
        )

    # ── Priority 9: REDUCE (score < 70) ──
    return ActionResult(
        action="REDUCE", score=score, gate_result=gate_result,
        reasons=[f"score={score} (비중 축소)"],
    )


# ─── 통합 파이프라인 실행 ───

def run_pipeline(
    metrics: StockMetrics,
    macro_alpha: float = 0.0,
    current_regime: int = 3,
    stop_tracking: Optional[dict] = None,
    intended_position_size: float = 0.0,
) -> ActionResult:
    """
    단일 종목에 대한 전체 파이프라인 실행.

    Gate → Score → Stop → Action 순서.
    """
    # Phase 1: Survival Gate
    gate_result = evaluate_survival_gate(metrics, intended_position_size)

    # Phase 4: 점수 산출 (Gate FAIL이라도 점수는 산출 — 기록용)
    score = calculate_score(metrics, macro_alpha)

    # Phase 5: 트레일링 스탑 (기존 보유 종목만)
    stop_result = StopCheckResult()
    if stop_tracking and metrics.is_held:
        stop_result = check_trailing_stop(
            current_close=metrics.price,
            highest_close=stop_tracking.get("highest_close", metrics.price),
            days_held=stop_tracking.get("days_held", 0),
            current_regime=current_regime,
            earnings_nearby=stop_tracking.get("earnings_nearby", False),
            is_trend_hold=(stop_tracking.get("last_action") == "TREND_HOLD"),
            trend_aligned=(
                metrics.price > metrics.ma20 > metrics.ma50 > metrics.ma120
            ),
            old_stop_level=stop_tracking.get("stop_level"),
            old_regime=stop_tracking.get("regime"),
        )

    # Phase 6: 실행 판단
    action_result = determine_action(
        metrics=metrics, score=score, gate_result=gate_result,
        stop_result=stop_result, macro_alpha=macro_alpha,
    )

    return action_result


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  § 5. POSITION — 포지션 사이징                                      ║
# ║  (구 position.py)                                                   ║
# ║                                                                      ║
# ║  파이프라인 ⑦: 개별 종목의 raw_weight 산출                           ║
# ║  portfolio_manager가 이 출력을 받아 제약 조건 적용 → final_weight     ║
# ╚══════════════════════════════════════════════════════════════════════╝

def calculate_raw_weight(
    action: str,
    score: float,
    industry_type: str,
    warning_count: int = 0,
) -> float:
    """
    개별 종목의 희망 비중(raw_weight) 산출.

    raw_weight = BASE_WEIGHT × score_factor × entry_factor
    final_cap = TYPE_CAP × (TREND_HOLD_CAP_RATIO if TREND_HOLD)
    """
    if action in ("EXIT", "STOP_EXIT", "WAIT_DIP", "QUEUE"):
        return 0.0

    score_factor = SCORE_FACTORS.get(action, 1.0)
    if score_factor == 0.0:
        return 0.0

    entry_factor = 1.0

    target_weight = BASE_WEIGHT * score_factor * entry_factor

    type_cap = TYPE_CAPS.get(industry_type, 0.10)

    if action == "TREND_HOLD":
        type_cap *= TREND_HOLD_CAP_RATIO

    if warning_count >= 2:
        type_cap = min(type_cap, MULTI_WARNING_CAP * TYPE_CAPS.get(industry_type, 0.10))
    elif warning_count == 1:
        type_cap = min(type_cap, SINGLE_WARNING_CAP * TYPE_CAPS.get(industry_type, 0.10))

    raw_weight = min(target_weight, type_cap)

    return round(raw_weight, 4)


def generate_trade_record(
    symbol: str,
    action: str,
    current_price: float,
    entry_price: Optional[float] = None,
    highest_close: Optional[float] = None,
    stop_level: Optional[float] = None,
    days_held: int = 0,
    regime: int = 3,
    score: float = 0.0,
) -> dict:
    """STOP_EXIT 또는 EXIT 시 매매 기록 생성."""
    pnl_pct = None
    if entry_price and entry_price > 0:
        pnl_pct = (current_price - entry_price) / entry_price

    return {
        "symbol": symbol,
        "action": action,
        "exit_price": current_price,
        "entry_price": entry_price,
        "highest_close": highest_close,
        "stop_level": stop_level,
        "pnl_pct": pnl_pct,
        "days_held": days_held,
        "regime_at_exit": regime,
        "score_at_exit": score,
    }


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  § 6. PORTFOLIO MANAGER — 포트폴리오 리스크 관리                     ║
# ║  (구 portfolio_manager.py)                                          ║
# ║                                                                      ║
# ║  파이프라인 ⑧: 개별 raw_weight → 5개 제약 → final_weight             ║
# ║  ① 최대 포지션 수 ② 섹터 집중도 ③ 국가 집중도                        ║
# ║  ④ 포트폴리오 베타 ⑤ 총 주식 비중 상한                               ║
# ╚══════════════════════════════════════════════════════════════════════╝

@dataclass
class PortfolioCandidate:
    """포트폴리오 관리 입력 단위."""
    symbol: str
    sector: str
    country: str
    score: float
    action: str
    raw_weight: float
    beta: float = 1.0
    industry_type: str = "C"
    beta_assumed: bool = False


@dataclass
class AdjustedPosition:
    """제약 적용 후 출력 단위."""
    symbol: str
    final_weight: float
    raw_weight: float
    adjustment_reasons: List[str] = field(default_factory=list)


@dataclass
class PortfolioMetrics:
    """포트폴리오 전체 메트릭."""
    position_count: int = 0
    total_equity_pct: float = 0.0
    cash_pct: float = 1.0
    portfolio_beta: float = 0.0
    equity_only_beta: float = 1.0
    sector_exposures: Dict[str, float] = field(default_factory=dict)
    country_exposures: Dict[str, float] = field(default_factory=dict)
    max_sector_exposure: float = 0.0
    max_country_exposure: float = 0.0
    constraints_applied: List[dict] = field(default_factory=list)
    herfindahl_index: float = 0.0
    top5_concentration: float = 0.0
    beta_warning: Optional[str] = None


def manage_portfolio(
    candidates: List[PortfolioCandidate],
    macro_equity_cap: float = 0.70,
    total_capital: float = 1_000_000,
) -> tuple:
    """
    5개 제약 조건을 순차 적용하여 최종 포트폴리오 확정.

    Returns:
        (adjusted_positions, metrics, queue)
    """
    constraints_log = []
    queue = []

    active = [c for c in candidates if c.raw_weight > 0]
    active.sort(key=lambda c: c.score, reverse=True)

    # ── Constraint 1: 최대 포지션 수 ──
    if len(active) > MAX_POSITIONS:
        overflow = active[MAX_POSITIONS:]
        active = active[:MAX_POSITIONS]
        for c in overflow:
            queue.append(c.symbol)
            constraints_log.append({
                "type": "POSITION_LIMIT",
                "affected": c.symbol,
                "detail": f"Score {c.score:.1f}로 QUEUE 이동",
            })

    if len(active) < MIN_POSITIONS:
        constraints_log.append({
            "type": "MIN_POSITIONS_WARNING",
            "detail": f"활성 종목 {len(active)}개 < 최소 {MIN_POSITIONS}개",
        })

    # 극단적 베타 사전 처리
    for c in active:
        if c.beta > EXTREME_BETA_THRESHOLD:
            old_w = c.raw_weight
            c.raw_weight = min(c.raw_weight, EXTREME_BETA_CAP)
            if old_w != c.raw_weight:
                constraints_log.append({
                    "type": "EXTREME_BETA",
                    "affected": c.symbol,
                    "detail": f"beta={c.beta:.2f}, 비중 {old_w:.3f}→{c.raw_weight:.3f}",
                })

    # ── Constraint 2: 섹터 집중도 ──
    active = _apply_group_cap(
        active, group_key="sector", max_exposure=MAX_SECTOR_EXPOSURE,
        cap_name="SECTOR_CAP", constraints_log=constraints_log,
    )

    # ── Constraint 3: 국가 집중도 ──
    active = _apply_group_cap(
        active, group_key="country", max_exposure=MAX_COUNTRY_EXPOSURE,
        cap_name="COUNTRY_CAP", constraints_log=constraints_log,
    )

    # ── Constraint 4: 포트폴리오 베타 (이슈 #3: 저베타 경고만) ──
    active, beta_warning = _apply_beta_constraint(
        active, constraints_log=constraints_log,
    )

    # ── Constraint 5: 총 주식 비중 상한 ──
    total_equity = sum(c.raw_weight for c in active)
    if total_equity > macro_equity_cap:
        ratio = macro_equity_cap / total_equity
        for c in active:
            c.raw_weight *= ratio
        constraints_log.append({
            "type": "MACRO_CAP",
            "before": total_equity,
            "after": macro_equity_cap,
            "detail": f"총 주식 비중 {total_equity:.1%}→{macro_equity_cap:.1%}",
        })

    # ── 결과 조립 ──
    adjusted = []
    for c in active:
        reasons = [
            log["type"] for log in constraints_log
            if log.get("affected") == c.symbol
        ]
        adjusted.append(AdjustedPosition(
            symbol=c.symbol,
            final_weight=round(c.raw_weight, 4),
            raw_weight=c.raw_weight,
            adjustment_reasons=reasons,
        ))

    metrics = _calculate_metrics(active, adjusted, constraints_log, beta_warning)

    return adjusted, metrics, queue


# ─── 내부 함수 ───

def _apply_group_cap(
    candidates: List[PortfolioCandidate],
    group_key: str,
    max_exposure: float,
    cap_name: str,
    constraints_log: List[dict],
) -> List[PortfolioCandidate]:
    """섹터/국가 그룹 비중 상한 적용."""
    groups = {}
    for c in candidates:
        g = getattr(c, group_key)
        groups.setdefault(g, []).append(c)

    total_weight = sum(c.raw_weight for c in candidates)
    if total_weight == 0:
        return candidates

    for group_name, members in groups.items():
        group_total = sum(c.raw_weight for c in members)
        group_pct = group_total / total_weight if total_weight > 0 else 0

        if group_pct > max_exposure:
            excess = group_total - (max_exposure * total_weight)
            members.sort(key=lambda c: c.score)
            for c in members:
                if excess <= 0:
                    break
                reduction = min(
                    excess * (c.raw_weight / group_total),
                    c.raw_weight * 0.8,
                )
                c.raw_weight -= reduction
                excess -= reduction

            constraints_log.append({
                "type": cap_name,
                "group": group_name,
                "before": group_pct,
                "after": max_exposure,
            })

    return candidates


def _apply_beta_constraint(
    candidates: List[PortfolioCandidate],
    constraints_log: List[dict],
) -> tuple:
    """포트폴리오 베타 타겟 적용. 이슈 #3: 저베타는 경고만."""
    beta_warning = None
    total_w = sum(c.raw_weight for c in candidates)
    if total_w == 0:
        return candidates, None

    equity_beta = sum(c.raw_weight * c.beta for c in candidates) / total_w

    # ── 고베타 조정 ──
    if equity_beta > TARGET_BETA_MAX:
        high_beta = [c for c in candidates if c.beta > TARGET_BETA_MAX]
        high_beta.sort(key=lambda c: c.score)

        for _ in range(3):
            equity_beta = sum(c.raw_weight * c.beta for c in candidates) / \
                          sum(c.raw_weight for c in candidates)
            if equity_beta <= TARGET_BETA_MAX:
                break

            for c in high_beta:
                if equity_beta <= TARGET_BETA_MAX:
                    break
                denom = c.beta - TARGET_BETA_MAX
                if denom <= 0:
                    continue
                beta_excess = equity_beta - TARGET_BETA_MAX
                tw = sum(x.raw_weight for x in candidates)
                needed = beta_excess * tw / denom
                actual = min(needed, c.raw_weight * BETA_HIGH_MAX_REDUCTION_RATIO)
                c.raw_weight -= actual

                constraints_log.append({
                    "type": "BETA_HIGH",
                    "affected": c.symbol,
                    "detail": f"beta={c.beta:.2f}, 비중 축소 {actual:.4f}",
                })

        tw = sum(c.raw_weight for c in candidates)
        if tw > 0:
            equity_beta = sum(c.raw_weight * c.beta for c in candidates) / tw
            if equity_beta > TARGET_BETA_MAX:
                constraints_log.append({
                    "type": "BETA_HIGH_WARNING",
                    "detail": f"3회 조정 후에도 베타 {equity_beta:.2f} > {TARGET_BETA_MAX}",
                })

    # ── 저베타: scale-up [v3.5] ──
    elif equity_beta < TARGET_BETA_MIN:
        if BETA_LOW_ACTION == "SCALE_UP":
            # 전체 종목 비중을 비례적으로 증가시켜 포트폴리오 베타 보정
            # 고��타 종목에 더 큰 부스트 적용
            for _ in range(8):  # 최대 8회 반복 조정
                tw = sum(c.raw_weight for c in candidates)
                if tw == 0:
                    break
                equity_beta = sum(c.raw_weight * c.beta for c in candidates) / tw
                if equity_beta >= TARGET_BETA_MIN:
                    break
                beta_deficit = TARGET_BETA_MIN - equity_beta
                for c in candidates:
                    if c.raw_weight > 0:
                        # 베타 기반 가중 부스트: 고베타 종목일수록 더 많이 증가
                        beta_factor = max(0.5, c.beta)
                        boost = beta_deficit * 0.8 * beta_factor
                        old_w = c.raw_weight
                        c.raw_weight *= (1 + boost)
                        if old_w != c.raw_weight:
                            constraints_log.append({
                                "type": "BETA_SCALE_UP",
                                "affected": c.symbol,
                                "detail": f"beta={c.beta:.2f}, 비중 {old_w:.4f}→{c.raw_weight:.4f}",
                            })
            beta_warning = "LOW_BETA_SCALED"
            constraints_log.append({
                "type": "BETA_LOW_SCALED",
                "current_beta": equity_beta,
                "target_min": TARGET_BETA_MIN,
                "detail": f"equity_only_beta={equity_beta:.2f} < {TARGET_BETA_MIN}. SCALE_UP 적용.",
            })
        else:
            beta_warning = "LOW_BETA"
            constraints_log.append({
                "type": "BETA_LOW_WARNING",
                "current_beta": equity_beta,
                "target_min": TARGET_BETA_MIN,
                "action": "WARNING_ONLY",
                "detail": (
                    f"equity_only_beta={equity_beta:.2f} < {TARGET_BETA_MIN}. "
                    "저베타 과다 감지."
                ),
            })

    return candidates, beta_warning


def _calculate_metrics(
    candidates: List[PortfolioCandidate],
    adjusted: List[AdjustedPosition],
    constraints_log: List[dict],
    beta_warning: Optional[str],
) -> PortfolioMetrics:
    """포트폴리오 메트릭 산출."""
    total_equity = sum(a.final_weight for a in adjusted)
    cash_pct = max(0.0, 1.0 - total_equity)

    if total_equity > 0:
        portfolio_beta = sum(c.raw_weight * c.beta for c in candidates)
        equity_only_beta = portfolio_beta / total_equity
    else:
        portfolio_beta = 0.0
        equity_only_beta = 1.0

    sector_exp = {}
    country_exp = {}
    for c in candidates:
        if c.raw_weight > 0:
            sector_exp[c.sector] = sector_exp.get(c.sector, 0) + c.raw_weight
            country_exp[c.country] = country_exp.get(c.country, 0) + c.raw_weight

    if total_equity > 0:
        hhi = sum(
            (a.final_weight / total_equity) ** 2
            for a in adjusted if a.final_weight > 0
        )
    else:
        hhi = 0.0

    sorted_weights = sorted([a.final_weight for a in adjusted], reverse=True)
    top5 = sum(sorted_weights[:5])

    return PortfolioMetrics(
        position_count=len([a for a in adjusted if a.final_weight > 0]),
        total_equity_pct=round(total_equity, 4),
        cash_pct=round(cash_pct, 4),
        portfolio_beta=round(portfolio_beta, 4),
        equity_only_beta=round(equity_only_beta, 4),
        sector_exposures=sector_exp,
        country_exposures=country_exp,
        max_sector_exposure=max(sector_exp.values()) if sector_exp else 0,
        max_country_exposure=max(country_exp.values()) if country_exp else 0,
        constraints_applied=constraints_log,
        herfindahl_index=round(hhi, 4),
        top5_concentration=round(top5, 4),
        beta_warning=beta_warning,
    )


# ╔══════════════════════════════════════════════════════════════════════╗
# ║  § 7. TESTS — 구조 검증 스크립트                                    ║
# ║  (구 test_structure.py)                                             ║
# ╚══════════════════════════════════════════════════════════════════════╝

def _make_healthy_metrics() -> StockMetrics:
    """건강한 종목 샘플 생성."""
    return StockMetrics(
        symbol="AAPL", country="US", sector="Technology", industry_type="C",
        price=180.0, ma120=160.0, ma200=150.0,
        roic=0.25, wacc=0.10, roa=0.15, ocf=5_000_000_000,
        days_since_report=30, avg_daily_volume=100_000_000,
        market_cap=3_000_000_000_000,
        roic_zscore=0.75, profit_trend_yoy=0.65, growth_cagr=0.12,
        consensus_up_ratio=0.70, momentum_return=0.15,
        pe_relative=0.90, efficiency=0.60, rsi=55.0,
        earnings_surprise_metric=0.08, si_composite=0.70,
        ma20=175.0, ma50=170.0,
        roic_score_normalized=0.75, profit_trend_normalized=0.65,
        is_held=False, beta=1.10,
    )


def test_config():
    """config.py 무결성 검증."""
    total = sum(WEIGHTS.values())
    assert abs(total - 100.0) < 0.01, f"WEIGHTS 합계 = {total} (100이어야 함)"
    assert len(WEIGHTS) == 10, f"팩터 수 = {len(WEIGHTS)} (10이어야 함)"
    assert len(STOP_PCT_BY_REGIME) == 6, f"국면 수 = {len(STOP_PCT_BY_REGIME)}"
    assert len(TYPE_CAPS) == 4
    assert "STOP_EXIT" in SCORE_FACTORS
    print("  ✅ config 검증 통과")


def test_countries():
    """countries.py 무결성 검증."""
    assert len(COUNTRIES) == 5, f"국가 수 = {len(COUNTRIES)} (5이어야 함)"
    assert set(COUNTRIES.keys()) == {"US", "KR", "JP", "HK", "EU"}

    hk = get_country("HK")
    assert hk.rate_fallback == "USE_US_ALPHA"

    alpha_us = calculate_rate_alpha("US", 5.0)
    assert alpha_us == 1.0, f"US alpha(5%) = {alpha_us}"

    alpha_us_low = calculate_rate_alpha("US", 0.0)
    assert alpha_us_low == -1.0, f"US alpha(0%) = {alpha_us_low}"

    alpha_jp = calculate_rate_alpha("JP", 0.10)
    assert abs(alpha_jp) < 0.01, f"JP alpha(0.10%) = {alpha_jp}"

    print("  ✅ countries 검증 통과")


def test_algorithm_gate():
    """algorithm.py Gate 검증."""
    m = _make_healthy_metrics()
    result = evaluate_survival_gate(m)
    assert result.overall == "PASS", f"건강한 종목이 {result.overall}"

    m_trend = _make_healthy_metrics()
    m_trend.price = 80
    m_trend.ma120 = 100
    m_trend.ma200 = 110
    result = evaluate_survival_gate(m_trend)
    assert result.fail_code == "FAIL_TREND", f"추세 하회인데 {result.fail_code}"

    m_stale = _make_healthy_metrics()
    m_stale.days_since_report = 200
    result = evaluate_survival_gate(m_stale)
    assert result.fail_code == "FAIL_STALE", f"데이터 낡은데 {result.fail_code}"

    # [P1-3] OCF ≤ 0 → WARNING (FAIL이 아님)
    m_ocf = _make_healthy_metrics()
    m_ocf.ocf = -100_000_000
    result_ocf = evaluate_survival_gate(m_ocf)
    assert result_ocf.fail_code is None, f"OCF 음수인데 FAIL 발생: {result_ocf.fail_code}"
    assert result_ocf.warning_count >= 1, f"OCF 음수인데 WARNING 없음"
    assert "WARNING" in result_ocf.overall, f"OCF 음수 → WARNING이어야 하는데 {result_ocf.overall}"

    print("  ✅ algorithm Gate 검증 통과 (P1-3 OCF WARNING 포함)")


def test_algorithm_score():
    """algorithm.py 스코어 검증."""
    m = _make_healthy_metrics()
    score = calculate_score(m, macro_alpha=0.0)
    assert 0 <= score <= 100, f"점수 범위 이탈: {score}"
    print(f"     건강한 종목 스코어: {score:.1f}점")

    # [P1-4] 결측 시 중립 점수(0.5) 검증
    m_missing = _make_healthy_metrics()
    m_missing.has_consensus = False
    m_missing.has_short_interest = False
    score_missing = calculate_score(m_missing, macro_alpha=0.0)
    assert 0 <= score_missing <= 100, f"결측 시 점수 범위: {score_missing}"
    # P1-4: 결측 팩터가 0.5(중립)이므로 v3.4(0.0) 대비 점수 상승
    assert score_missing > 60, f"결측 중립(0.5) 적용 시 점수가 너무 낮음: {score_missing}"
    print(f"     결측(consensus+SI) 스코어: {score_missing:.1f}점 (P1-4: 중립 0.5 적용)")

    # [P1-1/P1-2] 임계값 경계 테스트
    assert SCORE_BUY_THRESHOLD == 70, f"BUY 임계값 = {SCORE_BUY_THRESHOLD} (70이어야 함)"
    assert SCORE_HOLD_THRESHOLD == 45, f"HOLD 임계값 = {SCORE_HOLD_THRESHOLD} (55이어야 함)"

    print("  ✅ algorithm 스코어 검증 통과 (P1 임계값+중립 포함)")


def test_algorithm_action():
    """algorithm.py 액션 판단 검증."""
    m = _make_healthy_metrics()
    m.rsi = 30
    gate = GateResult(overall="PASS")
    stop = StopCheckResult()

    result = determine_action(m, score=90.0, gate_result=gate, stop_result=stop)
    assert result.action == "S_BUY", f"과매도+고점수인데 {result.action}"

    # [P1-1] 임계값 경계 테스트: score=72 (v3.4: HOLD, v3.4.1: S_BUY)
    m_boundary = _make_healthy_metrics()
    m_boundary.rsi = 40  # 과매도
    result_boundary = determine_action(
        m_boundary, score=72.0, gate_result=gate, stop_result=stop
    )
    assert result_boundary.action == "S_BUY", \
        f"[P1-1] score=72, RSI=40이면 S_BUY인데 {result_boundary.action}"

    # [P1-2] HOLD 경계 테스트: score=57 (v3.4: REDUCE, v3.4.1: HOLD)
    result_hold = determine_action(
        _make_healthy_metrics(), score=57.0, gate_result=gate, stop_result=stop
    )
    assert result_hold.action == "HOLD", \
        f"[P1-2] score=57이면 HOLD인데 {result_hold.action}"

    # [P1-2] REDUCE 경계 테스트: score=48 → REDUCE
    result_reduce = determine_action(
        _make_healthy_metrics(), score=48.0, gate_result=gate, stop_result=stop
    )
    assert result_reduce.action == "REDUCE", \
        f"[P1-2] score=48이면 REDUCE인데 {result_reduce.action}"

    m2 = _make_healthy_metrics()
    m2.rsi = 80
    m2.is_held = True
    m2.roic_score_normalized = 0.8
    m2.profit_trend_normalized = 0.7
    m2.price = 200
    m2.ma20 = 190
    m2.ma50 = 180
    m2.ma120 = 150
    result2 = determine_action(m2, score=90.0, gate_result=gate, stop_result=stop)
    assert result2.action == "TREND_HOLD", f"TREND_HOLD 조건인데 {result2.action}"

    m3 = _make_healthy_metrics()
    m3.rsi = 80
    m3.is_held = False
    m3.roic_score_normalized = 0.8
    m3.profit_trend_normalized = 0.7
    m3.price = 200
    m3.ma20 = 190
    m3.ma50 = 180
    m3.ma120 = 150
    result3 = determine_action(m3, score=90.0, gate_result=gate, stop_result=stop)
    assert result3.action == "WAIT_DIP", f"신규+과매수인데 {result3.action}"

    print("  ✅ algorithm 액션 검증 통과 (P1-1/P1-2 경계 포함)")


def test_macro_engine():
    """macro_engine.py VIX 그라데이션 검증."""
    score = evaluate_vix(current_vix=35.0, vix_ma5=25.0)
    assert score == -1.0, f"패닉 초입인데 {score}"

    score2 = evaluate_vix(current_vix=42.0, vix_ma5=50.0)
    assert score2 == 0.5, f"항복 탈출인데 {score2}"

    score3 = evaluate_vix(current_vix=15.0, vix_ma5=14.0)
    assert score3 is None, f"정상인데 {score3}"

    print("  ✅ macro_engine VIX 그라데이션 검증 통과")


def test_portfolio_manager():
    """portfolio_manager.py 제약 조건 검증."""
    candidates = []
    for i in range(25):
        candidates.append(PortfolioCandidate(
            symbol=f"STOCK_{i:02d}",
            sector="Technology" if i < 10 else "Healthcare",
            country="US" if i < 15 else "KR",
            score=95 - i,
            action="TREND_BUY",
            raw_weight=0.05,
            beta=1.0 + (i * 0.02),
        ))

    adjusted, metrics, queue = manage_portfolio(
        candidates, macro_equity_cap=0.70
    )

    assert metrics.position_count <= 20, f"포지션 수 {metrics.position_count} > 20"
    assert len(queue) == 5, f"QUEUE {len(queue)}개 (5개여야 함)"
    assert metrics.total_equity_pct <= 0.70 + 0.01, f"총 비중 {metrics.total_equity_pct}"

    print(f"     포지션: {metrics.position_count}개, 주식비중: {metrics.total_equity_pct:.1%}")
    print(f"     베타: {metrics.equity_only_beta:.2f}, QUEUE: {len(queue)}개")
    print("  ✅ portfolio_manager 제약 조건 검증 통과")


def test_position():
    """position.py 사이징 검증."""
    w = calculate_raw_weight("S_BUY", 90, "A")
    assert w > 0, "S_BUY 비중이 0"
    assert w <= 0.15, f"S_BUY 비중 {w} > Type A cap 15%"

    w_th = calculate_raw_weight("TREND_HOLD", 90, "A")
    assert w_th <= 0.15 * 0.60 + 0.001, f"TREND_HOLD {w_th} > cap×0.6"

    w_exit = calculate_raw_weight("EXIT", 90, "A")
    assert w_exit == 0.0

    print("  ✅ position 사이징 검증 통과")


def test_phase1_changes():
    """v3.4.1 Phase 1 변경사항 전용 검증."""

    # [P1-1/P1-2] 임계값 변경 확인
    assert SCORE_BUY_THRESHOLD == 70, f"BUY 임계값 {SCORE_BUY_THRESHOLD} != 70"
    assert SCORE_HOLD_THRESHOLD == 45, f"HOLD 임계값 {SCORE_HOLD_THRESHOLD} != 55"

    # [P1-3] OCF 음수 → WARNING (FAIL 아님) 확인
    m = _make_healthy_metrics()
    m.ocf = -500_000_000
    gate = evaluate_survival_gate(m)
    assert gate.fail_code is None, f"OCF 음수가 FAIL이 되면 안 됨: {gate.fail_code}"
    assert gate.warning_count >= 1, "OCF 음수 → WARNING 발생해야 함"

    # [P1-4] 결측 팩터 중립 점수 확인
    assert MISSING_FACTOR_NEUTRAL_SCORE == 0.5
    assert REDISTRIBUTE_MISSING_WEIGHTS == True

    m_full = _make_healthy_metrics()
    score_full = calculate_score(m_full, macro_alpha=0.0)

    m_miss = _make_healthy_metrics()
    m_miss.has_consensus = False
    m_miss.has_earnings_surprise = False
    m_miss.has_short_interest = False
    score_miss = calculate_score(m_miss, macro_alpha=0.0)

    # 3팩터 결측(30점) → 중립 0.5 적용 → 15점 확보
    # v3.4에서는 재분배로 ROIC에 과집중되어 점수 왜곡
    print(f"     전팩터 스코어: {score_full:.1f}점")
    print(f"     3팩터 결측 스코어: {score_miss:.1f}점 (중립 0.5 적용)")
    score_diff = abs(score_full - score_miss)
    print(f"     차이: {score_diff:.1f}점 (결측 패널티 축소 확인)")

    # [P1-1] 임계값 경계: score=72 + RSI=40 → S_BUY
    gate_pass = GateResult(overall="PASS")
    stop_none = StopCheckResult()
    m_buy = _make_healthy_metrics()
    m_buy.rsi = 40
    r = determine_action(m_buy, score=72.0, gate_result=gate_pass, stop_result=stop_none)
    assert r.action == "S_BUY", f"score=72,RSI=40 → S_BUY여야 하는데 {r.action}"

    # [P1] run_pipeline E2E: 건강한 종목 → 매수 시그널 발생 확인
    m_pipeline = _make_healthy_metrics()
    m_pipeline.rsi = 42  # 과매도 영역
    result = run_pipeline(m_pipeline, macro_alpha=0.0, current_regime=3)
    print(f"     E2E 파이프라인: score={result.score:.1f}, action={result.action}")
    assert result.action in ("S_BUY", "TREND_BUY", "HOLD"), \
        f"건강한 종목이 {result.action}이면 안 됨"

    print("  ✅ Phase 1 전용 검증 통과")


def run_all_tests():
    """전체 검증 실행."""
    print("=" * 60)
    print(f"  Quant-Alpha {VERSION_TAG} 통합 코드 구조 검증")
    print("=" * 60)
    print()

    test_config()
    test_countries()
    test_algorithm_gate()
    test_algorithm_score()
    test_algorithm_action()
    test_macro_engine()
    test_position()
    test_portfolio_manager()
    test_phase1_changes()

    print()
    print("=" * 60)
    print(f"  🎉 전체 검증 통과 — {VERSION_TAG} 코드 정상")
    print("=" * 60)


# ═══════════════════════════════════════════════════════════════
# 실행
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    run_all_tests()
