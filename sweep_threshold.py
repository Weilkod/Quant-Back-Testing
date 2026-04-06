#!/usr/bin/env python3
"""
BUY 임계값 파라미터 Sweep — 과적합 검증
SCORE_BUY_THRESHOLD: [54, 57, 60, 63, 66]
SCORE_HOLD_THRESHOLD: BUY × 0.75 (비율 유지)
"""
import json
import time
import importlib
import numpy as np

# Sweep 설정
BUY_VALUES = [54, 57, 60, 63, 66]
BASELINE_BUY = 60


def run_single(buy_thresh: int, hold_thresh: int) -> dict:
    """단일 BUY/HOLD 임계값 조합으로 백테스트 실행."""
    # 모듈 캐시 초기화 후 재로드
    import data_loader
    data_loader.clear_caches()

    # quant_alpha 모듈 재로드 및 임계값 패치
    import quant_alpha_v3_4_1_phase1 as qa
    importlib.reload(qa)
    qa.SCORE_BUY_THRESHOLD = buy_thresh
    qa.SCORE_HOLD_THRESHOLD = hold_thresh

    # backtest_engine 재로드 (quant_alpha 변경 반영)
    import backtest_engine as be
    importlib.reload(be)

    # 실행
    engine = be.BacktestEngine(100_000_000)
    engine.run()
    return engine.results()


def main():
    print("=" * 70)
    print("  BUY Threshold Sweep — 과적합 검증 (메인 시나리오 2013-2024)")
    print("=" * 70)

    all_results = {}

    for buy_val in BUY_VALUES:
        hold_val = round(buy_val * 0.75)
        label = f"BUY={buy_val}/HOLD={hold_val}"
        print(f"\n{'─' * 70}")
        print(f"  [{label}] 백테스트 시작...")
        print(f"{'─' * 70}")

        t0 = time.time()
        result = run_single(buy_val, hold_val)
        elapsed = time.time() - t0

        s = result["summary"]
        all_results[buy_val] = {
            "buy": buy_val,
            "hold": hold_val,
            "cagr": s["cagr_s"],
            "cagr_bench": s["cagr_b"],
            "excess": s["excess"],
            "sharpe": s["sh_s"],
            "mdd": s["mdd_s"],
            "alpha": s["alpha"],
            "beta": s["beta"],
            "info_ratio": s["ir"],
            "win_rate": s["wr"],
            "elapsed_sec": round(elapsed, 1),
        }
        print(f"  → CAGR: {s['cagr_s']:+.2f}%, Sharpe: {s['sh_s']:.3f}, "
              f"MDD: {s['mdd_s']:.2f}%, Alpha: {s['alpha']:+.2f}%, "
              f"Time: {elapsed:.1f}s")

    # ── 결과 비교 테이블 ──
    print("\n" + "=" * 70)
    print("  Sweep 결과 비교")
    print("=" * 70)

    baseline = all_results[BASELINE_BUY]

    header = f"{'BUY':>5} {'HOLD':>5} │ {'CAGR':>8} {'Sharpe':>8} {'MDD':>8} {'Alpha':>8} {'Beta':>6} │ {'Δ CAGR':>8} {'Δ Sharpe':>9}"
    print(header)
    print("─" * len(header))

    for buy_val in BUY_VALUES:
        r = all_results[buy_val]
        d_cagr = r["cagr"] - baseline["cagr"]
        d_sharpe = r["sharpe"] - baseline["sharpe"]
        marker = " ◄ baseline" if buy_val == BASELINE_BUY else ""
        print(f"{r['buy']:>5} {r['hold']:>5} │ {r['cagr']:>+7.2f}% {r['sharpe']:>7.3f} {r['mdd']:>7.2f}% {r['alpha']:>+7.2f}% {r['beta']:>6.3f} │ {d_cagr:>+7.2f}% {d_sharpe:>+8.3f}{marker}")

    # ── 민감도 판정 ──
    cagrs = [all_results[b]["cagr"] for b in BUY_VALUES]
    sharpes = [all_results[b]["sharpe"] for b in BUY_VALUES]
    cagr_range = max(cagrs) - min(cagrs)
    sharpe_range = max(sharpes) - min(sharpes)
    cagr_pct_change = (cagr_range / abs(baseline["cagr"]) * 100) if baseline["cagr"] != 0 else 0
    sharpe_pct_change = (sharpe_range / baseline["sharpe"] * 100) if baseline["sharpe"] != 0 else 0

    print(f"\n  CAGR 범위:   {min(cagrs):+.2f}% ~ {max(cagrs):+.2f}%  (변동폭: {cagr_range:.2f}%, 기준대비 {cagr_pct_change:.1f}%)")
    print(f"  Sharpe 범위: {min(sharpes):.3f} ~ {max(sharpes):.3f}  (변동폭: {sharpe_range:.3f}, 기준대비 {sharpe_pct_change:.1f}%)")

    if cagr_pct_change < 15 and sharpe_pct_change < 15:
        verdict = "ROBUST — 파라미터 로버스트 (과적합 우려 낮음)"
    elif cagr_pct_change > 30 or sharpe_pct_change > 30:
        verdict = "SENSITIVE — 파라미터 민감 (과적합 가능성 높음)"
    else:
        verdict = "MODERATE — 보통 수준 민감도 (추가 검증 필요)"

    print(f"\n  판정: {verdict}")

    # JSON 저장
    with open("sweep_results.json", "w") as f:
        json.dump({
            "sweep_config": {
                "parameter": "SCORE_BUY_THRESHOLD",
                "values": BUY_VALUES,
                "hold_ratio": 0.75,
                "baseline": BASELINE_BUY,
                "scenario": "main (2013-2024)",
            },
            "results": all_results,
            "sensitivity": {
                "cagr_range": round(cagr_range, 4),
                "sharpe_range": round(sharpe_range, 4),
                "cagr_pct_change": round(cagr_pct_change, 2),
                "sharpe_pct_change": round(sharpe_pct_change, 2),
                "verdict": verdict,
            },
        }, f, indent=2, ensure_ascii=False)

    print(f"\n  결과 저장: sweep_results.json")
    print("=" * 70)


if __name__ == "__main__":
    main()
