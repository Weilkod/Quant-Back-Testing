#!/usr/bin/env python3
"""
TYPE_CAP 파라미터 Sweep — 포트폴리오 집중도 vs 수익 분석
C형(대형 안정) TYPE_CAP: [0.06, 0.08, 0.10, 0.12, 0.14]
나머지 타입은 C형 대비 비율 유지 (A=1.5x, B=1.2x, D=2.0x)
"""
import json
import time
import importlib
import numpy as np


CAP_C_VALUES = [0.06, 0.08, 0.10, 0.12, 0.14]
BASELINE_CAP_C = 0.10
# 기존 비율: A=0.15(1.5x), B=0.12(1.2x), C=0.10(1.0x), D=0.20(2.0x)
TYPE_RATIOS = {"A": 1.5, "B": 1.2, "C": 1.0, "D": 2.0}


def run_single(cap_c: float) -> dict:
    """단일 TYPE_CAP 조합으로 백테스트 실행."""
    import data_loader
    data_loader.clear_caches()

    import quant_alpha_v3_4_1_phase1 as qa
    importlib.reload(qa)

    # TYPE_CAP 패치 (C형 기준 비율 유지)
    for t, ratio in TYPE_RATIOS.items():
        qa.TYPE_CAPS[t] = round(cap_c * ratio, 4)

    import backtest_engine as be
    importlib.reload(be)

    engine = be.BacktestEngine(100_000_000)
    engine.run()
    result = engine.results()

    # 포지션/현금 통계
    avg_pos = sum(engine.pos_h) / len(engine.pos_h) if engine.pos_h else 0
    avg_cash = sum(engine.cash_h) / len(engine.cash_h) if engine.cash_h else 0

    return result, avg_pos, avg_cash, dict(engine.act_cnt)


def main():
    print("=" * 70)
    print("  TYPE_CAP Sweep — 포트폴리오 집중도 분석 (메인 시나리오)")
    print("=" * 70)

    all_results = {}

    for cap_c in CAP_C_VALUES:
        caps = {t: round(cap_c * r, 4) for t, r in TYPE_RATIOS.items()}
        label = f"C={cap_c:.2f} (A={caps['A']:.2f}, B={caps['B']:.2f}, D={caps['D']:.2f})"
        print(f"\n{'─' * 70}")
        print(f"  [{label}] 백테스트 시작...")
        print(f"{'─' * 70}")

        t0 = time.time()
        result, avg_pos, avg_cash, act_cnt = run_single(cap_c)
        elapsed = time.time() - t0

        s = result["summary"]
        v = result["validation"]
        all_results[str(cap_c)] = {
            "cap_c": cap_c,
            "caps": {t: round(cap_c * r, 4) for t, r in TYPE_RATIOS.items()},
            "cagr": s["cagr_s"],
            "cagr_bench": s["cagr_b"],
            "excess": s["excess"],
            "sharpe": s["sh_s"],
            "mdd": s["mdd_s"],
            "alpha": s["alpha"],
            "beta": s["beta"],
            "info_ratio": s["ir"],
            "win_rate": s["wr"],
            "avg_positions": round(avg_pos, 1),
            "avg_cash_pct": round(avg_cash * 100, 1),
            "trades": s.get("trades", 0),
            "is_sharpe": v["is_sh"],
            "oos_sharpe": v["oos_sh"],
            "sharpe_diff": v["sh_diff"],
            "actions": act_cnt,
            "elapsed_sec": round(elapsed, 1),
        }
        print(f"  → CAGR: {s['cagr_s']:+.2f}%, Sharpe: {s['sh_s']:.3f}, "
              f"MDD: {s['mdd_s']:.2f}%, Pos: {avg_pos:.1f}, Cash: {avg_cash*100:.1f}%, "
              f"Time: {elapsed:.1f}s")

    # ── 결과 비교 테이블 ──
    print("\n" + "=" * 70)
    print("  Sweep 결과 비교")
    print("=" * 70)

    baseline = all_results[str(BASELINE_CAP_C)]

    header = f"{'CAP_C':>6} │ {'CAGR':>8} {'Sharpe':>8} {'MDD':>8} {'Alpha':>8} {'Beta':>6} {'Pos':>5} {'Cash':>6} │ {'Δ CAGR':>8} {'Δ Sharpe':>9}"
    print(header)
    print("─" * len(header))

    for cap_c in CAP_C_VALUES:
        r = all_results[str(cap_c)]
        d_cagr = r["cagr"] - baseline["cagr"]
        d_sharpe = r["sharpe"] - baseline["sharpe"]
        marker = " ◄ baseline" if cap_c == BASELINE_CAP_C else ""
        print(f"{r['cap_c']:>6.2f} │ {r['cagr']:>+7.2f}% {r['sharpe']:>7.3f} {r['mdd']:>7.2f}% "
              f"{r['alpha']:>+7.2f}% {r['beta']:>6.3f} {r['avg_positions']:>5.1f} {r['avg_cash_pct']:>5.1f}% │ "
              f"{d_cagr:>+7.2f}% {d_sharpe:>+8.3f}{marker}")

    # ── 민감도 판정 ──
    cagrs = [all_results[str(c)]["cagr"] for c in CAP_C_VALUES]
    sharpes = [all_results[str(c)]["sharpe"] for c in CAP_C_VALUES]
    cagr_range = max(cagrs) - min(cagrs)
    sharpe_range = max(sharpes) - min(sharpes)
    cagr_pct = (cagr_range / abs(baseline["cagr"]) * 100) if baseline["cagr"] != 0 else 0
    sharpe_pct = (sharpe_range / baseline["sharpe"] * 100) if baseline["sharpe"] != 0 else 0

    print(f"\n  CAGR 범위:   {min(cagrs):+.2f}% ~ {max(cagrs):+.2f}%  (변동폭: {cagr_range:.2f}%, 기준대비 {cagr_pct:.1f}%)")
    print(f"  Sharpe 범위: {min(sharpes):.3f} ~ {max(sharpes):.3f}  (변동폭: {sharpe_range:.3f}, 기준대비 {sharpe_pct:.1f}%)")

    positions = [all_results[str(c)]["avg_positions"] for c in CAP_C_VALUES]
    cashes = [all_results[str(c)]["avg_cash_pct"] for c in CAP_C_VALUES]
    print(f"  포지션 범위: {min(positions):.1f} ~ {max(positions):.1f}개")
    print(f"  현금비중 범위: {min(cashes):.1f}% ~ {max(cashes):.1f}%")

    if cagr_pct < 15 and sharpe_pct < 15:
        verdict = "ROBUST — 파라미터 로버스트 (과적합 우려 낮음)"
    elif cagr_pct > 30 or sharpe_pct > 30:
        verdict = "SENSITIVE — 파라미터 민감 (과적합 가능성 높음)"
    else:
        verdict = "MODERATE — 보통 수준 민감도 (추가 검증 필요)"

    print(f"\n  판정: {verdict}")

    # JSON 저장
    with open("sweep_type_cap_results.json", "w") as f:
        json.dump({
            "sweep_config": {
                "parameter": "TYPE_CAP_C",
                "values": CAP_C_VALUES,
                "type_ratios": TYPE_RATIOS,
                "baseline": BASELINE_CAP_C,
                "base_weight": 0.07,
                "scenario": "main (2013-2024)",
            },
            "results": all_results,
            "sensitivity": {
                "cagr_range": round(cagr_range, 4),
                "sharpe_range": round(sharpe_range, 4),
                "cagr_pct_change": round(cagr_pct, 2),
                "sharpe_pct_change": round(sharpe_pct, 2),
                "verdict": verdict,
            },
        }, f, indent=2, ensure_ascii=False)

    print(f"\n  결과 저장: sweep_type_cap_results.json")
    print("=" * 70)


if __name__ == "__main__":
    main()
