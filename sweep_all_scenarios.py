#!/usr/bin/env python3
"""
멀티 시나리오 Sweep — BUY 임계값 + TYPE_CAP (dotcom & subprime)
메인 시나리오는 기존 sweep 결과(sweep_results.json, sweep_type_cap_results.json) 사용.
"""
import json
import os
import sys
import time
import importlib

# ── Sweep 설정 ──
BUY_VALUES = [54, 57, 60, 63, 66]
BASELINE_BUY = 60

CAP_C_VALUES = [0.06, 0.08, 0.10, 0.12, 0.14]
BASELINE_CAP_C = 0.10
TYPE_RATIOS = {"A": 1.5, "B": 1.2, "C": 1.0, "D": 2.0}

SCENARIOS = [
    {"key": "dotcom",   "data_dir": "data_dotcom",   "label": "Dotcom (1994-2006)"},
    {"key": "subprime", "data_dir": "data_subprime", "label": "Subprime (2002-2014)"},
]


def _reload_modules():
    """data_loader, quant_alpha, backtest_engine을 재로드하여 환경 변경 반영."""
    import data_loader
    importlib.reload(data_loader)
    data_loader.clear_caches()

    import quant_alpha_v3_4_1_phase1 as qa
    importlib.reload(qa)

    import backtest_engine as be
    importlib.reload(be)

    return data_loader, qa, be


def run_buy_single(buy_thresh: int, hold_thresh: int) -> dict:
    """단일 BUY/HOLD 임계값 조합으로 백테스트 실행."""
    dl, qa, be = _reload_modules()
    qa.SCORE_BUY_THRESHOLD = buy_thresh
    qa.SCORE_HOLD_THRESHOLD = hold_thresh

    engine = be.BacktestEngine(100_000_000)
    engine.run()
    return engine.results()


def run_cap_single(cap_c: float):
    """단일 TYPE_CAP 조합으로 백테스트 실행."""
    dl, qa, be = _reload_modules()
    for t, ratio in TYPE_RATIOS.items():
        qa.TYPE_CAPS[t] = round(cap_c * ratio, 4)

    engine = be.BacktestEngine(100_000_000)
    engine.run()
    result = engine.results()

    avg_pos = sum(engine.pos_h) / len(engine.pos_h) if engine.pos_h else 0
    avg_cash = sum(engine.cash_h) / len(engine.cash_h) if engine.cash_h else 0
    return result, avg_pos, avg_cash, dict(engine.act_cnt)


def sensitivity_verdict(cagr_pct, sharpe_pct):
    if cagr_pct < 15 and sharpe_pct < 15:
        return "ROBUST — 파라미터 로버스트 (과적합 우려 낮음)"
    elif cagr_pct > 30 or sharpe_pct > 30:
        return "SENSITIVE — 파라미터 민감 (과적합 가능성 높음)"
    return "MODERATE — 보통 수준 민감도 (추가 검증 필요)"


def run_buy_sweep(scenario):
    """BUY 임계값 sweep 실행."""
    label = scenario["label"]
    print(f"\n{'=' * 70}")
    print(f"  BUY Threshold Sweep — {label}")
    print(f"{'=' * 70}")

    all_results = {}
    for buy_val in BUY_VALUES:
        hold_val = round(buy_val * 0.75)
        print(f"\n  [{label}] BUY={buy_val}/HOLD={hold_val} 백테스트 시작...")

        t0 = time.time()
        result = run_buy_single(buy_val, hold_val)
        elapsed = time.time() - t0

        s = result["summary"]
        all_results[buy_val] = {
            "buy": buy_val, "hold": hold_val,
            "cagr": s["cagr_s"], "cagr_bench": s["cagr_b"], "excess": s["excess"],
            "sharpe": s["sh_s"], "mdd": s["mdd_s"],
            "alpha": s["alpha"], "beta": s["beta"],
            "info_ratio": s["ir"], "win_rate": s["wr"],
            "elapsed_sec": round(elapsed, 1),
        }
        print(f"  -> CAGR: {s['cagr_s']:+.2f}%, Sharpe: {s['sh_s']:.3f}, "
              f"MDD: {s['mdd_s']:.2f}%, Time: {elapsed:.1f}s")

    # 비교 테이블
    baseline = all_results[BASELINE_BUY]
    print(f"\n  {'BUY':>5} {'HOLD':>5} | {'CAGR':>8} {'Sharpe':>8} {'MDD':>8} {'Alpha':>8} | {'d CAGR':>8} {'d Sharpe':>9}")
    print("  " + "-" * 75)
    for buy_val in BUY_VALUES:
        r = all_results[buy_val]
        d_cagr = r["cagr"] - baseline["cagr"]
        d_sharpe = r["sharpe"] - baseline["sharpe"]
        marker = " << baseline" if buy_val == BASELINE_BUY else ""
        print(f"  {r['buy']:>5} {r['hold']:>5} | {r['cagr']:>+7.2f}% {r['sharpe']:>7.3f} "
              f"{r['mdd']:>7.2f}% {r['alpha']:>+7.2f}% | {d_cagr:>+7.2f}% {d_sharpe:>+8.3f}{marker}")

    # 민감도
    cagrs = [all_results[b]["cagr"] for b in BUY_VALUES]
    sharpes = [all_results[b]["sharpe"] for b in BUY_VALUES]
    cagr_range = max(cagrs) - min(cagrs)
    sharpe_range = max(sharpes) - min(sharpes)
    cagr_pct = (cagr_range / abs(baseline["cagr"]) * 100) if baseline["cagr"] != 0 else 0
    sharpe_pct = (sharpe_range / baseline["sharpe"] * 100) if baseline["sharpe"] != 0 else 0
    verdict = sensitivity_verdict(cagr_pct, sharpe_pct)

    print(f"\n  CAGR 범위: {min(cagrs):+.2f}% ~ {max(cagrs):+.2f}% (변동폭: {cagr_range:.2f}%, {cagr_pct:.1f}%)")
    print(f"  Sharpe 범위: {min(sharpes):.3f} ~ {max(sharpes):.3f} (변동폭: {sharpe_range:.3f}, {sharpe_pct:.1f}%)")
    print(f"  판정: {verdict}")

    return {
        "sweep_config": {
            "parameter": "SCORE_BUY_THRESHOLD",
            "values": BUY_VALUES, "hold_ratio": 0.75,
            "baseline": BASELINE_BUY,
            "scenario": label,
        },
        "results": all_results,
        "sensitivity": {
            "cagr_range": round(cagr_range, 4),
            "sharpe_range": round(sharpe_range, 4),
            "cagr_pct_change": round(cagr_pct, 2),
            "sharpe_pct_change": round(sharpe_pct, 2),
            "verdict": verdict,
        },
    }


def run_type_cap_sweep(scenario):
    """TYPE_CAP sweep 실행."""
    label = scenario["label"]
    print(f"\n{'=' * 70}")
    print(f"  TYPE_CAP Sweep — {label}")
    print(f"{'=' * 70}")

    all_results = {}
    for cap_c in CAP_C_VALUES:
        caps = {t: round(cap_c * r, 4) for t, r in TYPE_RATIOS.items()}
        print(f"\n  [{label}] C={cap_c:.2f} (A={caps['A']:.2f}, B={caps['B']:.2f}, D={caps['D']:.2f}) 시작...")

        t0 = time.time()
        result, avg_pos, avg_cash, act_cnt = run_cap_single(cap_c)
        elapsed = time.time() - t0

        s = result["summary"]
        v = result["validation"]
        all_results[str(cap_c)] = {
            "cap_c": cap_c,
            "caps": caps,
            "cagr": s["cagr_s"], "cagr_bench": s["cagr_b"], "excess": s["excess"],
            "sharpe": s["sh_s"], "mdd": s["mdd_s"],
            "alpha": s["alpha"], "beta": s["beta"],
            "info_ratio": s["ir"], "win_rate": s["wr"],
            "avg_positions": round(avg_pos, 1),
            "avg_cash_pct": round(avg_cash * 100, 1),
            "trades": s.get("trades", 0),
            "is_sharpe": v["is_sh"], "oos_sharpe": v["oos_sh"], "sharpe_diff": v["sh_diff"],
            "actions": act_cnt,
            "elapsed_sec": round(elapsed, 1),
        }
        print(f"  -> CAGR: {s['cagr_s']:+.2f}%, Sharpe: {s['sh_s']:.3f}, "
              f"MDD: {s['mdd_s']:.2f}%, Pos: {avg_pos:.1f}, Cash: {avg_cash*100:.1f}%, "
              f"Time: {elapsed:.1f}s")

    # 비교 테이블
    baseline = all_results[str(BASELINE_CAP_C)]
    print(f"\n  {'CAP_C':>6} | {'CAGR':>8} {'Sharpe':>8} {'MDD':>8} {'Alpha':>8} {'Pos':>5} {'Cash':>6} | {'d CAGR':>8} {'d Sharpe':>9}")
    print("  " + "-" * 80)
    for cap_c in CAP_C_VALUES:
        r = all_results[str(cap_c)]
        d_cagr = r["cagr"] - baseline["cagr"]
        d_sharpe = r["sharpe"] - baseline["sharpe"]
        marker = " << baseline" if cap_c == BASELINE_CAP_C else ""
        print(f"  {r['cap_c']:>6.2f} | {r['cagr']:>+7.2f}% {r['sharpe']:>7.3f} {r['mdd']:>7.2f}% "
              f"{r['alpha']:>+7.2f}% {r['avg_positions']:>5.1f} {r['avg_cash_pct']:>5.1f}% | "
              f"{d_cagr:>+7.2f}% {d_sharpe:>+8.3f}{marker}")

    # 민감도
    cagrs = [all_results[str(c)]["cagr"] for c in CAP_C_VALUES]
    sharpes = [all_results[str(c)]["sharpe"] for c in CAP_C_VALUES]
    cagr_range = max(cagrs) - min(cagrs)
    sharpe_range = max(sharpes) - min(sharpes)
    cagr_pct = (cagr_range / abs(baseline["cagr"]) * 100) if baseline["cagr"] != 0 else 0
    sharpe_pct = (sharpe_range / baseline["sharpe"] * 100) if baseline["sharpe"] != 0 else 0
    verdict = sensitivity_verdict(cagr_pct, sharpe_pct)

    print(f"\n  CAGR 범위: {min(cagrs):+.2f}% ~ {max(cagrs):+.2f}% (변동폭: {cagr_range:.2f}%, {cagr_pct:.1f}%)")
    print(f"  Sharpe 범위: {min(sharpes):.3f} ~ {max(sharpes):.3f} (변동폭: {sharpe_range:.3f}, {sharpe_pct:.1f}%)")
    print(f"  판정: {verdict}")

    return {
        "sweep_config": {
            "parameter": "TYPE_CAP_C",
            "values": CAP_C_VALUES,
            "type_ratios": TYPE_RATIOS,
            "baseline": BASELINE_CAP_C,
            "base_weight": 0.07,
            "scenario": label,
        },
        "results": all_results,
        "sensitivity": {
            "cagr_range": round(cagr_range, 4),
            "sharpe_range": round(sharpe_range, 4),
            "cagr_pct_change": round(cagr_pct, 2),
            "sharpe_pct_change": round(sharpe_pct, 2),
            "verdict": verdict,
        },
    }


def print_cross_scenario_summary(all_data):
    """3개 시나리오 통합 비교 (메인 결과 포함)."""
    print("\n" + "=" * 70)
    print("  Cross-Scenario Summary — BUY Threshold Sweep")
    print("=" * 70)

    # 메인 결과 로드 시도
    main_buy = None
    if os.path.exists("sweep_results.json"):
        with open("sweep_results.json") as f:
            main_buy = json.load(f)

    print(f"\n  {'Scenario':<25} | {'CAGR Range':>12} {'Sharpe Range':>14} | {'Verdict'}")
    print("  " + "-" * 80)

    if main_buy:
        s = main_buy["sensitivity"]
        print(f"  {'Main (2013-2024)':<25} | {s['cagr_range']:>10.2f}%  {s['sharpe_range']:>12.3f}  | {s['verdict']}")

    for sc in SCENARIOS:
        key = sc["key"]
        if f"buy_{key}" in all_data:
            s = all_data[f"buy_{key}"]["sensitivity"]
            print(f"  {sc['label']:<25} | {s['cagr_range']:>10.2f}%  {s['sharpe_range']:>12.3f}  | {s['verdict']}")

    print(f"\n{'=' * 70}")
    print("  Cross-Scenario Summary — TYPE_CAP Sweep")
    print("=" * 70)

    main_cap = None
    if os.path.exists("sweep_type_cap_results.json"):
        with open("sweep_type_cap_results.json") as f:
            main_cap = json.load(f)

    print(f"\n  {'Scenario':<25} | {'CAGR Range':>12} {'Sharpe Range':>14} | {'Verdict'}")
    print("  " + "-" * 80)

    if main_cap:
        s = main_cap["sensitivity"]
        print(f"  {'Main (2013-2024)':<25} | {s['cagr_range']:>10.2f}%  {s['sharpe_range']:>12.3f}  | {s['verdict']}")

    for sc in SCENARIOS:
        key = sc["key"]
        if f"cap_{key}" in all_data:
            s = all_data[f"cap_{key}"]["sensitivity"]
            print(f"  {sc['label']:<25} | {s['cagr_range']:>10.2f}%  {s['sharpe_range']:>12.3f}  | {s['verdict']}")

    print("=" * 70)


def main():
    total_start = time.time()
    print("=" * 70)
    print("  Multi-Scenario Sweep — BUY Threshold + TYPE_CAP")
    print("  Scenarios: Dotcom (1994-2006), Subprime (2002-2014)")
    print("=" * 70)

    all_data = {}

    for scenario in SCENARIOS:
        key = scenario["key"]
        data_dir = scenario["data_dir"]
        label = scenario["label"]

        print(f"\n\n{'#' * 70}")
        print(f"  SCENARIO: {label}  (BACKTEST_DATA={data_dir})")
        print(f"{'#' * 70}")

        # 시나리오 전환: 환경변수 설정
        os.environ["BACKTEST_DATA"] = data_dir

        # ── BUY Threshold Sweep ──
        buy_data = run_buy_sweep(scenario)
        all_data[f"buy_{key}"] = buy_data

        fname = f"sweep_results_{key}.json"
        with open(fname, "w") as f:
            json.dump(buy_data, f, indent=2, ensure_ascii=False)
        print(f"\n  결과 저장: {fname}")

        # ── TYPE_CAP Sweep ──
        cap_data = run_type_cap_sweep(scenario)
        all_data[f"cap_{key}"] = cap_data

        fname = f"sweep_type_cap_results_{key}.json"
        with open(fname, "w") as f:
            json.dump(cap_data, f, indent=2, ensure_ascii=False)
        print(f"\n  결과 저장: {fname}")

    # 환경 복원
    os.environ.pop("BACKTEST_DATA", None)

    # ── 시나리오 간 비교 ──
    print_cross_scenario_summary(all_data)

    # 통합 JSON 저장
    summary = {
        "scenarios": {sc["key"]: sc["label"] for sc in SCENARIOS},
        "buy_sweep": {},
        "type_cap_sweep": {},
    }
    for sc in SCENARIOS:
        k = sc["key"]
        if f"buy_{k}" in all_data:
            summary["buy_sweep"][k] = all_data[f"buy_{k}"]["sensitivity"]
        if f"cap_{k}" in all_data:
            summary["type_cap_sweep"][k] = all_data[f"cap_{k}"]["sensitivity"]

    # 메인 결과 포함
    if os.path.exists("sweep_results.json"):
        with open("sweep_results.json") as f:
            summary["buy_sweep"]["main"] = json.load(f)["sensitivity"]
    if os.path.exists("sweep_type_cap_results.json"):
        with open("sweep_type_cap_results.json") as f:
            summary["type_cap_sweep"]["main"] = json.load(f)["sensitivity"]

    with open("sweep_all_scenarios_summary.json", "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    total_elapsed = time.time() - total_start
    print(f"\n  총 소요시간: {total_elapsed:.1f}s")
    print(f"  통합 결과 저장: sweep_all_scenarios_summary.json")
    print("=" * 70)


if __name__ == "__main__":
    main()
