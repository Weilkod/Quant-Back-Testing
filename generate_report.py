#!/usr/bin/env python3
"""Generate sweep_multi_scenario_report.html from JSON result files."""
import json, os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Load all JSON data
with open("sweep_results.json") as f: main_buy = json.load(f)
with open("sweep_results_dotcom.json") as f: dot_buy = json.load(f)
with open("sweep_results_subprime.json") as f: sub_buy = json.load(f)
with open("sweep_type_cap_results.json") as f: main_tc = json.load(f)
with open("sweep_type_cap_results_dotcom.json") as f: dot_tc = json.load(f)
with open("sweep_type_cap_results_subprime.json") as f: sub_tc = json.load(f)

CSS = """
:root{--bg:#f8f9fa;--card:#fff;--text:#1a1a2e;--text2:#555;--border:#dee2e6;--accent:#2563eb;--red:#dc2626;--green:#16a34a;--orange:#d97706;--yellow-bg:#fef3c7;--red-bg:#fee2e2;--green-bg:#dcfce7;--blue-bg:#dbeafe}
@media(prefers-color-scheme:dark){:root{--bg:#0f172a;--card:#1e293b;--text:#e2e8f0;--text2:#94a3b8;--border:#334155;--accent:#60a5fa;--red:#f87171;--green:#4ade80;--orange:#fbbf24;--yellow-bg:#422006;--red-bg:#450a0a;--green-bg:#052e16;--blue-bg:#172554}}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:var(--bg);color:var(--text);line-height:1.6;padding:20px}
.container{max-width:1200px;margin:0 auto}
h1{font-size:1.6rem;margin-bottom:4px}
h2{font-size:1.2rem;margin:30px 0 12px;padding-bottom:6px;border-bottom:2px solid var(--accent);color:var(--accent)}
h3{font-size:1rem;margin:18px 0 8px;color:var(--text)}
.subtitle{color:var(--text2);font-size:0.85rem;margin-bottom:24px}
.card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:20px;margin-bottom:20px}
table{width:100%;border-collapse:collapse;font-size:0.85rem;margin:10px 0}
th{background:var(--accent);color:#fff;padding:8px 10px;text-align:center;font-weight:600;white-space:nowrap}
td{padding:7px 10px;border-bottom:1px solid var(--border);text-align:center}
tr:nth-child(even){background:rgba(0,0,0,.03)}
@media(prefers-color-scheme:dark){tr:nth-child(even){background:rgba(255,255,255,.03)}}
tr:hover{background:rgba(37,99,235,.08)}
tr.baseline{background:var(--blue-bg) !important;font-weight:600}
.pos{color:var(--green)}.neg{color:var(--red)}.warn{color:var(--orange)}
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:0.75rem;font-weight:600}
.badge-red{background:var(--red-bg);color:var(--red)}
.badge-green{background:var(--green-bg);color:var(--green)}
.badge-yellow{background:var(--yellow-bg);color:var(--orange)}
.badge-blue{background:var(--blue-bg);color:var(--accent)}
.highlight-box{border-left:4px solid var(--accent);background:var(--blue-bg);padding:16px 20px;border-radius:0 8px 8px 0;margin:16px 0}
.highlight-box.warning{border-left-color:var(--orange);background:var(--yellow-bg)}
.highlight-box.danger{border-left-color:var(--red);background:var(--red-bg)}
.highlight-box.success{border-left-color:var(--green);background:var(--green-bg)}
.highlight-box h4{margin-bottom:6px;font-size:0.95rem}
.highlight-box p,.highlight-box li{font-size:0.85rem;color:var(--text2)}
.highlight-box ul{margin:6px 0 0 18px}
.mono{font-family:'SF Mono',Consolas,monospace;font-size:0.8rem}
.metric-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin:12px 0}
.metric-item{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:12px;text-align:center}
.metric-item .label{font-size:0.75rem;color:var(--text2);margin-bottom:4px}
.metric-item .value{font-size:1.3rem;font-weight:700}
.flow-diagram{display:flex;align-items:center;justify-content:center;gap:8px;flex-wrap:wrap;margin:14px 0;font-size:0.8rem}
.flow-box{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:8px 14px;text-align:center}
.flow-arrow{color:var(--text2);font-size:1.2rem}
@media(max-width:700px){.metric-grid{grid-template-columns:1fr 1fr}table{font-size:0.75rem}td,th{padding:5px 6px}}
"""

def badge(verdict):
    if "ROBUST" in verdict:
        return '<span class="badge badge-green">ROBUST</span>'
    elif "MODERATE" in verdict:
        return '<span class="badge badge-yellow">MODERATE</span>'
    else:
        return '<span class="badge badge-red">SENSITIVE</span>'

def sign(v):
    return f"+{v}" if v > 0 else str(v)

def color_class(v):
    if v > 0: return "pos"
    if v < 0: return "neg"
    return ""

def buy_table(data, scenario_name):
    results = data["results"]
    sens = data["sensitivity"]
    baseline = str(data["sweep_config"]["baseline"])
    rows = ""
    for k in ["54","57","60","63","66"]:
        r = results[k]
        bl = 'class="baseline"' if k == baseline else ""
        dcagr = r["cagr"] - results[baseline]["cagr"]
        dsharpe = r["sharpe"] - results[baseline]["sharpe"]
        rows += f"""<tr {bl}>
<td>{r['buy']}</td><td>{r['hold']}</td>
<td>{sign(r['cagr'])}%</td><td>{r['sharpe']:.3f}</td><td>{r['mdd']:.2f}%</td>
<td>{sign(r['alpha'])}%</td><td>{r['beta']:.3f}</td><td>{r['info_ratio']:.3f}</td><td>{r['win_rate']:.1f}%</td>
<td class="{color_class(dcagr)}">{dcagr:+.2f}%</td>
<td class="{color_class(dsharpe)}">{dsharpe:+.3f}</td>
</tr>"""

    return f"""<div class="card">
<h3>{scenario_name} {badge(sens['verdict'])}</h3>
<table>
<tr><th>BUY</th><th>HOLD</th><th>CAGR</th><th>Sharpe</th><th>MDD</th><th>Alpha</th><th>Beta</th><th>IR</th><th>Win Rate</th><th>&Delta;CAGR</th><th>&Delta;Sharpe</th></tr>
{rows}
</table>
<p style="font-size:0.8rem;color:var(--text2);margin-top:8px">
CAGR 변동폭: <strong>{sens['cagr_range']:.2f}%p</strong> ({sens['cagr_pct_change']:.2f}%) | Sharpe 변동폭: <strong>{sens['sharpe_range']:.3f}</strong> ({sens['sharpe_pct_change']:.2f}%)
</p></div>"""


def tc_table(data, scenario_name):
    results = data["results"]
    sens = data["sensitivity"]
    baseline = str(data["sweep_config"]["baseline"])
    rows = ""
    for k in ["0.06","0.08","0.1","0.12","0.14"]:
        r = results[k]
        bl = 'class="baseline"' if k == baseline else ""
        dcagr = r["cagr"] - results[baseline]["cagr"]
        dsharpe = r["sharpe"] - results[baseline]["sharpe"]
        caps = r["caps"]
        cap_str = f'A:{caps["A"]:.0%} B:{caps["B"]:.1%} C:{caps["C"]:.0%} D:{caps["D"]:.0%}'
        avg_pos = r.get("avg_positions", "-")
        avg_cash = r.get("avg_cash_pct", "-")
        is_sh = r.get("is_sharpe", "-")
        oos_sh = r.get("oos_sharpe", "-")
        rows += f"""<tr {bl}>
<td>{r['cap_c']:.0%}</td><td style="font-size:0.75rem">{cap_str}</td>
<td>{sign(r['cagr'])}%</td><td>{r['sharpe']:.3f}</td><td>{r['mdd']:.2f}%</td>
<td>{avg_cash if isinstance(avg_cash, str) else f'{avg_cash:.1f}'}%</td>
<td>{is_sh if isinstance(is_sh, str) else f'{is_sh:.3f}'}</td>
<td>{oos_sh if isinstance(oos_sh, str) else f'{oos_sh:.3f}'}</td>
<td class="{color_class(dcagr)}">{dcagr:+.2f}%</td>
<td class="{color_class(dsharpe)}">{dsharpe:+.3f}</td>
</tr>"""

    return f"""<div class="card">
<h3>{scenario_name} {badge(sens['verdict'])}</h3>
<table>
<tr><th>CAP_C</th><th>All Caps</th><th>CAGR</th><th>Sharpe</th><th>MDD</th><th>Cash%</th><th>IS Sharpe</th><th>OOS Sharpe</th><th>&Delta;CAGR</th><th>&Delta;Sharpe</th></tr>
{rows}
</table>
<p style="font-size:0.8rem;color:var(--text2);margin-top:8px">
CAGR 변동폭: <strong>{sens['cagr_range']:.2f}%p</strong> ({sens['cagr_pct_change']:.2f}%) | Sharpe 변동폭: <strong>{sens['sharpe_range']:.3f}</strong> ({sens['sharpe_pct_change']:.2f}%)
</p></div>"""


# Build cross-scenario summary tables
def cross_summary_table(datasets, param_name):
    rows = ""
    for name, data in datasets:
        s = data["sensitivity"]
        rows += f"""<tr>
<td>{name}</td>
<td>{s['cagr_range']:.2f}%p</td><td>{s['sharpe_range']:.3f}</td>
<td>{s['cagr_pct_change']:.2f}%</td><td>{s['sharpe_pct_change']:.2f}%</td>
<td>{badge(s['verdict'])}</td>
</tr>"""
    return f"""<table>
<tr><th>Scenario</th><th>CAGR Range</th><th>Sharpe Range</th><th>CAGR%</th><th>Sharpe%</th><th>Verdict</th></tr>
{rows}
</table>"""


html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Multi-Scenario Sweep 종합 보고서</title>
<style>{CSS}</style>
</head>
<body>
<div class="container">

<h1>Multi-Scenario Parameter Sweep 종합 보고서</h1>
<p class="subtitle">Quant-Alpha v3.5 | BUY Threshold + TYPE_CAP Sweep | 3 Scenarios | 2026-04-06</p>

<!-- ============================================================ -->
<h2>Executive Summary</h2>
<!-- ============================================================ -->
<div class="card">
<div class="metric-grid">
<div class="metric-item">
<div class="label">총 백테스트 수</div>
<div class="value">30</div>
</div>
<div class="metric-item">
<div class="label">시나리오</div>
<div class="value">3</div>
</div>
<div class="metric-item">
<div class="label">Sweep 파라미터</div>
<div class="value">2</div>
</div>
<div class="metric-item">
<div class="label">BUY Threshold</div>
<div class="value pos">ROBUST</div>
</div>
<div class="metric-item">
<div class="label">TYPE_CAP</div>
<div class="value warn">MIXED</div>
</div>
</div>

<div class="highlight-box success">
<h4>BUY Threshold: 3개 시나리오 모두 ROBUST</h4>
<p>SCORE_BUY_THRESHOLD [54-66] 변경 시 CAGR 변동 최대 2.01%, Sharpe 변동 최대 13.11%. 
Constraint 5 (총 비중 70% 상한)가 개별 종목 비중 차이를 정규화하여 파라미터 변경의 영향을 흡수.</p>
</div>

<div class="highlight-box warning">
<h4>TYPE_CAP: 메인은 ROBUST, 닷컴/서브프라임은 SENSITIVE</h4>
<ul>
<li>메인 (2013-2024): CAGR 11.52%, Sharpe 9.77% &rarr; <strong>ROBUST</strong></li>
<li>닷컴 (1994-2006): CAGR 41.07%, Sharpe 22.93% &rarr; <strong>SENSITIVE</strong></li>
<li>서브프라임 (2002-2014): CAGR 9.33%, Sharpe 54.10% &rarr; <strong>SENSITIVE</strong></li>
</ul>
<p style="margin-top:8px">TYPE_CAP은 현금 배분 비율을 직접 제어하므로 시장 환경에 따라 민감도가 크게 달라진다.</p>
</div>
</div>

<!-- ============================================================ -->
<h2>1. 테스트 설정</h2>
<!-- ============================================================ -->
<div class="card">
<table>
<tr><th>항목</th><th>BUY Threshold Sweep</th><th>TYPE_CAP Sweep</th></tr>
<tr><td>파라미터</td><td><code>SCORE_BUY_THRESHOLD</code></td><td><code>TYPE_CAP_C</code> (기준 캡)</td></tr>
<tr><td>Sweep 값</td><td>54, 57, <strong>60</strong>, 63, 66</td><td>6%, 8%, <strong>10%</strong>, 12%, 14%</td></tr>
<tr><td>연동 규칙</td><td>HOLD = round(BUY &times; 0.75)</td><td>A=1.5x, B=1.2x, C=1.0x, D=2.0x</td></tr>
<tr><td>BASE_WEIGHT</td><td colspan="2">0.07 (7%)</td></tr>
<tr><td>초기 자본</td><td colspan="2">$100,000,000</td></tr>
</table>

<h3>시나리오 구성</h3>
<table>
<tr><th>시나리오</th><th>기간</th><th>특성</th><th>데이터 경로</th></tr>
<tr><td>메인</td><td>2013 ~ 2024</td><td>현대 강세장 + COVID 폭락</td><td>data/</td></tr>
<tr><td>닷컴</td><td>1994 ~ 2006</td><td>IT 버블 &rarr; 붕괴 &rarr; 회복</td><td>data_dotcom/</td></tr>
<tr><td>서브프라임</td><td>2002 ~ 2014</td><td>금융위기 &rarr; 회복</td><td>data_subprime/</td></tr>
</table>
</div>

<!-- ============================================================ -->
<h2>2. BUY Threshold Sweep 결과</h2>
<!-- ============================================================ -->

{buy_table(main_buy, "메인 시나리오 (2013-2024)")}
{buy_table(dot_buy, "닷컴 시나리오 (1994-2006)")}
{buy_table(sub_buy, "서브프라임 시나리오 (2002-2014)")}

<div class="card">
<h3>BUY Threshold Cross-Scenario 비교</h3>
{cross_summary_table([
    ("메인 (2013-2024)", main_buy),
    ("닷컴 (1994-2006)", dot_buy),
    ("서브프라임 (2002-2014)", sub_buy),
], "BUY")}

<div class="highlight-box success">
<h4>결론: BUY Threshold는 과적합 우려 없음</h4>
<p>3개 시나리오 모두에서 CAGR 변동이 2%p 이내, Sharpe 변동이 0.05 이내로 매우 안정적.
이는 <code>manage_portfolio()</code>의 Constraint 5 (총 비중 70% 상한)가 
개별 종목의 매수 점수 차이를 흡수하기 때문.</p>
</div>
</div>

<!-- ============================================================ -->
<h2>3. TYPE_CAP Sweep 결과</h2>
<!-- ============================================================ -->

{tc_table(main_tc, "메인 시나리오 (2013-2024)")}
{tc_table(dot_tc, "닷컴 시나리오 (1994-2006)")}
{tc_table(sub_tc, "서브프라임 시나리오 (2002-2014)")}

<div class="card">
<h3>TYPE_CAP Cross-Scenario 비교</h3>
{cross_summary_table([
    ("메인 (2013-2024)", main_tc),
    ("닷컴 (1994-2006)", dot_tc),
    ("서브프라임 (2002-2014)", sub_tc),
], "TYPE_CAP")}

<div class="highlight-box warning">
<h4>결론: TYPE_CAP은 시나리오별 민감도 차이 존재</h4>
<ul>
<li><strong>메인</strong>: CAP_C 0.10 이상에서 CAGR/Sharpe 수렴 &rarr; ROBUST</li>
<li><strong>닷컴</strong>: CAP_C가 클수록 CAGR 단조 증가 (6.49% &rarr; 9.96%), MDD도 악화 (-11% &rarr; -20%) &rarr; SENSITIVE</li>
<li><strong>서브프라임</strong>: CAGR 절대 변동폭은 작지만(0.42%p), Sharpe 기저가 낮아 비율 변동 54% &rarr; SENSITIVE</li>
</ul>
</div>
</div>

<!-- ============================================================ -->
<h2>4. IS/OOS Sharpe 분석 (TYPE_CAP)</h2>
<!-- ============================================================ -->
<div class="card">
<h3>In-Sample vs Out-of-Sample Sharpe 비교</h3>
<table>
<tr><th>Scenario</th><th>CAP_C</th><th>IS Sharpe</th><th>OOS Sharpe</th><th>Gap</th><th>평가</th></tr>"""

# IS/OOS rows for all scenarios with TYPE_CAP
for sname, data in [("메인", main_tc), ("닷컴", dot_tc), ("서브프라임", sub_tc)]:
    for k in ["0.06","0.08","0.1","0.12","0.14"]:
        r = data["results"][k]
        iss = r.get("is_sharpe", 0)
        ooss = r.get("oos_sharpe", 0)
        gap = r.get("sharpe_diff", 0)
        bl = ' class="baseline"' if k == "0.1" else ""
        if gap > 0.5:
            evalstr = '<span class="badge badge-yellow">Gap &gt; 0.5</span>'
        elif gap > 0.3:
            evalstr = '<span class="badge badge-blue">Moderate</span>'
        else:
            evalstr = '<span class="badge badge-green">Good</span>'
        html += f"""<tr{bl}><td>{sname}</td><td>{float(k):.0%}</td><td>{iss:.3f}</td><td>{ooss:.3f}</td><td>{gap:.3f}</td><td>{evalstr}</td></tr>"""

html += """</table>

<div class="highlight-box">
<h4>IS/OOS Gap 해석</h4>
<ul>
<li><strong>메인 시나리오</strong>: IS Sharpe 1.065~1.173 vs OOS 0.482~0.544. Gap 0.54~0.63으로 IS 과적합 징후 존재하나, OOS에서도 양의 Sharpe 유지.</li>
<li><strong>닷컴 시나리오</strong>: IS 0.516~0.589 vs OOS 0.262~0.547. CAP_C가 클수록 IS/OOS Gap이 줄어드는 긍정적 패턴.</li>
<li><strong>서브프라임 시나리오</strong>: IS가 음수(-0.109~-0.076)이나 OOS는 양수(0.342~0.415). IS 구간이 금융위기 직격 포함으로 전략 자체가 힘든 구간.</li>
</ul>
</div>
</div>

<!-- ============================================================ -->
<h2>5. 구조적 분석</h2>
<!-- ============================================================ -->
<div class="card">
<h3>5-1. BUY Threshold가 둔감한 이유</h3>
<div class="flow-diagram">
<div class="flow-box">BUY 점수<br>변경</div>
<div class="flow-arrow">&rarr;</div>
<div class="flow-box">매수 종목 수<br>소폭 변동</div>
<div class="flow-arrow">&rarr;</div>
<div class="flow-box">개별 비중<br>변동</div>
<div class="flow-arrow">&rarr;</div>
<div class="flow-box" style="border-color:var(--accent);border-width:2px"><strong>Constraint 5</strong><br>총 비중 70% 상한<br>정규화</div>
<div class="flow-arrow">&rarr;</div>
<div class="flow-box" style="background:var(--green-bg)">최종 성과<br>거의 동일</div>
</div>
<p style="font-size:0.85rem;color:var(--text2)">
<code>manage_portfolio()</code>의 Constraint 5는 총 주식 비중을 70%로 제한합니다.
BUY 임계값을 낮추면 더 많은 종목이 매수 신호를 받지만, 총 비중 상한에 의해
개별 종목 비중이 균등하게 축소되므로 전체 포트폴리오 성과는 거의 변하지 않습니다.
</p>

<h3 style="margin-top:24px">5-2. TYPE_CAP이 민감한 이유</h3>
<div class="flow-diagram">
<div class="flow-box">TYPE_CAP<br>변경</div>
<div class="flow-arrow">&rarr;</div>
<div class="flow-box">종목별 최대<br>비중 변동</div>
<div class="flow-arrow">&rarr;</div>
<div class="flow-box" style="border-color:var(--red);border-width:2px"><strong>Cash 비율</strong><br>직접 변동</div>
<div class="flow-arrow">&rarr;</div>
<div class="flow-box">시장 노출도<br>(Beta) 변동</div>
<div class="flow-arrow">&rarr;</div>
<div class="flow-box" style="background:var(--yellow-bg)">CAGR/MDD<br>Trade-off</div>
</div>
<p style="font-size:0.85rem;color:var(--text2)">
TYPE_CAP은 개별 종목의 최대 허용 비중을 제어합니다. CAP이 작으면 종목당 투입 가능한 금액이 줄어
현금 비율이 높아지고 (CAP_C=6%일 때 Cash 36~68%), 반대로 CAP이 크면 시장 노출이 커져
CAGR은 올라가지만 MDD도 악화됩니다.
</p>

<h3 style="margin-top:24px">5-3. 시나리오별 Cash 비율 변화</h3>
<table>
<tr><th>CAP_C</th><th>메인 Cash%</th><th>닷컴 Cash%</th><th>서브프라임 Cash%</th></tr>"""

for k in ["0.06","0.08","0.1","0.12","0.14"]:
    mc = main_tc["results"][k].get("avg_cash_pct", "-")
    dc = dot_tc["results"][k].get("avg_cash_pct", "-")
    sc = sub_tc["results"][k].get("avg_cash_pct", "-")
    html += f"""<tr{'class="baseline"' if k=="0.1" else ""}>
<td>{float(k):.0%}</td>
<td>{mc:.1f}%</td><td>{dc:.1f}%</td><td>{sc:.1f}%</td></tr>"""

html += """</table>
<p style="font-size:0.8rem;color:var(--text2);margin-top:8px">
CAP_C 6% &rarr; 14%로 갈수록 현금 비율이 급감. 닷컴/서브프라임은 데이터가 적어 변동이 더 크게 나타남.
</p>
</div>

<!-- ============================================================ -->
<h2>6. 종합 판정 및 권고</h2>
<!-- ============================================================ -->
<div class="card">
<div class="metric-grid">
<div class="metric-item">
<div class="label">BUY Threshold (3 scenarios)</div>
<div class="value pos">ROBUST</div>
</div>
<div class="metric-item">
<div class="label">TYPE_CAP - 메인</div>
<div class="value pos">ROBUST</div>
</div>
<div class="metric-item">
<div class="label">TYPE_CAP - 닷컴</div>
<div class="value neg">SENSITIVE</div>
</div>
<div class="metric-item">
<div class="label">TYPE_CAP - 서브프라임</div>
<div class="value neg">SENSITIVE</div>
</div>
</div>

<div class="highlight-box">
<h4>종합 판정</h4>
<ul>
<li><strong>BUY Threshold (SCORE_BUY_THRESHOLD = 60)</strong>: 3개 시나리오 모두 ROBUST. 현재 값 유지 권장. 과적합 우려 없음.</li>
<li><strong>TYPE_CAP (CAP_C = 10%)</strong>: 메인에서는 ROBUST이나, 닷컴/서브프라임에서 SENSITIVE. 이는 TYPE_CAP이 현금 비율을 직접 제어하는 구조적 특성에 기인하며, 특정 값에 과적합된 것이 아니라 파라미터 자체가 성과에 직접적 영향을 미치는 것.</li>
</ul>
</div>

<div class="highlight-box success">
<h4>권고 사항</h4>
<ul>
<li><strong>현재 설정 유지</strong>: BUY=60, TYPE_CAP_C=10%는 3개 시나리오 평균 성과와 리스크 밸런스 측면에서 적절.</li>
<li><strong>TYPE_CAP 모니터링</strong>: TYPE_CAP은 시장 환경별 Cash 비율에 직접 영향을 주므로, 시장 국면 전환 시 Cash 비율 모니터링 필요.</li>
<li><strong>CAP_C=10% 선택 근거</strong>: 닷컴에서는 더 높은 CAP이 유리하지만 MDD 악화(-20%), 서브프라임에서는 낮은 CAP이 MDD 방어에 유리(-13%). 10%는 중간 균형점.</li>
<li><strong>IS/OOS Gap 주시</strong>: 메인 시나리오에서 IS/OOS Sharpe Gap이 0.5 이상이므로, 향후 OOS 성과 모니터링 지속 필요.</li>
</ul>
</div>
</div>

<p style="text-align:center;color:var(--text2);font-size:0.75rem;margin-top:30px;padding-top:20px;border-top:1px solid var(--border)">
Generated by Quant-Alpha Sweep Analysis | 30 backtests across 3 market regimes | 2026-04-06
</p>

</div>
</body>
</html>"""

with open("sweep_multi_scenario_report.html", "w", encoding="utf-8") as f:
    f.write(html)

print(f"Report generated: sweep_multi_scenario_report.html ({len(html):,} bytes)")
