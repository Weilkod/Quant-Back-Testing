#!/usr/bin/env python3
"""
Quant-Alpha v3.4 — Backtesting Engine
네트워크 제한: 합성 시장 데이터 사용 (2019-01 ~ 2025-12)
시장 레짐, 팩터 프리미엄, 섹터 상관구조 반영
"""
import numpy as np
import json, math
from datetime import datetime, timedelta
from quant_alpha_v3_4_unified import (
    StockMetrics, GateResult, StopCheckResult, ActionResult,
    PortfolioCandidate, AdjustedPosition, PortfolioMetrics,
    evaluate_survival_gate, calculate_score, check_trailing_stop,
    determine_action, run_pipeline, calculate_raw_weight, manage_portfolio,
    evaluate_vix, calculate_macro_score, classify_regime,
    WEIGHTS, STOP_PCT_BY_REGIME, REGIME_NAMES, REGIME_EQUITY_CAP,
    SCORE_BUY_THRESHOLD, SCORE_HOLD_THRESHOLD, MAX_POSITIONS,
)
np.random.seed(2024)

SECTORS = ["Technology","Healthcare","Financials","ConsumerDisc","Industrials",
           "Communication","ConsumerStaples","Energy","Utilities","Materials","RealEstate"]
SECTOR_BETAS = {"Technology":1.25,"Healthcare":0.85,"Financials":1.15,"ConsumerDisc":1.20,
    "Industrials":1.05,"Communication":1.10,"ConsumerStaples":0.65,"Energy":1.30,
    "Utilities":0.55,"Materials":1.00,"RealEstate":0.90}
INDUSTRY_TYPES = ["A","B","C","D"]

def gen_dates(s="2019-01-02",e="2025-12-31"):
    dates=[];c=datetime.strptime(s,"%Y-%m-%d");ed=datetime.strptime(e,"%Y-%m-%d")
    while c<=ed:
        if c.weekday()<5: dates.append(c)
        c+=timedelta(days=1)
    return dates

def regime_params(d):
    y,m=d.year,d.month
    if y==2019: return {"mu":0.0008,"sig":0.010,"vix":16,"reg":3}
    if y==2020 and m<=3: return {"mu":-0.004,"sig":0.040,"vix":65,"reg":6}
    if y==2020 and m<=6: return {"mu":0.0025,"sig":0.022,"vix":30,"reg":1}
    if y==2020: return {"mu":0.0012,"sig":0.014,"vix":22,"reg":1}
    if y==2021 and m<=6: return {"mu":0.0010,"sig":0.011,"vix":18,"reg":3}
    if y==2021: return {"mu":0.0006,"sig":0.012,"vix":20,"reg":5}
    if y==2022 and m<=6: return {"mu":-0.0012,"sig":0.016,"vix":28,"reg":4}
    if y==2022: return {"mu":-0.0006,"sig":0.015,"vix":25,"reg":6}
    if y==2023 and m<=6: return {"mu":0.0008,"sig":0.011,"vix":17,"reg":1}
    if y==2023: return {"mu":0.0005,"sig":0.010,"vix":15,"reg":3}
    if y==2024 and m<=6: return {"mu":0.0009,"sig":0.009,"vix":14,"reg":3}
    if y==2024: return {"mu":0.0006,"sig":0.011,"vix":16,"reg":5}
    if y==2025 and m<=6: return {"mu":0.0004,"sig":0.012,"vix":18,"reg":4}
    return {"mu":0.0003,"sig":0.013,"vix":20,"reg":4}

def gen_bench(dates):
    p=[3230.0]
    for i in range(1,len(dates)):
        rp=regime_params(dates[i])
        r=rp["mu"]+rp["sig"]*np.random.standard_t(df=5)
        p.append(p[-1]*(1+r))
    return np.array(p)

def gen_stocks(n=80):
    stocks=[]
    for i in range(n):
        sec=SECTORS[i%len(SECTORS)]
        stocks.append({"sym":f"STK_{i:03d}","sec":sec,"cntry":"US",
            "itype":INDUSTRY_TYPES[i%4],"beta":SECTOR_BETAS[sec]+np.random.normal(0,0.15),
            "q":np.random.uniform(0.2,0.9),"malpha":np.random.normal(0,0.0003),
            "mcap":np.random.lognormal(24,1.2)})
    return stocks

def sim_metrics(stk,date,mkt_ret,reg):
    q=stk["q"]; beta=stk["beta"]; n=np.random.normal
    roic_z=min(1,max(0,q*0.8+n(0,0.12)*0.3))
    pt=min(1,max(0,q*0.7+n(0,0.15)))
    gc=q*0.25-0.05+n(0,0.06)
    cu=min(1,max(0,q*0.6+0.2+n(0,0.15)))
    has_con=np.random.random()>0.15
    mr=mkt_ret*beta*62+stk["malpha"]*62+n(0,0.08)
    pe=max(0.3,min(3.0,2.0-q*1.5+n(0,0.3)))
    eff=min(1,max(0,q*0.5+0.3+n(0,0.1)))
    rsi=max(10,min(95,50+n(0,15)+(mkt_ret*500)))
    es=q*0.15-0.03+n(0,0.06)
    si=min(1,max(0,0.5+(q-0.5)*0.4+n(0,0.1)))
    price=100+q*100+n(0,20)
    ma120=price*(0.95+n(0,0.03)); ma200=price*(0.92+n(0,0.04))
    ma20=price*(1.0+n(0,0.01)); ma50=price*(0.98+n(0,0.02))
    roic_v=max(0,q*0.25+n(0,0.05)); wacc_v=0.08+n(0,0.02); roa_v=max(0,q*0.15+n(0,0.03))
    has_es=np.random.random()>0.1; has_si=np.random.random()>0.1
    return StockMetrics(
        symbol=stk["sym"],country=stk["cntry"],sector=stk["sec"],industry_type=stk["itype"],
        price=price,ma120=ma120,ma200=ma200,roic=roic_v,wacc=wacc_v,roa=roa_v,
        ocf=max(0,q*5e9+n(0,1e9)),days_since_report=int(np.random.uniform(5,80)),
        avg_daily_volume=stk["mcap"]*0.005,market_cap=stk["mcap"],
        roic_zscore=roic_z,profit_trend_yoy=pt,growth_cagr=gc,
        consensus_up_ratio=cu if has_con else None,momentum_return=mr,
        pe_relative=pe,efficiency=eff,rsi=rsi,
        earnings_surprise_metric=es if has_es else None,
        si_composite=si if has_si else None,
        ma20=ma20,ma50=ma50,roic_score_normalized=roic_z,profit_trend_normalized=pt,
        is_held=False,beta=beta,has_consensus=has_con,has_earnings_surprise=has_es,has_short_interest=has_si)

class BacktestEngine:
    def __init__(self,cap=100_000_000):
        self.cap0=cap; self.cap=cap
        self.dates=gen_dates(); self.stocks=gen_stocks(80)
        self.bench=gen_bench(self.dates)
        self.holdings={}; self.pv=[]; self.bv=[]; self.cash_h=[]
        self.reg_h=[]; self.trades=[]; self.reb_cnt=0; self.act_cnt={}
        self.dd_h=[]; self.pos_h=[]; self.monthly_s=[]; self.monthly_b=[]

    def run(self):
        print("="*60)
        print(f"  Quant-Alpha v3.4 Backtest: {self.dates[0]:%Y-%m-%d} ~ {self.dates[-1]:%Y-%m-%d}")
        print(f"  Universe: {len(self.stocks)} stocks, Capital: ${self.cap0:,.0f}")
        print("="*60)
        peak=self.cap0; reb_int=10
        for di,dt in enumerate(self.dates):
            rp=regime_params(dt); reg=rp["reg"]
            vix=max(10,rp["vix"]+np.random.normal(0,3))
            ms=calculate_macro_score({"growth":0.2,"liquidity":0.1,"innovation":0.15,"inflation":-0.05,"risk":-0.1},vix,rp["vix"])
            regime,ecap=classify_regime(ms,0.0,reg)
            mret=0.0
            if di>0: mret=(self.bench[di]-self.bench[di-1])/self.bench[di-1]
            pret=0.0
            for sym,h in list(self.holdings.items()):
                stk=next((s for s in self.stocks if s["sym"]==sym),None)
                if stk:
                    sr=mret*stk["beta"]+stk["malpha"]+np.random.normal(0,0.005)
                    pret+=h["w"]*sr; h["dh"]+=1; h["cp"]*=(1+sr)
                    if h["cp"]>h["hc"]: h["hc"]=h["cp"]
            cw=1.0-sum(h["w"] for h in self.holdings.values())
            pret+=cw*(0.04/252)
            self.cap*=(1+pret)
            self.pv.append(self.cap)
            self.bv.append(self.bench[di]/self.bench[0]*self.cap0)
            self.cash_h.append(cw); self.reg_h.append(regime)
            self.pos_h.append(len(self.holdings))
            if self.cap>peak: peak=self.cap
            self.dd_h.append((self.cap-peak)/peak)
            # Tier1: stop check
            for sym,h in list(self.holdings.items()):
                sr=check_trailing_stop(h["cp"],h["hc"],h["dh"],regime,False,h["act"]=="TREND_HOLD",True)
                if sr.triggered:
                    pnl=(h["cp"]-h["ep"])/h["ep"]
                    self.trades.append({"d":dt.strftime("%Y-%m-%d"),"s":sym,"a":"STOP_EXIT","pnl":pnl,"dh":h["dh"],"r":regime})
                    self.act_cnt["STOP_EXIT"]=self.act_cnt.get("STOP_EXIT",0)+1
                    del self.holdings[sym]
            # Tier3: rebalance
            if di%reb_int==0 and di>0:
                self.reb_cnt+=1; self._reb(di,dt,regime,ecap,ms,mret)
        print(f"\n  Rebalances: {self.reb_cnt}, Trades: {len(self.trades)}")
        print(f"  Final: ${self.cap:,.0f}")

    def _reb(self,di,dt,regime,ecap,ms,mret):
        cands=[]
        for stk in self.stocks:
            m=sim_metrics(stk,dt,mret,regime)
            if stk["sym"] in self.holdings: m.is_held=True
            res=run_pipeline(m,0.0,regime,None,self.cap*0.05)
            self.act_cnt[res.action]=self.act_cnt.get(res.action,0)+1
            rw=calculate_raw_weight(res.action,res.score,stk["itype"],res.gate_result.warning_count)
            if rw>0 or m.is_held:
                cands.append(PortfolioCandidate(stk["sym"],stk["sec"],stk["cntry"],res.score,res.action,rw,stk["beta"],stk["itype"]))
                if not m.is_held and res.action in("S_BUY","TREND_BUY"):
                    self.trades.append({"d":dt.strftime("%Y-%m-%d"),"s":stk["sym"],"a":res.action,"sc":res.score,"r":regime})
                elif m.is_held and res.action in("EXIT","REDUCE") and stk["sym"] in self.holdings:
                    h=self.holdings[stk["sym"]]
                    self.trades.append({"d":dt.strftime("%Y-%m-%d"),"s":stk["sym"],"a":res.action,"pnl":(h["cp"]-h["ep"])/h["ep"],"dh":h["dh"],"r":regime})
        if cands:
            adj,met,q=manage_portfolio(cands,ecap)
            nh={}
            for p in adj:
                if p.final_weight>0.005:
                    if p.symbol in self.holdings:
                        o=self.holdings[p.symbol]
                        nh[p.symbol]={"w":p.final_weight,"ep":o["ep"],"hc":o["hc"],"cp":o["cp"],"dh":o["dh"],"ed":o["ed"],
                            "act":next((c.action for c in cands if c.symbol==p.symbol),"HOLD")}
                    else:
                        nh[p.symbol]={"w":p.final_weight,"ep":100+np.random.normal(0,10),"hc":100,"cp":100,"dh":0,
                            "ed":dt.strftime("%Y-%m-%d"),"act":next((c.action for c in cands if c.symbol==p.symbol),"HOLD")}
            self.holdings=nh

    def results(self):
        pv=np.array(self.pv); bv=np.array(self.bv)
        sr=np.diff(pv)/pv[:-1]; br=np.diff(bv)/bv[:-1]
        ny=len(self.dates)/252; rf=0.04
        tr_s=(pv[-1]/pv[0])-1; tr_b=(bv[-1]/bv[0])-1
        cagr_s=(pv[-1]/pv[0])**(1/ny)-1; cagr_b=(bv[-1]/bv[0])**(1/ny)-1
        vol_s=np.std(sr)*np.sqrt(252); vol_b=np.std(br)*np.sqrt(252)
        sh_s=(cagr_s-rf)/vol_s if vol_s>0 else 0; sh_b=(cagr_b-rf)/vol_b if vol_b>0 else 0
        pk_s=np.maximum.accumulate(pv); mdd_s=np.min((pv-pk_s)/pk_s)
        pk_b=np.maximum.accumulate(bv); mdd_b=np.min((bv-pk_b)/pk_b)
        es=sr-rf/252; eb=br-rf/252
        beta=np.cov(es,eb)[0,1]/np.var(eb) if np.var(eb)>0 else 1.0
        alpha=(cagr_s-rf)-beta*(cagr_b-rf)
        te=np.std(sr-br)*np.sqrt(252); ir=(cagr_s-cagr_b)/te if te>0 else 0
        ds=sr[sr<0]; dv=np.std(ds)*np.sqrt(252) if len(ds)>0 else vol_s
        sortino=(cagr_s-rf)/dv if dv>0 else 0
        calmar=cagr_s/abs(mdd_s) if mdd_s!=0 else 0
        tp=[t for t in self.trades if "pnl" in t and t["pnl"] is not None]
        if tp:
            wr=len([t for t in tp if t["pnl"]>0])/len(tp)
            aw=np.mean([t["pnl"] for t in tp if t["pnl"]>0]) if any(t["pnl"]>0 for t in tp) else 0
            al=np.mean([t["pnl"] for t in tp if t["pnl"]<=0]) if any(t["pnl"]<=0 for t in tp) else 0
            pf=abs(aw*wr/(al*(1-wr))) if al!=0 and wr<1 else 0
        else: wr=aw=al=pf=0
        # yearly
        yl,ys,yb=[],[],[]
        cy=self.dates[0].year; yss=pv[0]; ybs=bv[0]
        for i in range(1,len(self.dates)):
            if self.dates[i].year!=cy or i==len(self.dates)-1:
                ys.append((pv[i-1]/yss)-1); yb.append((bv[i-1]/ybs)-1)
                yl.append(str(cy)); yss=pv[i-1]; ybs=bv[i-1]; cy=self.dates[i].year
        # monthly
        ml,ms2,mb2=[],[],[]
        cm=self.dates[0].month; mss=pv[0]; mbs=bv[0]
        for i in range(1,len(self.dates)):
            if self.dates[i].month!=cm or i==len(self.dates)-1:
                ms2.append((pv[i-1]/mss)-1); mb2.append((bv[i-1]/mbs)-1)
                ml.append(self.dates[i-1].strftime("%Y-%m")); mss=pv[i-1]; mbs=bv[i-1]; cm=self.dates[i].month
        # IS/OOS
        si=int(len(pv)*0.7)
        is_r=(pv[si]/pv[0])**(252/si)-1; is_v=np.std(sr[:si])*np.sqrt(252); is_sh=(is_r-rf)/is_v if is_v>0 else 0
        oos_r=(pv[-1]/pv[si])**(252/(len(pv)-si))-1; oos_v=np.std(sr[si:])*np.sqrt(252); oos_sh=(oos_r-rf)/oos_v if oos_v>0 else 0
        # stress
        def pr(vals,s,e):
            sd=datetime.strptime(s,"%Y-%m-%d"); ed=datetime.strptime(e,"%Y-%m-%d")
            si2=ei=None
            for i,d in enumerate(self.dates):
                if d>=sd and si2 is None: si2=i
                if d<=ed: ei=i
            return (vals[ei]/vals[si2])-1 if si2 and ei and ei>si2 else 0
        sp={"COVID Crash (2020.02-03)":("2020-02-01","2020-03-31"),"COVID Recovery (2020.04-06)":("2020-04-01","2020-06-30"),
            "Rate Hike (2022.01-06)":("2022-01-01","2022-06-30"),"Bear Market (2022.07-12)":("2022-07-01","2022-12-31"),
            "2023 Recovery":("2023-01-01","2023-06-30"),"2024 Bull":("2024-01-01","2024-06-30")}
        st_s={k:pr(pv,v[0],v[1]) for k,v in sp.items()}
        st_b={k:pr(bv,v[0],v[1]) for k,v in sp.items()}
        crit={"excess_return_positive":(cagr_s-cagr_b)>0,"sharpe_above_05":sh_s>0.5,
            "mdd_within_120pct":abs(mdd_s)<abs(mdd_b)*1.2,"is_oos_sharpe_diff_lt_03":abs(is_sh-oos_sh)<0.3,"beta_in_range":0.8<=beta<=1.2}
        sr2=max(1,len(self.dates)//300)
        dl=[self.dates[i].strftime("%Y-%m-%d") for i in range(0,len(self.dates),sr2)]
        pvs=[pv[i]/self.cap0 for i in range(0,len(pv),sr2)]
        bvs=[bv[i]/self.cap0 for i in range(0,len(bv),sr2)]
        dds=[self.dd_h[i] for i in range(0,len(self.dd_h),sr2)]
        pcs=[self.pos_h[i] for i in range(0,len(self.pos_h),sr2)]
        chs=[self.cash_h[i] for i in range(0,len(self.cash_h),sr2)]
        rgs=[self.reg_h[i] for i in range(0,len(self.reg_h),sr2)]
        return {"summary":{"period":f"{self.dates[0]:%Y-%m-%d} ~ {self.dates[-1]:%Y-%m-%d}",
            "days":len(self.dates),"cap0":self.cap0,"final_s":round(pv[-1]),"final_b":round(bv[-1]),
            "tr_s":round(tr_s*100,2),"tr_b":round(tr_b*100,2),
            "cagr_s":round(cagr_s*100,2),"cagr_b":round(cagr_b*100,2),
            "excess":round((cagr_s-cagr_b)*100,2),
            "vol_s":round(vol_s*100,2),"vol_b":round(vol_b*100,2),
            "sh_s":round(sh_s,3),"sh_b":round(sh_b,3),"sortino":round(sortino,3),"calmar":round(calmar,3),
            "mdd_s":round(mdd_s*100,2),"mdd_b":round(mdd_b*100,2),
            "alpha":round(alpha*100,2),"beta":round(beta,3),
            "ir":round(ir,3),"te":round(te*100,2),
            "wr":round(wr*100,1),"aw":round(aw*100,2),"al":round(al*100,2),"pf":round(pf,2),
            "trades":len(self.trades),"rebs":self.reb_cnt},
            "validation":{"is_sh":round(is_sh,3),"oos_sh":round(oos_sh,3),"sh_diff":round(abs(is_sh-oos_sh),3),
                "criteria":crit,"all_pass":all(crit.values())},
            "stress_s":{k:round(v*100,2) for k,v in st_s.items()},
            "stress_b":{k:round(v*100,2) for k,v in st_b.items()},
            "actions":self.act_cnt,
            "yearly":{"l":yl,"s":[round(y*100,2) for y in ys],"b":[round(y*100,2) for y in yb]},
            "monthly":{"l":ml[-36:],"s":[round(m*100,2) for m in ms2[-36:]],"b":[round(m*100,2) for m in mb2[-36:]]},
            "charts":{"dates":dl,"pv":[round(v,4) for v in pvs],"bv":[round(v,4) for v in bvs],
                "dd":[round(d*100,2) for d in dds],"pos":pcs,"cash":[round(c*100,1) for c in chs],"reg":rgs}}

if __name__=="__main__":
    e=BacktestEngine(100_000_000); e.run(); r=e.results()
    class NpEncoder(json.JSONEncoder):
        def default(self,obj):
            if isinstance(obj,(np.integer,)): return int(obj)
            if isinstance(obj,(np.floating,)): return float(obj)
            if isinstance(obj,(np.bool_,)): return bool(obj)
            if isinstance(obj,(np.ndarray,)): return obj.tolist()
            return super().default(obj)
    with open("backtest_results.json","w") as f: json.dump(r,f,indent=2,ensure_ascii=False,cls=NpEncoder)
    s=r["summary"]
    print(f"\n{'='*60}\n  결과 요약\n{'='*60}")
    print(f"  전략 CAGR:    {s['cagr_s']:+.2f}%\n  벤치 CAGR:    {s['cagr_b']:+.2f}%\n  초과수익:     {s['excess']:+.2f}%")
    print(f"  Sharpe:       {s['sh_s']:.3f} (벤치: {s['sh_b']:.3f})\n  MDD:          {s['mdd_s']:.2f}% (벤치: {s['mdd_b']:.2f}%)")
    print(f"  Alpha:        {s['alpha']:+.2f}%\n  Beta:         {s['beta']:.3f}\n  Info Ratio:   {s['ir']:.3f}\n  Win Rate:     {s['wr']:.1f}%")
    v=r["validation"]
    print(f"\n  IS Sharpe:    {v['is_sh']:.3f}\n  OOS Sharpe:   {v['oos_sh']:.3f}\n  Sharpe Diff:  {v['sh_diff']:.3f}")
    print(f"  충족기준 전체통과: {'✅' if v['all_pass'] else '❌'}")
    for k,v2 in v["criteria"].items(): print(f"    {'✅' if v2 else '❌'} {k}")
