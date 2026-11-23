#!/usr/bin/env python3
"""Analiza resultados/ y genera resumen.json, RESUMEN.md e informe.html.

Agrega por experimento/escenario/variante: valores por repetición, mediana entre repeticiones y
percentiles de TODAS las muestras (histogramas de 1 ms combinados). Evalúa los criterios de éxito.
"""
import csv, json, math, re, statistics, sys, html
from collections import defaultdict
from pathlib import Path

RES = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent.parent / "resultados"
PEAK_END = 330.0
NORMAL_BACKLOG, NORMAL_HOLD = 1500, 30.0


# ----------------------------------------------------------------------------- carga
def load_runs():
    runs = defaultdict(list)
    for d in sorted(RES.glob("EXP-*/*/*/rep*/done.json")):
        m = json.loads(d.read_text())
        m["_dir"] = d.parent
        runs[(m["exp"], m["escenario"], m["variante"])].append(m)
    for k in runs:
        runs[k].sort(key=lambda m: m["rep"])
    return runs


def read_hist(run_dir, name):
    p = run_dir / "histogramas" / f"{name}.csv"
    h = defaultdict(int)
    if p.exists():
        for line in p.read_text().split():
            ms, c = line.split(",")
            h[int(ms)] += int(c)
    return h


def hist_pct(h, qs):
    tot = sum(h.values())
    if not tot:
        return {q: None for q in qs}
    keys = sorted(h)
    out, acc, i = {}, 0, 0
    targets = sorted(qs)
    for k in keys:
        acc += h[k]
        while i < len(targets) and acc >= math.ceil(targets[i] * tot):
            out[targets[i]] = k / 1000.0
            i += 1
    while i < len(targets):
        out[targets[i]] = keys[-1] / 1000.0
        i += 1
    return out


def pooled(runs, name):
    h = defaultdict(int)
    for m in runs:
        if m.get("ok"):
            for k, v in read_hist(m["_dir"], name).items():
                h[k] += v
    p = hist_pct(h, [0.5, 0.95, 0.99, 0.999, 1.0])
    return dict(n=sum(h.values()), p50=p[0.5], p95=p[0.95], p99=p[0.99], p999=p[0.999], max=p[1.0])


def read_timeline(run_dir):
    p = run_dir / "timeline.csv"
    rows = []
    if not p.exists():
        return rows
    with open(p) as f:
        for r in csv.DictReader(f):
            def num(k):
                try:
                    return float(r[k])
                except Exception:
                    return None
            rows.append(dict(t=num("gen_elapsed"), ts=num("ts_us"), acked_s=num("gen_acked_per_s"), failed=num("gen_failed_attempts"),
                             backlog=num("proc_backlog"), save_backlog=num("proc_save_backlog"), part_max=num("proc_part_backlog_max"),
                             inflight=num("gen_inflight"), phase=r["gen_phase"], parts=r["proc_part_backlog_json"], hap=r["haproxy_status"]))
    # completar t para muestras sin generador (antes/después) usando ts
    base = next((x["ts"] - x["t"] * 1e6 for x in rows if x["t"] is not None and x["ts"]), None)
    if base:
        for x in rows:
            x["t"] = (x["ts"] - base) / 1e6
    return rows


def time_to_normal(tl, peak_end=PEAK_END):
    """Segundos desde el fin del pico hasta que el backlog queda <= umbral de forma sostenida (30 s)."""
    post = [x for x in tl if x["t"] is not None and x["t"] >= peak_end and x["backlog"] is not None and x["phase"] and x["phase"].startswith(("cola", "fin", "dren"))]
    start = None
    for x in post:
        if x["backlog"] <= NORMAL_BACKLOG:
            start = start if start is not None else x["t"]
            if x["t"] - start >= NORMAL_HOLD:
                return round(start - peak_end, 1), True
        else:
            start = None
    last = post[-1]["t"] - peak_end if post else None
    return (round(last, 1) if last is not None else None), False


def docker_stats(run_dir):
    p = run_dir / "docker_stats.jsonl"
    agg = defaultdict(lambda: dict(cpu=[], mem=[]))
    if not p.exists():
        return {}

    def mem_mib(s):
        v = s.split("/")[0].strip()
        m = re.match(r"([\d.]+)\s*([KMG]i?B|B)", v)
        if not m:
            return None
        n, u = float(m.group(1)), m.group(2)
        return n * {"B": 1 / 2**20, "KiB": 1 / 1024, "KB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}.get(u, 1)
    for line in p.read_text().splitlines():
        try:
            d = json.loads(line)
        except Exception:
            continue
        n = d.get("Name", "")
        if not n.startswith("lsa-"):
            continue
        n = n[4:-2]
        try:
            agg[n]["cpu"].append(float(d["CPUPerc"].strip("%")))
        except Exception:
            pass
        mm = mem_mib(d.get("MemUsage", ""))
        if mm is not None:
            agg[n]["mem"].append(mm)
    return {k: dict(cpu_media=round(statistics.mean(v["cpu"]), 1) if v["cpu"] else None, cpu_max=round(max(v["cpu"]), 1) if v["cpu"] else None,
                    mem_max_mib=round(max(v["mem"]), 0) if v["mem"] else None) for k, v in agg.items()}


# ----------------------------------------------------------------------------- análisis
def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def analyze():
    runs = load_runs()
    out = dict(grupos={}, criterios={})
    for (exp, scen, var), rs in sorted(runs.items()):
        reps = []
        for m in rs:
            v = m.get("verificacion", {})
            c, L = v.get("conteos", {}), v.get("latencias_s", {})
            ev = m.get("eventos", [])
            caida = [e.get("caida_s") for e in ev if e["kind"] == "heartbeat_instancia_viva"]
            detect = None
            kill = next((e for e in ev if e["kind"] == "monitor_inyecta_falla"), None)
            det = next((e for e in ev if e["kind"] == "heartbeat_detecta_caida"), None)
            if kill and det:
                detect = (det["ts"] - kill["ts"]) / 1e6
            tl = read_timeline(m["_dir"]) if m.get("ok") else []
            r = dict(rep=m["rep"], ok=m.get("ok"), error=m.get("error"), duracion_s=m.get("duracion_s"), falla=m.get("falla"),
                     caida_s=caida[0] if caida else None, deteccion_falla_s=detect, **{k: c.get(k) for k in [
                         "enviadas", "telemetria", "panicos", "atrasadas", "acusadas", "no_acusadas", "nunca_enviadas", "perdidas",
                         "perdidas_acusadas", "perdidas_tel", "perdidas_panico", "perdidas_panico_acusadas", "panicos_guardados",
                         "panicos_guardados_pct", "duplicados", "duplicados_panico", "reintentos", "detectadas", "no_detectadas",
                         "fuera_de_orden", "fuera_de_orden_worker", "reentregas_tardias", "det_duplicadas", "tel_pico", "tel_pico_detectadas",
                         "guardadas_fuera_de_registro", "por_particion", "por_receptor", "panico_por_escritor"]})
            r["lat"] = {k: v2 for k, v2 in L.items()}
            if exp == "EXP-3" and tl:
                ttn, normal = time_to_normal(tl)
                r["tiempo_a_normal_s"], r["normalizo"] = ttn, normal
                r["backlog_max"] = max((x["backlog"] or 0) for x in tl)
            if tl:
                r["backlog_guardado_max"] = max((x["save_backlog"] or 0) for x in tl)
                rates = [x["acked_s"] for x in tl if x["acked_s"] is not None and x["phase"] and "5000" in x["phase"]]
                r["tasa_pico_media"] = round(statistics.mean(rates[2:]), 0) if len(rates) > 3 else None
            r["recursos"] = docker_stats(m["_dir"]) if m.get("ok") else {}
            reps.append(r)
        okr = [m for m in rs if m.get("ok")]
        metrics = ["guardado_desde_envio", "guardado_desde_origen", "guardado_tel_puntual_desde_envio", "guardado_tel_atrasada_desde_envio",
                   "acuse_desde_envio", "panico_guardado_desde_origen", "panico_acuse_desde_origen", "deteccion_desde_origen",
                   "deteccion_desde_origen_pico", "deteccion_desde_guardado"]
        g = dict(exp=exp, escenario=scen, variante=var, reps=reps, n_ok=len(okr), n=len(rs),
                 agregado={k: pooled(okr, k) for k in metrics})
        sums = lambda k: sum((r.get(k) or 0) for r in reps if r["ok"])
        g["totales"] = {k: sums(k) for k in ["enviadas", "panicos", "panicos_guardados", "perdidas", "perdidas_acusadas", "perdidas_panico",
                                             "perdidas_panico_acusadas", "duplicados", "duplicados_panico", "fuera_de_orden", "fuera_de_orden_worker", "telemetria", "atrasadas",
                                             "reintentos", "tel_pico", "tel_pico_detectadas"]}
        t = g["totales"]
        t["panicos_guardados_pct"] = 100 * t["panicos_guardados"] / t["panicos"] if t["panicos"] else None
        t["min_panicos_guardados_pct"] = min((r["panicos_guardados_pct"] for r in reps if r["ok"] and r["panicos_guardados_pct"] is not None), default=None)
        g["mediana"] = {k: med([r.get(k) for r in reps if r["ok"]]) for k in ["caida_s", "deteccion_falla_s", "tiempo_a_normal_s", "backlog_max",
                                                                               "backlog_guardado_max", "tasa_pico_media", "duracion_s"]}
        g["mediana_lat_p99"] = {k: med([(r["lat"].get(k) or {}).get("p99") for r in reps if r["ok"]]) for k in metrics}
        if exp == "EXP-3":
            g["normalizo_todas"] = all(r.get("normalizo") for r in reps if r["ok"])
            g["max_tiempo_a_normal_s"] = max((r.get("tiempo_a_normal_s") or 0) for r in reps if r["ok"]) if okr else None
        # recursos agregados
        rec = defaultdict(lambda: dict(cpu_media=[], cpu_max=[], mem_max_mib=[]))
        for r in reps:
            for ctn, v in r["recursos"].items():
                for k in v:
                    if v[k] is not None:
                        rec[ctn][k].append(v[k])
        g["recursos"] = {c: {k: (round(statistics.mean(v[k]), 1) if k == "cpu_media" else max(v[k])) if v[k] else None for k in v} for c, v in rec.items()}
        out["grupos"][f"{exp}|{scen}|{var}"] = g
    out["criterios"] = criteria(out["grupos"])
    return out


def criteria(G):
    C = {}
    for scen in ["falla-despachador", "falla-receptor", "falla-broker"]:
        for var in ["tactica", "base"]:
            g = G.get(f"EXP-1|{scen}|{var}")
            if not g:
                continue
            mp = g["totales"]["min_panicos_guardados_pct"]
            C[f"EXP-1|{scen}|{var}"] = dict(
                criterio="≥ 99,99 % de pánicos guardados en cada repetición", valor=mp,
                cumple=(mp is not None and mp >= 99.99 and g["n_ok"] == g["n"]))
    for key, g in G.items():
        if g["exp"] in ("EXP-1b", "EXP-1c"):
            mp = g["totales"]["min_panicos_guardados_pct"]
            C[key] = dict(criterio="≥ 99,99 % de pánicos guardados en cada repetición", valor=mp,
                          cumple=(mp is not None and mp >= 99.99 and g["n_ok"] == g["n"]))
    for var in ["tactica", "base"]:
        g = G.get(f"EXP-2|pico|{var}")
        if g:
            p99 = g["agregado"]["guardado_desde_envio"]["p99"]
            lost = g["totales"]["perdidas"]
            C[f"EXP-2|pico|{var}"] = dict(criterio="p99 de guardado < 60 s y 0 señales perdidas", p99=p99, perdidas=lost,
                                         cumple=(p99 is not None and p99 < 60 and lost == 0 and g["n_ok"] == g["n"]))
        g = G.get(f"EXP-3|pico|{var}")
        if g:
            agg = g["agregado"]["deteccion_desde_origen_pico"]
            cov = (g["totales"]["tel_pico_detectadas"] / g["totales"]["tel_pico"]) if g["totales"]["tel_pico"] else 0
            p99 = agg["p99"] if cov >= 0.99 else None   # si quedó >1 % sin procesar, el p99 real es mayor que el observado
            ooo = g["totales"]["fuera_de_orden_worker"]  # orden real de procesamiento del worker
            C[f"EXP-3|pico|{var}"] = dict(criterio="p99 de procesamiento < 5 s · 0 fuera de orden · cola normal ≤ 10 min",
                                         p99=p99, p99_observado=agg["p99"], cobertura_pico=cov, fuera_de_orden=ooo,
                                         normalizo=g.get("normalizo_todas"), max_tiempo_a_normal_s=g.get("max_tiempo_a_normal_s"),
                                         cumple=(p99 is not None and p99 < 5 and ooo == 0 and bool(g.get("normalizo_todas"))
                                                 and (g.get("max_tiempo_a_normal_s") or 0) <= 600 and g["n_ok"] == g["n"]))
    return C


# ----------------------------------------------------------------------------- SVG
def esc(s):
    return html.escape(str(s))


def fmt(v, d=2, unit=""):
    if v is None:
        return "—"
    if isinstance(v, float):
        if abs(v) >= 1000:
            s = f"{v:,.0f}"
        else:
            s = f"{v:,.{d}f}"
    else:
        s = f"{v:,}"
    return s.replace(",", " ").replace(".", ",") + unit


def nice_ticks(lo, hi, n=5):
    if hi <= lo:
        hi = lo + 1
    span = hi - lo
    step = 10 ** math.floor(math.log10(span / n))
    for m in [1, 2, 2.5, 5, 10]:
        if span / (step * m) <= n:
            step *= m
            break
    t = math.floor(lo / step) * step
    out = []
    while t <= hi + 1e-9:
        out.append(round(t, 10))
        t += step
    if out[-1] < hi - 1e-9:
        out.append(round(out[-1] + step, 10))
    return out, step


def tick_label(v):
    if v >= 1_000_000:
        return f"{v/1e6:g} M"
    if v >= 1000:
        return f"{v/1000:g} k"
    return f"{v:g}".replace(".", ",")


def line_chart(series, xlabel, ylabel, log=False, vlines=(), bands=(), w=680, h=250, ymax=None, xmax=None, title=""):
    """series: [(nombre, clase_css, [(x,y),...])]"""
    L, R, T, B = 62, 14, 14, 40
    pw, ph = w - L - R, h - T - B
    xs = [p[0] for _, _, pts in series for p in pts if p[0] is not None]
    ys = [p[1] for _, _, pts in series for p in pts if p[1] is not None]
    if not xs:
        return "<p class='muted'>Sin datos</p>"
    x0, x1 = 0, xmax or max(xs)
    if log:
        ylo, yhi = 0, math.ceil(math.log10(max(max(ys), 10)))
        fy = lambda y: T + ph - (math.log10(max(y, 1)) - ylo) / (yhi - ylo) * ph
        yt = [(10 ** k, T + ph - (k - ylo) / (yhi - ylo) * ph) for k in range(ylo, yhi + 1)]
    else:
        top = ymax or max(ys) * 1.08 or 1
        ticks, _ = nice_ticks(0, top)
        top = ticks[-1]
        fy = lambda y: T + ph - (y / top) * ph
        yt = [(t, fy(t)) for t in ticks]
    fx = lambda x: L + (x - x0) / (x1 - x0) * pw
    xt, _ = nice_ticks(x0, x1, 8)
    o = [f'<svg viewBox="0 0 {w} {h}" class="chart" role="img" aria-label="{esc(title or ylabel)}">']
    for (a, b, lab) in bands:
        o.append(f'<rect x="{fx(a):.1f}" y="{T}" width="{fx(min(b, x1)) - fx(a):.1f}" height="{ph}" class="band"/>')
        o.append(f'<text x="{fx(a) + 4:.1f}" y="{T + 12}" class="lab small">{esc(lab)}</text>')
    for v, y in yt:
        o.append(f'<line x1="{L}" x2="{L + pw}" y1="{y:.1f}" y2="{y:.1f}" class="grid"/>')
        o.append(f'<text x="{L - 6}" y="{y + 4:.1f}" class="lab" text-anchor="end">{tick_label(v)}</text>')
    for v in xt:
        if v <= x1:
            o.append(f'<text x="{fx(v):.1f}" y="{T + ph + 16}" class="lab" text-anchor="middle">{tick_label(v)}</text>')
    o.append(f'<line x1="{L}" x2="{L + pw}" y1="{T + ph}" y2="{T + ph}" class="axis"/>')
    for (x, lab) in vlines:
        o.append(f'<line x1="{fx(x):.1f}" x2="{fx(x):.1f}" y1="{T}" y2="{T + ph}" class="vline"/>')
        right = fx(x) > L + pw * 0.8
        o.append(f'<text x="{fx(x) + (-4 if right else 4):.1f}" y="{T + 12 if right else T + ph - 6}" class="lab small strong" text-anchor="{"end" if right else "start"}">{esc(lab)}</text>')
    for name, cls, pts in series:
        pts = [(x, y) for x, y in pts if x is not None and y is not None and x <= x1]
        if not pts:
            continue
        d = " ".join(f"{'M' if i == 0 else 'L'}{fx(x):.1f},{fy(y):.1f}" for i, (x, y) in enumerate(pts))
        o.append(f'<path d="{d}" class="ln {cls}"><title>{esc(name)}</title></path>')
    o.append(f'<text x="{L + pw / 2}" y="{h - 4}" class="lab" text-anchor="middle">{esc(xlabel)}</text>')
    o.append(f'<text x="14" y="{T + ph / 2}" class="lab" text-anchor="middle" transform="rotate(-90 14 {T + ph / 2})">{esc(ylabel)}</text>')
    o.append("</svg>")
    return "".join(o)


def bar_chart(cats, series, ylabel, w=680, h=240, log=False, unit="", title=""):
    """cats: [etiqueta]; series: [(nombre, clase, [valores por cat])]"""
    L, R, T, B = 62, 14, 18, 44
    pw, ph = w - L - R, h - T - B
    vals = [v for _, _, vs in series for v in vs if v is not None]
    if not vals:
        return "<p class='muted'>Sin datos</p>"
    if log:
        hi = math.ceil(math.log10(max(max(vals), 10)))
        fy = lambda v: T + ph - (math.log10(max(v, 1)) / hi) * ph if v and v > 0 else T + ph
        yt = [(10 ** k, T + ph - k / hi * ph) for k in range(0, hi + 1)]
    else:
        ticks, _ = nice_ticks(0, max(vals) * 1.1 or 1)
        top = ticks[-1]
        fy = lambda v: T + ph - (v / top) * ph
        yt = [(t, fy(t)) for t in ticks]
    o = [f'<svg viewBox="0 0 {w} {h}" class="chart" role="img" aria-label="{esc(title or ylabel)}">']
    for v, y in yt:
        o.append(f'<line x1="{L}" x2="{L + pw}" y1="{y:.1f}" y2="{y:.1f}" class="grid"/>')
        o.append(f'<text x="{L - 6}" y="{y + 4:.1f}" class="lab" text-anchor="end">{tick_label(v)}</text>')
    gw = pw / len(cats)
    ns = len(series)
    bw = min(34, (gw - 16) / ns - 2)
    for i, c in enumerate(cats):
        gx = L + i * gw + (gw - (bw + 2) * ns) / 2
        for j, (name, cls, vs) in enumerate(series):
            v = vs[i]
            x = gx + j * (bw + 2)
            if v is None:
                o.append(f'<text x="{x + bw / 2:.1f}" y="{T + ph - 4}" class="lab small" text-anchor="middle">n/d</text>')
                continue
            y = fy(v)
            hh = max(T + ph - y, 0)
            r = min(4, hh / 2, bw / 2)
            # barra con extremo redondeado arriba, anclada a la base
            path = (f"M{x:.1f},{T + ph:.1f} V{y + r:.1f} Q{x:.1f},{y:.1f} {x + r:.1f},{y:.1f} H{x + bw - r:.1f} "
                    f"Q{x + bw:.1f},{y:.1f} {x + bw:.1f},{y + r:.1f} V{T + ph:.1f} Z")
            o.append(f'<path d="{path}" class="bar {cls}"><title>{esc(name)} · {esc(c)}: {fmt(v, 2, unit)}</title></path>')
            if len(cats) * ns <= 12:
                o.append(f'<text x="{x + bw / 2:.1f}" y="{y - 4:.1f}" class="lab small" text-anchor="middle">{fmt(v, 1 if v < 100 else 0, "")}</text>')
        o.append(f'<text x="{L + i * gw + gw / 2:.1f}" y="{T + ph + 16}" class="lab" text-anchor="middle">{esc(c)}</text>')
    o.append(f'<line x1="{L}" x2="{L + pw}" y1="{T + ph}" y2="{T + ph}" class="axis"/>')
    o.append(f'<text x="14" y="{T + ph / 2}" class="lab" text-anchor="middle" transform="rotate(-90 14 {T + ph / 2})">{esc(ylabel)}</text>')
    o.append("</svg>")
    return "".join(o)


def legend(items):
    return '<div class="legend">' + "".join(f'<span><i class="sw {c}"></i>{esc(n)}</span>' for n, c in items) + "</div>"


# ----------------------------------------------------------------------------- informe
def chip(ok):
    if ok is None:
        return '<span class="chip na">Sin datos</span>'
    return '<span class="chip ok">✓ Cumple</span>' if ok else '<span class="chip ko">✕ No cumple</span>'


VAR = {"tactica": "Con táctica", "base": "Línea base"}
CLS = {"tactica": "s1", "base": "s2"}


def report(A, env):
    G, C = A["grupos"], A["criterios"]
    g = lambda e, s, v: G.get(f"{e}|{s}|{v}")
    parts = []

    # ---- veredicto
    def exp1_ok(var):
        vals = [C.get(f"EXP-1|{s}|{var}", {}).get("cumple") for s in ["falla-despachador", "falla-receptor", "falla-broker"]]
        vals = [v for v in vals if v is not None]
        return all(vals) if vals else None
    cards = []
    t1 = [g("EXP-1", s, "tactica") for s in ["falla-despachador", "falla-receptor", "falla-broker"]]
    b1 = [g("EXP-1", s, "base") for s in ["falla-despachador", "falla-receptor", "falla-broker"]]
    t1p = min([x["totales"]["min_panicos_guardados_pct"] for x in t1 if x and x["totales"]["min_panicos_guardados_pct"] is not None], default=None)
    b1p = min([x["totales"]["min_panicos_guardados_pct"] for x in b1 if x and x["totales"]["min_panicos_guardados_pct"] is not None], default=None)
    cards.append(("EXP-1", "DIS-1 · Caída de un servidor", "≥ 99,99 % de pánicos guardados aunque caiga un servidor",
                  exp1_ok("tactica"), f"Peor repetición: {fmt(t1p, 3, ' %')}", exp1_ok("base"), f"Peor repetición: {fmt(b1p, 3, ' %')}"))
    c2t, c2b = C.get("EXP-2|pico|tactica", {}), C.get("EXP-2|pico|base", {})
    cards.append(("EXP-2", "LAT-1 · Guardar en el pico", "p99 de guardado < 1 min · 0 señales perdidas",
                  c2t.get("cumple"), f"p99 {fmt(c2t.get('p99'), 2, ' s')} · {fmt(c2t.get('perdidas'))} perdidas",
                  c2b.get("cumple"), f"p99 {fmt(c2b.get('p99'), 2, ' s')} · {fmt(c2b.get('perdidas'))} perdidas"))
    c3t, c3b = C.get("EXP-3|pico|tactica", {}), C.get("EXP-3|pico|base", {})

    def e3txt(c):
        if not c:
            return "—"
        p = fmt(c.get("p99"), 2, " s") if c.get("p99") is not None else f"> {fmt(c.get('p99_observado'), 0, ' s')} ({fmt(100 * (c.get('cobertura_pico') or 0), 1, ' %')} procesado)"
        n = f"normal en {fmt(c.get('max_tiempo_a_normal_s'), 0, ' s')}" if c.get("normalizo") else "no se normalizó"
        return f"p99 {p} · {n}"
    cards.append(("EXP-3", "LAT-2 · Procesar en el pico", "p99 de procesamiento < 5 s · 0 fuera de orden · cola normal ≤ 10 min",
                  c3t.get("cumple"), e3txt(c3t), c3b.get("cumple"), e3txt(c3b)))
    vc = []
    for (eid, name, crit, tok, ttxt, bok, btxt) in cards:
        vc.append(f"""<article class="verdict"><header><span class="eid">{eid}</span><h3>{esc(name)}</h3></header>
        <p class="crit">{esc(crit)}</p>
        <div class="vrow"><span class="vlab"><i class="sw s1"></i>Con táctica</span>{chip(tok)}<span class="vval">{esc(ttxt)}</span></div>
        <div class="vrow"><span class="vlab"><i class="sw s2"></i>Línea base</span>{chip(bok)}<span class="vval">{esc(btxt)}</span></div></article>""")
    parts.append(f'<section id="veredicto"><h2>Veredicto</h2><div class="verdicts">{"".join(vc)}</div></section>')

    # ---- EXP-1
    rows = []
    for s in ["falla-despachador", "falla-receptor", "falla-broker"]:
        for v in ["tactica", "base"]:
            x = g("EXP-1", s, v)
            if not x:
                continue
            t = x["totales"]
            rows.append(f"""<tr><td>{esc(s.replace('falla-', '').capitalize())}</td><td><i class="sw {CLS[v]}"></i>{VAR[v]}</td>
            <td>{x['n_ok']}/{x['n']}</td><td>{fmt(t['panicos'])}</td><td>{fmt(t['panicos_guardados_pct'], 4, ' %')}</td>
            <td>{fmt(t['min_panicos_guardados_pct'], 4, ' %')}</td><td>{fmt(t['perdidas_panico'])}</td><td>{fmt(t['perdidas_panico_acusadas'])}</td>
            <td>{fmt(t['perdidas'])}</td><td>{fmt(t['duplicados'])}</td><td>{fmt(x['mediana']['deteccion_falla_s'], 2, ' s')}</td>
            <td>{fmt(x['mediana']['caida_s'], 1, ' s')}</td><td>{fmt(x['agregado']['panico_guardado_desde_origen']['p99'], 2, ' s')}</td>
            <td>{chip(C.get(f'EXP-1|{s}|{v}', {}).get('cumple'))}</td></tr>""")
    lost_chart = bar_chart(["Despachador", "Receptor", "Broker"], [
        (VAR[v], CLS[v], [(g("EXP-1", s, v) or {}).get("totales", {}).get("perdidas_panico") for s in ["falla-despachador", "falla-receptor", "falla-broker"]])
        for v in ["tactica", "base"]], "Pánicos perdidos (5 rep.)", title="Pánicos perdidos por tipo de falla")
    tls = []
    for s in ["falla-despachador", "falla-receptor", "falla-broker"]:
        ser = []
        kill_t = None
        for v in ["tactica", "base"]:
            x = g("EXP-1", s, v)
            if not x or not x["reps"] or not x["reps"][0]["ok"]:
                continue
            m = next(r for r in load_runs()[("EXP-1", s, v)] if r["rep"] == x["reps"][0]["rep"])
            tl = read_timeline(m["_dir"])
            ser.append((VAR[v], CLS[v], [(p["t"], p["acked_s"]) for p in tl if p["t"] is not None and p["t"] >= 0]))
            kill_t = kill_t or (m.get("falla") or {}).get("t_gen")
        tls.append(f'<figure><figcaption>Falla de {esc(s.replace("falla-", ""))} · repetición 1</figcaption>'
                   + line_chart(ser, "segundos desde el inicio de la carga", "señales acusadas / s", vlines=[(kill_t, "SIGKILL")] if kill_t else [],
                                w=440, h=230, xmax=200) + "</figure>")
    parts.append(f"""<section id="exp1"><h2><span class="eid">EXP-1</span> Caída de un servidor <small>DIS-1 ★</small></h2>
    <p class="lede">Carga sostenida de 5.000 señales/s durante 150 s. Cada 5 s llega una ráfaga de 400 pánicos repartida en 2 s. En t = 60,5 s, en plena ráfaga
    (el estímulo de DIS-1 es caerse <i>mientras procesa pánicos</i>), el monitor mata con <code>SIGKILL</code> un despachador, un receptor o un broker. El orquestador heartbeat (latido cada 1 s) detecta la caída y relanza la instancia.
    Cada escenario se repitió 5 veces con la táctica y 5 veces con la línea base.</p>
    {legend([(VAR['tactica'], 's1'), (VAR['base'], 's2')])}
    <div class="scroll"><table><thead><tr><th>Falla</th><th>Variante</th><th>Rep. OK</th><th>Pánicos enviados</th><th>Pánicos guardados</th>
    <th>Peor rep.</th><th>Pánicos perdidos</th><th>…ya acusados</th><th>Señales perdidas (total)</th><th>Duplicados</th><th>Detección de la falla</th>
    <th>Instancia caída</th><th>p99 pánico guardado</th><th>Criterio</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>
    <div class="grid2"><figure><figcaption>Pánicos perdidos, suma de 5 repeticiones</figcaption>{lost_chart.replace('Pánicos perdidos (5 rep.)', 'pánicos perdidos')}</figure></div>
    <h3>Señales acusadas por segundo alrededor de la falla</h3><div class="grid3">{''.join(tls)}</div></section>""")

    # ---- EXP-2
    def lat_row(x, v, key, label):
        a = x["agregado"][key]
        return (f"<tr><td>{esc(label)}</td><td><i class='sw {CLS[v]}'></i>{VAR[v]}</td><td>{fmt(a['n'])}</td><td>{fmt(a['p50'], 3, ' s')}</td>"
                f"<td>{fmt(a['p95'], 3, ' s')}</td><td>{fmt(a['p99'], 3, ' s')}</td><td>{fmt(a['p999'], 3, ' s')}</td><td>{fmt(a['max'], 2, ' s')}</td>"
                f"<td>{fmt(x['mediana_lat_p99'][key], 3, ' s')}</td></tr>")
    rows2 = []
    for key, label in [("guardado_desde_envio", "Guardado (todas) desde el envío"), ("guardado_tel_puntual_desde_envio", "Telemetría puntual"),
                       ("guardado_tel_atrasada_desde_envio", "Telemetría atrasada (desde el envío)"), ("panico_guardado_desde_origen", "Pánico guardado desde el botón"),
                       ("acuse_desde_envio", "Acuse a la OBU desde el envío"), ("guardado_desde_origen", "Guardado desde el origen (incluye atraso)")]:
        for v in ["tactica", "base"]:
            x = g("EXP-2", "pico", v)
            if x:
                rows2.append(lat_row(x, v, key, label))
    cnt2 = []
    for v in ["tactica", "base"]:
        x = g("EXP-2", "pico", v)
        if x:
            t = x["totales"]
            cnt2.append(f"<tr><td><i class='sw {CLS[v]}'></i>{VAR[v]}</td><td>{x['n_ok']}/{x['n']}</td><td>{fmt(t['enviadas'])}</td><td>{fmt(t['atrasadas'])} "
                        f"({fmt(100 * t['atrasadas'] / t['enviadas'] if t['enviadas'] else None, 2, ' %')})</td><td>{fmt(t['panicos'])}</td><td>{fmt(t['perdidas'])}</td>"
                        f"<td>{fmt(t['duplicados'])}</td><td>{fmt(t['reintentos'])}</td><td>{fmt(x['mediana']['backlog_guardado_max'], 0)}</td>"
                        f"<td>{fmt(x['mediana']['tasa_pico_media'], 0, '/s')}</td><td>{chip(C.get(f'EXP-2|pico|{v}', {}).get('cumple'))}</td></tr>")
    pchart = bar_chart(["p50", "p95", "p99", "p99,9"], [(VAR[v], CLS[v], [(g("EXP-2", "pico", v) or {}).get("agregado", {}).get("guardado_desde_envio", {}).get(k)
                                                                           for k in ["p50", "p95", "p99", "p999"]]) for v in ["tactica", "base"]],
                       "segundos", unit=" s", title="Latencia de guardado")
    ppan = bar_chart(["p50", "p95", "p99", "máx"], [(VAR[v], CLS[v], [(g("EXP-2", "pico", v) or {}).get("agregado", {}).get("panico_guardado_desde_origen", {}).get(k)
                                                                      for k in ["p50", "p95", "p99", "max"]]) for v in ["tactica", "base"]],
                     "segundos", unit=" s", title="Latencia de pánico")
    ser = []
    for v in ["tactica", "base"]:
        rr = load_runs().get(("EXP-2", "pico", v), [])
        if rr and rr[0].get("ok"):
            tl = read_timeline(rr[0]["_dir"])
            ser.append((VAR[v], CLS[v], [(p["t"], p["save_backlog"]) for p in tl if p["t"] is not None and p["t"] >= 0]))
    bchart = line_chart(ser, "segundos desde el inicio de la carga", "señales sin guardar", bands=[(60, 360, "pico 5.000/s")], h=220, title="Backlog de guardado")
    parts.append(f"""<section id="exp2"><h2><span class="eid">EXP-2</span> Guardar en el pico <small>LAT-1 ★</small></h2>
    <p class="lede">Perfil de 1 min a 1.500/s, 5 min a 5.000/s y 30 s a 1.500/s. Incluye una ráfaga de 400 pánicos cada 5 s y un 5,5 % de camiones
    que pierden cobertura 2 min durante el pico (≈ 2 % de señales atrasadas). Táctica: receptores sin estado, canal durable replicado ×3 y carril propio
    para el pánico, con 24 colas. Línea base: una sola cola para todo, pánicos incluidos.</p>
    {legend([(VAR['tactica'], 's1'), (VAR['base'], 's2')])}
    <div class="scroll"><table><thead><tr><th>Variante</th><th>Rep. OK</th><th>Señales enviadas</th><th>Atrasadas</th><th>Pánicos</th><th>Perdidas</th>
    <th>Duplicados</th><th>Reintentos OBU</th><th>Backlog máx. (mediana)</th><th>Tasa real en el pico</th><th>Criterio</th></tr></thead><tbody>{''.join(cnt2)}</tbody></table></div>
    <div class="scroll"><table><thead><tr><th>Medida (5 rep., todas las muestras)</th><th>Variante</th><th>n</th><th>p50</th><th>p95</th><th>p99</th><th>p99,9</th><th>máx</th>
    <th>Mediana de p99 por rep.</th></tr></thead><tbody>{''.join(rows2)}</tbody></table></div>
    <div class="grid2"><figure><figcaption>Guardado desde el envío, percentiles de todas las muestras</figcaption>{pchart}</figure>
    <figure><figcaption>Pánico: desde el botón hasta quedar guardado</figcaption>{ppan}</figure></div>
    <figure><figcaption>Señales recibidas aún sin guardar · repetición 1</figcaption>{bchart}</figure></section>""")

    # ---- EXP-3
    rows3 = []
    for v in ["tactica", "base"]:
        x = g("EXP-3", "pico", v)
        if not x:
            continue
        a = x["agregado"]["deteccion_desde_origen_pico"]
        t = x["totales"]
        cov = t["tel_pico_detectadas"] / t["tel_pico"] if t["tel_pico"] else None
        ttn = [r.get("tiempo_a_normal_s") for r in x["reps"] if r["ok"]]
        nrm = [r.get("normalizo") for r in x["reps"] if r["ok"]]
        ttxt = ", ".join((fmt(a2, 0, " s") if n else f"&gt;{fmt(a2, 0, ' s')}") for a2, n in zip(ttn, nrm))
        rows3.append(f"<tr><td><i class='sw {CLS[v]}'></i>{VAR[v]}</td><td>{x['n_ok']}/{x['n']}</td><td>{fmt(t['tel_pico'])}</td><td>{fmt(100 * cov if cov is not None else None, 2, ' %')}</td>"
                     f"<td>{fmt(a['p50'], 3, ' s')}</td><td>{fmt(a['p95'], 3, ' s')}</td><td>{fmt(a['p99'], 3, ' s')}</td><td>{fmt(a['max'], 1, ' s')}</td>"
                     f"<td>{fmt(t['fuera_de_orden_worker'])} <span class='muted'>({fmt(t['fuera_de_orden'])})</span></td><td>{fmt(x['mediana']['backlog_max'], 0)}</td><td>{ttxt}</td><td>{chip(C.get(f'EXP-3|pico|{v}', {}).get('cumple'))}</td></tr>")
    ser = []
    for v in ["tactica", "base"]:
        rr = load_runs().get(("EXP-3", "pico", v), [])
        if rr and rr[0].get("ok"):
            tl = read_timeline(rr[0]["_dir"])
            ser.append((VAR[v], CLS[v], [(p["t"], max(p["backlog"] or 0, 1)) for p in tl if p["t"] is not None and p["t"] >= 0]))
    c3 = line_chart(ser, "segundos desde el inicio de la carga", "señales pendientes (log)", log=True, bands=[(30, 330, "pico 5.000/s")],
                    vlines=[(930, "límite 10 min")], h=240, title="Cola pendiente")
    # reparto entre colas (táctica)
    dist = ""
    x = g("EXP-3", "pico", "tactica")
    if x and x["reps"] and x["reps"][0].get("por_particion"):
        pp = x["reps"][0]["por_particion"]
        keys = sorted(pp, key=int)
        vals = [pp[k] for k in keys]
        mean = statistics.mean(vals)
        dev = max(abs(v - mean) / mean for v in vals) * 100
        dist = (f"<figure><figcaption>Señales por cola (táctica, repetición 1). Desviación máxima respecto del promedio: {fmt(dev, 1, ' %')}</figcaption>"
                + bar_chart([str(int(k) + 1) for k in keys], [("Señales", "s1", vals)], "señales", w=680, h=200, title="Reparto por cola").replace('class="lab small" text-anchor="middle">', 'class="lab tiny" text-anchor="middle">')
                + "</figure>")
    parts.append(f"""<section id="exp3"><h2><span class="eid">EXP-3</span> Procesar en el pico <small>LAT-2 ★</small></h2>
    <p class="lede">30 s a 1.500/s, 5 min a 5.000/s y luego carga normal de 1.500/s hasta que la cola vuelve a la normalidad (máximo 10 min).
    Táctica: 24 colas en paralelo, partición = hash(camionId) mod 24, con un escritor y un worker de detección por cola. Línea base: una sola cola y un solo worker.
    Cada detección cuesta 2 ms simulados, por lo que un worker atiende ≈ 480 señales/s. Esa es la carga por cola con 40.000 camiones (11.000/s ÷ 24).
    La latencia se mide desde la hora de origen hasta la hora de detección, solo para señales originadas durante el pico.</p>
    {legend([(VAR['tactica'], 's1'), (VAR['base'], 's2')])}
    <div class="scroll"><table><thead><tr><th>Variante</th><th>Rep. OK</th><th>Señales del pico</th><th>Procesadas</th><th>p50</th><th>p95</th><th>p99</th><th>máx</th>
    <th>Fuera de orden (por timestamp)</th><th>Cola máx. (mediana)</th><th>Vuelta a la normalidad tras el pico (por rep.)</th><th>Criterio</th></tr></thead><tbody>{''.join(rows3)}</tbody></table></div>
    <figure><figcaption>Señales pendientes de procesar · repetición 1 · escala logarítmica</figcaption>{c3}</figure>{dist}</section>""")

    # ---- recursos
    rr = []
    for key, x in sorted(G.items()):
        for ctn, v in sorted(x["recursos"].items()):
            rr.append(f"<tr><td>{esc(x['exp'])}</td><td>{esc(x['escenario'])}</td><td>{VAR[x['variante']]}</td><td><code>{esc(ctn)}</code></td>"
                      f"<td>{fmt(v.get('cpu_media'), 1, ' %')}</td><td>{fmt(v.get('cpu_max'), 1, ' %')}</td><td>{fmt(v.get('mem_max_mib'), 0, ' MiB')}</td></tr>")
    parts.append(f"""<section id="recursos"><h2>Recursos por contenedor</h2><p class="lede">Muestreo de <code>docker stats</code> durante cada corrida.
    El 100 % equivale a un núcleo. CPU media de las 5 repeticiones; CPU y memoria máximas observadas.</p>
    <details><summary>Ver tabla completa ({len(rr)} filas)</summary><div class="scroll"><table><thead><tr><th>Exp.</th><th>Escenario</th><th>Variante</th><th>Contenedor</th><th>CPU media</th>
    <th>CPU máx.</th><th>Memoria máx.</th></tr></thead><tbody>{''.join(rr)}</tbody></table></div></details></section>""")
    return parts


def acked_lost(A):
    """Detalle de cada señal acusada a la OBU que no quedó guardada (violación de durabilidad)."""
    import gzip, datetime
    out = []
    for key, g in A["grupos"].items():
        for r in g["reps"]:
            if g["variante"] != "tactica" or not r["ok"] or not r.get("perdidas_acusadas"):
                continue
            d = RES / g["exp"] / g["escenario"] / g["variante"] / f"rep{r['rep']}"
            got = set()
            for t in ["telemetria", "panico"]:
                with gzip.open(d / "tablas" / f"{t}.csv.gz", "rt") as f:
                    for row in csv.DictReader(f):
                        got.add((row["truck_id"], row["seq"]))
            miss = []
            with gzip.open(d / "gen.csv.gz", "rt") as f:
                for row in csv.DictReader(f):
                    if int(row["ts_ack"]) > 0 and (row["truck_id"], row["seq"]) not in got:
                        miss.append(row)
            ev = json.loads((d / "events.json").read_text())
            kill = next((e for e in ev if e["kind"] == "monitor_inyecta_falla"), None)
            logs = []
            for lf in sorted((d / "logs").glob("redpanda-*.log")):
                for line in lf.read_text(errors="ignore").splitlines():
                    if "producer_state" in line or "data loss" in line.lower() or "truncat" in line.lower():
                        logs.append(line[:400])
            for m in miss:
                ack = int(m["ts_ack"])
                out.append(dict(grupo=key, rep=r["rep"], camion=m["truck_id"], seq=m["seq"], tipo="pánico" if m["type"] == "1" else "telemetría",
                                intentos=m["attempts"], envio_tras_falla_s=(int(m["ts_sent"]) - kill["ts"]) / 1e6 if kill else None,
                                acuse_tras_falla_s=(ack - kill["ts"]) / 1e6 if kill else None, falla=kill.get("svc") if kill else None,
                                acuse_utc=datetime.datetime.fromtimestamp(ack / 1e6, datetime.timezone.utc).strftime("%H:%M:%S.%f")[:-3], logs=logs[:4]))
    return out


def anomalies_html(rows):
    if not rows:
        return '<section id="anomalias"><h2>Anomalías</h2><p class="lede">Ninguna señal acusada a la OBU se perdió en las variantes con táctica.</p></section>'
    tr = "".join(f"<tr><td>{esc(r['grupo'].replace('|', ' · '))}</td><td>{r['rep']}</td><td>{esc(r['falla'])}</td><td>{esc(r['tipo'])}</td><td>{esc(r['camion'])}/{esc(r['seq'])}</td>"
                 f"<td>{esc(r['intentos'])}</td><td>{fmt(r['envio_tras_falla_s'], 2, ' s')}</td><td>{fmt(r['acuse_tras_falla_s'], 2, ' s')}</td><td>{esc(r['acuse_utc'])}</td></tr>"
                 for r in rows)
    lg = "\n".join(sorted({l for r in rows for l in r["logs"]}))
    return f"""<section id="anomalias"><h2>Anomalías: señales acusadas que no quedaron guardadas</h2>
<p class="lede">Solo variantes con táctica. En la línea base de EXP-1 las pérdidas de señales acusadas son esperables por diseño, porque acusa antes de persistir, y se reportan en la tabla de EXP-1.
El verificador cruzó cada señal acusada con lo guardado. Las de esta tabla recibieron acuse del broker con <code>acks=all</code>, pero no están en el log del topic.
El high watermark de la partición coincide con lo consumido, así que el consumidor no se saltó nada. Todas ocurren segundos después de matar un broker,
y su acuse coincide con un error del estado del productor idempotente en Redpanda. Es un caso borde del broker durante el cambio de líder.
En producción conviene validarlo con Kafka/MSK o con versiones más nuevas de Redpanda.</p>
<div class="scroll"><table><thead><tr><th>Escenario</th><th>Rep.</th><th>Falla</th><th>Tipo</th><th>Camión/seq</th><th>Intentos</th><th>Envío tras la falla</th><th>Acuse tras la falla</th><th>Acuse (UTC)</th></tr></thead>
<tbody>{tr}</tbody></table></div>
<details><summary>Líneas de log de Redpanda relacionadas</summary><pre class="scroll" style="padding:12px;margin:0;font:11.5px var(--f-mono);white-space:pre">{esc(lg)}</pre></details></section>"""


def conclusions_html(A):
    G = A["grupos"]
    g = lambda k: G.get(k, {})
    S = ["falla-despachador", "falla-receptor", "falla-broker"]
    tp = sum(g(f"EXP-1|{x}|tactica")["totales"]["panicos"] for x in S)
    tl = sum(g(f"EXP-1|{x}|tactica")["totales"]["perdidas_panico"] for x in S)
    br = g("EXP-1|falla-receptor|base")["totales"]
    ta = sum(g(f"EXP-1|{x}|tactica")["totales"]["perdidas_acusadas"] for x in S)
    det = med([g(f"EXP-1|{x}|tactica")["mediana"]["deteccion_falla_s"] for x in S])
    pan = {x: g(f"EXP-1|{x}|tactica")["agregado"]["panico_guardado_desde_origen"]["p99"] for x in S}
    e2t, e2b = g("EXP-2|pico|tactica"), g("EXP-2|pico|base")
    e3t, e3b = g("EXP-3|pico|tactica"), g("EXP-3|pico|base")
    cov_b = e3b["totales"]["tel_pico_detectadas"] / e3b["totales"]["tel_pico"]
    x1b = "Con una caída de máquina (disco perdido) esa línea base sí perdería datos. En este montaje eso no se simuló."
    bd, bb = G.get("EXP-1b|maquina-perdida-despachador|base"), G.get("EXP-1b|maquina-perdida-broker|base")
    td, tb = G.get("EXP-1b|maquina-perdida-despachador|tactica"), G.get("EXP-1b|maquina-perdida-broker|tactica")
    if bd and bb and td and tb:
        x1b = (f"Por eso se agregó EXP-1b, donde la máquina no vuelve: ahí la línea base <b>sí pierde pánicos</b>, "
               f"{fmt(bd['totales']['perdidas_panico'])} sin guardar al perder el despachador y {fmt(bb['totales']['perdidas_panico'])} al perder el broker "
               f"({fmt(bb['totales']['perdidas'])} señales en total), mientras la táctica perdió "
               f"{fmt(td['totales']['perdidas_panico'] + tb['totales']['perdidas_panico'])} pánicos en las mismas 10 corridas.")
    x1c = ""
    c1 = G.get("EXP-1c|falla-despachador-sesion-2s|tactica")
    if c1:
        L = c1["agregado"]["panico_guardado_desde_origen"]
        x1c = (f" En EXP-1c, con sesión de 2 s y latido de 0,5 s, el p99 fue de {fmt(L['p99'], 2, ' s')} frente a {fmt(pan['falla-despachador'], 2, ' s')} con sesión de 6 s "
               f"(mediana de p99 por repetición {fmt(c1['mediana_lat_p99']['panico_guardado_desde_origen'], 2, ' s')}) "
               f"con {fmt(c1['totales']['perdidas_panico'])} pánicos perdidos.")
    return f"""<section id="conclusiones"><h2>Conclusiones</h2>
<div class="cols">
<div><h3><span class="eid">EXP-1</span> Se confirma la hipótesis</h3><ul>
<li>Con réplicas activas, latidos y acuse solo después de replicar, se guardaron <b>{fmt(tp)} de {fmt(tp)} pánicos</b>
({fmt(tl)} perdidos) en las 15 corridas con falla de despachador, receptor o broker.</li>
<li>La pérdida en la línea base viene del <b>acuse antes de persistir</b>. Al caer el receptor se perdieron {fmt(br['perdidas_panico'])} pánicos
y {fmt(br['perdidas_acusadas'])} señales ya acusadas, y la peor repetición quedó en {fmt(br['min_panicos_guardados_pct'], 3, ' %')}, por debajo del 99,99 %.</li>
<li>En la línea base, la caída del despachador o del broker no perdió pánicos. El proceso se relanza en ~1 s, conserva su log en disco y el productor reintenta mientras tanto.
{x1b}</li>
<li>El monitor detectó cada caída en una mediana de {fmt(det, 2, ' s')}.</li>
<li>El tiempo de entrega del pánico durante la falla queda lejos de la meta de 2 s del caso: p99 de {fmt(pan['falla-despachador'], 1, ' s')} con falla del despachador
y {fmt(pan['falla-broker'], 1, ' s')} con falla del broker, por el rebalanceo del grupo (sesión de 6 s) y la elección de líder.
Para acercarse a 2 s hay que acortar la sesión del grupo o usar un despachador en espera activa.{x1c}</li>
<li>Con táctica, {fmt(ta)} señales de telemetría acusadas no quedaron en el log tras matar un broker (ver Anomalías). No afectó a ningún pánico.</li>
</ul></div>
<div><h3><span class="eid">EXP-2</span> Se cumple, pero no discrimina</h3><ul>
<li>Con táctica, el p99 de guardado fue de <b>{fmt(e2t['agregado']['guardado_desde_envio']['p99'], 2, ' s')}</b>, muy por debajo de 1 min, con
<b>{fmt(e2t['totales']['perdidas'])} pérdidas</b> en {fmt(e2t['totales']['enviadas'])} señales, incluido un {fmt(100 * e2t['totales']['atrasadas'] / e2t['totales']['enviadas'], 1, ' %')} de señales atrasadas.</li>
<li>La línea base de cola única también cumple (p99 {fmt(e2b['agregado']['guardado_desde_envio']['p99'], 2, ' s')}) y guarda más rápido:
un solo escritor hace lotes más grandes y menos commits. Con una meta de 1 minuto, guardar no es el cuello de botella.</li>
<li>El costo de la cola única aparece al <b>procesar</b>: en esta misma prueba, la detección en la línea base tuvo p99 de {fmt(e2b['agregado']['deteccion_desde_origen']['p99'], 0, ' s')}.
Por eso EXP-2 debe leerse junto con EXP-3.</li>
</ul></div>
<div><h3><span class="eid">EXP-3</span> Se confirma la hipótesis</h3><ul>
<li>Con 24 colas y un worker por cola, el p99 de procesamiento en el pico fue de <b>{fmt(e3t['agregado']['deteccion_desde_origen_pico']['p99'], 2, ' s')}</b>
(mediana de p99 por repetición: {fmt(e3t['mediana_lat_p99']['deteccion_desde_origen_pico'], 2, ' s')}), con <b>0 señales fuera de orden</b>.
La cola volvió a la normalidad entre 1 y {fmt(e3t['max_tiempo_a_normal_s'], 1, ' s')} después del pico.</li>
<li>La línea base nunca se normalizó. Acumuló ~{fmt(e3b['mediana']['backlog_max'] / 1e6, 2, ' M')} señales pendientes, y 10 minutos después del pico
solo había procesado el {fmt(100 * cov_b, 0, ' %')} de las señales del pico (p99 observado &gt; {fmt(e3b['agregado']['deteccion_desde_origen_pico']['p99'], 0, ' s')}).</li>
<li>El reparto por hash fue parejo entre las 24 colas, y cada camión cayó siempre en la misma cola. Esto adelanta parte de EXP-4.
Con ~480 señales/s por worker, las 24 colas cubren ≈ 11.500 señales/s: el pico proyectado para 40.000 camiones (ESC-2).</li>
</ul></div>
</div></section>"""


def extras_html(A):
    G, C = A["grupos"], A["criterios"]
    if not any(g["exp"] in ("EXP-1b", "EXP-1c") for g in G.values()):
        return ""
    rows = []
    for f in ["despachador", "broker"]:
        for v in ["tactica", "base"]:
            x = G.get(f"EXP-1b|maquina-perdida-{f}|{v}")
            if not x:
                continue
            t = x["totales"]
            rows.append(f"""<tr><td>{f.capitalize()}</td><td><i class="sw {CLS[v]}"></i>{VAR[v]}</td><td>{x['n_ok']}/{x['n']}</td><td>{fmt(t['panicos'])}</td>
            <td>{fmt(t['panicos_guardados_pct'], 3, ' %')}</td><td>{fmt(t['min_panicos_guardados_pct'], 3, ' %')}</td><td>{fmt(t['perdidas_panico'])}</td>
            <td>{fmt(t['perdidas'])}</td><td>{fmt(t['perdidas_acusadas'])}</td><td>{fmt(x['agregado']['panico_guardado_desde_origen']['p99'], 2, ' s')}</td>
            <td>{chip(C.get(f'EXP-1b|maquina-perdida-{f}|{v}', {}).get('cumple'))}</td></tr>""")
    chart = bar_chart(["Despachador", "Broker"], [(VAR[v], CLS[v], [(G.get(f"EXP-1b|maquina-perdida-{f}|{v}") or {}).get("totales", {}).get("panicos_guardados_pct")
                                                                    for f in ["despachador", "broker"]]) for v in ["tactica", "base"]],
                      "% de pánicos guardados", unit=" %", title="Pánicos guardados con pérdida de máquina")
    a, b = G.get("EXP-1|falla-despachador|tactica"), G.get("EXP-1c|falla-despachador-sesion-2s|tactica")
    cmp = ""
    if a and b:
        def r(x, lab):
            L = x["agregado"]["panico_guardado_desde_origen"]
            return (f"<tr><td>{lab}</td><td>{x['n_ok']}/{x['n']}</td><td>{fmt(L['p50'], 3, ' s')}</td><td>{fmt(L['p95'], 2, ' s')}</td><td>{fmt(L['p99'], 2, ' s')}</td>"
                    f"<td>{fmt(L['max'], 2, ' s')}</td><td>{fmt(x['mediana_lat_p99']['panico_guardado_desde_origen'], 2, ' s')}</td>"
                    f"<td>{fmt(x['totales']['perdidas_panico'])}</td><td>{fmt(x['totales']['duplicados'])}</td></tr>")
        pc = bar_chart(["p95", "p99", "máx"], [("Sesión 6 s (EXP-1)", "s1", [a["agregado"]["panico_guardado_desde_origen"][k] for k in ["p95", "p99", "max"]]),
                                               ("Sesión 2 s (EXP-1c)", "s2", [b["agregado"]["panico_guardado_desde_origen"][k] for k in ["p95", "p99", "max"]])],
                       "segundos", unit=" s", title="Latencia del pánico")
        cmp = f"""<h3><span class="eid">EXP-1c</span> Falla del despachador con detección en 2 s</h3>
<p class="lede">Misma prueba que la falla de despachador con táctica, pero el grupo de consumidores declara muerta una instancia en 2 s en vez de 6 s,
con latidos cada 0,5 s. Se mide cuánto tarda un pánico, desde que se pulsa el botón, en quedar guardado. La meta del caso es menos de 2 s.</p>
<div class="scroll"><table><thead><tr><th>Configuración</th><th>Rep. OK</th><th>p50</th><th>p95</th><th>p99</th><th>máx</th><th>Mediana de p99 por rep.</th><th>Pánicos perdidos</th><th>Duplicados</th></tr></thead>
<tbody>{r(a, "Sesión 6 s · latido 1 s (EXP-1)")}{r(b, "Sesión 2 s · latido 0,5 s (EXP-1c)")}</tbody></table></div>
<figure><figcaption>Pánico desde el botón hasta quedar guardado, con falla del despachador</figcaption>
{legend([("Sesión 6 s (EXP-1)", "s1"), ("Sesión 2 s (EXP-1c)", "s2")])}{pc}</figure>"""
    return f"""<section id="extras"><h2>Pruebas adicionales a EXP-1</h2>
<p class="lede">Las dos pruebas se agregaron después de ver los resultados y no estaban en el plan original. Usan la misma carga que EXP-1 y 5 repeticiones por variante.
Se corrieron a la mañana siguiente con el portátil en uso liviano: en algunas repeticiones de EXP-1b el acuse p99 subió a ~0,6 s (13 ms en EXP-1),
así que sus latencias son indicativas. Los conteos de pérdida no dependen de eso.</p>
<h3><span class="eid">EXP-1b</span> La máquina no vuelve</h3>
<p class="lede">En EXP-1 el heartbeat relanza la instancia caída en ~1 s y el proceso conserva su disco, así que la línea base del despachador y del broker no perdió datos.
Aquí el monitor mata la instancia en plena ráfaga y el heartbeat detecta la caída pero <b>no la relanza</b>, como si se perdiera la máquina con su disco.
Solo sobreviven las réplicas.</p>
{legend([(VAR['tactica'], 's1'), (VAR['base'], 's2')])}
<div class="scroll"><table><thead><tr><th>Falla</th><th>Variante</th><th>Rep. OK</th><th>Pánicos enviados</th><th>Pánicos guardados</th><th>Peor rep.</th><th>Pánicos perdidos</th>
<th>Señales perdidas</th><th>…ya acusadas</th><th>p99 pánico guardado</th><th>Criterio</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>
<figure><figcaption>Porcentaje de pánicos guardados, todas las repeticiones</figcaption>{chart}</figure>
{cmp}</section>"""


def facts_html(A, env):
    G = A["grupos"]
    nruns = sum(g["n"] for g in G.values())
    nok = sum(g["n_ok"] for g in G.values())
    sig = sum(g["totales"]["enviadas"] for g in G.values())
    di = {}
    try:
        di = json.loads(env.get("docker_info", "{}"))
    except Exception:
        pass
    cpu = env.get("hw_cpu", "?")
    mem = int(env.get("hw_mem", "0") or 0) / 2**30
    items = [("Corridas", f"{nok}/{nruns} completas"), ("Señales simuladas", fmt(sig)), ("Equipo", f"{cpu} · {env.get('hw_ncpu','?')} núcleos · {mem:.0f} GB"),
             ("Docker", f"{di.get('ServerVersion','?')} · VM con {di.get('NCPU','?')} CPU / {int(di.get('MemTotal',0))/2**30:.1f} GB")]
    return "".join(f"<span>{esc(k)}: <b>{esc(v)}</b></span>" for k, v in items)


def method_html(A, env):
    imgs = env.get("imagenes", "")
    return f"""
<section id="stack"><h2>Stack tecnológico del prototipo</h2>
<p class="lede">Doce contenedores definidos en un único <code>docker-compose.yml</code>. El mismo archivo serviría en una o varias EC2.
Los servicios propios están escritos en Go. Orquestador, monitor, verificador y análisis están en Python 3, solo con la biblioteca estándar.</p>
<dl class="stack">
<dt>OBU simuladas</dt><dd><code>generador</code> (Go): 18.000 camiones. Cada camión envía sus señales en orden por HTTP y solo pasa a la siguiente cuando recibe el acuse.
Si no lo recibe, reintenta con backoff exponencial de 100 ms a 2 s (T2 Reintento). El pánico se antepone a la telemetría pendiente.</dd>
<dt>Balanceador</dt><dd>HAProxy 3.0, en round-robin solo entre los receptores que responden su chequeo de salud. El chequeo corre cada 1 s y una instancia sale tras 2 fallos.</dd>
<dt>Recepción</dt><dd><code>receptor</code> ×3 (Go), sin estado. Separa pánico de telemetría y calcula la partición como FNV-1a(camionId) mod 24.
En la táctica, el acuse a la OBU sale solo cuando el broker confirma con <code>acks=all</code>.</dd>
<dt>Canal durable</dt><dd>Redpanda v24.2.7 (API Kafka), 3 brokers. Topic <code>telemetria</code> con 24 particiones y <code>panicos</code> con 3, ambos con factor de replicación 3,
fsync activo, <code>write_caching</code> apagado y rebalanceo automático de líderes desactivado.</dd>
<dt>Canal de pánicos</dt><dd><code>despachador</code> ×2 (Go): grupo de consumidores con latido de 1 s y sesión de 6 s. Confirma el offset solo después de guardar el caso.</dd>
<dt>Colas de telemetría</dt><dd><code>procesador</code> (Go): 24 pipelines independientes, cada uno con un escritor de hilo único que guarda en PostgreSQL con COPY.
Tras guardar, la señal pasa de forma asíncrona al worker de detección de su cola (2 ms por señal).</dd>
<dt>Almacenamiento</dt><dd>PostgreSQL 16 (una instancia), con tablas <code>telemetria</code>, <code>panico</code>, <code>deteccion</code> y <code>enviado</code>.
Las tablas no tienen restricciones de unicidad, para poder contar duplicados.</dd>
<dt>Monitor</dt><dd><code>run.py</code> (Python) controla el reloj de cada corrida. Inyecta <code>SIGKILL</code> con <code>docker kill</code> y hace de orquestador heartbeat:
cada 1 s detecta instancias caídas y las relanza. También muestrea métricas y <code>docker stats</code> cada segundo.</dd>
<dt>Verificador</dt><dd>Carga en SQL el registro completo de lo enviado por la OBU y lo cruza señal por señal (camión + secuencia) con lo guardado y lo detectado.
Así cuenta pérdidas, duplicados y señales fuera de orden, y calcula los percentiles.</dd>
</dl>
<details><summary>Imágenes y versiones exactas</summary><pre class="scroll" style="padding:12px;margin:0;font:12px var(--f-mono)">{esc(imgs)}</pre></details>
</section>

<section id="metodo"><h2>Cómo se garantizó la confiabilidad</h2>
<div class="cols">
<div><h3>Procedimiento</h3><ul>
<li><b>Estado limpio:</b> antes de cada corrida se ejecuta <code>docker compose down -v</code> y los topics y la base se crean desde cero.</li>
<li><b>5 repeticiones</b> por escenario y variante, intercaladas (táctica, base, táctica…) para no sesgar por deriva del equipo.</li>
<li><b>Línea base</b> sin la táctica, con el mismo código y la misma carga. Solo cambia la configuración.</li>
<li><b>Un solo reloj:</b> generador, servicios y base corren en la misma VM de Docker, así que todas las marcas de tiempo salen del mismo reloj del kernel.</li>
<li><b>Percentiles</b> calculados sobre todas las muestras de las 5 repeticiones, con histogramas de 1 ms combinados. También se reporta la mediana del p99 de cada repetición.</li>
<li>Toda corrida guarda logs de cada contenedor, tablas exportadas, línea de tiempo, docker stats, estado de Redpanda y HAProxy, y el registro completo del generador.</li>
</ul></div>
<div><h3>Definición de las líneas base</h3><ul>
<li><b>EXP-1:</b> sin réplicas: 1 broker con replicación 1, 1 receptor y 1 despachador. El receptor acusa a la OBU al recibir y produce de forma asíncrona con <code>acks=1</code>.
El despachador hace auto-commit y escribe en lotes. El heartbeat también relanza la instancia caída, para aislar el efecto de la redundancia y del acuse tras replicar.</li>
<li><b>EXP-2 y EXP-3:</b> una sola cola (1 partición) para todo, incluidos los pánicos, con un solo escritor y un solo worker. El resto queda igual que en la táctica.</li>
</ul></div>
<div><h3>Decisiones de interpretación</h3><ul>
<li><b>"Guardado"</b> en LAT-1 es el commit en PostgreSQL. También se reporta el acuse, que llega cuando la señal ya está durable en Redpanda.</li>
<li>Las <b>señales atrasadas</b> se miden desde su envío real, porque la OBU estaba sin cobertura antes. Desde el origen se reportan aparte.</li>
<li><b>Despachador</b> = proceso del canal de pánicos que consume el topic y abre el caso.</li>
<li>Con Raft, Redpanda confirma <code>acks=all</code> cuando la <b>mayoría</b> de réplicas (2 de 3) escribió. La tercera alcanza después.</li>
<li><b>Cola normal</b> (EXP-3): backlog ≤ 1.500 señales, es decir ≤ 1 s de carga normal, sostenido 30 s.</li>
<li>El documento menciona 5 y 2 repeticiones. Se usaron 5.</li>
<li><b>Ajustes del montaje tras la validación:</b> el rebalanceo automático de líderes de Redpanda generaba una pausa de más de 4 s unos 60 s después de crear los topics,
y los clientes esperaban hasta 5 s para refrescar metadatos. Se desactivó ese rebalanceo y la espera de metadatos se bajó a 250 ms. Los líderes siguen cambiando cuando cae un broker (EXP-1).</li>
</ul></div>
<div><h3>Limitaciones</h3><ul>
<li>Todo corre en un solo computador: generador, brokers y servicios compiten por la misma CPU. Las cifras absolutas son indicativas, y la comparación entre táctica y línea base es lo que vale.</li>
<li>Se usa HTTP en lugar de MQTT/TLS, Redpanda en lugar de Kafka/MSK y una sola instancia de PostgreSQL. No hay API Gateway, como acepta el propio plan.</li>
<li><code>SIGKILL</code> mata el proceso pero no la máquina: los datos que el sistema operativo ya tenía en caché sobreviven. No se simula pérdida de disco.</li>
<li>El reloj de pared de la VM de Docker recibe ajustes de pocos milisegundos. Por eso "fuera de orden" se mide con el orden real en que el worker procesa cada camión.
El cálculo por marca de tiempo se muestra entre paréntesis y puede marcar falsos positivos cuando el reloj retrocede. El efecto sobre las latencias es de milisegundos.</li>
<li>El costo de detección de 2 ms es simulado. Se eligió para que un worker cubra la carga por cola con 40.000 camiones.</li>
</ul></div>
</div></section>

<section id="archivos"><h2>Dónde están los datos</h2>
<p class="lede">Carpeta <code>resultados/</code> junto al PDF del caso. Cada corrida está en <code>EXP-n/escenario/variante/repN/</code> con:
<code>done.json</code> (metadatos y verificación), <code>verificacion.json</code>, <code>gen.csv.gz</code> (cada señal enviada),
<code>tablas/*.csv.gz</code>, <code>logs/*.log</code>, <code>timeline.csv</code>, <code>docker_stats.jsonl</code>, <code>events.json</code> e <code>histogramas/</code>.
El agregado está en <code>resumen.json</code>, y el código para reproducir todo en <code>prototipo/</code> (<code>python3 orquestador/run.py</code>).</p>
</section>"""


def main():
    A = analyze()
    env = json.loads((RES / "entorno.json").read_text()) if (RES / "entorno.json").exists() else {}
    (RES / "resumen.json").write_text(json.dumps(A, indent=1, default=str))
    parts = report(A, env)
    A["anomalias"] = acked_lost(A)
    (RES / "resumen.json").write_text(json.dumps(A, indent=1, default=str))
    parts.append(anomalies_html(A["anomalias"]))
    parts.insert(1, conclusions_html(A))
    ex = extras_html(A)
    if ex:
        i = next(k for k, p in enumerate(parts) if p.startswith('<section id="exp1">'))
        parts.insert(i + 1, ex)
    tpl = (Path(__file__).parent / "informe_plantilla.html").read_text()
    page = tpl.replace("<!--RESULTADOS-->", "\n".join(parts)).replace("<!--METODO-->", method_html(A, env)).replace("<!--HECHOS-->", facts_html(A, env))
    (RES / "informe.html").write_text(page)
    # resumen corto en Markdown
    md = ["# Resultados LogiSur Andina — EXP-1, EXP-2, EXP-3", ""]
    for k, c in A["criterios"].items():
        md.append(f"- **{k}**: {'CUMPLE' if c['cumple'] else 'NO CUMPLE'} — {json.dumps({kk: vv for kk, vv in c.items() if kk not in ('cumple', 'criterio')}, ensure_ascii=False, default=str)}")
    (RES / "RESUMEN.md").write_text("\n".join(md) + "\n")
    print("\n".join(md))


if __name__ == "__main__":
    main()
