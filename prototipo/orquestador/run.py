#!/usr/bin/env python3
"""Orquestador de experimentos LogiSur Andina (EXP-1, EXP-2, EXP-3).

Uso:  python3 run.py [--quick] [--only EXP-1,EXP-2] [--reps 5]

Por cada corrida (experimento × variante × repetición):
  1. Estado limpio (docker compose down -v) y montaje de la variante (táctica o línea base).
  2. Hilos del monitor: línea de tiempo (1 s), docker stats, orquestador heartbeat (1 s: detecta
     contenedores caídos y los relanza), inyección de fallas (EXP-1) y corte de la cola (EXP-3).
  3. Al terminar: verificador (SQL) de pérdidas, duplicados, orden y latencias; histogramas;
     export de tablas, logs de todos los contenedores, estado de Redpanda/HAProxy; down -v.
Cada corrida termina con done.json; las ya completas se saltan (reanudable).
"""
import argparse, csv, datetime as dt, gzip, json, os, random, shutil, subprocess, sys, threading, time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent          # prototipo/
RES = ROOT.parent / "resultados"
PROJECT = "lsa"
PG = ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "lsa", "-d", "lsa", "-v", "ON_ERROR_STOP=1"]

# ----------------------------------------------------------------------------- variantes
TACTICA = dict(brokers=[0, 1, 2], rf=3, tel_parts=24, panic_lane=1, ack="replicated", commit="after_write",
               receptors=[1, 2, 3], despachadores=[1, 2], haproxy="haproxy-3.cfg")
BASE_EXP1 = dict(brokers=[0], rf=1, tel_parts=24, panic_lane=1, ack="immediate", commit="auto",
                 receptors=[1], despachadores=[1], haproxy="haproxy-1.cfg")
BASE_COLA_UNICA = dict(TACTICA, tel_parts=1, panic_lane=0, despachadores=[])

LOAD_EXP1 = dict(PROFILE="150:5000", PANIC_EVERY_S=5, PANIC_BURST=400, PANIC_START_S=5, PANIC_SPREAD_S=2, DRAIN_MAX_S=90)
LOAD_EXP2 = dict(PROFILE="60:1500,300:5000,30:1500", PANIC_EVERY_S=5, PANIC_BURST=400, PANIC_START_S=5, PANIC_SPREAD_S=2,
                 OUTAGE_FRAC=0.055, OUTAGE_S=120, OUTAGE_FROM_S=60, OUTAGE_TO_S=240, DRAIN_MAX_S=90)
LOAD_EXP3 = dict(PROFILE="30:1500,300:5000", TAIL_RATE=1500, TAIL_MAX_S=600, DRAIN_MAX_S=90)

EXP1_KILL_AT = 60.5   # dentro de la ráfaga de pánicos que empieza en t=60 s y dura 2 s
EXP3_PEAK = (30.0, 330.0)        # ventana del pico (s desde inicio del generador)
EXP3_NORMAL_BACKLOG = 1500       # "cola normal": backlog <= 1 s de carga normal
EXP3_NORMAL_HOLD = 30.0          # sostenido 30 s
EXP3_MIN_TAIL = 60.0


def experiments(quick=False):
    l1, l2, l3 = dict(LOAD_EXP1), dict(LOAD_EXP2), dict(LOAD_EXP3)
    kill_at = EXP1_KILL_AT
    if quick:
        l1.update(PROFILE="40:5000", DRAIN_MAX_S=40)
        l2.update(PROFILE="10:1500,30:5000,10:1500", OUTAGE_S=15, OUTAGE_FROM_S=10, OUTAGE_TO_S=20, DRAIN_MAX_S=40)
        l3.update(PROFILE="10:1500,30:5000", TAIL_MAX_S=60, DRAIN_MAX_S=30)
        kill_at = 15.5
    out = []
    for falla in ["despachador", "receptor", "broker"]:
        out.append(dict(exp="EXP-1", scen=f"falla-{falla}", variant="tactica", cfg=TACTICA, load=l1, kill=falla, kill_at=kill_at))
        out.append(dict(exp="EXP-1", scen=f"falla-{falla}", variant="base", cfg=BASE_EXP1, load=l1, kill=falla, kill_at=kill_at))
    out.append(dict(exp="EXP-2", scen="pico", variant="tactica", cfg=TACTICA, load=l2))
    out.append(dict(exp="EXP-2", scen="pico", variant="base", cfg=BASE_COLA_UNICA, load=l2))
    out.append(dict(exp="EXP-3", scen="pico", variant="tactica", cfg=TACTICA, load=l3, quick=quick))
    out.append(dict(exp="EXP-3", scen="pico", variant="base", cfg=BASE_COLA_UNICA, load=l3, quick=quick))
    return out


def extras():
    """Pruebas adicionales a EXP-1 (no estaban en el plan original)."""
    out = []
    for falla in ["despachador", "broker"]:
        for var, cfg in [("tactica", TACTICA), ("base", BASE_EXP1)]:
            out.append(dict(exp="EXP-1b", scen=f"maquina-perdida-{falla}", variant=var, cfg=cfg, load=dict(LOAD_EXP1),
                            kill=falla, kill_at=EXP1_KILL_AT, no_restart=True))
    out.append(dict(exp="EXP-1c", scen="falla-despachador-sesion-2s", variant="tactica",
                    cfg=dict(TACTICA, session_ms=2000, heartbeat_ms=500), load=dict(LOAD_EXP1), kill="despachador", kill_at=EXP1_KILL_AT))
    return out


# ----------------------------------------------------------------------------- utilidades
def now_us():
    return int(time.time() * 1e6)


def log(msg):
    line = f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(RES / "orquestador.log", "a") as f:
        f.write(line + "\n")


def sh(cmd, env=None, check=True, timeout=600, stdout=subprocess.PIPE, stdin=None, input=None):
    r = subprocess.run(cmd, cwd=ROOT, env=env, stdout=stdout, stderr=subprocess.PIPE, stdin=stdin,
                       input=input, timeout=timeout, text=(input is not None or stdout == subprocess.PIPE))
    if check and r.returncode != 0:
        raise RuntimeError(f"falló {' '.join(map(str, cmd))}: {r.stderr if isinstance(r.stderr, str) else r.stderr.decode(errors='ignore')}")
    return r.stdout if stdout == subprocess.PIPE else r


def http_json(url, timeout=2):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def http_text(url, timeout=2):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.read().decode()
    except Exception:
        return None


def cname(svc):
    return f"{PROJECT}-{svc}-1"


def running_containers():
    out = sh(["docker", "ps", "--filter", f"label=com.docker.compose.project={PROJECT}", "--format", "{{.Names}}"], check=False)
    return set(out.split()) if out else set()


def psql(sql, timeout=600):
    return sh(PG + ["-At", "-c", sql], timeout=timeout).strip()


def wait(cond, timeout, every=1.0, what=""):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if cond():
                return True
        except Exception:
            pass
        time.sleep(every)
    raise TimeoutError(f"timeout esperando: {what}")


# ----------------------------------------------------------------------------- montaje
def compose_env(cfg, load, run_dir):
    env = dict(os.environ)
    env.update(
        BROKERS=",".join(f"redpanda-{b}:9092" for b in cfg["brokers"]),
        TEL_PARTITIONS=str(cfg["tel_parts"]), PANIC_PARTITIONS="3", PANIC_LANE=str(cfg["panic_lane"]),
        ACK_MODE=cfg["ack"], COMMIT_MODE=cfg["commit"], HAPROXY_CFG=cfg["haproxy"], RUN_DIR=str(run_dir),
        DETECT_COST_US="2000", TRUCKS="18000",
        SESSION_TIMEOUT_MS=str(cfg.get("session_ms", 6000)), HEARTBEAT_MS=str(cfg.get("heartbeat_ms", 1000)),
    )
    env.update({k: str(v) for k, v in load.items()})
    return env


def services(cfg):
    return ([f"redpanda-{b}" for b in cfg["brokers"]] + ["postgres"] + [f"receptor-{r}" for r in cfg["receptors"]]
            + [f"despachador-{d}" for d in cfg["despachadores"]] + ["procesador", "haproxy"])


def setup(cfg, env):
    sh(["docker", "compose", "down", "-v", "--remove-orphans"], env=env, timeout=300)
    brokers = [f"redpanda-{b}" for b in cfg["brokers"]]
    sh(["docker", "compose", "up", "-d"] + brokers + ["postgres"], env=env)
    expect = "[" + " ".join(str(b) for b in cfg["brokers"]) + "]"

    def healthy():
        h = sh(["docker", "compose", "exec", "-T", "redpanda-0", "rpk", "cluster", "health"], env=env, check=False)
        return f"All nodes:                        {expect}" in h and "Healthy:                          true" in h
    wait(healthy, 180, 2, "cluster redpanda sano")
    rpk = ["docker", "compose", "exec", "-T", "redpanda-0", "rpk"]
    sh(rpk + ["cluster", "config", "set", "write_caching_default", "false"], env=env)
    # El rebalanceo automático de líderes introduce pausas de varios segundos ajenas al experimento.
    sh(rpk + ["cluster", "config", "set", "enable_leader_balancer", "false"], env=env)
    if cfg.get("session_ms", 6000) < 6000:
        # Redpanda rechaza sesiones de grupo menores a group_min_session_timeout_ms (6 s por defecto)
        sh(rpk + ["cluster", "config", "set", "group_min_session_timeout_ms", str(cfg["session_ms"])], env=env)
    sh(rpk + ["topic", "create", "telemetria", "-p", str(cfg["tel_parts"]), "-r", str(cfg["rf"])], env=env)
    if cfg["panic_lane"]:
        sh(rpk + ["topic", "create", "panicos", "-p", "3", "-r", str(cfg["rf"])], env=env)
    wait(lambda: psql("select count(*) from enviado") == "0", 120, 2, "postgres listo")
    time.sleep(2)
    wait(lambda: psql("select count(*) from enviado") == "0", 60, 2, "postgres listo (2)")
    rest = [s for s in services(cfg) if not s.startswith("redpanda") and s != "postgres"]
    sh(["docker", "compose", "up", "-d"] + rest, env=env)

    def lb_ok():
        st = http_text("http://127.0.0.1:8404/stats;csv")
        if not st:
            return False
        ups = [l for l in st.splitlines() if l.startswith("receptores,r") and ",UP," in l]
        return len(ups) == len(cfg["receptors"]) and http_json("http://127.0.0.1:8082/metrics") is not None
    wait(lb_ok, 120, 1, "balanceador con receptores UP y procesador")
    if cfg["despachadores"]:
        time.sleep(3)  # que el grupo de despachadores termine de unirse


# ----------------------------------------------------------------------------- hilos del monitor
class Monitor:
    def __init__(self, run_dir, watched, cfg):
        self.run_dir, self.watched, self.cfg = run_dir, set(watched), cfg
        self.stop_ev = threading.Event()
        self.events = []
        self.lock = threading.Lock()
        self.last_gen, self.last_proc = None, None
        self.no_restart = set()   # servicios cuya "máquina" se perdió: el heartbeat detecta pero no relanza
        self.threads = [threading.Thread(target=f, daemon=True) for f in (self.timeline, self.stats, self.heartbeat)]

    def event(self, kind, **kw):
        e = dict(ts=now_us(), kind=kind, **kw)
        with self.lock:
            self.events.append(e)
        log(f"   evento {kind} {kw}")

    def start(self):
        for t in self.threads:
            t.start()

    def stop(self):
        self.stop_ev.set()
        for t in self.threads:
            t.join(timeout=15)
        (self.run_dir / "events.json").write_text(json.dumps(self.events, indent=1))

    def timeline(self):
        f = open(self.run_dir / "timeline.csv", "w", newline="")
        w = csv.writer(f)
        w.writerow(["ts_us", "gen_elapsed", "gen_phase", "gen_target_rate", "gen_acked_per_s", "gen_emitted", "gen_acked",
                    "gen_failed_attempts", "gen_inflight", "gen_pending", "proc_kafka_lag", "proc_consumed", "proc_saved",
                    "proc_detected", "proc_save_backlog", "proc_backlog", "proc_part_backlog_max", "proc_part_backlog_json",
                    "haproxy_status"])
        while not self.stop_ev.is_set():
            t0 = time.time()
            g = http_json("http://127.0.0.1:9100/status", 0.8) or {}
            p = http_json("http://127.0.0.1:8082/metrics", 0.8) or {}
            hs = ""
            st = http_text("http://127.0.0.1:8404/stats;csv", 0.8)
            if st:
                hs = ";".join(l.split(",")[1] + "=" + l.split(",")[17] for l in st.splitlines() if l.startswith("receptores,r"))
            parts = [x.get("backlog", 0) for x in p.get("partitions", [])]
            if g:
                self.last_gen = g
            if p:
                self.last_proc = p
            w.writerow([now_us(), g.get("elapsed"), g.get("phase"), g.get("targetRate"), g.get("ackedPerSec"), g.get("emitted"),
                        g.get("acked"), g.get("failedAttempts"), g.get("inflight"), g.get("pending"), p.get("kafkaLag"),
                        p.get("consumed"), p.get("saved"), p.get("detected"), p.get("saveBacklog"), p.get("backlog"),
                        max(parts) if parts else None, json.dumps(parts) if parts else "", hs])
            f.flush()
            time.sleep(max(0, 1 - (time.time() - t0)))
        f.close()

    def stats(self):
        with open(self.run_dir / "docker_stats.jsonl", "w") as f:
            while not self.stop_ev.is_set():
                out = sh(["docker", "stats", "--no-stream", "--format", "{{json .}}"], check=False, timeout=30) or ""
                ts = now_us()
                for line in out.splitlines():
                    try:
                        d = json.loads(line)
                        d["ts_us"] = ts
                        f.write(json.dumps(d) + "\n")
                    except Exception:
                        pass
                f.flush()

    def heartbeat(self):
        """Orquestador heartbeat: latido cada 1 s; si una instancia deja de latir, la relanza."""
        down_since = {}
        while not self.stop_ev.is_set():
            t0 = time.time()
            alive = running_containers()
            for svc in list(self.watched):
                n = cname(svc)
                if n not in alive and n not in down_since:
                    down_since[n] = now_us()
                    self.event("heartbeat_detecta_caida", svc=svc)
                    if svc in self.no_restart:
                        self.event("heartbeat_no_relanza_maquina_perdida", svc=svc)
                        continue
                    r = subprocess.run(["docker", "start", n], cwd=ROOT, capture_output=True, text=True)
                    self.event("heartbeat_relanza", svc=svc, ok=r.returncode == 0, err=r.stderr.strip()[:200])
                elif n in alive and n in down_since:
                    self.event("heartbeat_instancia_viva", svc=svc, caida_s=(now_us() - down_since.pop(n)) / 1e6)
            time.sleep(max(0, 1 - (time.time() - t0)))


# ----------------------------------------------------------------------------- verificador
def pct_query(expr, where):
    return f"""SELECT json_build_object('n', count(*), 'min', min(v), 'p50', percentile_disc(0.5) WITHIN GROUP (ORDER BY v),
      'p95', percentile_disc(0.95) WITHIN GROUP (ORDER BY v), 'p99', percentile_disc(0.99) WITHIN GROUP (ORDER BY v),
      'p999', percentile_disc(0.999) WITHIN GROUP (ORDER BY v), 'max', max(v), 'mean', avg(v))
      FROM (SELECT ({expr})/1e6 AS v FROM j WHERE {where}) q"""


def hist_query(expr, where):
    # histograma con resolución de 1 ms (para percentiles agregados entre repeticiones)
    return f"COPY (SELECT floor(({expr})/1000.0)::bigint AS ms, count(*) FROM j WHERE {where} GROUP BY 1 ORDER BY 1) TO STDOUT WITH CSV"


def verify(run_dir, spec, gen_start_us):
    gz = run_dir / "gen.csv.gz"
    with open(gz, "rb") as fz:
        data = gzip.decompress(fz.read())
    subprocess.run(PG + ["-c", "\\copy enviado FROM STDIN WITH CSV HEADER"], cwd=ROOT, input=data, check=True,
                   capture_output=True, timeout=900)
    peak0 = gen_start_us + int(EXP3_PEAK[0] * 1e6)
    peak1 = gen_start_us + int(EXP3_PEAK[1] * 1e6)
    build = f"""
    DROP TABLE IF EXISTS st, j, dt, oo;
    CREATE TABLE st AS SELECT truck_id, seq, min(saved_at) saved_at, count(*) n FROM
      (SELECT truck_id, seq, saved_at FROM telemetria UNION ALL SELECT truck_id, seq, saved_at FROM panico) x GROUP BY 1,2;
    CREATE TABLE dt AS SELECT truck_id, seq, min(detected_at) detected_at, count(*) nd FROM deteccion GROUP BY 1,2;
    CREATE TABLE j AS SELECT e.*, s.saved_at, coalesce(s.n,0) n, d.detected_at, coalesce(d.nd,0) nd,
      (e.ts_origin BETWEEN {peak0} AND {peak1}) AS in_peak
      FROM enviado e LEFT JOIN st s USING (truck_id, seq) LEFT JOIN dt d USING (truck_id, seq);
    -- orden por vehículo: dentro de cada camión, en orden de detección, ¿aparece un seq menor que uno ya visto?
    CREATE TABLE oo AS SELECT truck_id, seq, detected_at,
      max(seq) OVER (PARTITION BY truck_id ORDER BY detected_at, seq ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_max
      FROM deteccion;
    """
    sh(PG + ["-c", build], timeout=900)
    counts = json.loads(psql("""SELECT json_build_object(
      'enviadas', count(*), 'telemetria', count(*) FILTER (WHERE type=0), 'panicos', count(*) FILTER (WHERE type=1),
      'atrasadas', count(*) FILTER (WHERE late), 'acusadas', count(*) FILTER (WHERE ts_ack>0),
      'nunca_enviadas', count(*) FILTER (WHERE ts_sent=0), 'no_acusadas', count(*) FILTER (WHERE ts_ack=0),
      'perdidas', count(*) FILTER (WHERE n=0), 'perdidas_acusadas', count(*) FILTER (WHERE n=0 AND ts_ack>0),
      'perdidas_tel', count(*) FILTER (WHERE n=0 AND type=0), 'perdidas_panico', count(*) FILTER (WHERE n=0 AND type=1),
      'perdidas_panico_acusadas', count(*) FILTER (WHERE n=0 AND type=1 AND ts_ack>0),
      'panicos_guardados', count(*) FILTER (WHERE n>0 AND type=1),
      'duplicados', coalesce(sum(n-1) FILTER (WHERE n>1),0), 'duplicados_panico', coalesce(sum(n-1) FILTER (WHERE n>1 AND type=1),0),
      'reintentos', coalesce(sum(attempts-1) FILTER (WHERE attempts>1),0),
      'detectadas', count(*) FILTER (WHERE type=0 AND nd>0), 'no_detectadas', count(*) FILTER (WHERE type=0 AND nd=0 AND n>0),
      'det_duplicadas', coalesce(sum(nd-1) FILTER (WHERE nd>1),0),
      'tel_pico', count(*) FILTER (WHERE type=0 AND in_peak), 'tel_pico_detectadas', count(*) FILTER (WHERE type=0 AND in_peak AND nd>0)
      ) FROM j"""))
    counts["guardadas_fuera_de_registro"] = int(psql("SELECT count(*) FROM st LEFT JOIN enviado e USING (truck_id, seq) WHERE e.truck_id IS NULL"))
    # fuera de orden real: un seq menor a uno ya procesado del mismo camión, que no es una re-entrega duplicada
    counts["fuera_de_orden"] = int(psql("SELECT count(*) FROM oo JOIN dt USING (truck_id, seq) "
                                        "WHERE oo.prev_max IS NOT NULL AND oo.seq < oo.prev_max AND dt.nd = 1"))
    counts["reentregas_tardias"] = int(psql("SELECT count(*) FROM oo JOIN dt USING (truck_id, seq) "
                                            "WHERE oo.prev_max IS NOT NULL AND oo.seq < oo.prev_max AND dt.nd > 1"))
    counts["fuera_de_orden_worker"] = int(psql("SELECT count(*) FROM deteccion WHERE ooo=2"))
    counts["panicos_guardados_pct"] = 100.0 * counts["panicos_guardados"] / counts["panicos"] if counts["panicos"] else None
    counts["por_receptor"] = json.loads(psql("SELECT coalesce(json_object_agg(receptor, c),'{}') FROM (SELECT receptor, count(*) c FROM telemetria GROUP BY 1) x"))
    counts["por_particion"] = json.loads(psql("SELECT coalesce(json_object_agg(part, c),'{}') FROM (SELECT part, count(*) c FROM telemetria GROUP BY 1) x"))
    counts["panico_por_escritor"] = json.loads(psql("SELECT coalesce(json_object_agg(writer, c),'{}') FROM (SELECT writer, count(*) c FROM panico GROUP BY 1) x"))
    counts["anomalias"] = json.loads(psql("SELECT coalesce(json_object_agg(anomaly, c),'{}') FROM (SELECT anomaly, count(*) c FROM deteccion GROUP BY 1) x"))

    metrics = {
        "guardado_desde_envio": ("saved_at - ts_sent", "n>0 AND ts_sent>0"),
        "guardado_desde_origen": ("saved_at - ts_origin", "n>0"),
        "guardado_tel_puntual_desde_envio": ("saved_at - ts_sent", "n>0 AND type=0 AND NOT late AND ts_sent>0"),
        "guardado_tel_atrasada_desde_envio": ("saved_at - ts_sent", "n>0 AND type=0 AND late AND ts_sent>0"),
        "guardado_tel_atrasada_desde_origen": ("saved_at - ts_origin", "n>0 AND type=0 AND late"),
        "acuse_desde_envio": ("ts_ack - ts_sent", "ts_ack>0"),
        "panico_guardado_desde_origen": ("saved_at - ts_origin", "n>0 AND type=1"),
        "panico_acuse_desde_origen": ("ts_ack - ts_origin", "ts_ack>0 AND type=1"),
        "deteccion_desde_origen": ("detected_at - ts_origin", "type=0 AND nd>0"),
        "deteccion_desde_origen_pico": ("detected_at - ts_origin", "type=0 AND nd>0 AND in_peak"),
        "deteccion_desde_guardado": ("detected_at - saved_at", "type=0 AND nd>0 AND n>0"),
    }
    lat = {}
    hdir = run_dir / "histogramas"
    hdir.mkdir(exist_ok=True)
    for name, (expr, where) in metrics.items():
        lat[name] = json.loads(psql(pct_query(expr, where)))
        if lat[name]["n"]:
            (hdir / f"{name}.csv").write_text(psql(hist_query(expr, where)) + "\n")
    res = dict(conteos=counts, latencias_s=lat)
    (run_dir / "verificacion.json").write_text(json.dumps(res, indent=1))
    return res


def export_all(run_dir, env, cfg):
    tdir = run_dir / "tablas"
    tdir.mkdir(exist_ok=True)
    for t in ["telemetria", "panico", "deteccion"]:
        with gzip.open(tdir / f"{t}.csv.gz", "wb", compresslevel=3) as fz:
            p = subprocess.Popen(PG + ["-c", f"COPY {t} TO STDOUT WITH CSV HEADER"], cwd=ROOT, stdout=subprocess.PIPE)
            shutil.copyfileobj(p.stdout, fz, 1 << 20)
            p.wait()
    ldir = run_dir / "logs"
    ldir.mkdir(exist_ok=True)
    for svc in services(cfg) + ["generador"]:
        with open(ldir / f"{svc}.log", "w") as f:
            subprocess.run(["docker", "compose", "logs", "--no-color", "--timestamps", svc], cwd=ROOT, env=env, stdout=f,
                           stderr=subprocess.STDOUT, timeout=300)
    rpk = ["docker", "compose", "exec", "-T", "redpanda-0", "rpk"]
    snap = {}
    for name, args in {"cluster_health": ["cluster", "health"], "brokers": ["redpanda", "admin", "brokers", "list"],
                       "topic_telemetria": ["topic", "describe", "telemetria", "-a"],
                       "topic_panicos": ["topic", "describe", "panicos", "-a"],
                       "grupo_despachadores": ["group", "describe", "despachadores"],
                       "cluster_config": ["cluster", "config", "export", "-f", "/dev/stdout"]}.items():
        r = subprocess.run(rpk + args, cwd=ROOT, env=env, capture_output=True, text=True, timeout=60)
        snap[name] = r.stdout + r.stderr
    (run_dir / "redpanda_estado.json").write_text(json.dumps(snap, indent=1))
    (run_dir / "haproxy_stats.csv").write_text(http_text("http://127.0.0.1:8404/stats;csv") or "")
    insp = sh(["docker", "inspect"] + [cname(s) for s in services(cfg) + ["generador"]], check=False, timeout=60)
    (run_dir / "docker_inspect.json").write_text(insp or "")
    for svc in [f"receptor-{r}" for r in cfg["receptors"]]:
        m = sh(["docker", "compose", "exec", "-T", svc, "curl", "-s", "localhost:8080/metrics"], env=env, check=False)
        (run_dir / f"metricas_{svc}.json").write_text(m or "")
    for d in cfg["despachadores"]:
        m = sh(["docker", "compose", "exec", "-T", f"despachador-{d}", "curl", "-s", "localhost:8081/metrics"], env=env, check=False)
        (run_dir / f"metricas_despachador-{d}.json").write_text(m or "")
    (run_dir / "metricas_procesador.json").write_text(json.dumps(http_json("http://127.0.0.1:8082/metrics") or {}))


# ----------------------------------------------------------------------------- una corrida
def run_one(spec, rep, seed):
    exp, scen, variant, cfg, load = spec["exp"], spec["scen"], spec["variant"], spec["cfg"], spec["load"]
    run_dir = RES / exp / scen / variant / f"rep{rep}"
    if (run_dir / "done.json").exists() and json.loads((run_dir / "done.json").read_text()).get("ok"):
        log(f"-- {exp}/{scen}/{variant}/rep{rep}: ya completa, se salta")
        return
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)
    env = compose_env(cfg, load, run_dir)
    rnd = random.Random(seed)
    meta = dict(exp=exp, escenario=scen, variante=variant, rep=rep, seed=seed, cfg=cfg, carga=load,
                inicio=dt.datetime.now().isoformat())
    log(f"== {exp}/{scen}/{variant}/rep{rep}  inicio")
    t_run = time.time()
    mon = None
    try:
        setup(cfg, env)
        meta["setup_s"] = round(time.time() - t_run, 1)
        mon = Monitor(run_dir, services(cfg), cfg)
        mon.start()
        sh(["docker", "compose", "up", "-d", "generador"], env=env)
        wait(lambda: http_json("http://127.0.0.1:9100/status") is not None, 60, 0.5, "generador")
        g0 = http_json("http://127.0.0.1:9100/status")
        gen_start_us = g0["ts"] - int(g0["elapsed"] * 1e6)
        mon.event("generador_inicia", gen_start_us=gen_start_us)
        killed = False
        stopped = False
        normal_since = None
        guard = sum(float(p.split(":")[0]) for p in load["PROFILE"].split(",")) + float(load.get("TAIL_MAX_S", 0) if load.get("TAIL_RATE") else 0) + float(load["DRAIN_MAX_S"]) + 300
        t_gen = time.time()
        while True:
            if cname("generador") not in running_containers():
                break
            if time.time() - t_gen > guard:
                raise RuntimeError("la corrida excedió el tiempo máximo")
            g = http_json("http://127.0.0.1:9100/status", 1) or {}
            el = g.get("elapsed", 0)
            if spec.get("kill") and not killed and el >= spec["kill_at"] - 1.2:
                # espera fina hasta el instante exacto (reloj del generador)
                time.sleep(max(0.0, (gen_start_us + spec["kill_at"] * 1e6 - now_us()) / 1e6))
                el = (now_us() - gen_start_us) / 1e6
                kind = spec["kill"]
                if kind == "despachador":
                    target = f"despachador-{rnd.choice(cfg['despachadores'])}"
                elif kind == "receptor":
                    target = f"receptor-{rnd.choice(cfg['receptors'])}"
                else:
                    target = f"redpanda-{rnd.choice(cfg['brokers'])}"
                if spec.get("no_restart"):
                    mon.no_restart.add(target)
                mon.event("monitor_inyecta_falla", svc=target, senal="SIGKILL", gen_elapsed=el, maquina_perdida=bool(spec.get("no_restart")))
                sh(["docker", "kill", "-s", "KILL", cname(target)], check=False)
                meta["falla"] = dict(svc=target, t_gen=el)
                killed = True
            if spec["exp"] == "EXP-3" and not stopped and g.get("phase", "").startswith("cola"):
                p = http_json("http://127.0.0.1:8082/metrics", 1) or {}
                bl = p.get("backlog")
                tail_el = el - (10 + 30 if spec.get("quick") else EXP3_PEAK[1])
                if bl is not None and bl <= EXP3_NORMAL_BACKLOG:
                    normal_since = normal_since or time.time()
                else:
                    normal_since = None
                if normal_since and time.time() - normal_since >= EXP3_NORMAL_HOLD and tail_el >= (20 if spec.get("quick") else EXP3_MIN_TAIL):
                    mon.event("monitor_cola_normal_detiene_generador", backlog=bl, gen_elapsed=el)
                    http_text("http://127.0.0.1:9100/stop")
                    stopped = True
            time.sleep(1)
        mon.event("generador_termina")
        meta["generador"] = json.loads((run_dir / "gen_summary.json").read_text())
        # Esperar a que todo lo acusado quede guardado (EXP-1/EXP-2) o un breve vaciado (EXP-3).
        if spec["exp"] in ("EXP-1", "EXP-2"):
            prev, stable, t0 = None, 0, time.time()
            while time.time() - t0 < 300 and stable < 3:
                p = http_json("http://127.0.0.1:8082/metrics", 2) or {}
                c = psql("select (select count(*) from telemetria) + (select count(*) from panico)")
                stable = stable + 1 if (c == prev and p.get("saveBacklog", 1) == 0) else 0
                prev = c
                time.sleep(2)
            meta["espera_guardado_s"] = round(time.time() - t0, 1)
        else:
            time.sleep(5)
        mon.stop()
        mon_events = mon.events
        mon = None
        meta["eventos"] = mon_events
        log("   verificando…")
        meta["verificacion"] = verify(run_dir, spec, gen_start_us)
        log("   exportando tablas y logs…")
        export_all(run_dir, env, cfg)
        meta["ok"] = True
    except Exception as e:
        meta["ok"] = False
        meta["error"] = repr(e)
        log(f"   ERROR: {e!r}")
        try:
            ldir = run_dir / "logs"
            ldir.mkdir(exist_ok=True)
            for svc in services(cfg) + ["generador"]:
                with open(ldir / f"{svc}.log", "w") as f:
                    subprocess.run(["docker", "compose", "logs", "--no-color", "--timestamps", svc], cwd=ROOT, env=env,
                                   stdout=f, stderr=subprocess.STDOUT, timeout=300)
        except Exception:
            pass
    finally:
        if mon:
            mon.stop()
        sh(["docker", "compose", "down", "-v", "--remove-orphans"], env=env, check=False, timeout=300)
        meta["fin"] = dt.datetime.now().isoformat()
        meta["duracion_s"] = round(time.time() - t_run, 1)
        (run_dir / "done.json").write_text(json.dumps(meta, indent=1, default=str))
        v = meta.get("verificacion", {}).get("conteos", {})
        log(f"== {exp}/{scen}/{variant}/rep{rep}  ok={meta['ok']} {meta['duracion_s']}s  enviadas={v.get('enviadas')} "
            f"perdidas={v.get('perdidas')} perdidas_panico={v.get('perdidas_panico')} dup={v.get('duplicados')} "
            f"fuera_orden={v.get('fuera_de_orden')}")
    return meta


def environment_info():
    info = {}
    for k, cmd in {"docker_version": ["docker", "version", "--format", "{{json .}}"],
                   "docker_info": ["docker", "info", "--format", "{{json .}}"],
                   "compose_version": ["docker", "compose", "version"],
                   "imagenes": ["docker", "images", "--format", "{{.Repository}}:{{.Tag}} {{.ID}} {{.Size}}"],
                   "hw_cpu": ["sysctl", "-n", "machdep.cpu.brand_string"], "hw_ncpu": ["sysctl", "-n", "hw.ncpu"],
                   "hw_mem": ["sysctl", "-n", "hw.memsize"], "os": ["sw_vers"], "python": [sys.executable, "--version"]}.items():
        r = subprocess.run(cmd, capture_output=True, text=True)
        info[k] = (r.stdout or r.stderr).strip()
    info["compose_config"] = subprocess.run(["docker", "compose", "config"], cwd=ROOT, capture_output=True, text=True).stdout
    (RES / "entorno.json").write_text(json.dumps(info, indent=1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--only", default="")
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--results", default="")
    ap.add_argument("--extras", action="store_true", help="solo las pruebas adicionales EXP-1b y EXP-1c")
    a = ap.parse_args()
    global RES
    if a.results:
        RES = Path(a.results)
    RES.mkdir(parents=True, exist_ok=True)
    if not a.extras:
        environment_info()
    specs = extras() if a.extras else experiments(a.quick)
    if a.only:
        specs = [s for s in specs if s["exp"] in a.only.split(",")]
    exps = sorted({s["exp"] for s in specs})
    log(f"### inicio: {len(specs)} escenarios × {a.reps} repeticiones ({', '.join(exps)}) quick={a.quick}")
    for e in exps:
        for rep in range(1, a.reps + 1):
            for i, s in enumerate([s for s in specs if s["exp"] == e]):
                run_one(s, rep, seed=1000 * rep + i)
    log("### fin de todas las corridas")


if __name__ == "__main__":
    main()
