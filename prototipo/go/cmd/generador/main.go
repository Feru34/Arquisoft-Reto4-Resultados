// Generador de carga: simula las OBU de TRUCKS camiones.
//
//   - Un despachador central emite señales a la tasa exacta del perfil (PROFILE="seg:tasa,...")
//     repartiéndolas en round-robin entre camiones (cada camión reporta cada TRUCKS/tasa segundos).
//   - Cada camión envía SUS señales en orden y solo pasa a la siguiente al recibir el acuse
//     (táctica T2 Reintento: reintenta con backoff hasta recibir 200).
//   - Ráfagas de pánicos: cada PANIC_EVERY_S segundos, PANIC_BURST camiones aprietan el botón.
//     El pánico tiene prioridad sobre la telemetría pendiente del mismo camión.
//   - Pérdida de cobertura: una fracción OUTAGE_FRAC de camiones queda sin cobertura OUTAGE_S
//     segundos; acumula sus señales y las envía al recuperarla (señales atrasadas).
//   - TAIL_RATE>0: tras el perfil sigue a esa tasa hasta recibir POST /stop o TAIL_MAX_S.
//
// Registra cada señal en OUT_DIR/gen.csv.gz: truck,seq,type,origin,sent,ack,attempts,late
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"lsa/internal/common"
)

type sig struct {
	s     common.Signal
	ack   int64
	sent  bool
}

type truck struct {
	id               int
	mu               sync.Mutex
	seq              int
	tel, pan         []*sig
	wake             chan struct{}
	outStart, outEnd int64
}

type phase struct {
	dur  float64
	rate float64
}

var (
	emitted, panics, acked, attemptsTotal, failedAttempts, logged, inflight atomic.Int64
	stopTail                                                                atomic.Bool
	genDone                                                                 = make(chan struct{})
	drainDeadline                                                           atomic.Int64
	curRate                                                                 atomic.Int64
	phaseName                                                               atomic.Value
)

type row struct {
	truck, seq, typ          int
	origin, sent, ack        int64
	attempts                 int
	late                     bool
}

func parseProfile(p string) []phase {
	var out []phase
	for _, part := range strings.Split(p, ",") {
		kv := strings.Split(strings.TrimSpace(part), ":")
		d, _ := strconv.ParseFloat(kv[0], 64)
		r, _ := strconv.ParseFloat(kv[1], 64)
		out = append(out, phase{d, r})
	}
	return out
}

func (t *truck) enqueue(typ string, origin int64) {
	t.mu.Lock()
	t.seq++
	s := &sig{s: common.Signal{TruckID: t.id, Seq: t.seq, Type: typ, TsOrigin: origin}}
	s.s.Late = t.outStart > 0 && origin >= t.outStart && origin < t.outEnd
	s.s.Temp = 4 + rand.NormFloat64()
	if rand.Float64() < 0.005 {
		s.s.Temp = 9 + rand.Float64()*5
	}
	s.s.Speed = math.Max(0, 60+rand.NormFloat64()*20)
	s.s.Door = rand.Float64() < 0.002
	s.s.Lat = -4 + rand.Float64()*15
	s.s.Lon = -80 + rand.Float64()*8
	if typ == "panic" {
		t.pan = append(t.pan, s)
	} else {
		t.tel = append(t.tel, s)
	}
	t.mu.Unlock()
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

func (t *truck) next() *sig {
	for {
		t.mu.Lock()
		if len(t.pan) > 0 {
			s := t.pan[0]
			t.pan = t.pan[1:]
			t.mu.Unlock()
			return s
		}
		if len(t.tel) > 0 {
			s := t.tel[0]
			t.tel[0] = nil
			t.tel = t.tel[1:]
			t.mu.Unlock()
			return s
		}
		t.mu.Unlock()
		select {
		case <-t.wake:
		case <-genDone:
			t.mu.Lock()
			empty := len(t.pan) == 0 && len(t.tel) == 0
			t.mu.Unlock()
			if empty {
				return nil
			}
		}
	}
}

func main() {
	target := common.Env("TARGET", "http://haproxy:8080/signal")
	nTrucks := common.EnvInt("TRUCKS", 18000)
	profile := parseProfile(common.Env("PROFILE", "60:1500,300:5000,30:1500"))
	tailRate := common.EnvFloat("TAIL_RATE", 0)
	tailMax := common.EnvFloat("TAIL_MAX_S", 600)
	panicEvery := common.EnvFloat("PANIC_EVERY_S", 0)
	panicBurst := common.EnvInt("PANIC_BURST", 0)
	panicStart := common.EnvFloat("PANIC_START_S", 10)
	panicSpread := common.EnvFloat("PANIC_SPREAD_S", 0) // la ráfaga se reparte en este intervalo
	outFrac := common.EnvFloat("OUTAGE_FRAC", 0)
	outDur := common.EnvFloat("OUTAGE_S", 120)
	outFrom := common.EnvFloat("OUTAGE_FROM_S", 60)
	outTo := common.EnvFloat("OUTAGE_TO_S", 240)
	attemptTimeout := time.Duration(common.EnvInt("ATTEMPT_TIMEOUT_MS", 5000)) * time.Millisecond
	drainMax := common.EnvFloat("DRAIN_MAX_S", 90)
	outDir := common.Env("OUT_DIR", "/results")
	phaseName.Store("init")

	tr := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        30000,
		MaxIdleConnsPerHost: 30000,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	client := &http.Client{Transport: tr}

	start := time.Now()
	startUs := start.UnixMicro()
	trucks := make([]*truck, nTrucks)
	for i := range trucks {
		trucks[i] = &truck{id: i + 1, wake: make(chan struct{}, 1)}
	}
	if outFrac > 0 {
		for _, i := range rand.Perm(nTrucks)[:int(outFrac*float64(nTrucks))] {
			st := outFrom + rand.Float64()*(outTo-outFrom)
			trucks[i].outStart = startUs + int64(st*1e6)
			trucks[i].outEnd = trucks[i].outStart + int64(outDur*1e6)
		}
	}
	order := rand.Perm(nTrucks)

	f, err := os.Create(outDir + "/gen.csv.gz")
	if err != nil {
		log.Fatal(err)
	}
	gz := gzip.NewWriter(f)
	rows := make(chan row, 1_000_000)
	writerDone := make(chan struct{})
	go func() {
		buf := &bytes.Buffer{}
		buf.WriteString("truck_id,seq,type,ts_origin,ts_sent,ts_ack,attempts,late\n")
		for r := range rows {
			fmt.Fprintf(buf, "%d,%d,%d,%d,%d,%d,%d,%t\n", r.truck, r.seq, r.typ, r.origin, r.sent, r.ack, r.attempts, r.late)
			if buf.Len() > 1<<20 {
				gz.Write(buf.Bytes())
				buf.Reset()
			}
			logged.Add(1)
		}
		gz.Write(buf.Bytes())
		gz.Close()
		f.Close()
		close(writerDone)
	}()

	// Control y estado.
	var lastAcked atomic.Int64
	var achieved atomic.Int64
	go func() {
		for range time.Tick(time.Second) {
			a := acked.Load()
			achieved.Store(a - lastAcked.Load())
			lastAcked.Store(a)
		}
	}()
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"ts": common.NowMicro(), "elapsed": time.Since(start).Seconds(), "phase": phaseName.Load(),
			"targetRate": curRate.Load(), "ackedPerSec": achieved.Load(), "emitted": emitted.Load(),
			"panics": panics.Load(), "acked": acked.Load(), "attempts": attemptsTotal.Load(),
			"failedAttempts": failedAttempts.Load(), "logged": logged.Load(), "inflight": inflight.Load(),
			"pending": emitted.Load() - logged.Load(),
		})
	})
	http.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) { stopTail.Store(true); w.WriteHeader(200) })
	go http.ListenAndServe(":9100", nil)

	// Emisores por camión.
	var wg sync.WaitGroup
	for _, t := range trucks {
		wg.Add(1)
		go func(t *truck) {
			defer wg.Done()
			for {
				s := t.next()
				if s == nil {
					return
				}
				typ := 0
				if s.s.Type == "panic" {
					typ = 1
				}
				dd := drainDeadline.Load()
				if dd > 0 && common.NowMicro() > dd {
					rows <- row{t.id, s.s.Seq, typ, s.s.TsOrigin, 0, 0, 0, s.s.Late}
					continue
				}
				if now := common.NowMicro(); t.outStart > 0 && now >= t.outStart && now < t.outEnd {
					time.Sleep(time.Duration(t.outEnd-now) * time.Microsecond)
				}
				s.s.TsSent = common.NowMicro()
				var ack int64
				attempt := 0
				for {
					attempt++
					s.s.Attempt = attempt
					body, _ := json.Marshal(&s.s)
					ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
					req, _ := http.NewRequestWithContext(ctx, "POST", target, bytes.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					inflight.Add(1)
					resp, err := client.Do(req)
					inflight.Add(-1)
					attemptsTotal.Add(1)
					ok := false
					if err == nil {
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
						ok = resp.StatusCode == 200
					}
					cancel()
					if ok {
						ack = common.NowMicro()
						acked.Add(1)
						break
					}
					failedAttempts.Add(1)
					if dd := drainDeadline.Load(); dd > 0 && common.NowMicro() > dd {
						break
					}
					back := time.Duration(math.Min(2000, 100*math.Pow(2, float64(attempt-1)))) * time.Millisecond
					time.Sleep(back/2 + time.Duration(rand.Int64N(int64(back/2)+1)))
				}
				rows <- row{t.id, s.s.Seq, typ, s.s.TsOrigin, s.s.TsSent, ack, attempt, s.s.Late}
			}
		}(t)
	}

	// Despachador central de la tasa.
	var totalProfile float64
	for _, p := range profile {
		totalProfile += p.dur
	}
	rateAt := func(el float64) (float64, string) {
		acc := 0.0
		for i, p := range profile {
			if el < acc+p.dur {
				return p.rate, fmt.Sprintf("fase%d:%.0f/s", i+1, p.rate)
			}
			acc += p.dur
		}
		if tailRate > 0 && !stopTail.Load() && el < totalProfile+tailMax {
			return tailRate, fmt.Sprintf("cola:%.0f/s", tailRate)
		}
		return -1, "fin"
	}
	var target64 float64
	var nEmitted int64
	nextBurst := panicStart
	last := start
	tick := time.NewTicker(time.Millisecond)
	for now := range tick.C {
		el := now.Sub(start).Seconds()
		rate, name := rateAt(el)
		if rate < 0 {
			break
		}
		phaseName.Store(name)
		curRate.Store(int64(rate))
		target64 += rate * now.Sub(last).Seconds()
		last = now
		nowUs := now.UnixMicro()
		for float64(nEmitted) < target64 {
			trucks[order[nEmitted%int64(nTrucks)]].enqueue("tel", nowUs)
			nEmitted++
			emitted.Add(1)
		}
		if panicEvery > 0 && el >= nextBurst && el < totalProfile {
			nextBurst += panicEvery
			n := 0
			for _, i := range rand.Perm(nTrucks) {
				t := trucks[i]
				if t.outStart > 0 && nowUs >= t.outStart && nowUs < t.outEnd {
					continue
				}
				if panicSpread > 0 {
					t := t
					time.AfterFunc(time.Duration(rand.Float64()*panicSpread*float64(time.Second)), func() {
						t.enqueue("panic", common.NowMicro())
						emitted.Add(1)
						panics.Add(1)
					})
				} else {
					t.enqueue("panic", nowUs)
					emitted.Add(1)
					panics.Add(1)
				}
				if n++; n >= panicBurst {
					break
				}
			}
		}
	}
	tick.Stop()
	phaseName.Store("drenando")
	curRate.Store(0)
	drainDeadline.Store(common.NowMicro() + int64(drainMax*1e6))
	close(genDone)
	wg.Wait()
	close(rows)
	<-writerDone
	phaseName.Store("terminado")
	sum := map[string]any{
		"durationS": time.Since(start).Seconds(), "emitted": emitted.Load(), "panics": panics.Load(),
		"acked": acked.Load(), "attempts": attemptsTotal.Load(), "failedAttempts": failedAttempts.Load(),
		"trucks": nTrucks, "profile": common.Env("PROFILE", ""), "tailRate": tailRate,
		"startUs": startUs, "endUs": common.NowMicro(),
	}
	b, _ := json.MarshalIndent(sum, "", "  ")
	os.WriteFile(outDir+"/gen_summary.json", b, 0o644)
	log.Printf("generador terminado: %s", b)
	time.Sleep(3 * time.Second) // permite al monitor leer el estado final
}
