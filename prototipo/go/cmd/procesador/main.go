// Procesador de telemetría: N pipelines en paralelo, uno por cola (partición):
//
//	cola i -> escritor i (hilo único, guarda en PostgreSQL) -> worker de detección i (asíncrono, después de guardar)
//
// Con PARTITIONS=24 es la táctica (T3 particionamiento + T4 concurrencia). Con PARTITIONS=1 es la
// línea base "una sola cola y un solo worker". El costo de detección por señal es DETECT_COST_US.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"lsa/internal/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type partState struct {
	hw, next                  atomic.Int64
	consumed, saved, detected atomic.Int64
	in                        chan []*kgo.Record
	dq                        *queue
}

// queue es una cola FIFO sin límite (la detección es asíncrona y no debe frenar el guardado).
type queue struct {
	mu    sync.Mutex
	cond  *sync.Cond
	items []common.Signal
	head  int
}

func newQueue() *queue { q := &queue{}; q.cond = sync.NewCond(&q.mu); return q }
func (q *queue) push(s []common.Signal) {
	q.mu.Lock()
	q.items = append(q.items, s...)
	q.mu.Unlock()
	q.cond.Signal()
}
func (q *queue) pop() common.Signal {
	q.mu.Lock()
	for q.head >= len(q.items) {
		q.cond.Wait()
	}
	s := q.items[q.head]
	q.head++
	if q.head > 65536 && q.head*2 > len(q.items) {
		q.items = append([]common.Signal(nil), q.items[q.head:]...)
		q.head = 0
	}
	q.mu.Unlock()
	return s
}

type detRow struct {
	truck, seq int
	part       int16
	origin     int64
	sent       int64
	detected   int64
	anomaly    int16
	ooo        int16
}

var telCols = []string{"truck_id", "seq", "part", "ts_origin", "ts_sent", "ts_recv", "receptor", "late"}
var panCols = []string{"truck_id", "seq", "part", "ts_origin", "ts_sent", "ts_recv", "receptor", "writer"}
var detCols = []string{"truck_id", "seq", "part", "ts_origin", "ts_sent", "detected_at", "anomaly", "ooo"}

func copyRetry(ctx context.Context, pool *pgxpool.Pool, table string, cols []string, rows [][]any) {
	if len(rows) == 0 {
		return
	}
	for {
		if _, err := pool.CopyFrom(ctx, pgx.Identifier{table}, cols, pgx.CopyFromRows(rows)); err == nil {
			return
		} else {
			log.Printf("copy %s error, reintento: %v", table, err)
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func main() {
	ctx := context.Background()
	topic := common.Env("TEL_TOPIC", "telemetria")
	nparts := common.EnvInt("PARTITIONS", 24)
	cost := time.Duration(common.EnvInt("DETECT_COST_US", 2000)) * time.Microsecond
	pool := common.PGPool(ctx, nparts+8)

	parts := make([]*partState, nparts)
	offs := map[int32]kgo.Offset{}
	for i := range parts {
		parts[i] = &partState{in: make(chan []*kgo.Record, 100000), dq: newQueue()}
		offs[int32(i)] = kgo.NewOffset().AtStart()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(common.Brokers()...),
		kgo.MetadataMinAge(250*time.Millisecond), // reaccionar rápido a cambios de líder
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offs}),
		kgo.FetchMaxWait(50*time.Millisecond),
		kgo.ClientID("procesador"),
	)
	if err != nil {
		log.Fatal(err)
	}

	detOut := make(chan detRow, 200000)

	// Escritor i: hilo único por cola.
	for i, ps := range parts {
		go func(i int, ps *partState) {
			for batch := range ps.in {
				// junta lo que ya esté disponible, hasta 2000 registros
			more:
				for len(batch) < 2000 {
					select {
					case b := <-ps.in:
						batch = append(batch, b...)
					default:
						break more
					}
				}
				var tel, pan [][]any
				sigs := make([]common.Signal, 0, len(batch))
				for _, r := range batch {
					var s common.Signal
					if json.Unmarshal(r.Value, &s) != nil {
						continue
					}
					if s.Type == "panic" {
						pan = append(pan, []any{s.TruckID, s.Seq, int16(i), s.TsOrigin, s.TsSent, s.TsRecv, int16(s.Receptor), "procesador"})
					} else {
						tel = append(tel, []any{s.TruckID, s.Seq, int16(i), s.TsOrigin, s.TsSent, s.TsRecv, int16(s.Receptor), s.Late})
						sigs = append(sigs, s)
					}
				}
				copyRetry(ctx, pool, "telemetria", telCols, tel)
				copyRetry(ctx, pool, "panico", panCols, pan)
				ps.saved.Add(int64(len(batch)))
				ps.detected.Add(int64(len(batch) - len(sigs))) // los pánicos no pasan por detección
				ps.dq.push(sigs)
			}
		}(i, ps)
	}

	// Worker de detección i: un worker por cola, asíncrono después de guardar.
	for i, ps := range parts {
		go func(i int, ps *partState) {
			maxSeq := map[int]int{}
			for {
				s := ps.dq.pop()
				time.Sleep(cost) // costo simulado de evaluar reglas (accidente, temperatura, desvío)
				var anomaly int16
				switch {
				case s.Temp > 8 || s.Temp < -25:
					anomaly = 1
				case s.Door && s.Speed > 5:
					anomaly = 2
				case s.Speed > 110:
					anomaly = 3
				}
				var ooo int16
				if m, ok := maxSeq[s.TruckID]; ok {
					if s.Seq == m {
						ooo = 1
					} else if s.Seq < m {
						ooo = 2
					}
				}
				if s.Seq > maxSeq[s.TruckID] {
					maxSeq[s.TruckID] = s.Seq
				}
				detOut <- detRow{s.TruckID, s.Seq, int16(i), s.TsOrigin, s.TsSent, common.NowMicro(), anomaly, ooo}
				ps.detected.Add(1)
			}
		}(i, ps)
	}

	// Escritor de resultados de detección.
	go func() {
		rows := make([][]any, 0, 4000)
		t := time.NewTicker(100 * time.Millisecond)
		flush := func() {
			copyRetry(ctx, pool, "deteccion", detCols, rows)
			rows = rows[:0]
		}
		for {
			select {
			case d := <-detOut:
				rows = append(rows, []any{d.truck, d.seq, d.part, d.origin, d.sent, d.detected, d.anomaly, d.ooo})
				if len(rows) >= 4000 {
					flush()
				}
			case <-t.C:
				flush()
			}
		}
	}()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		var lag, consumed, saved, detected int64
		per := make([]map[string]int64, nparts)
		for i, ps := range parts {
			l := ps.hw.Load() - ps.next.Load()
			if l < 0 {
				l = 0
			}
			c, s, d := ps.consumed.Load(), ps.saved.Load(), ps.detected.Load()
			lag, consumed, saved, detected = lag+l, consumed+c, saved+s, detected+d
			per[i] = map[string]int64{"lag": l, "consumed": c, "saved": s, "detected": d, "backlog": l + c - d}
		}
		json.NewEncoder(w).Encode(map[string]any{
			"ts": common.NowMicro(), "kafkaLag": lag, "consumed": consumed, "saved": saved, "detected": detected,
			"saveQueue": consumed - saved, "detectQueue": saved - detected, "backlog": lag + consumed - detected,
			"saveBacklog": lag + consumed - saved, "partitions": per,
		})
	})
	go http.ListenAndServe(":8082", nil)
	log.Printf("procesador listo partitions=%d cost=%v", nparts, cost)

	go func() {
		for range time.Tick(10 * time.Second) {
			var c, s, d int64
			for _, ps := range parts {
				c, s, d = c+ps.consumed.Load(), s+ps.saved.Load(), d+ps.detected.Load()
			}
			log.Printf("consumed=%d saved=%d detected=%d", c, s, d)
		}
	}()

	for {
		fs := cl.PollFetches(ctx)
		fs.EachError(func(t string, p int32, err error) { log.Printf("fetch error %s/%d: %v", t, p, err) })
		fs.EachPartition(func(p kgo.FetchTopicPartition) {
			ps := parts[p.Partition]
			ps.hw.Store(p.HighWatermark)
			if n := len(p.Records); n > 0 {
				ps.next.Store(p.Records[n-1].Offset + 1)
				ps.consumed.Add(int64(n))
				ps.in <- p.Records
			}
		})
	}
}
