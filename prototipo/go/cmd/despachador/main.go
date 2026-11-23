// Despachador del canal de pánicos: consume el topic de pánicos (grupo de consumidores con
// latido cada 1 s) y abre el caso guardándolo en PostgreSQL (tabla panico).
//
// COMMIT_MODE=after_write (táctica): confirma el offset SOLO después de que el caso quedó guardado.
// COMMIT_MODE=auto (línea base): auto-commit periódico de lo leído + escritura asíncrona en lotes.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"lsa/internal/common"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

var polled, written atomic.Int64

var cols = []string{"truck_id", "seq", "part", "ts_origin", "ts_sent", "ts_recv", "receptor", "writer"}

func rowsOf(recs []*kgo.Record, writer string) [][]any {
	rows := make([][]any, 0, len(recs))
	for _, r := range recs {
		var s common.Signal
		if err := json.Unmarshal(r.Value, &s); err != nil {
			continue
		}
		rows = append(rows, []any{s.TruckID, s.Seq, int16(r.Partition), s.TsOrigin, s.TsSent, s.TsRecv, int16(s.Receptor), writer})
	}
	return rows
}

func write(ctx context.Context, pool *pgxpool.Pool, rows [][]any) {
	for {
		_, err := pool.CopyFrom(ctx, pgx.Identifier{"panico"}, cols, pgx.CopyFromRows(rows))
		if err == nil {
			written.Add(int64(len(rows)))
			return
		}
		log.Printf("error escribiendo pánicos, reintento: %v", err)
		time.Sleep(200 * time.Millisecond)
	}
}

func main() {
	ctx := context.Background()
	instance := common.EnvInt("INSTANCE", 1)
	writer := fmt.Sprintf("despachador-%d", instance)
	mode := common.Env("COMMIT_MODE", "after_write")
	topic := common.Env("PANIC_TOPIC", "panicos")
	pool := common.PGPool(ctx, 4)

	opts := []kgo.Opt{
		kgo.SeedBrokers(common.Brokers()...),
		kgo.MetadataMinAge(250*time.Millisecond), // reaccionar rápido a cambios de líder
		kgo.ConsumerGroup("despachadores"),
		kgo.ConsumeTopics(topic),
		kgo.ClientID(writer),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(time.Duration(common.EnvInt("SESSION_TIMEOUT_MS", 6000)) * time.Millisecond),
		kgo.HeartbeatInterval(time.Duration(common.EnvInt("HEARTBEAT_MS", 1000)) * time.Millisecond), // latido
		kgo.RebalanceTimeout(10 * time.Second),
		kgo.FetchMaxWait(100 * time.Millisecond),
	}
	if mode == "after_write" {
		opts = append(opts, kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	} else {
		opts = append(opts, kgo.AutoCommitInterval(1*time.Second))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"instance": instance, "polled": polled.Load(), "written": written.Load()})
	})
	go http.ListenAndServe(":8081", nil)
	go func() {
		for range time.Tick(10 * time.Second) {
			log.Printf("polled=%d written=%d", polled.Load(), written.Load())
		}
	}()
	log.Printf("%s listo commit=%s", writer, mode)

	var buf chan []*kgo.Record
	if mode != "after_write" {
		// Línea base: lo leído se encola en memoria y se escribe cada 500 ms.
		buf = make(chan []*kgo.Record, 10000)
		go func() {
			var pending []*kgo.Record
			t := time.NewTicker(500 * time.Millisecond)
			for {
				select {
				case b := <-buf:
					pending = append(pending, b...)
				case <-t.C:
					if len(pending) > 0 {
						write(ctx, pool, rowsOf(pending, writer))
						pending = nil
					}
				}
			}
		}()
	}

	for {
		fs := cl.PollFetches(ctx)
		fs.EachError(func(t string, p int32, err error) { log.Printf("fetch error %s/%d: %v", t, p, err) })
		recs := fs.Records()
		polled.Add(int64(len(recs)))
		if mode == "after_write" {
			if len(recs) > 0 {
				write(ctx, pool, rowsOf(recs, writer))
				if err := cl.CommitRecords(ctx, recs...); err != nil {
					log.Printf("commit error: %v", err)
				}
			}
			cl.AllowRebalance()
		} else if len(recs) > 0 {
			buf <- recs
		}
	}
}
