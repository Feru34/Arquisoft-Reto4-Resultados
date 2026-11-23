// Receptor de señales: sin estado. Divide pánico/telemetría y particiona por hash(camionId).
//
// ACK_MODE=replicated (táctica): responde 200 a la OBU solo cuando el broker confirma la
// escritura con acks=all (mayoría Raft de las réplicas). Si falla, responde 503 y la OBU reintenta.
// ACK_MODE=immediate (línea base): responde 200 al recibir y produce de forma asíncrona con acks=1;
// si la producción falla después, la señal se pierde (ya fue acusada).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"lsa/internal/common"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	received, acked, failed, asyncLost atomic.Int64
)

func main() {
	instance := common.EnvInt("INSTANCE", 1)
	ackMode := common.Env("ACK_MODE", "replicated")
	panicLane := common.Env("PANIC_LANE", "1") == "1"
	telTopic := common.Env("TEL_TOPIC", "telemetria")
	panicTopic := common.Env("PANIC_TOPIC", "panicos")
	telParts := common.EnvInt("TEL_PARTITIONS", 24)
	panicParts := common.EnvInt("PANIC_PARTITIONS", 3)
	produceTimeout := time.Duration(common.EnvInt("PRODUCE_TIMEOUT_MS", 4000)) * time.Millisecond

	opts := []kgo.Opt{
		kgo.SeedBrokers(common.Brokers()...),
		kgo.MetadataMinAge(250*time.Millisecond), // reaccionar rápido a cambios de líder
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ClientID(fmt.Sprintf("receptor-%d", instance)),
		kgo.MaxBufferedRecords(1_000_000),
	}
	if ackMode == "replicated" {
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	} else {
		opts = append(opts,
			kgo.RequiredAcks(kgo.LeaderAck()),
			kgo.DisableIdempotentWrite(),
			kgo.ProducerLinger(50*time.Millisecond),
			kgo.RecordDeliveryTimeout(3*time.Second),
		)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"instance": instance, "received": received.Load(), "acked": acked.Load(),
			"failed": failed.Load(), "asyncLost": asyncLost.Load(),
		})
	})
	mux.HandleFunc("/signal", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "bad body", 400)
			return
		}
		var s common.Signal
		if err := json.Unmarshal(body, &s); err != nil {
			http.Error(w, "bad json", 400)
			return
		}
		received.Add(1)
		s.TsRecv = common.NowMicro()
		s.Receptor = instance
		val, _ := json.Marshal(&s)
		rec := &kgo.Record{Key: []byte(fmt.Sprint(s.TruckID)), Value: val}
		if s.Type == "panic" && panicLane {
			rec.Topic, rec.Partition = panicTopic, common.Partition(s.TruckID, panicParts)
		} else {
			rec.Topic, rec.Partition = telTopic, common.Partition(s.TruckID, telParts)
		}
		if ackMode == "replicated" {
			ctx, cancel := context.WithTimeout(r.Context(), produceTimeout)
			defer cancel()
			if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
				if failed.Add(1)%200 == 1 {
					log.Printf("fallo al producir (total=%d): %v", failed.Load(), err)
				}
				http.Error(w, "no guardado", 503)
				return
			}
			acked.Add(1)
			w.WriteHeader(200)
			return
		}
		// Línea base: acuse inmediato, producción asíncrona.
		cl.Produce(context.Background(), rec, func(_ *kgo.Record, err error) {
			if err != nil {
				if asyncLost.Add(1)%200 == 1 {
					log.Printf("señal acusada pero NO producida (total=%d): %v", asyncLost.Load(), err)
				}
			}
		})
		acked.Add(1)
		w.WriteHeader(200)
	})
	go func() {
		for range time.Tick(10 * time.Second) {
			log.Printf("received=%d acked=%d failed=%d asyncLost=%d", received.Load(), acked.Load(), failed.Load(), asyncLost.Load())
		}
	}()
	log.Printf("receptor %d listo ack=%s panicLane=%v telParts=%d", instance, ackMode, panicLane, telParts)
	srv := &http.Server{Addr: ":8080", Handler: mux, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 120 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
