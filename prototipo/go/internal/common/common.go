// Package common contiene tipos y utilidades compartidas por los servicios del prototipo LogiSur Andina.
package common

import (
	"context"
	"hash/fnv"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Signal es la señal que envía la OBU de un camión.
type Signal struct {
	TruckID  int     `json:"truckId"`
	Seq      int     `json:"seq"`
	Type     string  `json:"type"` // "tel" | "panic"
	TsOrigin int64   `json:"tsOrigin"` // µs epoch: momento en que se generó en el camión
	TsSent   int64   `json:"tsSent"`   // µs epoch: primer intento de envío
	Attempt  int     `json:"attempt"`
	Late     bool    `json:"late"` // generada mientras el camión no tenía cobertura
	Temp     float64 `json:"temp"`
	Speed    float64 `json:"speed"`
	Lat      float64 `json:"lat"`
	Lon      float64 `json:"lon"`
	Door     bool    `json:"door"`
	TsRecv   int64   `json:"tsRecv,omitempty"`  // µs epoch: llegada al receptor
	Receptor int     `json:"receptor,omitempty"` // instancia receptora
}

// NowMicro devuelve el instante actual en microsegundos.
func NowMicro() int64 { return time.Now().UnixMicro() }

// Partition aplica el hash determinista del camión (FNV-1a 32 bits) módulo n.
func Partition(truckID int, n int) int32 {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(strconv.Itoa(truckID)))
	return int32(h.Sum32() % uint32(n))
}

func Env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func EnvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("env %s=%q no es entero", k, v)
		}
		return i
	}
	return def
}

func EnvFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			log.Fatalf("env %s=%q no es número", k, v)
		}
		return f
	}
	return def
}

func Brokers() []string { return strings.Split(Env("BROKERS", "redpanda-0:9092"), ",") }

// PGPool abre un pool de conexiones a PostgreSQL, reintentando hasta que esté disponible.
func PGPool(ctx context.Context, maxConns int) *pgxpool.Pool {
	dsn := Env("PG_DSN", "postgres://lsa:lsa@postgres:5432/lsa")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MaxConns = int32(maxConns)
	for i := 0; ; i++ {
		p, err := pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if err = p.Ping(ctx); err == nil {
				return p
			}
			p.Close()
		}
		if i > 120 {
			log.Fatalf("postgres no disponible: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
