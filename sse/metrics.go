// Package sse реалізує однонапрямлену трансляцію телеметрії сервера через Server-Sent Events.
package sse

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"runtime"
	"time"
)

// DefaultInterval — період відправлення метрик.
const DefaultInterval = 1 * time.Second

// Metrics — знімок стану процесу, що надсилається клієнту.
type Metrics struct {
	Time       string  `json:"time"`       // поточний час сервера (RFC 3339)
	Goroutines int     `json:"goroutines"` // кількість активних горутин
	AllocKB    uint64  `json:"alloc_kb"`   // виділена пам'ять, КБ
	AllocMB    float64 `json:"alloc_mb"`   // виділена пам'ять, МБ
}

// Collect зчитує поточні метрики процесу за допомогою пакета runtime.
func Collect() Metrics {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return Metrics{
		Time:       time.Now().Format(time.RFC3339),
		Goroutines: runtime.NumGoroutine(),
		AllocKB:    ms.Alloc / 1024,
		AllocMB:    math.Round(float64(ms.Alloc)/(1024*1024)*100) / 100,
	}
}

// MetricsStreamHandler повертає обробник GET /api/v1/metrics/stream,
// який щоінтервалу надсилає клієнту метрики у форматі SSE.
func MetricsStreamHandler(interval time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// http.Flusher потрібен, щоб виштовхувати дані в сокет, не закриваючи відповідь
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}

		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("Connection", "keep-alive")
		h.Set("X-Accel-Buffering", "no") // вимикає буферизацію у nginx, якщо він стоїть попереду

		log.Printf("[SSE] Нове підключення клієнта телеметрії: %s", r.RemoteAddr)
		// defer виконається при будь-якому виході з обробника
		defer log.Printf("[SSE] Клієнт %s розірвав з'єднання. Очищення ресурсів виконано.", r.RemoteAddr)

		ticker := time.NewTicker(interval)
		defer ticker.Stop() // зупиняємо таймер, щоб не було витоку

		// перше повідомлення одразу, щоб клієнт не чекав інтервал
		if err := writeEvent(w, flusher, Collect()); err != nil {
			return
		}

		for {
			select {
			case <-r.Context().Done(): // клієнт закрив вкладку або сервер зупиняється
				return
			case <-ticker.C:
				if err := writeEvent(w, flusher, Collect()); err != nil {
					return
				}
			}
		}
	}
}

// writeEvent серіалізує метрики в JSON, записує подію у форматі SSE ("data: ...\n\n") та виштовхує буфер.
func writeEvent(w http.ResponseWriter, f http.Flusher, m Metrics) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
		return err
	}
	f.Flush()
	return nil
}
