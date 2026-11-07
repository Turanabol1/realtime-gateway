package sse

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestCollect(t *testing.T) {
	m := Collect()
	if m.Goroutines < 1 {
		t.Errorf("goroutines = %d, want >= 1", m.Goroutines)
	}
	if m.AllocKB == 0 {
		t.Error("alloc_kb = 0, очікувалась ненульова виділена пам'ять")
	}
	if _, err := time.Parse(time.RFC3339, m.Time); err != nil {
		t.Errorf("time %q не у форматі RFC 3339: %v", m.Time, err)
	}
}

func TestStreamSendsEventsWithSSEHeaders(t *testing.T) {
	srv := httptest.NewServer(MetricsStreamHandler(10 * time.Millisecond))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-cache" {
		t.Errorf("Cache-Control = %q", got)
	}
	if got := resp.Header.Get("Connection"); got != "keep-alive" {
		t.Errorf("Connection = %q", got)
	}

	reader := bufio.NewReader(resp.Body)
	for i := 0; i < 3; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("подія %d: %v", i, err)
		}
		if !strings.HasPrefix(line, "data: ") {
			t.Fatalf("рядок %q не починається з \"data: \"", line)
		}
		var m Metrics
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &m); err != nil {
			t.Fatalf("некоректний JSON %q: %v", line, err)
		}
		if m.Goroutines < 1 {
			t.Errorf("goroutines = %d", m.Goroutines)
		}
		if blank, _ := reader.ReadString('\n'); blank != "\n" { // подія завершується порожнім рядком (\n\n)
			t.Fatalf("після події очікувався порожній рядок, отримано %q", blank)
		}
	}
}

func TestHandlerReturnsWhenClientDisconnects(t *testing.T) {
	handler := MetricsStreamHandler(10 * time.Millisecond)
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		handler(w, r)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		t.Fatal(err)
	}

	cancel() // клієнт розриває з'єднання
	resp.Body.Close()

	select {
	case <-done: // обробник завершився: горутина не витікає
	case <-time.After(2 * time.Second):
		t.Fatal("обробник не завершився після розриву з'єднання клієнтом (витік горутини)")
	}
}

// plainWriter не реалізує http.Flusher.
type plainWriter struct{ http.ResponseWriter }

func TestStreamingUnsupported(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	MetricsStreamHandler(time.Millisecond)(plainWriter{rec}, req)
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
}
