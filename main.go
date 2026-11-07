// Real-Time Operations Gateway: SSE-стрімінг метрик і WebSocket-шлюз із кімнатами та Heartbeat.
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"realtime-gateway/sse"
	"realtime-gateway/web"
	"realtime-gateway/ws"
)

func main() {
	log.Println("=== Сервіс реального часу (SSE & WebSockets Gateway) ===")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	hub := ws.NewHub(ws.DefaultConfig())
	go hub.Run()
	log.Println("[СЕРВЕР] Диспетчер WebSocket Hub успішно запущено")

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/metrics/stream", sse.MetricsStreamHandler(sse.DefaultInterval))
	mux.HandleFunc("GET /api/v1/ws", ws.ServeWS(hub))
	mux.Handle("GET /", http.FileServer(http.FS(web.Files))) // вебклієнт

	// baseCtx скасовується під час зупинки: через нього завершуються SSE-обробники
	baseCtx, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()

	// WriteTimeout навмисно не задано: він обірвав би довготривалі SSE-потоки
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return baseCtx },
	}

	stop, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	go func() {
		log.Printf("[СЕРВЕР] HTTP-слухач відкрито на порту :%s", port)
		log.Printf("[СЕРВЕР] Вебклієнт: http://localhost:%s/", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[СЕРВЕР] помилка: %v", err)
		}
	}()

	<-stop.Done() // Ctrl+C або SIGTERM
	log.Println("[СЕРВЕР] Зупинка сервера...")

	cancelBase() // завершує SSE-обробники
	hub.Stop()   // коректно закриває WebSocket-з'єднання

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[СЕРВЕР] помилка під час зупинки: %v", err)
	}
	log.Println("[СЕРВЕР] Сервер зупинено")
}
