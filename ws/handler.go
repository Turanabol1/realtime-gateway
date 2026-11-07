package ws

import (
	"log"
	"net/http"
	"regexp"
	"strings"

	"github.com/gorilla/websocket"
)

// roomNameRe обмежує назву кімнати безпечним набором символів (захист від сміття в логах).
var roomNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

// upgrader переводить HTTP-з'єднання на протокол WebSocket (відповідь 101 Switching Protocols).
var upgrader = websocket.Upgrader{
	// Дозволяє підключення з будь-яких джерел, як вимагає завдання.
	// У виробничому середовищі тут потрібна перевірка списку дозволених доменів.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ServeWS повертає обробник GET /api/v1/ws?room=<кімната>&name=<ім'я>.
func ServeWS(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		room := r.URL.Query().Get("room")
		if room == "" {
			room = DefaultRoom
		}
		if !roomNameRe.MatchString(room) {
			http.Error(w, "invalid room name: use 1-32 characters [A-Za-z0-9_-]", http.StatusBadRequest)
			return
		}

		name := strings.TrimSpace(r.URL.Query().Get("name"))
		if name == "" {
			name = r.RemoteAddr
		}
		if runes := []rune(name); len(runes) > 32 {
			name = string(runes[:32])
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("[WS] Upgrade failed: %v", err)
			return
		}

		client := &Client{
			hub:  hub,
			conn: conn,
			send: make(chan []byte, sendBufferSize),
			room: room,
			name: name,
			addr: r.RemoteAddr,
		}

		select {
		case hub.register <- client:
		case <-hub.done:
			conn.Close()
			return
		}
		log.Printf("[WS] Новий клієнт підключився до кімнати [%s]: %s", room, client.addr)

		go client.writePump()
		client.readPump() // блокує до розриву з'єднання
	}
}
