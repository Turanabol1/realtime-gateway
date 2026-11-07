package ws

import (
	"log"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	sendBufferSize = 256  // місткість персонального каналу відправки
	maxMessageSize = 4096 // максимальний розмір вхідного повідомлення, байт
)

// Client — одне WebSocket-з'єднання, прив'язане до кімнати.
type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte // буферизований канал вихідних повідомлень
	room string
	name string // ім'я відправника (параметр name або адреса)
	addr string
}

// readPump читає повідомлення із сокета й передає їх у Hub.
// Це єдина горутина, яка читає з conn. Вихід із неї означає розрив з'єднання.
func (c *Client) readPump() {
	defer func() {
		select {
		case c.hub.unregister <- c:
		case <-c.hub.done: // Hub уже зупинено
		}
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	// якщо за PongWait не прийде жодного фрейму, ReadMessage поверне помилку
	_ = c.conn.SetReadDeadline(time.Now().Add(c.hub.cfg.PongWait))
	c.conn.SetPongHandler(func(string) error {
		log.Printf("[HEARTBEAT] Pong отримано від клієнта %s", c.addr)
		return c.conn.SetReadDeadline(time.Now().Add(c.hub.cfg.PongWait))
	})

	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[WS] Розрив з'єднання з %s: %v", c.addr, err)
			}
			return
		}

		text := strings.TrimSpace(string(data))
		if text == "" {
			continue
		}
		msg := Message{
			Room:   c.room,
			Sender: c.name,
			Text:   text,
			Time:   time.Now().Format(time.RFC3339),
		}
		select {
		case c.hub.broadcast <- msg:
		case <-c.hub.done:
			return
		}
	}
}

// writePump пише повідомлення з каналу send у сокет і шле Ping.
// Це єдина горутина, яка пише в conn (websocket.Conn не потокобезпечний для запису).
func (c *Client) writePump() {
	ticker := time.NewTicker(c.hub.cfg.PingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.hub.cfg.WriteWait))
			if !ok { // Hub закрив канал: коректно завершуємо з'єднання
				_ = c.conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.hub.cfg.WriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
			log.Printf("[HEARTBEAT] Ping надіслано клієнту %s", c.addr)
		}
	}
}
