// Package ws реалізує WebSocket-сервер на базі патерну Hub з тематичними кімнатами та Heartbeat.
package ws

import (
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultRoom — кімната, яка призначається, якщо параметр room відсутній.
const DefaultRoom = "general"

// Message — повідомлення, яке Hub розсилає клієнтам однієї кімнати.
type Message struct {
	Room   string `json:"room"`   // цільова кімната
	Sender string `json:"sender"` // відправник
	Text   string `json:"text"`
	Time   string `json:"time"`
}

// Config містить тайминги Heartbeat. PingPeriod має бути меншим за PongWait.
type Config struct {
	WriteWait  time.Duration // дедлайн запису одного фрейму
	PongWait   time.Duration // скільки чекаємо на Pong, перш ніж закрити "завислий" сокет
	PingPeriod time.Duration // інтервал відправлення Ping (30–50 с за завданням)
}

// DefaultConfig повертає значення за замовчуванням.
func DefaultConfig() Config {
	return Config{
		WriteWait:  10 * time.Second,
		PongWait:   45 * time.Second,
		PingPeriod: 30 * time.Second,
	}
}

// Hub — диспетчер: єдина горутина, яка володіє мапою клієнтів,
// тому доступ до неї не потребує м'ютексів.
type Hub struct {
	cfg        Config
	clients    map[*Client]bool
	broadcast  chan Message
	register   chan *Client
	unregister chan *Client

	done     chan struct{} // закривається при зупинці Hub
	stopOnce sync.Once
	count    atomic.Int64 // кількість підключених клієнтів (для тестів і моніторингу)
}

// NewHub створює Hub із заданими таймингами.
func NewHub(cfg Config) *Hub {
	return &Hub{
		cfg:        cfg,
		clients:    make(map[*Client]bool),
		broadcast:  make(chan Message),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		done:       make(chan struct{}),
	}
}

// ClientCount повертає кількість зареєстрованих клієнтів.
func (h *Hub) ClientCount() int { return int(h.count.Load()) }

// Stop зупиняє Hub: усі клієнти отримують закриття каналу send і коректно завершують з'єднання.
func (h *Hub) Stop() { h.stopOnce.Do(func() { close(h.done) }) }

// Run — цикл диспетчера. Запускається в окремій горутині.
func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.clients[c] = true
			h.count.Store(int64(len(h.clients)))

		case c := <-h.unregister:
			h.remove(c)

		case msg := <-h.broadcast:
			h.deliver(msg)

		case <-h.done:
			for c := range h.clients {
				close(c.send)
				delete(h.clients, c)
			}
			h.count.Store(0)
			return
		}
	}
}

// deliver ретранслює повідомлення виключно клієнтам тієї ж кімнати, що й відправник.
func (h *Hub) deliver(msg Message) {
	log.Printf("[HUB BROADCAST] [Кімната: %s] Отримано повідомлення: %q", msg.Room, msg.Text)

	payload, err := json.Marshal(msg)
	if err != nil {
		log.Printf("[HUB] Помилка серіалізації повідомлення: %v", err)
		return
	}

	notified := 0
	for c := range h.clients {
		if c.room != msg.Room {
			continue
		}
		select {
		case c.send <- payload:
			notified++
		default:
			// буфер клієнта переповнений (повільний або завислий): відключаємо,
			// щоб він не гальмував розсилку решті
			log.Printf("[HUB] Клієнт %s не встигає читати, з'єднання закривається", c.addr)
			h.remove(c)
		}
	}
	log.Printf("[HUB BROADCAST] Сповіщено клієнтів у кімнаті [%s]: %d", msg.Room, notified)
}

// remove видаляє клієнта з мапи та закриває його канал send (рівно один раз).
func (h *Hub) remove(c *Client) {
	if h.clients[c] {
		delete(h.clients, c)
		close(c.send)
		h.count.Store(int64(len(h.clients)))
		log.Printf("[WS] Клієнт відключився від кімнати [%s]: %s", c.room, c.addr)
	}
}
