package ws

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func testConfig() Config {
	return Config{WriteWait: time.Second, PongWait: 45 * time.Second, PingPeriod: 30 * time.Second}
}

func newTestServer(t *testing.T, cfg Config) (*Hub, *httptest.Server) {
	t.Helper()
	hub := NewHub(cfg)
	go hub.Run()
	srv := httptest.NewServer(ServeWS(hub))
	t.Cleanup(func() {
		hub.Stop()
		srv.Close()
	})
	return hub, srv
}

func dial(t *testing.T, srv *httptest.Server, query string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/?" + query
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial %q: %v", query, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func readMessage(conn *websocket.Conn, timeout time.Duration) (Message, error) {
	var m Message
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	_, data, err := conn.ReadMessage()
	if err != nil {
		return m, err
	}
	err = json.Unmarshal(data, &m)
	return m, err
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("не дочекалися: %s", what)
}

func TestBroadcastIsLimitedToSenderRoom(t *testing.T) {
	hub, srv := newTestServer(t, testConfig())
	alice := dial(t, srv, "room=alerts&name=alice")
	bob := dial(t, srv, "room=alerts&name=bob")
	carol := dial(t, srv, "room=general&name=carol")
	waitFor(t, "3 клієнти", func() bool { return hub.ClientCount() == 3 })

	if err := alice.WriteMessage(websocket.TextMessage, []byte("Увага! Перегрів вузла node-03")); err != nil {
		t.Fatal(err)
	}

	for name, conn := range map[string]*websocket.Conn{"alice": alice, "bob": bob} {
		m, err := readMessage(conn, time.Second)
		if err != nil {
			t.Fatalf("%s не отримав повідомлення: %v", name, err)
		}
		if m.Room != "alerts" || m.Sender != "alice" || m.Text != "Увага! Перегрів вузла node-03" {
			t.Errorf("%s отримав %+v", name, m)
		}
	}

	// carol у кімнаті general не має нічого отримати
	if m, err := readMessage(carol, 200*time.Millisecond); err == nil {
		t.Errorf("клієнт з іншої кімнати отримав повідомлення: %+v", m)
	}
}

func TestDefaultRoomIsGeneral(t *testing.T) {
	hub, srv := newTestServer(t, testConfig())
	conn := dial(t, srv, "")
	waitFor(t, "1 клієнт", func() bool { return hub.ClientCount() == 1 })

	_ = conn.WriteMessage(websocket.TextMessage, []byte("hello"))
	m, err := readMessage(conn, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if m.Room != DefaultRoom {
		t.Errorf("room = %q, want %q", m.Room, DefaultRoom)
	}
}

func TestInvalidRoomNameRejected(t *testing.T) {
	_, srv := newTestServer(t, testConfig())
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/?room=bad%20room!"
	_, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if err == nil {
		t.Fatal("очікувалась помилка рукостискання")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		t.Errorf("response = %v, want 400", resp)
	}
}

func TestClientUnregisteredOnDisconnect(t *testing.T) {
	hub, srv := newTestServer(t, testConfig())
	conn := dial(t, srv, "room=alerts")
	waitFor(t, "1 клієнт", func() bool { return hub.ClientCount() == 1 })

	conn.Close()
	waitFor(t, "0 клієнтів після розриву", func() bool { return hub.ClientCount() == 0 })
}

func TestHeartbeatDropsUnresponsiveClient(t *testing.T) {
	cfg := Config{WriteWait: time.Second, PingPeriod: 20 * time.Millisecond, PongWait: 80 * time.Millisecond}
	hub, srv := newTestServer(t, cfg)

	// клієнт підключається, але нічого не читає, тому на Ping не відповідає Pong
	_ = dial(t, srv, "room=alerts")
	waitFor(t, "1 клієнт", func() bool { return hub.ClientCount() == 1 })
	waitFor(t, "відключення завислого клієнта", func() bool { return hub.ClientCount() == 0 })
}

func TestHeartbeatKeepsResponsiveClientConnected(t *testing.T) {
	cfg := Config{WriteWait: time.Second, PingPeriod: 20 * time.Millisecond, PongWait: 80 * time.Millisecond}
	hub, srv := newTestServer(t, cfg)

	conn := dial(t, srv, "room=alerts")
	received := make(chan Message, 1)
	go func() { // читання автоматично відповідає на Ping фреймом Pong
		for {
			var m Message
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if json.Unmarshal(data, &m) == nil {
				received <- m
			}
		}
	}()
	waitFor(t, "1 клієнт", func() bool { return hub.ClientCount() == 1 })

	time.Sleep(400 * time.Millisecond) // значно більше за PongWait
	if hub.ClientCount() != 1 {
		t.Fatal("клієнт, який відповідає на Ping, був відключений")
	}

	_ = conn.WriteMessage(websocket.TextMessage, []byte("still alive"))
	select {
	case m := <-received:
		if m.Text != "still alive" {
			t.Errorf("text = %q", m.Text)
		}
	case <-time.After(time.Second):
		t.Fatal("повідомлення не доставлено")
	}
}

func TestHubStopClosesConnections(t *testing.T) {
	hub, srv := newTestServer(t, testConfig())
	conn := dial(t, srv, "room=alerts")
	waitFor(t, "1 клієнт", func() bool { return hub.ClientCount() == 1 })

	hub.Stop()

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := conn.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		t.Errorf("err = %v, очікувалось коректне закриття (1000)", err)
	}
}
