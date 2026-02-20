package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Event struct {
	Type   string   `json:"type"`
	User   string   `json:"user"`
	Target string   `json:"target"`
	Body   string   `json:"body"`
	List   []string `json:"list"`
	Time   string   `json:"time"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

var clients = make(map[string]*websocket.Conn)
var mutex = &sync.Mutex{}

func main() {
	http.HandleFunc("/", homeHandler)
	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/login", loginHandler)

	fmt.Println("Server running on http://localhost:8080")
	http.ListenAndServe(":8080", nil)
}

func homeHandler(w http.ResponseWriter, r *http.Request) {
	_, err := r.Cookie("username")
	if err != nil {
		fmt.Fprintf(w, `
			<html>
			<body style="display:grid; place-items:center; height:100vh; font-family:sans-serif;">
				<form action="/login" method="POST">
					<input type="text" name="username" placeholder="Enter your name" required>
					<button type="submit">Join Chat</button>
				</form>
			</body>
			</html>`)
		return
	}
	http.ServeFile(w, r, "index.html")
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		username := r.FormValue("username")
		http.SetCookie(w, &http.Cookie{
			Name:     "username",
			Value:    username,
			Expires:  time.Now().Add(24 * time.Hour),
			HttpOnly: false,
			Path:     "/",
		})
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	cookie, _ := r.Cookie("username")
	username := cookie.Value

	mutex.Lock()
	clients[username] = conn
	mutex.Unlock()

	broadcastUserList()

	defer func() {
		mutex.Lock()
		delete(clients, username)
		mutex.Unlock()
		broadcastUserList()
		conn.Close()
	}()

	for {
		var event Event
		err := conn.ReadJSON(&event)
		if err != nil {
			break
		}

		event.User = username
		event.Time = time.Now().Format("3:04 PM")

		if event.Target != "" && event.Target != "Global" {
			sendPrivateMessage(event)
		} else {
			broadcastMessage(event)
		}
	}
}

func broadcastMessage(event Event) {
	data, _ := json.Marshal(event)
	mutex.Lock()
	for _, clientConn := range clients {
		clientConn.WriteMessage(websocket.TextMessage, data)
	}
	mutex.Unlock()
}

func sendPrivateMessage(event Event) {
	data, _ := json.Marshal(event)
	mutex.Lock()
	if targetConn, ok := clients[event.Target]; ok {
		targetConn.WriteMessage(websocket.TextMessage, data)
	}
	if senderConn, ok := clients[event.User]; ok {
		senderConn.WriteMessage(websocket.TextMessage, data)
	}
	mutex.Unlock()
}

func broadcastUserList() {
	var userList []string
	mutex.Lock()
	for name := range clients {
		userList = append(userList, name)
	}
	mutex.Unlock()

	event := Event{Type: "users", List: userList}
	data, _ := json.Marshal(event)

	mutex.Lock()
	for _, clientConn := range clients {
		clientConn.WriteMessage(websocket.TextMessage, data)
	}
	mutex.Unlock()
}
