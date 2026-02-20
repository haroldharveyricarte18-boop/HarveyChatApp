package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver

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
var db *sql.DB

func main() {
	var err error

	// 1. DATABASE CONNECTION
	// Render automatically provides "DATABASE_URL" to your app
	connStr := os.Getenv("DATABASE_URL")

	// If we are running locally and DATABASE_URL isn't set, use your hardcoded one
	if connStr == "" {
		connStr = "postgresql://harvey_chat_db_user:6xwquB4EQSGYCDfUfirtWTTk6fz2I5wa@dpg-d6c28ml6ubrc73fa3kk0-a.oregon-postgres.render.com/harvey_chat_db"
	}

	db, err = sql.Open("postgres", connStr)
	if err != nil {
		fmt.Println("Error connecting to database:", err)
		return
	}

	// Create messages table if it doesn't exist
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		id SERIAL PRIMARY KEY,
		sender TEXT,
		target TEXT,
		body TEXT,
		time TEXT
	)`)
	if err != nil {
		fmt.Println("Error creating table:", err)
	}

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

	// Automatically load Global history when they first join so the screen isn't empty
	loadSpecificHistory(conn, username, "Global")

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

		// --- NEW LOGIC START ---
		// If the user clicked a name in the sidebar, we just load history and stop
		if event.Type == "load_history" {
			loadSpecificHistory(conn, username, event.Target)
			continue
		}
		// --- NEW LOGIC END ---

		event.User = username
		event.Time = time.Now().Format("3:04 PM")

		// Only save and broadcast if it's an actual message
		if event.Type == "message" {
			saveMessage(event)

			if event.Target != "" && event.Target != "Global" {
				sendPrivateMessage(event)
			} else {
				broadcastMessage(event)
			}
		}
	}
}

// Helper to save message to PostgreSQL
func saveMessage(e Event) {
	if e.Type != "message" {
		return
	}
	_, err := db.Exec("INSERT INTO messages (sender, target, body, time) VALUES ($1, $2, $3, $4)",
		e.User, e.Target, e.Body, e.Time)
	if err != nil {
		fmt.Println("Save error:", err)
	}
}

// Helper to load last 50 relevant messages
func loadChatHistory(conn *websocket.Conn, username string) {
	// Query messages that are Global OR involve this user
	rows, err := db.Query(`
		SELECT sender, target, body, time 
		FROM messages 
		WHERE target = 'Global' 
		OR target = $1 
		OR sender = $1 
		ORDER BY id ASC LIMIT 100`, username)

	if err != nil {
		fmt.Println("Load history error:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var e Event
		rows.Scan(&e.User, &e.Target, &e.Body, &e.Time)
		e.Type = "message"
		conn.WriteJSON(e)
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

func loadSpecificHistory(conn *websocket.Conn, username string, target string) {
	var rows *sql.Rows
	var err error

	if target == "Global" {
		// Fetch messages sent to everyone
		rows, err = db.Query("SELECT sender, target, body, time FROM messages WHERE target = 'Global' ORDER BY id ASC")
	} else {
		// Fetch private messages between you and this specific target
		rows, err = db.Query(`
			SELECT sender, target, body, time FROM messages 
			WHERE (sender = $1 AND target = $2) OR (sender = $2 AND target = $1)
			ORDER BY id ASC`, username, target)
	}

	if err != nil {
		fmt.Println("Load specific history error:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var e Event
		rows.Scan(&e.User, &e.Target, &e.Body, &e.Time)
		e.Type = "message"
		// Send each historical message back to the user's screen
		conn.WriteJSON(e)
	}
}
