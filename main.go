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
	Type    string   `json:"type"`
	User    string   `json:"user"`
	Target  string   `json:"target"`
	Body    string   `json:"body"`
	List    []string `json:"list"`
	Time    string   `json:"time"`
	IsImage bool     `json:"is_image"`
	ID      int      `json:"id"`
	IsRead  bool     `json:"is_read"`
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

	// Added is_image column to the table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		id SERIAL PRIMARY KEY,
		sender TEXT,
		target TEXT,
		body TEXT,
		time TEXT,
		is_image BOOLEAN DEFAULT FALSE,
		is_read BOOLEAN DEFAULT FALSE
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
	// IMPORTANT: Tell the browser NOT to cache this page
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")

	cookie, err := r.Cookie("username")

	// If the cookie is missing OR the value is empty, show login
	if err != nil || cookie.Value == "" {
		http.ServeFile(w, r, "login.html")
		return
	}

	// Only if a real username exists, show the dashboard
	http.ServeFile(w, r, "index.html")
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	// If they are just visiting the page (GET), show the file
	if r.Method == http.MethodGet {
		http.ServeFile(w, r, "login.html")
		return
	}

	// If they are submitting the form (POST), set the cookie
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

	cookie, err := r.Cookie("username")
	if err != nil {
		// If there is no cookie, don't allow the connection
		fmt.Println("WS Connection rejected: No username cookie")
		return
	}
	username := cookie.Value

	mutex.Lock()
	clients[username] = conn
	mutex.Unlock()

	// Automatically load Global history when they first join
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

		// 1. Handle History Requests
		if event.Type == "load_history" {
			loadSpecificHistory(conn, username, event.Target)
			continue
		}

		// 2. Handle Delete Requests (Must be independent of message type)
		if event.Type == "delete_message" {
			// Removes message from database only if sender matches current user
			_, err := db.Exec("DELETE FROM messages WHERE id = $1 AND sender = $2", event.ID, username)
			if err == nil {
				// Broadcast signal to all users to remove message from UI
				broadcastMessage(Event{Type: "message_deleted", ID: event.ID})
			} else {
				fmt.Println("Delete SQL error:", err)
			}
			continue
		}

		if event.Type == "clear_history" {
			if event.Target == "Global" {
				db.Exec("DELETE FROM messages WHERE target = 'Global'")
			} else {
				db.Exec("DELETE FROM messages WHERE (sender = $1 AND target = $2) OR (sender = $2 AND target = $1)", username, event.Target)
			}
			// Optional: broadcast a signal to the other user to clear their screen too
			continue
		}

		// 2.5 Handle Typing Signals (Broadcast to others)
		if event.Type == "typing" || event.Type == "stop_typing" {
			event.User = username
			if event.Target != "" && event.Target != "Global" {
				// Only send typing status to the specific person you are chatting with
				mutex.Lock()
				if targetConn, ok := clients[event.Target]; ok {
					targetConn.WriteJSON(event)
				}
				mutex.Unlock()
			} else {
				// Send typing status to everyone in Global Chat
				broadcastMessage(event)
			}
			continue
		}

		// 3. Handle Regular Messages
		if event.Type == "message" {
			event.User = username
			event.Time = time.Now().Format("3:04 PM")

			// Save message and retrieve its new database ID
			saveMessage(&event)

			if event.Target != "" && event.Target != "Global" {
				sendPrivateMessage(event)
			} else {
				broadcastMessage(event)
			}
		}
	}
}

func saveMessage(e *Event) { // Note the * before Event
	if e.Type != "message" {
		return
	}

	// We use QueryRow instead of Exec so we can catch the 'id'
	err := db.QueryRow(`
		INSERT INTO messages (sender, target, body, time, is_image) 
		VALUES ($1, $2, $3, $4, $5) 
		RETURNING id`,
		e.User, e.Target, e.Body, e.Time, e.IsImage).Scan(&e.ID)

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
		// Added id to the SELECT statement
		rows, err = db.Query("SELECT id, sender, target, body, time, is_image FROM messages WHERE target = 'Global' ORDER BY id ASC")
	} else {
		// Added id to the SELECT statement
		rows, err = db.Query(`
			SELECT id, sender, target, body, time, is_image FROM messages 
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
		// Added e.ID to Scan - it MUST be first because 'id' is first in the SELECT above
		err := rows.Scan(&e.ID, &e.User, &e.Target, &e.Body, &e.Time, &e.IsImage)
		if err != nil {
			fmt.Println("Scan error in history:", err)
			continue
		}

		e.Type = "message"
		// Send each historical message (now including its ID) to the user's screen
		conn.WriteJSON(e)
	}

	// Mark messages as read once they are loaded/seen
	if target != "Global" {
		db.Exec("UPDATE messages SET is_read = true WHERE sender = $1 AND target = $2", target, username)
	}
}
