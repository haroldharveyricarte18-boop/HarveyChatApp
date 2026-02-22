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
	Type       string   `json:"type"`
	User       string   `json:"user"`
	Target     string   `json:"target"`
	Body       string   `json:"body"`
	List       []string `json:"list"`
	Time       string   `json:"time"`
	IsImage    bool     `json:"is_image"`
	ID         int      `json:"id"`
	IsRead     bool     `json:"is_read"`
	UserAvatar string   `json:"user_avatar"`
}

type UserInfo struct {
	Username string `json:"username"`
	Avatar   string `json:"avatar"`
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

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		id SERIAL PRIMARY KEY,
		sender TEXT,
		target TEXT,
		body TEXT,
		time TEXT,
		is_image BOOLEAN DEFAULT FALSE,
		is_read BOOLEAN DEFAULT FALSE,
		user_avatar TEXT 
	)`)

	if err != nil {
		fmt.Println("Error creating table:", err)
	}

	// Create Users Table for permanent accounts
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
		id SERIAL PRIMARY KEY,
		username TEXT UNIQUE NOT NULL,
		password TEXT NOT NULL,
		avatar TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`)

	if err != nil {
		fmt.Println("Error creating users table:", err)
	}

	db.Exec("ALTER TABLE messages ADD COLUMN IF NOT EXISTS user_avatar TEXT")

	http.HandleFunc("/", homeHandler)
	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/login", loginHandler)
	http.HandleFunc("/logout", logoutHandler)

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
	if r.Method == http.MethodGet {
		http.ServeFile(w, r, "login.html")
		return
	}

	if r.Method == http.MethodPost {
		action := r.FormValue("action") // Sent by the button clicked
		username := r.FormValue("username")
		password := r.FormValue("password")

		if action == "register" {
			// Try to insert new user
			_, err := db.Exec("INSERT INTO users (username, password) VALUES ($1, $2)", username, password)
			if err != nil {
				// If error, likely username is taken
				http.Redirect(w, r, "/login?error=exists", http.StatusSeeOther)
				return
			}
		} else {
			// Login logic: Check if user exists and password matches
			var dbPassword string
			err := db.QueryRow("SELECT password FROM users WHERE username = $1", username).Scan(&dbPassword)
			if err != nil || dbPassword != password {
				// If error or password mismatch, reject
				http.Redirect(w, r, "/login?error=invalid", http.StatusSeeOther)
				return
			}
		}

		// If we reach here, either Registration or Login was successful
		http.SetCookie(w, &http.Cookie{
			Name:     "username",
			Value:    username,
			Expires:  time.Now().Add(30 * 24 * time.Hour), // Lasts 30 days
			HttpOnly: false,
			Path:     "/",
		})
		http.Redirect(w, r, "/", http.StatusSeeOther)
	}
}

func logoutHandler(w http.ResponseWriter, r *http.Request) {
	// We clear the cookie by setting its MaxAge to -1 (immediate deletion)
	http.SetCookie(w, &http.Cookie{
		Name:     "username",
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
		HttpOnly: false,
	})
	// Redirect them back to the login page
	http.Redirect(w, r, "/login", http.StatusSeeOther)
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

		if event.Type == "update_avatar" {
			// Update the permanent profile
			db.Exec("UPDATE users SET avatar = $1 WHERE username = $2", event.Body, username)

			// ALSO update all previous messages so your old chats show the new pic
			db.Exec("UPDATE messages SET user_avatar = $1 WHERE sender = $2", event.Body, username)

			broadcastUserList()
			continue
		}

		// 3. Handle Delete Requests
		if event.Type == "delete_message" {
			_, err := db.Exec("DELETE FROM messages WHERE id = $1 AND sender = $2", event.ID, username)
			if err == nil {
				broadcastMessage(Event{Type: "message_deleted", ID: event.ID})
			} else {
				fmt.Println("Delete SQL error:", err)
			}
			continue
		}

		// 4. Handle Clear History
		if event.Type == "clear_history" {
			if event.Target == "Global" {
				db.Exec("DELETE FROM messages WHERE target = 'Global'")
				broadcastMessage(Event{Type: "clear_chat_ui", Target: "Global"})
			} else {
				db.Exec("DELETE FROM messages WHERE (sender = $1 AND target = $2) OR (sender = $2 AND target = $1)", username, event.Target)
				sendPrivateMessage(Event{Type: "clear_chat_ui", Target: event.Target, User: username})
			}
			continue
		}

		// 5. Handle Typing Signals
		if event.Type == "typing" || event.Type == "stop_typing" {
			event.User = username
			if event.Target != "" && event.Target != "Global" {
				mutex.Lock()
				if targetConn, ok := clients[event.Target]; ok {
					targetConn.WriteJSON(event)
				}
				mutex.Unlock()
			} else {
				broadcastMessage(event)
			}
			continue
		}

		// 6. Handle Regular Messages
		if event.Type == "message" {
			event.User = username

			// Load PH Timezone
			loc, _ := time.LoadLocation("Asia/Manila")
			event.Time = time.Now().In(loc).Format("3:04 PM")

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

func saveMessage(e *Event) {
	if e.Type != "message" {
		return
	}

	err := db.QueryRow(`
		INSERT INTO messages (sender, target, body, time, is_image, user_avatar) 
		VALUES ($1, $2, $3, $4, $5, $6) 
		RETURNING id`,
		e.User, e.Target, e.Body, e.Time, e.IsImage, e.UserAvatar).Scan(&e.ID)

	if err != nil {
		fmt.Println("Save error:", err)
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
	var userInfoList []UserInfo

	// 1. Get ALL users and their permanent avatars from the users table
	rows, err := db.Query("SELECT username, COALESCE(avatar, '') FROM users")
	if err != nil {
		fmt.Println("Error fetching user list:", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var ui UserInfo
		if err := rows.Scan(&ui.Username, &ui.Avatar); err != nil {
			continue
		}

		// 2. Check if the user is currently online (in our clients map)
		mutex.Lock()
		_, isOnline := clients[ui.Username]
		mutex.Unlock()

		// Only show online users in the "Active Now" list
		if isOnline {
			userInfoList = append(userInfoList, ui)
		}
	}

	event := map[string]interface{}{
		"type":           "users",
		"user_info_list": userInfoList,
	}

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
		// Use COALESCE to handle old messages where user_avatar is NULL
		rows, err = db.Query("SELECT id, sender, target, body, time, is_image, COALESCE(user_avatar, '') FROM messages WHERE target = 'Global' ORDER BY id ASC")
	} else {
		// Use COALESCE to handle old messages where user_avatar is NULL
		rows, err = db.Query(`
			SELECT id, sender, target, body, time, is_image, COALESCE(user_avatar, '') FROM messages 
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
		// Scanning into &e.UserAvatar now works even for old messages thanks to COALESCE
		err := rows.Scan(&e.ID, &e.User, &e.Target, &e.Body, &e.Time, &e.IsImage, &e.UserAvatar)
		if err != nil {
			fmt.Println("Scan error in history:", err)
			continue
		}

		e.Type = "message"
		conn.WriteJSON(e)
	}

	// Mark messages as read once they are loaded/seen
	if target != "Global" {
		db.Exec("UPDATE messages SET is_read = true WHERE sender = $1 AND target = $2", target, username)
	}
}
