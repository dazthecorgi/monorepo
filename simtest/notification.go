package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"source.quilibrium.com/quilibrium/monorepo/simtest/shared"
)

type NotificationRouter struct {
	mu       sync.RWMutex
	channels map[string]chan shared.FrameNotification
}

func NewNotificationRouter() *NotificationRouter {
	return &NotificationRouter{
		channels: make(map[string]chan shared.FrameNotification),
	}
}

func (nr *NotificationRouter) Register(runID string, ch chan shared.FrameNotification) {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	nr.channels[runID] = ch
}

func (nr *NotificationRouter) Unregister(runID string) {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	delete(nr.channels, runID)
}

func (nr *NotificationRouter) Route(notification shared.FrameNotification) {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	if ch, ok := nr.channels[notification.RunID]; ok {
		ch <- notification
	} else {
		logger.Warnw("Notification for unknown run ID", "run_id", notification.RunID)
	}
}

// startNotificationServer creates, registers the handler, and starts the HTTP server.
func startNotificationServer(port string, bearerToken string, router *NotificationRouter) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/run-notification", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Missing authorization header", http.StatusUnauthorized)
			return
		}

		const bearerPrefix = "Bearer "
		if !strings.HasPrefix(authHeader, bearerPrefix) {
			http.Error(w, "Invalid authorization header format", http.StatusUnauthorized)
			return
		}

		token := strings.TrimPrefix(authHeader, bearerPrefix)
		if subtle.ConstantTimeCompare([]byte(token), []byte(bearerToken)) != 1 {
			http.Error(w, "Invalid bearer token", http.StatusUnauthorized)
			return
		}

		var notification shared.FrameNotification
		if err := json.NewDecoder(r.Body).Decode(&notification); err != nil {
			http.Error(w, fmt.Sprintf("Failed to decode notification: %v", err), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)

		logger.Debugw("Received notification", "run_id", notification.RunID, "frame_number", notification.FrameNumber, "type", notification.Type, "safety_error", notification.SafetyError)

		if notification.Type == shared.NotificationTypeTerminalFrame {
			router.Route(notification)
		}
	})

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Errorw("HTTP server error", "error", err)
		}
	}()
	logger.Debugw("HTTP notification server started", "port", port)

	return server
}
