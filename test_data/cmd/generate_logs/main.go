package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"
)

type LogEntry struct {
	Timestamp  time.Time `json:"timestamp"`
	AccountID  string    `json:"account_id"`
	Level      string    `json:"level"`
	Service    string    `json:"service"`
	RequestID  string    `json:"request_id"`
	HTTPMethod string    `json:"http_method"`
	StatusCode int       `json:"status_code"`
	Path       string    `json:"path"`
	Duration   int64     `json:"duration_ms"`
	UserAgent  string    `json:"user_agent"`
	IP         string    `json:"ip"`
	Message    string    `json:"message"`
	Error      string    `json:"error,omitempty"`
	StackTrace string    `json:"stack_trace,omitempty"`
}

func main() {
	// Create 10 different account IDs
	accountIDs := []string{
		"acc_1234567890",
		"acc_2345678901",
		"acc_3456789012",
		"acc_4567890123",
		"acc_5678901234",
		"acc_6789012345",
		"acc_7890123456",
		"acc_8901234567",
		"acc_9012345678",
		"acc_0123456789",
	}

	// Service names
	services := []string{
		"api-gateway",
		"user-service",
		"auth-service",
		"payment-service",
		"notification-service",
		"analytics-service",
	}

	// HTTP methods
	httpMethods := []string{
		"GET",
		"POST",
		"PUT",
		"DELETE",
		"PATCH",
	}

	// Common paths
	paths := []string{
		"/api/v1/users",
		"/api/v1/auth/login",
		"/api/v1/payments",
		"/api/v1/notifications",
		"/api/v1/analytics",
		"/api/v1/health",
	}

	// User agents
	userAgents := []string{
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
		"Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15",
		"Mozilla/5.0 (Android 12; Mobile; rv:68.0) Gecko/68.0 Firefox/96.0",
	}

	// Create the output file
	file, err := os.Create("test_data/logs_large.jsonl")
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		return
	}
	defer file.Close()

	// Generate 1 million log entries
	startTime := time.Now().Add(-24 * time.Hour) // Start from 24 hours ago
	for i := 0; i < 1000000; i++ {
		// Random time within the last 24 hours
		randomDuration := time.Duration(rand.Int63n(24*60*60*1000)) * time.Millisecond
		timestamp := startTime.Add(randomDuration)

		// Random values
		accountID := accountIDs[rand.Intn(len(accountIDs))]
		service := services[rand.Intn(len(services))]
		httpMethod := httpMethods[rand.Intn(len(httpMethods))]
		path := paths[rand.Intn(len(paths))]
		userAgent := userAgents[rand.Intn(len(userAgents))]
		ip := fmt.Sprintf("%d.%d.%d.%d", rand.Intn(256), rand.Intn(256), rand.Intn(256), rand.Intn(256))
		requestID := fmt.Sprintf("req_%d", rand.Intn(1000000))
		duration := rand.Int63n(1000) // Random duration up to 1000ms

		// Determine log level and status code
		level := "info"
		statusCode := 200
		var errMsg, stackTrace string

		// 5% chance of error
		if rand.Float32() < 0.05 {
			level = "error"
			statusCode = 500
			errMsg = "Internal server error"
			stackTrace = "goroutine 1 [running]:\nmain.main()\n\t/tmp/main.go:123 +0x456"
		} else if rand.Float32() < 0.1 {
			level = "warn"
			statusCode = 400
			errMsg = "Bad request"
		}

		// Create log entry
		entry := LogEntry{
			Timestamp:  timestamp,
			AccountID:  accountID,
			Level:      level,
			Service:    service,
			RequestID:  requestID,
			HTTPMethod: httpMethod,
			StatusCode: statusCode,
			Path:       path,
			Duration:   duration,
			UserAgent:  userAgent,
			IP:         ip,
			Message:    fmt.Sprintf("%s %s %d - %dms", httpMethod, path, statusCode, duration),
		}

		if errMsg != "" {
			entry.Error = errMsg
		}
		if stackTrace != "" {
			entry.StackTrace = stackTrace
		}

		// Marshal to JSON
		jsonData, err := json.Marshal(entry)
		if err != nil {
			fmt.Printf("Error marshaling JSON: %v\n", err)
			continue
		}

		// Write to file
		_, err = fmt.Fprintf(file, "%s\n", string(jsonData))
		if err != nil {
			fmt.Printf("Error writing to file: %v\n", err)
			continue
		}

		// Print progress every 100k rows
		if (i+1)%100000 == 0 {
			fmt.Printf("Generated %d rows...\n", i+1)
		}
	}

	fmt.Println("Successfully generated 1 million log entries in test_data/logs_large.jsonl")
}
