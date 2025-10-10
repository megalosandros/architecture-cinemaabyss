package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

// Структуры событий

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`     // опционально
	Rating      *float64 `json:"rating,omitempty"`      // опционально
	Genres      []string `json:"genres,omitempty"`      // опционально
	Description *string  `json:"description,omitempty"` // опционально
}

func (e MovieEvent) Validate() error {
	if e.MovieID == 0 {
		return errors.New("movie_id is required")
	}
	if strings.TrimSpace(e.Title) == "" {
		return errors.New("title is required")
	}
	if strings.TrimSpace(e.Action) == "" {
		return errors.New("action is required")
	}
	return nil
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  *string   `json:"username,omitempty"` // опционально
	Email     *string   `json:"email,omitempty"`    // опционально
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

func (e UserEvent) Validate() error {
	if e.UserID == 0 {
		return errors.New("user_id is required")
	}
	if strings.TrimSpace(e.Action) == "" {
		return errors.New("action is required")
	}
	if e.Timestamp.IsZero() {
		return errors.New("timestamp is required and must be valid datetime")
	}
	return nil
}

type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType *string   `json:"method_type,omitempty"` // опционально
}

func (e PaymentEvent) Validate() error {
	if e.PaymentID == 0 {
		return errors.New("payment_id is required")
	}
	if e.UserID == 0 {
		return errors.New("user_id is required")
	}
	if e.Amount == 0 {
		return errors.New("amount is required and must be > 0")
	}
	if strings.TrimSpace(e.Status) == "" {
		return errors.New("status is required")
	}
	if e.Timestamp.IsZero() {
		return errors.New("timestamp is required and must be valid datetime")
	}
	return nil
}

// Универсальная структура для отправки в Kafka с типом события и сериализованными данными
type EventEnvelope struct {
	Id        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
}

// --- Вспомогательные структуры для ответа ---

type Event struct {
	Id        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

// Структура для логирования полученных событий
type LoggedEvent struct {
	ReceivedAt time.Time     `json:"received_at"`
	Topic      string        `json:"topic"`
	Partition  int           `json:"partition"`
	Offset     int64         `json:"offset"`
	Key        string        `json:"key"`
	Event      EventEnvelope `json:"event"`
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	log.Printf("Starting events service with Kafka broker: %s", kafkaBroker)

	topics := map[string]string{
		"user":    "user-events",
		"payment": "payment-events",
		"movie":   "movie-events",
	}

	writers := make(map[string]*kafka.Writer)
	for _, topic := range topics {
		writers[topic] = kafka.NewWriter(kafka.WriterConfig{
			Brokers:  []string{kafkaBroker},
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		})
		defer writers[topic].Close()
	}

	// Запускаем consumer'ы для каждого топика
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf("Starting Kafka consumers for topics: %v", topics)
	for topicName, topic := range topics {
		log.Printf("Starting consumer for topic: %s (%s)", topic, topicName)
		go func(topicName, topic string) {
			if err := startKafkaConsumer(ctx, kafkaBroker, topic, topicName); err != nil {
				log.Printf("Error in consumer for topic %s: %v", topic, err)
			}
		}(topicName, topic)
	}

	http.HandleFunc("/api/events/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]bool{"status": true})
	})

	http.HandleFunc("/api/events/movie", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		var event MovieEvent
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&event); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
			return
		}

		if err := event.Validate(); err != nil {
			http.Error(w, fmt.Sprintf("Validation error: %v", err), http.StatusBadRequest)
			return
		}

		sendEvent(w, kafkaBroker, topics["movie"], "movie", event)
	})

	http.HandleFunc("/api/events/user", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		var event UserEvent
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&event); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
			return
		}

		if err := event.Validate(); err != nil {
			http.Error(w, fmt.Sprintf("Validation error: %v", err), http.StatusBadRequest)
			return
		}

		sendEvent(w, kafkaBroker, topics["user"], "user", event)
	})

	http.HandleFunc("/api/events/payment", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		var event PaymentEvent
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&event); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
			return
		}

		if err := event.Validate(); err != nil {
			http.Error(w, fmt.Sprintf("Validation error: %v", err), http.StatusBadRequest)
			return
		}

		sendEvent(w, kafkaBroker, topics["payment"], "payment", event)
	})

	srv := &http.Server{Addr: ":" + port}

	go func() {
		log.Printf("Starting server at :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down server...")
	ctxShutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctxShutdown); err != nil {
		log.Fatalf("server shutdown failed: %v", err)
	}

	log.Println("Server gracefully stopped")
}

// startKafkaConsumer запускает consumer для чтения сообщений из Kafka
func startKafkaConsumer(ctx context.Context, broker, topic, topicType string) error {
	log.Printf("Initializing Kafka consumer for topic: %s", topic)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  fmt.Sprintf("events-service-%s-consumer", topicType),
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	log.Printf("Kafka consumer started for topic: %s", topic)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping Kafka consumer for topic: %s", topic)
			return nil
		default:
			m, err := reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error reading message from topic %s: %v", topic, err)
				continue
			}

			// Логируем полученное сообщение
			logReceivedMessage(topic, m, topicType)
		}
	}
}

// logReceivedMessage логирует полученное сообщение из Kafka
func logReceivedMessage(topic string, m kafka.Message, topicType string) {
	var envelope EventEnvelope
	if err := json.Unmarshal(m.Value, &envelope); err != nil {
		log.Printf("Error unmarshaling message from topic %s: %v", topic, err)
		return
	}

	loggedEvent := LoggedEvent{
		ReceivedAt: time.Now().UTC(),
		Topic:      topic,
		Partition:  m.Partition,
		Offset:     m.Offset,
		Key:        string(m.Key),
		Event:      envelope,
	}

	// Логируем в JSON формате для удобства чтения
	logData, err := json.MarshalIndent(loggedEvent, "", "  ")
	if err != nil {
		log.Printf("Error marshaling logged event: %v", err)
		return
	}

	log.Printf("Received event from Kafka:\n%s", string(logData))
}

// sendEvent сериализует событие с типом и отправляет в Kafka через writer
func sendEvent(w http.ResponseWriter, broker, topic, eventType string, data interface{}) {
	envelope := EventEnvelope{
		Id:        uuid.New().String(),
		Type:      eventType,
		Timestamp: time.Now().UTC(),
		Data:      data,
	}

	b, err := json.Marshal(envelope)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to marshal event envelope: %v", err), http.StatusInternalServerError)
		return
	}

	conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to dial leader: %v", err), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	// Устанавливаем таймаут записи
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	// Пишем сообщение
	_, err = conn.WriteMessages(
		kafka.Message{
			Key:   []byte(eventType),
			Value: b,
		},
	)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to write message: %v", err), http.StatusInternalServerError)
		return
	}

	offset, err := conn.ReadLastOffset()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get last offset: %v", err), http.StatusInternalServerError)
		return
	}

	eventResp := EventResponse{
		Status:    "success",
		Partition: 0,
		Offset:    offset,
		Event: Event{
			Id:        envelope.Id,
			Type:      eventType,
			Timestamp: envelope.Timestamp,
			Payload:   data,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(eventResp)
}
