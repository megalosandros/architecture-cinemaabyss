package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

func main() {
	monolithURL := os.Getenv("MONOLITH_URL")
	moviesURL := os.Getenv("MOVIES_SERVICE_URL")
	eventsURL := os.Getenv("EVENTS_SERVICE_URL")

	if monolithURL == "" || moviesURL == "" || eventsURL == "" {
		log.Fatal("One or more required env variables are not set")
	}

	monolithTarget, err := url.Parse(monolithURL)
	if err != nil {
		log.Fatalf("Invalid MONOLITH_URL: %v", err)
	}

	moviesTarget, err := url.Parse(moviesURL)
	if err != nil {
		log.Fatalf("Invalid MOVIES_SERVICE_URL: %v", err)
	}

	eventsTarget, err := url.Parse(eventsURL)
	if err != nil {
		log.Fatalf("Invalid EVENTS_SERVICE_URL: %v", err)
	}

	moviesMigrationPercentStr := os.Getenv("MOVIES_MIGRATION_PERCENT")
	moviesMigrationPercent := 0
	if moviesMigrationPercentStr != "" {
		var err error
		moviesMigrationPercent, err = strconv.Atoi(moviesMigrationPercentStr)
		if err != nil || moviesMigrationPercent < 0 || moviesMigrationPercent > 100 {
			log.Fatalf("Invalid MOVIES_MIGRATION_PERCENT: %s", moviesMigrationPercentStr)
		}
	}

	gradualMigration := os.Getenv("GRADUAL_MIGRATION") == "true"

	log.Printf("Proxy configuration:")
	log.Printf("  MONOLITH_URL: %s", monolithURL)
	log.Printf("  MOVIES_SERVICE_URL: %s", moviesURL)
	log.Printf("  EVENTS_SERVICE_URL: %s", eventsURL)
	log.Printf("  GRADUAL_MIGRATION: %v", gradualMigration)
	log.Printf("  MOVIES_MIGRATION_PERCENT: %d%%", moviesMigrationPercent)

	// Создаем ReverseProxy для каждого бекенда
	monolithProxy := httputil.NewSingleHostReverseProxy(monolithTarget)
	moviesProxy := httputil.NewSingleHostReverseProxy(moviesTarget)
	eventsProxy := httputil.NewSingleHostReverseProxy(eventsTarget)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Received request: %s %s", r.Method, r.URL.Path)

		// Сначала обрабатываем /api/movies/health отдельно
		if r.URL.Path == "/api/movies/health" {
			log.Printf("[MOVIES] Proxying /api/movies/health to movies service")
			moviesProxy.ServeHTTP(w, r)
			return
		}

		// Выбираем прокси в зависимости от префикса пути
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/movies"):
			if gradualMigration {
				rnd := rand.Intn(100)
				log.Printf("[MOVIES] Random number: %d, Migration percent: %d", rnd, moviesMigrationPercent)

				if rnd < moviesMigrationPercent {
					log.Printf("[MOVIES] Gradual migration - routing to movies service (%d%% chance)", moviesMigrationPercent)
					moviesProxy.ServeHTTP(w, r)
				} else {
					log.Printf("[MOVIES] Gradual migration - routing to monolith service (%d%% chance)", 100-moviesMigrationPercent)
					monolithProxy.ServeHTTP(w, r)
				}
			} else {
				// Полный переход на MOVIES
				log.Printf("[MOVIES] Gradual migration disabled - routing all traffic to movies service")
				moviesProxy.ServeHTTP(w, r)
			}
		case strings.HasPrefix(r.URL.Path, "/api/events"):
			log.Printf("[EVENTS] Proxying /api/events to events service")
			eventsProxy.ServeHTTP(w, r)
		default:
			log.Printf("[DEFAULT] Proxying to monolith service")
			monolithProxy.ServeHTTP(w, r)
		}
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	log.Printf("Starting proxy on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
